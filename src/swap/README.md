# Swap service

.NET 10 service that turns an agreed negotiation into a swap: it takes the exclusive lock on both ads through the Ad service, waits for locker fees (mocked partner) with a deadline, and publishes the outcome. Implements `api/proto/taakht/swap/v1/swap.proto`, publishes `swap.events`, consumes `negotiation.events` (group `swap`).

## Run

```bash
make up                         # Postgres + Kafka (repo root)
cd src/swap/Taakht.Swap
PAYMENT_DEADLINE=2m dotnet run  # launchSettings.json sets ASPNETCORE_ENVIRONMENT=Development (gRPC reflection on)
```

Migrations are embedded and applied at startup (database `swap` must exist, as created by the compose setup).

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgres://taakht:taakht@localhost:5432/swap?sslmode=disable` |
| `KAFKA_BROKERS` | `localhost:9094` |
| `GRPC_ADDR` | `:9004` |
| `AD_ADDR` | `localhost:9001` |
| `PAYMENT_DEADLINE` | `1h` (accepts `30s`, `2m`, `1h`, `1h30m`) |

```bash
grpcurl -plaintext -H 'x-user-id: user-1' localhost:9004 taakht.swap.v1.SwapService/ListMySwaps
grpcurl -plaintext -H 'x-user-id: user-1' -d '{"swap_id":"<id>","user_id":"user-1"}' localhost:9004 taakht.swap.v1.SwapService/SimulateLockerFeePaid
```

## Behavior

- `AgreementReached` creates the swap (unique per negotiation) as `LOCKING`, calls `ad.LockAds(swap_id, ...)` with `x-user-id: system:swap`, then moves to `AWAITING_PAYMENT` (deadline = now + `PAYMENT_DEADLINE`, emits `ExclusiveLockAcquired`) or `REJECTED` (`SwapRejected`) when the Ad service answers `FAILED_PRECONDITION`/`NOT_FOUND`/`INVALID_ARGUMENT`/`PERMISSION_DENIED` (permanent refusals; other errors are retried). With no `LOCKER` leg it goes straight to `COMPLETED` (`SwapCompleted` is emitted too). Transient Ad errors throw, so the consumer retries; the handler is state-checked and each swap write is its own short transaction (no DB transaction is held across the gRPC call).
- `SimulateLockerFeePaid` marks the caller's own leg paid (mock webhook): `user_id` must equal the caller and the caller must be a party (`PERMISSION_DENIED`). It exists only when `ENABLE_DEV_ENDPOINTS=true` or the environment is Development (otherwise `UNIMPLEMENTED`); all locker legs paid gives `COMPLETED` + `SwapCompleted`.
- A sweeper (every 5 s) asks the partner mock for the payment status of overdue swaps and cancels the unpaid ones (`SwapCancelled`, `defaulting_user_id` = first unpaid locker leg owner). Every transition runs under a row lock with an UPDATE conditional on the status read, so a payment and the sweeper cannot both win.
- `GetSwap` / `ListMySwaps` only return swaps where the caller owns a leg (`PERMISSION_DENIED` otherwise).

## Layout

`Domain/` pure state machine (`SwapMachine`), `Application/` workflow, gRPC service, sweeper, `Infrastructure/` store (Npgsql + Dapper), Ad client, partner mock, event mapping, `migrations/` SQL.

## Tests

```bash
cd src/swap
dotnet test                                     # state machine + parser tests; Postgres tests are skipped
TEST_DATABASE_URL='postgres://taakht:taakht@localhost:5432/postgres?sslmode=disable' dotnet test
```

The Postgres tests create and drop a throwaway database on that server and use a fake `IAdClient`.
