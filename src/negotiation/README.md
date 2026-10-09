# Negotiation service

.NET 10 gRPC service: swap requests, Proposals, the four approvals, and `AgreementReached`. Contract: `api/proto/taakht/negotiation/v1`. Events go to `negotiation.events` through the outbox; it consumes `ad.events` and `swap.events` (consumer group `negotiation`).

## Layout

| Path | Content |
|---|---|
| `Taakht.Negotiation/Domain` | Pure rules (`SwapNegotiation` aggregate), no I/O |
| `Taakht.Negotiation/Application` | Use cases (`NegotiationService`), event handlers, ports (`IAdClient`, `ILockerEligibility` + mock) |
| `Taakht.Negotiation/Infrastructure` | Dapper/Npgsql repository, gRPC Ad client |
| `Taakht.Negotiation/Api` | gRPC service, proto mapping, domain-error to status-code interceptor |
| `Taakht.Negotiation/migrations` | Embedded SQL, applied at startup |

## Run

```bash
make up                       # Postgres + Kafka (repo root)
cd src/negotiation/Taakht.Negotiation
DATABASE_URL='postgres://taakht:taakht@localhost:5432/negotiation?sslmode=disable' dotnet run
```

The `negotiation` database must exist (compose creates it). `dotnet run` uses the Development profile, so gRPC reflection is on:

```bash
grpcurl -plaintext localhost:9003 list
grpcurl -plaintext -H 'x-user-id: user-1' -d '{"requester_ad_id":"...","target_ad_id":"..."}' \
  localhost:9003 taakht.negotiation.v1.NegotiationService/OpenNegotiation
```

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgres://taakht:taakht@localhost:5432/postgres?sslmode=disable` (set it to the `negotiation` database) |
| `KAFKA_BROKERS` | `localhost:9094` |
| `GRPC_ADDR` | `:9003` |
| `AD_ADDR` | `localhost:9001` |
| `NEGOTIATION_CAP` | `10` open negotiations per ad (inbound + outbound) |

## Test

```bash
dotnet test Negotiation.slnx                       # domain tests; Postgres tests skip themselves
TEST_DATABASE_URL='postgres://taakht:taakht@localhost:5432/postgres?sslmode=disable' dotnet test Negotiation.slnx
```

With `TEST_DATABASE_URL` set, the tests create a throwaway database on that server (and drop it afterwards) and use a fake `IAdClient`: approval flow, final sync check, the cap under 20 parallel `OpenNegotiation` calls, and the swap-event handlers.

## Behaviour worth knowing

- Approvals are never deleted; their `valid` flag is computed on read (AD: target equals the ad's current version in `ad_ref`; TERMS: target equals the active proposal number). `ad_ref` is fed by `AdPublished`/`AdEdited`/`AdReleased` and refreshed from every synchronous `ad.GetAd`.
- `ad_ref` keeps only `owner_id` and `current_version` (migration 002 dropped the never-current `status`); an event only overwrites a strictly newer version.
- All synchronous `ad.GetAd` calls run as the system identity `system:negotiation` (the Ad service hides unpublished ads and old versions from non-owners). Negotiation authorizes the real caller itself: the requester must own the requester ad, the target ad must be published, only parties may read or change a negotiation. A missing ad, someone else's requester ad and an unpublished target all answer `NOT_FOUND "ad not available"`. Every returned `Negotiation` also carries `requester_ad` and `target_ad` (fetched with `GetAd` as `system:negotiation`, one fetch per distinct ad per request; a failed fetch leaves the field empty).
- `SwapCancelled` (swap.events, e.g. payment timeout) cancels the AGREED negotiation named by its `negotiation_id` (only an event without the field falls back to the AGREED negotiation of that ad pair; ignored when none) and emits `NegotiationClosed` with the event reason. Competing negotiations cancelled earlier by `ExclusiveLockAcquired` stay cancelled. `SwapCompleted` changes nothing. `ExclusiveLockAcquired` is applied only when the negotiation it names exists, concerns exactly those two ads and is `AGREEMENT_PENDING` (or already `AGREED`); a stale event (an old swap, a cancelled negotiation) is ignored and cancels nobody.
- A sweeper (every 30 s, `AGREEMENT_PENDING_TIMEOUT`, default `10m`, min `10s`) republishes `AgreementReached` for negotiations still `AGREEMENT_PENDING` once per timeout period, up to 3 times, then cancels with reason `agreement timed out` (see the architecture document for the residual risk of a late lock).
- Price difference must be between 0 and 1 000 000 000 000.
- Reject restores the previous proposal and keeps no further history (a second reject in a row is `FAILED_PRECONDITION`). New proposals always get a fresh number.
- When the fourth approval lands, the service calls `ad.GetAd` for both ads; if either version differs from the approved one (or the ad is not published/hidden) the call is `ABORTED` and the negotiation stays `OPEN`.
- The mock locker eligibility rejects `user-4`.
