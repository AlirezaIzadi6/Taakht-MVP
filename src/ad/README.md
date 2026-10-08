# Ad service

Go service owning ads: versioned specs, publish/hide, the exclusive swap lock and its release/close. Contract: `api/proto/taakht/ad/v1`.

## Run

Start the infra from the repo root (`make up`), then from this directory:

```bash
go run .
```

Environment (defaults shown):

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgres://taakht:taakht@localhost:5432/ad?sslmode=disable` |
| `KAFKA_BROKERS` | `localhost:9094` |
| `GRPC_ADDR` | `:9001` |
| `ELIGIBILITY_FILE` | `../../config/eligibility.json` |

Migrations in `migrations/` are embedded and applied at startup. gRPC reflection is on (`TAAKHT_GRPC_REFLECTION=off` disables it). Every call needs the `x-user-id` metadata.

```bash
grpcurl -plaintext -H 'x-user-id: user-1' -d '{"spec":{"title":"Book","have_category":"books","want_categories":["tools"]}}' localhost:9001 taakht.ad.v1.AdService/CreateAd
```

## Behaviour

- New ads are hidden. `EditAd` creates a new version (`ABORTED` on a stale `expected_version`, `FAILED_PRECONDITION` when locked or closed) and always emits `AdEdited` with the full snapshot.
- `PublishAd` needs a want category or neighborhood and only works from hidden; `HideAd` only from published. Only the owner may edit, publish or hide.
- `LockAds` locks both ads in one transaction (row locks in id order), requires published or hidden and the agreed version, and is idempotent by `swap_id` via `ad_lock`.
- Consumes `swap.events` (group `ad`): `SwapCompleted` closes both ads, `SwapCancelled` restores the status held before the lock. Handlers only act on ads the swap still holds locked.

## Tests

```bash
go vet ./... && go test -race ./...
# Postgres-backed tests (create and drop a throwaway database per test):
TEST_DATABASE_URL='postgres://taakht:taakht@localhost:5432/postgres?sslmode=disable' go test -race ./...
```
