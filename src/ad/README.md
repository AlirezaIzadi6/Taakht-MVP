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

Migrations in `migrations/` are embedded and applied at startup. gRPC reflection is off unless `TAAKHT_GRPC_REFLECTION=on` (scripts/dev.sh sets it). Every call needs the `x-user-id` metadata.

```bash
grpcurl -plaintext -H 'x-user-id: user-1' -d '{"spec":{"title":"Book","have_category":"books","want_categories":["tools"]}}' localhost:9001 taakht.ad.v1.AdService/CreateAd
```

## Behaviour

- New ads are hidden. `EditAd` creates a new version (`ABORTED` on a stale `expected_version`, `FAILED_PRECONDITION` when locked or closed) and always emits `AdEdited` with the full snapshot.
- `PublishAd` needs a want category or neighborhood and only works from hidden; `HideAd` only from published. Only the owner may edit, publish or hide.
- `GetAd`: the owner reads any of their ads and versions; callers with a `system:` identity (proven by `x-internal-token`) (`system:negotiation`, `system:swap`) read anything; everyone else sees only PUBLISHED ads at the current version (`NOT_FOUND` otherwise, so existence is not leaked).
- `LockAds` is callable only as `system:swap` with a valid `x-internal-token` (`PERMISSION_DENIED` for other callers, `UNAUTHENTICATED` for a `system:` id without the token). It locks both ads in one transaction (row locks in id order), requires published or hidden and the agreed version, and is idempotent by `swap_id` via `ad_lock`.
- Spec limits: title trimmed, 1..120 chars; description <= 4000; want/neighborhood lists <= 20 entries of <= 64 chars, de-duplicated. `x-user-id` is 1..64 printable ASCII bytes (no spaces).
- Consumes `swap.events` (group `ad`): `SwapCompleted` closes both ads, `SwapCancelled` restores the status held before the lock. Handlers only act on ads the swap still holds locked. An undecodable payload is a permanent failure: logged, recorded as processed and skipped.

## Tests

```bash
go vet ./... && go test -race ./...
# Postgres-backed tests (create and drop a throwaway database per test):
TEST_DATABASE_URL='postgres://taakht:taakht@localhost:5432/postgres?sslmode=disable' go test -race ./...
```
