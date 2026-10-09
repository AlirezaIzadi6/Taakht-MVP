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
| `DATABASE_URL` | `postgres://taakht:taakht@127.0.0.1:5432/ad?sslmode=disable` |
| `KAFKA_BROKERS` | `127.0.0.1:9094` |
| `GRPC_ADDR` | `:9001` |
| `ELIGIBILITY_FILE` | `../../config/eligibility.json` |
| `DB_MAX_CONNS` / `DB_MIN_CONNS` / `REQUEST_TIMEOUT` | `20` / `2` / `15s` |

Shared settings (pool bounds `DB_MAX_CONNS` 20 / `DB_MIN_CONNS`, retention and prune settings, `INTERNAL_AUTH_TOKEN`) are described in [MVP service conventions](../../docs/guidelines/mvp-service-conventions.md). Use `127.0.0.1`, not `localhost`, for host-run services.

Migrations in `migrations/` are embedded and applied at startup. gRPC reflection is off unless `TAAKHT_GRPC_REFLECTION=on` (scripts/dev.sh sets it). Every call needs the `x-user-id` metadata.

```bash
grpcurl -plaintext -H 'x-user-id: user-1' -d '{"spec":{"title":"Book","have_category":"books","want_categories":["tools"]}}' localhost:9001 taakht.ad.v1.AdService/CreateAd
```

## Behaviour

- New ads are hidden. `EditAd` creates a new version (`ABORTED` on a stale `expected_version`, `FAILED_PRECONDITION` when locked or closed) and emits `AdEdited` with the full snapshot; a spec equal to the stored current one returns the current ad without a new version or event. Every event carries `seq`, the ad's `event_seq` counter.
- `PublishAd` needs a want category or neighborhood and only works from hidden; `HideAd` only from published. Only the owner may edit, publish or hide.
- `GetAd`: the owner reads any of their ads and versions; callers with a `system:` identity (proven by `x-internal-token`) (`system:negotiation`, `system:swap`) read anything; everyone else sees only PUBLISHED ads at the current version (`NOT_FOUND "ad not found"` otherwise, the same message for a missing ad, so existence is not leaked).
- `LockAds` is callable only as `system:swap` with a valid `x-internal-token` (`PERMISSION_DENIED` for other callers, `UNAUTHENTICATED` for a `system:` id without the token). It locks both ads in one transaction (row locks in id order), requires published or hidden and the agreed version, and is idempotent by `swap_id` via `ad_lock`.
- Spec limits: title trimmed, 1..120 chars; description <= 4000; want/neighborhood lists <= 20 entries of <= 64 chars, de-duplicated. `x-user-id` is 1..64 printable ASCII bytes (no spaces).
- Consumes `swap.events` (group `ad`): `SwapCompleted` closes both ads, `SwapCancelled` restores the status held before the lock. Handlers only act on ads the swap still holds locked. An undecodable payload is a permanent failure: logged at error level, recorded as processed, stored in `dead_letter` and skipped.

## Tests

```bash
go vet ./... && go test -race ./...
# Postgres-backed tests (create and drop a throwaway database per test):
TEST_DATABASE_URL='postgres://taakht:taakht@127.0.0.1:5432/postgres?sslmode=disable' go test -race ./...
```
