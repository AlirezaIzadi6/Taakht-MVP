# Matching service

Read model of published ads plus search and two-sided matching. Contracts: `api/proto/taakht/matching/v1`.

## Run

```bash
make up                      # Postgres + Kafka (repo root)
cd src/matching
go run .                     # gRPC on :9002
```

Environment (defaults): `DATABASE_URL=postgres://taakht:taakht@localhost:5432/matching?sslmode=disable`, `KAFKA_BROKERS=localhost:9094`, `GRPC_ADDR=:9002`, `ELIGIBILITY_FILE=../../config/eligibility.json`. gRPC reflection is off unless `TAAKHT_GRPC_REFLECTION=on` (scripts/dev.sh sets it).

Smoke test with fake ad events (service must be running): `go run ./cmd/fakead` publishes two matching `AdPublished` envelopes, then calls `Search` and `FindMatches`. The service logs `MATCH ...` and writes `MatchFound` to the outbox (topic `matching.events`).

Manual calls: `grpcurl -plaintext -H 'x-user-id: user-1' -d '{"criteria":{"want_categories":["tools"]}}' localhost:9002 taakht.matching.v1.MatchingService/Search`

## Behavior

- Consumes `ad.events` (group `matching`): `AdPublished`, `AdEdited`, `AdReleased` upsert the snapshot when its status is PUBLISHED, otherwise delete it; `AdHidden`, `AdLocked`, `AdClosed` delete. An event with a lower version than the indexed one is ignored (deletions always apply).
- After an ad is indexed, its top 5 two-sided matches get a `MatchFound` outbox event, once per pair (`notified_pair`).
- Undecodable event payloads are permanent failures (logged, recorded as processed, skipped) so one poison message cannot block the partition.
- `FindMatches`: a malformed `ad_id` is `INVALID_ARGUMENT`, database errors are `INTERNAL`; it needs the caller's ad to be in the index (published); otherwise `NOT_FOUND`.
- Score: `1.0` + `0.5` per side that names the other ad explicitly (not "open") + proximity `0..1` (`1/(1+km/10)` for the closest neighborhood pair, `1` for a shared neighborhood, `0.5` when a side names none). Code: `internal/scoring`.

## Tests

```bash
go vet ./... && go test -race ./...
TEST_DATABASE_URL=postgres://taakht:taakht@localhost:5432/postgres?sslmode=disable go test -race ./...   # adds Postgres tests (throwaway databases)
```
