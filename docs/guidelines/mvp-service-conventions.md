# MVP service conventions

Rules every service follows so four services in two languages fit together. Read with [mvp-plan.md](../product/mvp-plan.md) and the contracts in `api/proto`.

## Layout

| Path | Content |
|---|---|
| `api/proto/taakht/<svc>/v1` | Contracts. Changing a `.proto` needs agreement; run `make proto` afterwards |
| `gen/go` | Generated Go code (module `github.com/taakht/taakht/gen`, committed) |
| `libs/goplatform` | Shared Go helpers (module `github.com/taakht/taakht/libs/goplatform`) |
| `libs/dotnet/Taakht.Platform` | Shared .NET helpers, same responsibilities |
| `src/ad`, `src/matching` | Go services (modules in `go.work`) |
| `src/negotiation`, `src/swap` | .NET services, one `.slnx` each (`src/negotiation/Negotiation.slnx`) |
| `config/eligibility.json` | Allowed categories, neighborhoods (with coordinates), seed user ids |
| `deploy/docker-compose.yml` | Postgres + Kafka (`make up`) |

Services run on the host (`go run`, `dotnet run`), not in containers, during the MVP.

## Ports and environment

| Service | gRPC port | Health HTTP port (`HEALTH_ADDR`) | Database |
|---|---|---|---|
| ad | 9001 | 9101 | `ad` |
| matching | 9002 | 9102 | `matching` |
| negotiation | 9003 | 9103 | `negotiation` |
| swap | 9004 | 9104 | `swap` |

gRPC reflection is off unless `TAAKHT_GRPC_REFLECTION=on` (Go) or the Development environment (.NET); `scripts/dev.sh` turns it on. Dev-only RPCs need `ENABLE_DEV_ENDPOINTS=true` or Development.

Environment variables (with these defaults for local runs): `DATABASE_URL=postgres://taakht:taakht@127.0.0.1:5432/<db>?sslmode=disable` (the .NET services convert it to a connection string or use `ConnectionStrings__Default`), `KAFKA_BROKERS=127.0.0.1:9094`, `GRPC_ADDR=:<port>`, `AD_ADDR=127.0.0.1:9001`, `ELIGIBILITY_FILE=../../config/eligibility.json` (resolve relative to the working directory; also accept an absolute path). Swap only: `PAYMENT_DEADLINE`, code default `1h` (the design value; `scripts/dev.sh` exports `2m` for local runs, so `make dev` and the demo use 2 minutes; a Go-style duration, parse `1h`, `2m`, `30s`; startup fails unless it is greater than zero and at most `30d`, and absurdly large values are rejected before they can overflow). Swap only: `ENABLE_DEV_ENDPOINTS=true` (or the Development environment) enables `SimulateLockerFeePaid`. Negotiation only: `AGREEMENT_PENDING_TIMEOUT` (default `10m`) (how long a negotiation may wait in `AGREEMENT_PENDING` before each recovery step of the sweeper, see the architecture document; durations accept the `d` suffix; startup fails below `10s` or above `30d`), `NEGOTIATION_CAP` (default `10` live negotiations per ad). All services: `INTERNAL_AUTH_TOKEN` (shared secret that proves `system:*` callers; default `dev-internal-token`, DEV ONLY, with a startup warning; `scripts/dev.sh` exports it). All services: `DB_MAX_CONNS=20` and `DB_MIN_CONNS=2` (.NET default 5) bound the database pool (pgx `MaxConns`/`MinConns`, Npgsql `Maximum Pool Size`/`Minimum Pool Size`; a `pool_max_conns` / `Maximum Pool Size` written into `DATABASE_URL` is used only when the variable is unset; startup fails on a non-integer, a max below 1, or a min above the max). Four pools of 20 plus admin and test sessions must stay under the Postgres limit (`max_connections=200` in `deploy/docker-compose.yml`). .NET only: `DB_ACQUIRE_TIMEOUT=10s` (how long a request waits for a free pooled connection, 1 s to 5 min, written as Npgsql `Timeout`). Go only: `REQUEST_TIMEOUT=15s` (deadline given to a unary call that arrives without one, so a request waiting for a pooled connection fails fast; a Go duration). Use `127.0.0.1`, not `localhost`, for host-run services (see [running locally](running-locally.md)); the platform libraries rewrite a database host of exactly `localhost` to `127.0.0.1`.

## Operability

Every service exposes health, structured logs, a request id and Prometheus metrics (design: [architecture, Operability](../architecture/mvp-architecture.md#operability); how to use it: [Running locally](running-locally.md#health-logs-metrics)). Environment variables, same in Go and .NET:

| Env var | Default | Meaning |
|---|---|---|
| `HEALTH_ADDR` | `127.0.0.1:9101..9104` (table above) | `host:port` of the plain HTTP listener with `/healthz` (liveness), `/readyz` (readiness; 503 when a check fails) and `/metrics` (Prometheus text). Loopback by default; `0.0.0.0` exposes unauthenticated metrics to the network. The listener is HTTP/1.1 on its own port because the gRPC port serves cleartext HTTP/2 only. `scripts/dev.sh` sets it per service (`HEALTH_BIND` changes the host part). A listen failure stops the service. |
| `CONSUMER_CONCURRENCY` | `1` (`scripts/dev.sh` sets `4`) | Partitions a consumer handles in parallel; order is kept per partition (per key). Higher values were not load-tested; matching serializes its index updates with an advisory lock so parallel partitions cannot lose a `MatchFound`. |
| `LOG_FORMAT` | `text` | `json` writes one JSON object per line: `ts` (UTC), `level`, `service`, `msg`, `request_id`, `user_id` (when known) plus the event's fields (`method`, `code`, `duration_ms` for a call; `type`, `event_id` for a consumed event). Anything else is readable text. |
| `LOG_LEVEL` | `info` | Minimum level (`debug`, `info`, `warn`, `error`; .NET also `trace`, `critical`). |
| `READY_OUTBOX_AGE` | `60s` | Readiness fails when the oldest unpublished outbox row is older than this (relay stuck or Kafka unreachable). A Go duration in Go, `90s`/`10m` in .NET. |
| `READY_CONSUMER_STALE` | `60s` | .NET only: readiness fails when the consumer has not polled for this long. The Go services check group membership instead. |

Health is the standard gRPC health service on the gRPC port too (`grpc.health.v1.Health/Check`, exempt from `x-user-id`): service `""` and `liveness` are SERVING while the process is up, `readiness` is SERVING only while Postgres (`SELECT 1`, 2 s), the consumer and the outbox are healthy. Never put secrets, tokens or user text in a log line or a metric label; ids only. Metric labels are limited to gRPC method and code, pool state, consumer result and housekeeping table.

**Request id.** The metadata key `x-request-id` (1..128 printable ASCII bytes; anything else is replaced by a fresh UUID) is set by Envoy for every REST call. The server interceptor of each platform library makes it ambient and logs it, the client interceptors forward it (`RequestContext` in .NET, `observe.RequestIDClientInterceptor` in Go), `outbox.Add`/`Outbox.Wrap` store it in `Envelope.request_id` (field 6) and the consumers restore it before running a handler. Jobs without a caller (sweepers, housekeeping) have no request id. In Go, log with the `*Context` variants of `slog` (`slog.InfoContext(ctx, ...)`) to get `request_id` and `user_id` in a handler's lines.

## Overload

Database overload is a retryable `UNAVAILABLE` (503 at the edge) with a short message, never `UNKNOWN`/`INTERNAL`. One interceptor per platform library does this centrally and is registered first (outermost) in all four services: `server.OverloadInterceptor` in `libs/goplatform` and `OverloadInterceptor` in `Taakht.Platform`. It maps Postgres `53300`, `57P03`, `57P01` and class `08`, failed connects, network and pool-wait timeouts (Go: a deadline the server itself imposed; .NET: `TimeoutException`, non-SQL `NpgsqlException`). An error that already carries a status passes through unchanged. In Go, handlers must return database errors wrapped with `%w` (not as `status.Errorf(codes.Internal, ...)`) so the interceptor can see them; a plain non-overload error becomes `INTERNAL`. Do not hold a pooled connection or transaction across a call to another service.

## Identity

- The caller is the gRPC metadata key `x-user-id`. A shared interceptor reads it; a missing value is `UNAUTHENTICATED`.
- A valid id is an opaque token of 1..64 printable ASCII bytes (0x21-0x7E: no spaces, control characters or non-ASCII), identical in the Go and .NET libs; a repeated `x-user-id` or `x-internal-token` header is rejected too; anything else is `UNAUTHENTICATED`.
- Outgoing service-to-service calls forward the same `x-user-id`, except calls made on behalf of the system: Swap calls `LockAds` as `system:swap` and Negotiation calls `GetAd` as `system:negotiation`. The prefix `system:` is reserved; real user ids must never start with it, and a `system:` id is accepted only with the `x-internal-token` secret, which the platform client interceptors add (see [API conventions](api-conventions.md)).
- Seed users: `user-1` .. `user-4`. Envoy sets the header from the JWT `sub`; services never validate tokens.

## Errors (gRPC status codes)

`NOT_FOUND`, `PERMISSION_DENIED` (not a party / not the owner), `INVALID_ARGUMENT`, `FAILED_PRECONDITION` (state does not allow it, lock refused), `ABORTED` (stale version / stale proposal number: the caller should re-read), `ALREADY_EXISTS`, `RESOURCE_EXHAUSTED` (open-negotiation cap). Put a short human-readable message in the status.

## Database

- One database per service; never connect to another service's database.
- Plain SQL migrations in `<service>/migrations/NNN_name.sql`, applied in order at startup and tracked in `schema_migrations(version text primary key, applied_at timestamptz)`. Embed them in the binary. No ORM: Go uses `pgx/v5`, .NET uses `Npgsql` (+ `Dapper` if wanted).
- Ids are UUID strings generated by the service (`uuid` column type).
- Timestamps are `timestamptz`, UTC.

## Events

Topics: `ad.events`, `matching.events`, `negotiation.events`, `swap.events`. Each service publishes only to its own topic. Value = `taakht.common.v1.Envelope` (protobuf; `request_id` carries the request id of the call that caused the event) whose `type` is the payload's full proto name (e.g. `taakht.swap.v1.SwapCompleted`) and `aggregate_id` is also the Kafka key. Consumers ignore envelope types they do not know.

**Outbox** (every publishing service has this table; a business change and its event are written in the same transaction):

```sql
CREATE TABLE outbox (
  id           uuid PRIMARY KEY,           -- = Envelope.event_id
  topic        text        NOT NULL,
  key          text        NOT NULL,
  envelope     bytea       NOT NULL,       -- serialized Envelope
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
```

A relay loop (poll every ~200 ms) reads unpublished rows in `created_at, id` order with `FOR UPDATE SKIP LOCKED`, produces to Kafka, waits for the ack, then sets `published_at`. Delivery is at-least-once. Insert with `created_at = clock_timestamp()` (not the `now()` default, which is the transaction start and would leave events of one transaction unordered).

**Idempotent consumers** (every consuming service has this table; the dedupe insert and the handler's writes share one transaction, and the offset is committed after the transaction):

```sql
CREATE TABLE processed_events (
  consumer text NOT NULL,                  -- consumer group = service name
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
```

If `INSERT ... ON CONFLICT DO NOTHING` inserts nothing, the event was already handled: skip. A handler that fails returns an error; the message is retried (log + retry with backoff). An error that retrying cannot fix is wrapped as permanent (`consume.Permanent` in Go, `PermanentEventException` in .NET; decode failures are permanent automatically): the event is logged at error level, recorded in `processed_events` and stored in `dead_letter` (see below), and skipped. Handlers must also be safe against re-ordering across topics (check state, not just event arrival).

**Dead letters** (every consuming service has this table too, created by a migration):

```sql
CREATE TABLE dead_letter (
  consumer   text        NOT NULL,
  event_id   uuid        NOT NULL,
  topic      text        NOT NULL,
  payload    bytea       NOT NULL,        -- the serialized Envelope
  error      text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
```

The permanent-skip path inserts the `processed_events` row and the `dead_letter` row in one statement. An event whose id is not a UUID cannot be keyed and is only logged. `tools/dlq` lists, shows, replays and purges these rows (see [Running locally](running-locally.md#operations-dead-letters)); replay clears the consumer's `processed_events` marker for the event, otherwise the consumer would skip it as a duplicate.

Consumer groups are named after the service (`ad`, `matching`, `negotiation`, `swap`); start from the earliest offset.

**Housekeeping.** `outbox`, `processed_events` and `dead_letter` are pruned by a background job every service runs (Go: `housekeeping.Run` in `libs/goplatform/housekeeping`, wired in `main.go`; .NET: `AddTaakhtHousekeeping()`, a `BackgroundService`). It runs once about 30 s after startup and then every `PRUNE_INTERVAL`, deletes in batches of 1000 rows per statement until nothing is left, logs at info only when it deleted something, and never deletes unpublished outbox rows. Each service ships a `00N_housekeeping_indexes.sql` migration with the supporting indexes.

| Env var | Default | Meaning |
|---|---|---|
| `OUTBOX_RETENTION` | `24h` | Published outbox rows older than this (by `published_at`) are deleted. |
| `PROCESSED_EVENTS_RETENTION` | `7d` | `processed_events` rows older than this (by `processed_at`) are deleted. |
| `DEAD_LETTER_RETENTION` | `30d` | `dead_letter` rows older than this (by `created_at`) are deleted. |
| `PRUNE_INTERVAL` | `10m` | Time between sweeps. |

Two services add a retention of their own to the same job (same syntax, same `1m` minimum, validated at startup):

| Env var | Service | Default | Meaning |
|---|---|---|---|
| `TOMBSTONE_RETENTION` | matching | `7d` | Tombstone rows (`removed = true`) in `ad_index` older than this (by `removed_at`) are deleted. A tombstone is what stops a replayed older `ad.events` message from re-indexing a removed ad, so deleting it re-opens that replay window: keep this retention longer than the retention of the `ad.events` topic and than any planned offset reset. Live rows are never pruned. |
| `AD_LOCK_RETENTION` | ad | `30d` | `ad_lock` rows whose swap finished (state `released` or `closed`) and settled (`settled_at`, set by a trigger) longer ago than this are deleted. Rows still `locked` are never deleted. `ad_version` is deliberately never pruned: approvals and `GetAd(version)` refer to exact versions and the rows are the audit history of an ad. The only effect of pruning a lock row is that a `LockAds` retry for a swap finished that long ago would no longer be recognised as a repeat. |

Eligibility config (`ELIGIBILITY_FILE`) is re-read in ad and matching when its modification time or size changes (checked every 30 s, no signal needed). A file that fails to parse or validate (no categories or neighborhoods, blank ids, coordinates out of range) is rejected: the previous config stays in force and an error is logged once per file change. Startup still fails on an invalid file.

Durations accept the usual units (`90s`, `10m`, `24h`) and a `d` suffix (`7d`, `1d12h`); anything below `1m`, unparseable or implausibly large (more than 36500 days) fails startup. Tradeoff: deleting a `processed_events` row ends the idempotency guarantee for that event, so a redelivery after the retention would run the handler again. Keep `PROCESSED_EVENTS_RETENTION` much larger than any realistic redelivery window (Kafka topic retention, consumer-group offset resets, manual replays); lowering it saves space at the cost of weaker deduplication.

## Shared helper API

**Go** (`libs/goplatform`, packages under that module):

```go
// package db
func Open(ctx context.Context, url string) (*pgxpool.Pool, error)
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, dir string) error // fsys = embed.FS

// package identity
func ServerInterceptor() grpc.UnaryServerInterceptor // requires x-user-id
func ClientInterceptor() grpc.UnaryClientInterceptor // forwards x-user-id from incoming ctx
func UserID(ctx context.Context) string
func WithUserID(ctx context.Context, id string) context.Context // outgoing ctx for jobs without a caller

// package outbox
func Add(ctx context.Context, tx pgx.Tx, topic, key string, msg proto.Message) error // wraps msg in an Envelope
func RunRelay(ctx context.Context, pool *pgxpool.Pool, brokers []string) error

// package observe (logging, request id, health, metrics)
func SetupLogging(service string) *slog.Logger // LOG_FORMAT, LOG_LEVEL; call first in main
func New(Options) *Obs                          // health checks; (*Obs).Serve(ctx) runs the HTTP listener; server.Run registers grpc.health.v1
func RegisterDB(pool *pgxpool.Pool)             // pool and outbox gauges
func RequestID(ctx context.Context) string
func RequestIDClientInterceptor() grpc.UnaryClientInterceptor

// package consume
type Handler func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error
func Run(ctx context.Context, pool *pgxpool.Pool, brokers []string, group string, topics []string, handlers map[string]Handler) error // key = envelope type
```

Kafka client: `github.com/twmb/franz-go`. gRPC server helper: a small `server.Run(ctx, addr, register func(*grpc.Server))` with graceful shutdown and the interceptor is welcome.

**.NET** (`libs/dotnet/Taakht.Platform`, namespace `Taakht.Platform`): the same pieces with idiomatic names: `Migrator.MigrateAsync(NpgsqlDataSource, Assembly)` (embedded resources), a gRPC server interceptor + client interceptor for `x-user-id` and `CurrentUser.Id(ServerCallContext)`, `Outbox.AddAsync(NpgsqlConnection, NpgsqlTransaction, topic, key, IMessage)`, an `OutboxRelay : BackgroundService`, and `EventConsumer : BackgroundService` taking `(group, topics, Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>)`. Kafka client: `Confluent.Kafka`. Registration helpers: `services.AddTaakhtPlatform(configuration)`, `builder.AddTaakhtObservability(service, defaultHealthPort)` before it and `app.MapTaakhtObservability()` after `Build` (logging, request id and call metrics interceptors, health checks, `/metrics`), and `RequestContext` for the ambient request id.

## Quality bar

- Each service builds and its tests pass: `go vet ./... && go test -race ./...` / `dotnet test` (the .NET build treats warnings as errors).
- Business rules are unit-tested without Kafka or Postgres where possible; the lock/approval paths also get a Postgres-backed test (use the compose Postgres, skip when `TEST_DATABASE_URL` is unset).
- No cross-service tests live inside a service; those are the end-to-end scenario script.
- Code identifiers and comments in English; no TODO-comment backlog, keep comments to the why.
