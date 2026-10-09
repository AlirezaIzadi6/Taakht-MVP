# Running locally

Run the whole MVP (Postgres, Kafka and the four services) on your machine with one command. Services run on the host, not in containers; see [MVP service conventions](mvp-service-conventions.md).

## Prerequisites

- Docker (running), Git Bash on Windows, `make`, the Go toolchain, the .NET SDK from `global.json`, and [`grpcurl`](https://github.com/fullstorydev/grpcurl) for manual calls. `make prereqs` checks the first ones; see [Get started](../get-started.md).
- `make` and `go`/`grpcurl` must be on `PATH` in your shell (see Troubleshooting). `scripts/dev.sh` adds the usual Windows locations itself.

## Start, stop, status

```bash
make dev          # make up, build all services, start them in the background, wait for their ports
make dev-status   # which services are up
make dev-stop     # stop the services (Postgres and Kafka keep running)
make reset        # stop services, wipe ALL data (docker compose down -v), start a fresh infra
make e2e          # end-to-end tests (tests/e2e); needs `make dev`
make scenario     # scripted demo scenario; needs `make dev`
```

`scripts/dev.sh` offers more: `start [svc...]`, `stop`, `status`, `logs <svc>` (follows the log), `restart [svc]`. A service whose project does not exist yet is skipped with a warning.

Settings you can override through the environment: `PAYMENT_DEADLINE` (swap locker-fee deadline; `scripts/dev.sh` default `2m`, whereas the swap service started by hand defaults to `1h`), `WAIT_SECS` (startup wait per service, default 60), `KAFKA_BROKERS`, `DB_BASE`, `AD_ADDR`, `ELIGIBILITY_FILE`, `INTERNAL_AUTH_TOKEN`, `TAAKHT_GRPC_REFLECTION`. Pool sizes (`DB_MAX_CONNS`, `DB_MIN_CONNS`, `DB_ACQUIRE_TIMEOUT`) and the other service settings are documented in [MVP service conventions](mvp-service-conventions.md); they are not set by `dev.sh`, so the code defaults apply. Example: `PAYMENT_DEADLINE=30s make dev`.

Edge (Envoy) settings are read when the config is rendered, which `make up` (and so `make dev`) does first: `JWT_SIGNING_KEY` (HS256 secret of at least 32 characters; default is the public dev key, which `tools/devtoken` also uses), `JWT_KEY_ID`, `JWT_SIGNING_KEY_PREVIOUS` / `JWT_KEY_ID_PREVIOUS` (rotation), `ENVOY_CORS_ORIGINS` (default `http://localhost:3000,http://127.0.0.1:3000`) and `ENVOY_ENABLE_DEV_ROUTES` (`true` keeps `/v1/dev/...`; `make up`/`make dev` default it to `true` for local dev so the demo works, while `scripts/gen-envoy-config.sh` itself defaults to `false`; set `false` to harden). Envoy restarts automatically when the rendered config changes. See [Edge operations](#edge-operations-envoy).

## Ports

| Service | gRPC | Health HTTP (`/healthz` `/readyz` `/metrics`) | Database |
|---|---|---|---|
| ad | `127.0.0.1:9001` | `127.0.0.1:9101` | `ad` |
| matching | `127.0.0.1:9002` | `127.0.0.1:9102` | `matching` |
| negotiation | `127.0.0.1:9003` | `127.0.0.1:9103` | `negotiation` |
| swap | `127.0.0.1:9004` | `127.0.0.1:9104` | `swap` |
| Envoy gateway (REST + JWT) | `http://localhost:8080` | - |

Postgres is on `127.0.0.1:5432` (user and password `taakht`), Kafka on `127.0.0.1:9094`. Envoy is started by `make up` together with them. Postgres, Kafka and Envoy's public port 8080 are published on loopback only (`127.0.0.1`), so they are not reachable from other machines on your network.

## Edge operations (Envoy)

`gateway/envoy.yaml` is a template. `scripts/gen-envoy-config.sh` renders it to `deploy/envoy/envoy.yaml` and writes the JWT key set `deploy/envoy/jwks.json` (both git-ignored, mounted into the container). `make up` runs it automatically, also on a clean clone, and restarts Envoy when the output changed. With plain compose run it once yourself: `scripts/gen-envoy-config.sh && docker compose -f deploy/docker-compose.yml up -d`.

- **Signing key.** Default is the public dev key (`taakht-dev-secret-0123456789abcdef`), which `tools/devtoken` also defaults to, so nothing needs configuring locally. For anything shared: `export JWT_SIGNING_KEY=$(openssl rand -base64 48)` before `make up`, and give `tools/devtoken` the same variable.
- **Rotating the key (no invalid tokens).** 1) Render with both keys: `JWT_SIGNING_KEY=<new> JWT_KEY_ID=k2 JWT_SIGNING_KEY_PREVIOUS=<old> JWT_KEY_ID_PREVIOUS=dev-1 make up` (Envoy restarts; tokens signed with either key validate). 2) Issue new tokens with `JWT_SIGNING_KEY=<new> JWT_KEY_ID=k2`. 3) After the longest token lifetime (24 h for devtoken), render again without the `PREVIOUS` variables; old tokens then get 401.
- **Dev route.** `/v1/dev/swaps/...` (locker-fee simulation) is dropped from the rendered config unless `ENVOY_ENABLE_DEV_ROUTES=true`; the edge then answers a JSON 404. `make up` defaults it to `true` (local dev); run `ENVOY_ENABLE_DEV_ROUTES=false make up` to block it. Plain `scripts/gen-envoy-config.sh` defaults to blocked.
- **CORS.** `ENVOY_CORS_ORIGINS=https://app.example,http://localhost:3000 make up`. Credentials are off; `Authorization` and `Content-Type` are allowed; `x-request-id` is exposed.
- **Limits.** Route timeout 15 s, request body 1 MiB (413), request headers 16 KB, headers within 10 s and the whole request within 30 s, idle stream 30 s and idle connection 120 s, 1000 connections per listener (2000 per process). Only GETs are retried (connect failure or reset, 2 retries); POST and PUT never.
- **Logs and health.** Envoy logs one JSON line per request to stdout (`docker compose -f deploy/docker-compose.yml logs -f envoy`): request id, method, path, status, gRPC status, upstream cluster, attempts, duration, user id. `/healthz` is 200 while Envoy runs; `/readyz` is 200 only when every upstream passes its health check (503 otherwise). Responses carry `x-request-id` (generated, or the client's).
- **Tests.** `make gateway-test` (`scripts/test-gateway.sh`, needs `make dev`) checks all of the above; it restarts Envoy and briefly stops the matching service (set `GATEWAY_TEST_DISRUPTIVE=0` to skip those parts).

Internal token rotation (service to service): see [API conventions, Caller identity](api-conventions.md#caller-identity).

## Health, logs, metrics

`scripts/dev.sh start` waits until `GET /readyz` of each service answers 200 (database reachable, Kafka consumer joined, outbox not stuck) and `scripts/dev.sh status` has a READY column (`ready`, `not-ready`, `-`).

```bash
curl -s 127.0.0.1:9101/readyz     # 200 "ready" + one line per check, or 503 naming the failing check
curl -s 127.0.0.1:9103/healthz    # liveness: the process is up
curl -s 127.0.0.1:9104/metrics | grep taakht_
grpcurl -plaintext -d '{"service":"readiness"}' 127.0.0.1:9001 grpc.health.v1.Health/Check
```

Logs: `LOG_FORMAT=json make dev` writes one JSON object per line (`ts`, `level`, `service`, `msg`, `request_id`, `user_id`, ...) to `.run/logs/<svc>.log`. Follow one REST call through all services: `curl -si -H "Authorization: Bearer $T" 127.0.0.1:8080/v1/ads` returns `x-request-id`; `grep <id> .run/logs/*.log` shows it in every service it touched, including the asynchronous consumers.

Metrics UI (optional): `docker compose -f deploy/docker-compose.yml --profile observability up -d` starts Prometheus (`http://127.0.0.1:9090`) and Grafana (`http://127.0.0.1:3000`, admin/admin, dashboard "Taakht MVP overview"). Prometheus scrapes `host.docker.internal:9101..9104`; if the targets are DOWN because the health listeners are bound to loopback, start the services with `HEALTH_BIND=0.0.0.0 make dev` (exposes unauthenticated metrics to your network; use only on a trusted machine). Stop with `docker compose -f deploy/docker-compose.yml --profile observability down`.

## Demo and tests

```bash
make demo       # narrated REST demo through Envoy (:8080); needs make up + scripts/dev.sh start; DEMO_ARGS="--pause" to step through
make gateway-test  # edge checks: auth, CORS, limits, 404, dev route, key rotation, retries; needs make dev
make e2e        # end-to-end Go tests in tests/e2e; needs make dev
make scenario   # scripted demo scenario over gRPC; needs make dev
make chaos      # fault injection (about 10 min); kills services, stops Kafka and Postgres; stack must be unused
make load       # k6 load tests (needs k6); modest defaults, saturates the machine at higher settings
```

`make demo` additionally needs `jq` and `curl` on `PATH`.

The e2e tests have two modes (see `tests/e2e/README.md`). By default swap runs with the long payment deadline and `TestPaymentTimeout` is skipped. For the timeout test, start swap with `PAYMENT_DEADLINE=5s` and run `E2E_SHORT_DEADLINE=1 make e2e`; in that mode only `TestPaymentTimeout` runs and the other tests skip themselves.

## Operations: dead letters

`tools/dlq` inspects and replays the `dead_letter` table of one service database (permanently failed events; see the conventions). It is a Go module in the workspace; run it from the repo root. Point it at the database of the service that parked the event:

```bash
export DATABASE_URL='postgres://taakht:taakht@127.0.0.1:5432/ad?sslmode=disable'   # or --db URL
go run ./tools/dlq list                       # newest first: consumer, event_id, topic, type, created_at, error (--limit N, --full)
go run ./tools/dlq show <event_id>            # decoded envelope; payload as JSON for known types, hex otherwise
go run ./tools/dlq replay <event_id> --yes    # re-publish the original envelope to its topic with its original key
go run ./tools/dlq replay <event_id> --delete # ... and delete the dead_letter row after the broker acknowledged it
go run ./tools/dlq purge --older-than 30d     # delete old rows
```

`replay` and `purge` ask `[y/N]` on a terminal and refuse without one unless `--yes` is given. Replay uses `KAFKA_BROKERS` (default `127.0.0.1:9094`). It clears the consumer's `processed_events` marker for that event first (restored if the produce fails), because the consumer would otherwise drop the replay as a duplicate; other consumer groups already hold their own marker and ignore it. A replayed event that fails permanently again is parked again only after its row is deleted (`--delete`), since the old row is kept. Fix the cause (usually a handler or a producer bug) before replaying; an envelope whose payload cannot be decoded by any handler will fail again.

## Where things are

Everything generated lives in the git-ignored `.run/` directory:

| Path | Content |
|---|---|
| `.run/logs/<svc>.log` | Service output (`ad`, `matching`, `negotiation`, `swap`); `.run/logs/infra.log` is the last `make up` |
| `.run/pids/<svc>.pid` | Windows process id of the service |
| `.run/bin/` | Built binaries |

## Calling a service

gRPC reflection is enabled by `scripts/dev.sh` (`TAAKHT_GRPC_REFLECTION=on` for the Go services, the Development environment for the .NET ones); it is off by default elsewhere, so use `-proto`/`-import-path` there. Every call needs the caller in `x-user-id` (seed users `user-1` .. `user-4`). The internal identities `system:swap` and `system:negotiation` additionally need `-H 'x-internal-token: $INTERNAL_AUTH_TOKEN'`; `scripts/dev.sh` exports `INTERNAL_AUTH_TOKEN` to all four services (default `dev-internal-token`, DEV ONLY; each service logs a warning while the default is in use). Without the token such a call is `UNAUTHENTICATED`.

```bash
grpcurl -plaintext 127.0.0.1:9001 list                                   # services
grpcurl -plaintext 127.0.0.1:9001 describe taakht.ad.v1.AdService        # methods and messages

grpcurl -plaintext -H 'x-user-id: user-1' \
  -d '{"spec":{"title":"Book","have_category":"books","want_categories":["tools"]}}' \
  127.0.0.1:9001 taakht.ad.v1.AdService/CreateAd

grpcurl -plaintext -H 'x-user-id: user-1' -d '{"page_size":20}' 127.0.0.1:9001 taakht.ad.v1.AdService/ListMyAds
grpcurl -plaintext -H 'x-user-id: user-1' 127.0.0.1:9004 taakht.swap.v1.SwapService/ListMySwaps
```

New ads are hidden; publish with `AdService/PublishAd` to make matching see them.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `make: command not found` | Add the make folder to `PATH`, e.g. the WinGet one: `/c/Users/<you>/AppData/Local/Microsoft/WinGet/Packages/ezwinports.make_*/bin`. |
| `go` or `grpcurl` not found | Add `$(go env GOPATH)/bin` (usually `/c/Users/<you>/go/bin`) and the Go `bin` folder to `PATH`, then open a new shell. |
| `make up failed` | Start Docker Desktop and retry. Details are in `.run/logs/infra.log`. |
| `port 900x is busy with a process not started by dev.sh` | Another process owns the port. Find it with `netstat -ano \| grep :9001`, stop it (`taskkill //PID <pid> //T //F`), or run `scripts/dev.sh stop`, which also frees the four ports. |
| A service did not open its port in time | The script prints the log tail. Read `.run/logs/<svc>.log`; raise `WAIT_SECS` on a slow first build. |
| Kafka not ready / services log connection errors to `127.0.0.1:9094` | `docker compose -f deploy/docker-compose.yml ps` must show `kafka` healthy. `make reset` recreates it. Services retry, so a short delay after `make up` is normal. |
| New database connections take about 4 s on Windows (outbox lag of 4 s, HTTP calls that stall for 4 s whenever a .NET pool grows) | Use `127.0.0.1`, not `localhost`, for host-run services. `localhost` resolves to `::1` first; Docker publishes Postgres and Kafka on `127.0.0.1` only, so the refused IPv6 connect is paid again for every new physical connection (Npgsql twice, about 4.03 s against about 15 ms). `scripts/dev.sh` and the service defaults use `127.0.0.1`, and the platform libraries rewrite a database host of exactly `localhost`; set `DB_BASE`, `AD_ADDR` and `KAFKA_BROKERS` the same way if you override them. After changing the Kafka advertised listener, recreate Kafka with `make reset` (this wipes the data). |
| Stale or broken data | `make reset` wipes every database and Kafka topic. |
| `Permission denied` on `scripts/dev.sh` | `chmod +x scripts/dev.sh` (or call it as `bash scripts/dev.sh`). |
