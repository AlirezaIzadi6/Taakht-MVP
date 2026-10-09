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

Settings you can override through the environment: `PAYMENT_DEADLINE` (swap locker-fee deadline, default `2m`), `WAIT_SECS` (startup wait per service, default 60), `KAFKA_BROKERS`, `DB_BASE`, `AD_ADDR`, `ELIGIBILITY_FILE`. Example: `PAYMENT_DEADLINE=30s make dev`.

## Ports

| Service | gRPC | Database |
|---|---|---|
| ad | `localhost:9001` | `ad` |
| matching | `localhost:9002` | `matching` |
| negotiation | `localhost:9003` | `negotiation` |
| swap | `localhost:9004` | `swap` |
| Envoy gateway (REST + JWT) | `http://localhost:8080` | - |

Postgres is on `127.0.0.1:5432` (user and password `taakht`), Kafka on `127.0.0.1:9094`. Envoy is started by `make up` together with them. Postgres, Kafka and Envoy's public port 8080 are published on loopback only (`127.0.0.1`), so they are not reachable from other machines on your network.

## Demo and tests

```bash
make demo       # narrated REST demo through Envoy (:8080); needs make up + scripts/dev.sh start; DEMO_ARGS="--pause" to step through
make e2e        # end-to-end Go tests in tests/e2e; needs make dev
make scenario   # scripted demo scenario over gRPC; needs make dev
```

`make demo` additionally needs `jq` and `curl` on `PATH`.

The e2e tests have two modes (see `tests/e2e/README.md`). By default swap runs with the long payment deadline and `TestPaymentTimeout` is skipped. For the timeout test, start swap with `PAYMENT_DEADLINE=5s` and run `E2E_SHORT_DEADLINE=1 make e2e`; in that mode only `TestPaymentTimeout` runs and the other tests skip themselves.

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
grpcurl -plaintext localhost:9001 list                                   # services
grpcurl -plaintext localhost:9001 describe taakht.ad.v1.AdService        # methods and messages

grpcurl -plaintext -H 'x-user-id: user-1' \
  -d '{"spec":{"title":"Book","have_category":"books","want_categories":["tools"]}}' \
  localhost:9001 taakht.ad.v1.AdService/CreateAd

grpcurl -plaintext -H 'x-user-id: user-1' -d '{"page_size":20}' localhost:9001 taakht.ad.v1.AdService/ListMyAds
grpcurl -plaintext -H 'x-user-id: user-1' localhost:9004 taakht.swap.v1.SwapService/ListMySwaps
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
| Kafka not ready / services log connection errors to `localhost:9094` | `docker compose -f deploy/docker-compose.yml ps` must show `kafka` healthy. `make reset` recreates it. Services retry, so a short delay after `make up` is normal. |
| New database connections take about 4 s on Windows (outbox lag of 4 s, HTTP calls that stall for 4 s whenever a .NET pool grows) | Use `127.0.0.1`, not `localhost`, for host-run services. `localhost` resolves to `::1` first; Docker publishes Postgres and Kafka on `127.0.0.1` only, so the refused IPv6 connect is paid again for every new physical connection (Npgsql twice, about 4.03 s against about 15 ms). `scripts/dev.sh` and the service defaults use `127.0.0.1`, and the platform libraries rewrite a database host of exactly `localhost`; set `DB_BASE`, `AD_ADDR` and `KAFKA_BROKERS` the same way if you override them. After changing the Kafka advertised listener, recreate Kafka with `make reset` (this wipes the data). |
| Stale or broken data | `make reset` wipes every database and Kafka topic. |
| `Permission denied` on `scripts/dev.sh` | `chmod +x scripts/dev.sh` (or call it as `bash scripts/dev.sh`). |
