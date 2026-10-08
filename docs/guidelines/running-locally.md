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

Postgres is on `localhost:5432` (user and password `taakht`), Kafka on `localhost:9094`.

## Where things are

Everything generated lives in the git-ignored `.run/` directory:

| Path | Content |
|---|---|
| `.run/logs/<svc>.log` | Service output (`ad`, `matching`, `negotiation`, `swap`); `.run/logs/infra.log` is the last `make up` |
| `.run/pids/<svc>.pid` | Windows process id of the service |
| `.run/bin/` | Built binaries |

## Calling a service

gRPC reflection is enabled by `scripts/dev.sh` (`TAAKHT_GRPC_REFLECTION=on` for the Go services, the Development environment for the .NET ones); it is off by default elsewhere, so use `-proto`/`-import-path` there. Every call needs the caller in `x-user-id` (seed users `user-1` .. `user-4`).

```bash
grpcurl -plaintext localhost:9001 list                                   # services
grpcurl -plaintext localhost:9001 describe taakht.ad.v1.AdService        # methods and messages

grpcurl -plaintext -H 'x-user-id: user-1' \
  -d '{"spec":{"title":"Book","have_category":"books","want_categories":["tools"]}}' \
  localhost:9001 taakht.ad.v1.AdService/CreateAd

grpcurl -plaintext -H 'x-user-id: user-1' localhost:9001 taakht.ad.v1.AdService/ListMyAds
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
| Stale or broken data | `make reset` wipes every database and Kafka topic. |
| `Permission denied` on `scripts/dev.sh` | `chmod +x scripts/dev.sh` (or call it as `bash scripts/dev.sh`). |
