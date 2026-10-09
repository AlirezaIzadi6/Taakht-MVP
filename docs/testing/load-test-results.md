# Load test results

Scripts: [`tests/load`](../../tests/load) (k6 v2.0.0). Raw summaries: [`tests/load/results/`](../../tests/load/results) (`<date>-<script>-<label>.json`; the files without a label are the `make load` defaults). Measured on 2026-10-09 against the code at commit `b6a443c` (paginated lists, hardened saga). An earlier sweep on the pre-pagination code (2026-10-08) was discarded because the code changed; it showed the same two problems described under "What broke".

One run per setting. A single run is not a statistical sample.

## Environment

| Item | Value |
|---|---|
| Machine | Windows 11 Pro laptop, Intel Core i5-12500H (12 cores, 16 logical), 16 GB RAM |
| Docker | Docker Desktop 27.4.0, WSL2 backend (16 CPUs and 7.6 GB visible to the VM). Envoy, one Postgres (`max_connections=100`) and a single-broker Kafka run in containers |
| Services | `ad`, `matching` (Go), `negotiation`, `swap` (.NET) as host processes from `scripts/dev.sh`, Development mode, default settings, no tuning |
| Load generator | k6 on the same machine, so it competes for CPU with the system under test |
| Data already on the stack | The stack was not wiped: hundreds of ads per test user and thousands of negotiations and swaps from earlier e2e, chaos and load runs were present (the search index also holds other published ads, so a search always filled its limit). The numbers include that |
| Nobody else used the stack during the runs (stack lock held) | |

## Method

- Settings rise from modest to heavy until p95 degrades or errors appear. Each run: ramp, hold, ramp down (browse 10 s / 45 s / 5 s; negotiate 15 s / 45 s / 5 s).
- Latency is k6 `http_req_duration` over all requests of the run, polling requests included. Request counts and rates include setup and teardown requests. Error % is `http_req_failed` (a status the script did not expect for that call; 429 is expected in `contention.js`).
- `browse.js`: 200 published ads seeded in `setup()`; 75% `POST /v1/matching/search` (one random category, half with a random neighborhood, limit 20 or 50), 25% `GET /v1/ads` (first page, default size); 100 ms think time.
- `negotiate.js`: per iteration publish 2 ads, open a negotiation, approve ad, approve proposal by both sides; then poll every 100 ms. `time_to_agreed`: from sending the final approval until `GET /v1/negotiations/{id}` shows `AGREED`. `time_to_ads_closed`: from the same instant until the target ad is `CLOSED` (outbox, Kafka, swap, `LockAds`, completion event, ad). Default in-person terms: nobody owes a fee, so the swap completes at once. Both timings include up to 100 ms polling granularity.
- `contention.js`: see the [README](../../tests/load/README.md) (cap phase, simultaneous final approvals, swap count through `GET /v1/swaps`).

## browse.js (read path)

| VUs | Requests | Req/s | Errors | p50 ms | p95 ms | p99 ms | search p95 ms | list-my-ads p95 ms |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 5,371 | 84.2 | 0% | 8.7 | 16.1 | 19.6 | 15.2 | 17.8 |
| 25 | 12,527 | 197.1 | 0% | 8.8 | 17.5 | 23.2 | 16.3 | 20.2 |
| 25 (`make load`, 30 s) | 9,135 | 187.3 | 0% | 8.7 | 17.1 | 23.4 | 16.2 | 18.7 |
| 50 | 25,074 | 393.3 | 0% | 6.1 | 13.6 | 18.6 | 12.4 | 16.0 |
| 100 | 48,845 | 769.6 | 0% | 6.2 | 21.5 | 31.6 | 20.7 | 23.8 |
| 200 | 97,908 | 1,540.2 | 0% | 6.2 | 16.2 | 23.0 | 15.4 | 18.4 |
| 400 | 152,191 | 2,389.0 | 0% | 33.0 | 87.3 | 135.1 | 94.0 | 50.1 |
| 800 | 164,621 | 2,578.2 | 0% | 193.3 | 274.1 | 308.8 | 279.9 | 58.6 |
| 1500 | 132,098 | 2,094.4 | 36.0% | 127.3 | 1,291.4 | 1,434.7 | 1,320.2 | 262.3 |

Reading: up to 200 VUs (about 1,540 req/s) p95 stays under 25 ms with no errors. Throughput stops growing between 400 and 800 VUs (about 2,400 to 2,600 req/s) and latency then grows with the VU count (p50 33 ms at 400, 193 ms at 800): the knee is around 400 VUs. At 1500 VUs, 36% of the requests failed with `connectex: ... actively refused it` on `localhost:8080`; Envoy was still running and answering afterwards, so this is a connection-establishment limit on the host or in Docker Desktop's port forwarding (k6 runs on the same machine), not a service error; I did not investigate which. These are numbers for a mostly CPU-shared laptop and do not tell where a larger machine would saturate.

## negotiate.js (write path, full agreement)

| VUs | Iterations done | Requests | Req/s | Errors | HTTP p50 / p95 / p99 ms | open-negotiation p95 ms | time_to_agreed p50 / p95 ms | time_to_ads_closed p50 / p95 / p99 ms | Iterations reaching the end state |
|---:|---:|---:|---:|---:|---|---:|---|---|---:|
| 5 | 318 | 4,521 | 69.2 | 0% | 10.2 / 32.3 / 44.2 | 29.8 | 385 / 610 | 389 / 615 / 4,147 | 100% |
| 10 | 663 | 9,493 | 140.1 | 0% | 10.1 / 31.9 / 42.1 | 31.3 | 483 / 601 | 487 / 605 / 4,210 | 100% |
| 10 (`make load`, 30 s) | 507 | 7,062 | 140.5 | 0% | 10.2 / 32.3 / 43.4 | 32.5 | 481 / 606 | 485 / 611 / 711 | 100% |
| 20 | 1,098 | 17,132 | 259.8 | 0% | 10.7 / 38.0 / 49.8 | 34.8 | 607 / 824 | 612 / 830 / 4,564 | 100% |
| 40 | 1,469 | 28,751 | 438.3 | 0% | 12.4 / 43.6 / 61.2 | 43.0 | 1,196 / 1,557 | 1,202 / 1,564 / 1,650 | 100% |
| 80 | 1,206 | 45,235 | 689.9 | 0% | 16.6 / 44.3 / 73.2 | 54.9 | 3,814 / 4,414 | 3,822 / 4,428 / 4,621 | 100% |
| 160 | 583 | 22,975 | 213.9 | 0.5% | 23.0 / 4,103.7 / 8,169.2 | 4,125.6 | 2,903 / 9,552 | 2,902 / 9,451 / 29,916 | 45.5% |
| 320 | 754 | 30,320 | 268.2 | 0.3% | 34.7 / 4,980.9 / 8,352.0 | 11,542.1 | 3,499 / 27,898 | 3,378 / 27,749 / 28,686 | 21.4% |

Reading:

- Up to 40 VUs every iteration ended with the negotiation `AGREED` and the target ad `CLOSED` and no HTTP call failed. Completed iterations per run rose until 40 VUs (1,469 in about 65 s, about 22 swaps per second counting ramps) and fell at 80 (1,206) and 160 (583).
- The synchronous API stays fast (p95 under 45 ms to 80 VUs). What degrades first is the asynchronous chain: time to `AGREED` is 0.4 to 0.6 s up to 20 VUs, 1.2 s at 40, 3.8 s at 80. The knee for the end-to-end flow is about 40 VUs.
- At 160 and 320 VUs the system broke down: 54.5% and 78.6% of the iterations did not reach `AGREED` within the 30 s poll limit, open/approve calls failed or took 4 s or more, and p95 of all HTTP calls rose above 4 s. Failing checks: approve ad 21 failures at 160 VUs; open negotiation 14 and approve ad 46 failures at 320 VUs. Cause below.
- A recurring outlier: every low-load run has a few events that took about 4 s (p99 of `time_to_ads_closed` 4.1 to 4.6 s at 5, 10 and 20 VUs against p95 about 0.6 s; HTTP max about 4.1 s in every run up to 80 VUs). Root cause found later, see "Unexplained 4 s stalls: root cause".

## contention.js (one hot ad)

| Requesters | open 200 | open 429 | open 500 | Negotiations on the hot ad | Swaps not REJECTED/CANCELLED | Swaps REJECTED | Final approvals sent / answered 200 | Overall HTTP p95 ms |
|---:|---:|---:|---:|---:|---:|---:|---|---:|
| 40 | 10 | 30 | 0 | 10 | 1 (COMPLETED) | 6 | 10 / 10 | 211 |
| 40 (`make load`) | 10 | 30 | 0 | 10 | 1 | 9 | 10 / 10 | 4,235 |
| 100 | 10 | 86 | 4 | 10 | 1 | 9 | 10 / 10 | 4,267 |
| 200 | 10 | 168 | 22 | 10 | 1 | 9 | 10 / 10 | 4,251 |

Reading:

- Cap: in every run exactly 10 of the simultaneous `OpenNegotiation` calls created a negotiation, and the hot ad's negotiation list held exactly 10. At 40 requesters all other calls got 429.
- Lock race: all 10 final approvals were sent at the same wall-clock instant and all returned 200. In every run exactly one swap for the hot ad was `COMPLETED`; the others were `REJECTED` (6 to 9; in the first 40 run only 7 swaps existed in total, the other negotiations were cancelled before a swap was created), the losing negotiations ended `CANCELLED`, the winner `AGREED`, the hot ad `CLOSED`. The system settled about 1.1 s after the barrier. No double swap in any run.
- The p95 of about 4.2 s in the `make load` and heavier runs comes from some of the simultaneous open calls (a 4 s stall); in the first 40 run it did not occur (p95 211 ms). Not investigated.

## What broke under load

- **negotiation service, `OpenNegotiation` (and `ApproveAd`), 100+ concurrent callers:** HTTP 500 / gRPC `UNKNOWN` (`{"code":2,"message":"Exception was thrown by handler."}`) instead of 429 (4 of 100 and 22 of 200 calls in `contention.js`). The negotiation log shows `Npgsql.PostgresException 53300: sorry, too many clients already` (308 occurrences during the 160 and 320 VU runs). The Postgres container allows 100 connections in total, shared by all services and their outbox relays and consumers, while each service pool may grow on its own; under about 100 concurrent callers the connection limit is reached before the cap check can answer. Fixed afterwards, see "After the fix". Candidate fixes at the time: a smaller pool per service, a larger `max_connections`, or mapping connection exhaustion to `UNAVAILABLE`. The cap invariant and the single-swap invariant still held when this happened.
- **Overload does not recover quickly:** at 160 and 320 VUs more than half of the agreements were not visible as `AGREED` within 30 s. The swap service log showed a handful of failed event handlers (`EventConsumer`); I did not trace whether the agreements completed later.

## Bottleneck notes

- **Read path:** not investigated in depth. The saturation point is near 2,500 req/s with k6, Envoy (in the Docker VM) and the services sharing one laptop. No per-process profile was taken.
- **Write path:** during the 40 VU negotiate run (`docker stats`, percentages of one core): Postgres 174%, Envoy 60%, Kafka 38%; host counters showed the Docker VM (`vmmemwsl`) at about 670 and .NET processes about 150 of 1600 percentage points, with about 290 idle. So at the knee the machine is close to busy, with the database container the largest single consumer. A single sample; the chain was not profiled, and which hop limits the time to `AGREED` is not known.
- **Contention and overload:** the 500s come from Postgres connection exhaustion (log evidence above).

## Caveats

- Laptop, Windows with Docker Desktop (WSL2), k6 on the same machine, services in Development mode (verbose per-request logging to files), no tuning of pools, Kafka or Postgres, existing data on the stack.
- Single runs per setting; run-to-run noise exists. In the earlier (discarded) sweep the same 25 VU browse setting gave p95 values 4x apart.
- Numbers say nothing about production scale or a multi-node deployment and are not extrapolated.
- Negotiations and swaps from the runs remain in the databases (no delete in the API); ads were hidden by teardown or closed by their swap. `e2e` passed after the runs.
- `make load` uses browse 25 VUs 30 s, negotiate 10 VUs 30 s, contention 40 requesters.

## After the fix (2026-10-09)

Code: bounded pools (`DB_MAX_CONNS=20`, `DB_MIN_CONNS` 2 in Go and 5 in .NET, `DB_ACQUIRE_TIMEOUT=10s`), Postgres `max_connections=200`, overload mapped to `UNAVAILABLE` in all four services, the negotiation connection no longer held across calls to the Ad service, and `127.0.0.1` instead of `localhost` for the host-run services (see the root cause below). Same machine, same k6 scripts, one run per setting, nobody else on the stack. Raw files: `tests/load/results/2026-10-09-*-fix-*.json`.

**Not comparable one-to-one with the table above:** the stack was wiped (`make reset`) before these runs, so the databases were nearly empty, whereas the earlier runs ran on a database with thousands of rows from earlier tests. Kafka was recreated as well. Both changes may account for part of the difference; I did not run the old code on an empty stack to separate them.

| Check | Before | After |
|---|---|---|
| `53300` in `negotiation.log` during the heavy runs | 308 | 0 |
| `contention.js` 100 requesters: open 200 / 429 / 500 | 10 / 86 / 4 | 10 / 90 / 0 |
| `contention.js` 200 requesters: open 200 / 429 / 500 | 10 / 168 / 22 | 10 / 190 / 0 |
| `contention.js` invariants (100 and 200) | 10 negotiations, 1 swap not REJECTED/CANCELLED | same: 10 negotiations, 1 swap not REJECTED/CANCELLED, 10/10 final approvals 200 |
| `contention.js` overall HTTP p95 (100 / 200) | 4,267 / 4,251 ms | 766 / 756 ms |
| `negotiate.js` 5 VUs: HTTP max, `time_to_ads_closed` p99 | about 4.1 s, 4,147 ms | 82 ms, 624 ms |
| `negotiate.js` 40 VUs: HTTP p95 / max, `time_to_ads_closed` p95 / p99 | 43.6 ms / about 4.1 s, 1,564 / 1,650 ms | 42.8 / 290 ms, 1,481 / 1,585 ms |
| `negotiate.js` 160 VUs: HTTP p95, failed requests, iterations reaching the end state | 4,104 ms, 0.5%, 45.5% | 65 ms, 0%, 100% (943 of 943) |
| `negotiate.js` 320 VUs: HTTP p95, failed requests, iterations reaching the end state | 4,981 ms, 0.3%, 21.4% | 216 ms, 0%, 10.8% (107 of 995) |

- No HTTP 500 and no 503 occurred in any run after the fix (`http_req_failed` 0 in all of them), so the `UNAVAILABLE` path was not triggered live: with `max_connections=200` and pools of 20 the connection limit is no longer reached at these loads. That path is covered by the unit tests and by the tests that exhaust a pool of 2 connections (`libs/goplatform/server`, `Taakht.Platform.Tests`), which assert `UNAVAILABLE` and recovery.
- At 320 VUs the synchronous API stays fast but 89% of the iterations did not see `AGREED` within the 30 s poll limit (time to `AGREED` median 2.9 s, p95 26.9 s). The asynchronous chain (outbox, Kafka, consumers) is still the limit under heavy load; this was not investigated. At 160 VUs the chain delivered every iteration but slowly (time to `AGREED` median 15.4 s, p95 16.8 s).
- Outbox lag (`published_at - created_at`) read after the runs, over the whole life of the freshly wiped databases: negotiation max 1.06 s (p99 0.83 s), swap max 0.40 s, ad max 0.26 s, matching max 0.30 s. Before, the negotiation and swap databases showed lags of 3.7 to 4.2 s.
- Hot paths reviewed in `NegotiationService`: `OpenNegotiation` fetched both ads before opening a connection (good) and kept one connection for the cap check, but held that connection open while fetching the ads again for the response (`WithAdsAsync`). `ApproveAd` through `TryReachAgreement` held a connection while calling `GetAd` twice, and one branch opened a second connection while still holding the first. Now every remote call happens with no connection held, and the cap check and insert remain one transaction under the row locks. Swap's event consumer still holds its dedupe transaction across the `LockAds` call (known gap 6).
- `e2e` and `scripts/demo.sh` pass after the runs.

## Unexplained 4 s stalls: root cause

The roughly 4 s outliers listed above (p99 of `time_to_ads_closed`, HTTP max, the p95 of about 4.2 s in `contention.js`) come from opening a new physical Postgres connection from .NET with `Host=localhost`. On this Windows machine the name resolves to `::1` first; Docker publishes Postgres on `127.0.0.1` only, and a refused loopback connect to `::1` costs about 2 s in .NET, which Npgsql pays twice. Measured: a new connection to `localhost` takes about 4.03 s, to `127.0.0.1` about 15 ms. The stalls therefore appeared whenever a .NET service's pool had to grow, and showed up as outbox lag of 3.7 to 4.2 s only in the negotiation and swap databases, never in the Go services. Fix: the dev script and the service defaults use `127.0.0.1`, the platform libraries rewrite a database host of exactly `localhost` to `127.0.0.1`, Kafka advertises `127.0.0.1:9094`, and the .NET pools start with 5 connections. After the change the 4 s outliers are gone (table above).
