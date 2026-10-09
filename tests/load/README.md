# Load and stress tests (k6)

Three [k6](https://k6.io) scripts drive the stack through the Envoy REST edge (`http://localhost:8080`) with the dev JWTs of `user-1` .. `user-4`. Results are recorded in [`docs/testing/load-test-results.md`](../../docs/testing/load-test-results.md); raw summaries are in `results/`.

| Script | What it does |
|---|---|
| `browse.js` | Read path. `setup()` publishes ~200 ads over the categories and neighborhoods of `config/eligibility.json`; virtual users then call `POST /v1/matching/search` (75%) and `GET /v1/ads` (25%). Teardown hides the ads. |
| `negotiate.js` | Write path. Each iteration: publish 2 ads, open a negotiation, approve ad, approve proposal x2 (agreement), then poll until the negotiation is `AGREED` and the target ad is `CLOSED` (default in-person terms: the swap completes at once). Custom metrics `time_to_agreed` and `time_to_ads_closed` (end-to-end event propagation: outbox, Kafka, swap, lock, completion, ad). |
| `contention.js` | One hot ad. Phase 1: N users open negotiations on it at the same instant (cap is 10: expect exactly 10 x 200, the rest 429). Phase 2: the 10 negotiations are driven to agreement at the same instant. Phase 3: counts the swaps of the hot ad via `GET /v1/swaps` (expect exactly one not `REJECTED`/`CANCELLED`). |

## Running

Needs the stack up (`make dev`), `k6` on `PATH`, and a built `tools/devtoken` (built automatically when Go is available).

```bash
tests/load/gen-tokens.sh          # writes tests/load/.tokens.json (git-ignored); k6 cannot exec, so it reads this file
cd tests/load
k6 run browse.js                  # -e VUS=20 -e DURATION=60s -e ADS=200 -e THINK=0.1
k6 run negotiate.js               # -e VUS=10 -e DURATION=60s -e POLL_MS=100
k6 run contention.js              # -e REQUESTERS=40
make load                         # from the repo root: all three with modest defaults
```

`-e LABEL=x` appends `-x` to the result file name (`results/<date>-<script>-x.json`). Run from `tests/load` (the result path is relative). Tokens last 24 h; rerun `gen-tokens.sh` when they expire.

## Conventions

- Every ad title carries a per-run id (`[<id>]`), so runs are repeatable on a stack that already holds data.
- Teardown hides every still-`PUBLISHED` ad of the run (found through `GET /v1/ads`). Ads of completed swaps end `CLOSED` and need no cleanup. Negotiations and swaps cannot be deleted and stay.
- Thresholds are in the scripts but only record: this is not CI. A crossed threshold prints an error and makes k6 exit non-zero; `make load` ignores that.
- Take the stack lock (`mkdir .run/stack.lock`) before running if other people or agents use the stack: the runs create thousands of ads and saturate the machine.
- The list endpoints are paginated (newest first, `nextPageToken`); cleanup and the swap count walk the pages, and negotiations are listed with `?adId=`.
