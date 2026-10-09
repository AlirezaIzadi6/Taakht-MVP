# Taakht

**A goods-swap layer for a classifieds platform (designed around Divar), built as a microservices portfolio project.**

> **Status: one-week MVP built (2026-10-08).** Four services (Ad and Matching in Go, Negotiation and Swap in .NET 10) run locally behind an Envoy REST gateway with JWT validation. A scripted REST demo and end-to-end tests exercise the whole swap flow. Everything else in the design is mocked, cut or not built; see [MVP status](#mvp-status). The sections below describe the **full design**; the MVP implements a subset of it.

## The idea

Many people hold small, low-value items they no longer use. Selling them for cash is unattractive (high friction, low price, no trust mechanism), and informal swap groups have no matching or safety net.

Taakht lets users declare what they have and what they want, matches them with each other (also while they are away from the app), and walks both sides through negotiation and delivery. Money never goes through the platform: any price difference is settled directly between the two parties.

Honest framing: this is a **calculated bet for a Growth/Experiments team**, not a claim about the future of the platform. It deliberately targets low-value goods and takes no cut of the exchange (the only revenue idea is paid matching priority), so direct revenue is capped; the case rests on secondary metrics (engagement and reactivation of idle inventory).

## How it works

1. **Browse** anonymously by category and neighborhood. Browsing is a stateless query; nothing is stored and requests cannot be sent.
2. **Publish an Ad** to become discoverable: at least one filter plus an explicit "show me to others" opt-in. An Ad is persistent, versioned, may be partially specified on both sides ("have" / "want"), and stays active in the matching engine after restarts. It can be hidden at any time; it is never deleted, and once a deal is agreed on it, the Ad is closed for good (still not deleted). The user gets a notification when a match appears.
3. **Request and negotiate.** A Swap Request opens a **Negotiation** right away, with a chat and a versioned **Proposal** (items, delivery method, any agreed price difference, locker cost split). Each of your Ads can have a limited number of open negotiations at a time (configurable; the MVP default is 10); settle one before starting another beyond the limit. Every edit is a new Proposal version the other side must approve.
4. **Pin down the terms (optional).** Naming exact items is optional; the parties know what they are trading. To record exactly what was agreed, either side can edit their Ad, and the agreement is stored against that Ad version. Approvals are bound to the version of the terms they were given on.
5. **Lock.** When both sides approve the same Proposal, an agreement is reached, a **Swap** is created, an atomic exclusive lock is taken, competing requests are cancelled and their users notified, and the Ad leaves the index.
6. **Complete the swap.** The platform never touches money: any price difference is paid directly between the two parties, outside the system. The parties pick one of two delivery methods (a **locker**, or **in person / courier** arranged by themselves) and are responsible for checking the goods themselves: there is no post-delivery confirmation checklist, only the item-condition claim each side records beforehand.

## Architecture at a glance

A microservices system designed for the scale it would need to operate at (barter liquidity only exists on a large platform), run as a single-node demo. The distributed-systems problems (partial failure, eventual consistency, ordering, idempotency) are real in this demo, because they come from process and data boundaries, not from the number of machines. Scale itself is claimed only where it is measured.

Services are classified by how deep the design goes. The last column is what exists in the repository today.

| Service | Responsibility | Class | Design depth | MVP status |
|---|---|---|---|---|
| **Ad** | Owns Ads: creation, versioned edits, publish/hide, consumption by a Swap | Core | Full | Built (Go) |
| **Matching** | Geo/category/value/reputation-aware candidate discovery (Criteria against the Ad index); two-sided sync and async matching | Core | Full | Built (Go); plain SQL index, no reputation input |
| **Negotiation** | Concurrent Negotiations with versioned Proposals, state machine up to agreement | Core | Full | Built (.NET 10); no chat |
| **Settlement** | Exclusive lock, item-condition claims, tracking of both sides' obligations up to the final outcome of a Swap (no money involved) | Core | Full | Built as the **Swap** service (.NET 10): lock saga, mocked locker-fee step with deadline; no item-condition claims |
| **Communication** | Trade-scoped chat, system notifications (delivery mostly mocked), notifications to users watching an Ad | Supporting | Minimal | Not built; `MatchFound` and cancellations are log lines |
| **Reputation** | Event-sourced trust score (recomputable when the formula changes), anti-collusion limits; Matching keeps a local copy | Supporting | Minimal | Not built |
| **Delivery Orchestration** | Locker delivery tracking, claim forwarding (no content arbitration); in-person/courier hand-over is arranged by the parties and not tracked | Supporting | Minimal | Mocked inside the Swap service (simulated locker-fee payment) |
| **Provider adapters** | Everything outside the swap domain (see below) | Generic | Mock | Mocked (dev JWT issuer, seeded users, static eligibility file) |

### What is real and what is mocked

Every dependency outside the swap domain sits behind a small interface with a mock implementation. Swapping in a real implementation later is a wiring change, not a change to the core services.

| Interface | Phase 1 (this project) |
|---|---|
| `AuthProvider` | Issues a JWT for seeded users, no real password check (`tools/devtoken`; Envoy validates it) |
| `AdRegistryProvider` | Seeded fake ads/users and a small fixed category/neighborhood taxonomy (`config/eligibility.json`) |
| `KYCProvider` | Always passes |
| `DeliveryProvider` | Simulated locker statuses (the only delivery method integrated with a partner); locker eligibility fails for one seeded user to show the error path |
| `ReportProvider` | Records an abuse report (any user can be reported, not only a trade counterpart), no review workflow. Not built in the MVP |

There is deliberately no wallet, payment or escrow interface: money never flows through the platform.

Rule of thumb: mock an external dependency when it cannot realistically be built (licenses, partners) or when building it shows nothing interesting. Build for real anything that is part of the swap domain itself, such as the matching engine and its geo index.

## Key design decisions

- **Persistent, class-level, versioned Ad** instead of a toggle on an existing Divar ad.
- **Multi-candidate negotiation with a single atomic lock** at agreement.
- **Approvals bound to a specific Proposal version**, so an edit invalidates stale approvals.
- **Ports and Adapters** for all peripheral services.
- **No money in the system.** No escrow, wallet, deposit or commission on the exchange; price differences are settled between the parties. This removes the PSP/escrow licensing problem at the cost of any payment guarantee.
- **Two delivery methods only:** locker (integrated) and in-person/courier (arranged by the parties).
- **Eligibility Config**: a global whitelist of categories and pilot neighborhoods that also drives what users can pick.

## Tech stack

**Used in the MVP**

- **Languages:** Go 1.27 (Ad, Matching), .NET 10 (Negotiation, Swap)
- **Contracts:** Protocol Buffers in `api/proto`, the single source of truth for gRPC, REST routes and Kafka event payloads
- **Sync communication:** gRPC between services; Envoy gateway at the edge (REST/JSON to gRPC transcoding, JWT validation)
- **Async communication:** Apache Kafka, domain events published through a transactional outbox, consumed idempotently
- **Storage:** PostgreSQL, one database per service, no cross-database access. Search in Matching is plain SQL
- **Local infrastructure:** Docker Compose (Postgres, Kafka, Envoy); the services run as host processes
- **Testing:** unit and integration tests per service, `tests/e2e` end-to-end tests, fault-injection scenarios (`make chaos`), k6 load tests (`tests/load`, `make load`) with recorded results, a narrated REST demo script

**Planned in the full design, not used in the MVP**

- Elasticsearch for search and match scoring, Redis for geo queries and sorting, Logstash and the rest of the observability stack
- GraphQL as a possible complement to REST, rate limiting and load balancing at the gateway
- `TBD:` locking beyond the single-node setup, real-time channel, per-service service-to-service authentication (mTLS is the production target; see the ADR drafts), deployment topology and scaling approach

## Quick start

Prerequisites: Docker (running), Git Bash on Windows, `make`, Go, the .NET SDK from `global.json`, `jq` and `curl` (the demo script), and [`grpcurl`](https://github.com/fullstorydev/grpcurl) for manual gRPC calls. `make setup` only checks for git, .NET and Go and installs `lefthook` and `golangci-lint`; it never installs the SDKs. Details and troubleshooting: [Get started](docs/get-started.md) and [Running locally](docs/guidelines/running-locally.md).

```bash
git clone <repo-url>
cd Taakht
make setup        # check prerequisites, install lint tools, register git hooks (once)
make dev          # start Postgres, Kafka, Envoy and the four services in the background
make demo         # narrated REST demo through Envoy (about 25 s)
make e2e          # end-to-end tests over gRPC (tests/e2e), needs make dev
make dev-stop     # stop the services (infrastructure keeps running)
make reset        # stop services and wipe all data, start a fresh infrastructure
```

Other targets: `make dev-status` (with a readiness column; health, `/metrics` and an optional Prometheus/Grafana profile are described in [Running locally](docs/guidelines/running-locally.md#health-logs-metrics)), `make scenario` (scripted gRPC scenario), `make fmt`, `make lint` (what CI runs), `make test` (all unit tests, Go with `-race`), `make down` (stop infrastructure and delete its data). `make chaos` (fault injection, about 10 minutes, stops and starts services and containers) and `make load` (k6, needs `k6`) need the stack to be otherwise unused.

Calling the API by hand through Envoy (`:8080`) with a dev token for a seeded user (`user-1` .. `user-4`):

```bash
TOKEN=$(cd tools/devtoken && go run . user-1)
curl -s -X POST localhost:8080/v1/ads \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"title":"Book","haveCategory":"books","wantCategories":["tools"],"neighborhoodIds":["n-valiasr"]}' | jq
curl -s localhost:8080/v1/ads -H "Authorization: Bearer $TOKEN" | jq   # the caller's own ads
```

New Ads are hidden until published (`POST /v1/ads/{id}:publish`). Routes are listed in [API conventions](docs/guidelines/api-conventions.md); the full flow is in the [demo walkthrough](docs/product/demo-walkthrough.md). There is no product UI: everything is driven through API calls against seeded data.

## Repository layout

```text
Taakht/
├── api/proto/       Protobuf contracts (services and Kafka events)
├── gen/go/          generated Go code (make proto)
├── src/
│   ├── ad/          Ad service (Go)
│   ├── matching/    Matching service (Go)
│   ├── negotiation/ Negotiation service (.NET 10)
│   └── swap/        Swap service (.NET 10)
├── libs/
│   ├── goplatform/  shared Go code: DB, identity, gRPC server, outbox, idempotent consumer
│   └── dotnet/      the same for .NET (Taakht.Platform)
├── gateway/         Envoy config and gRPC descriptor (REST/JSON to gRPC, JWT)
├── deploy/          docker-compose.yml (Postgres, Kafka, Envoy) and DB init
├── config/          eligibility.json (categories, neighborhoods)
├── scripts/         dev.sh (start/stop services), demo.sh (REST demo)
├── tests/e2e/       end-to-end tests, fault-injection scenarios and scenario runner (Go)
├── tests/load/      k6 load tests and recorded raw results
├── tools/devtoken/  dev JWT generator
├── third_party/     vendored proto dependencies (google/api annotations)
├── docs/            design documents (see Documentation below)
├── Makefile         setup, dev, demo, e2e, chaos, load, fmt, lint, test
├── lefthook.yml     git hooks (pre-commit fixes, Conventional Commits check)
└── .github/         CI
```

Each .NET service has its own solution file and each Go service its own module. The Makefile and CI discover them automatically, so adding a service needs no configuration change.

## Contributing

- Commit messages and pull request titles follow [Conventional Commits](https://www.conventionalcommits.org), e.g. `feat(reputation): add score projection`. PRs are squash-merged, so the title becomes the commit message.
- Git hooks fix formatting on staged files; anything they cannot fix mechanically (nullable warnings, unchecked errors, missing `CancellationToken`/`context.Context`) must be fixed by hand.
- CI runs `ci-ok` as the single required check: format/lint, build and tests per solution or module, with unrelated stacks skipped. The end-to-end job is informational.
- Every architecture decision is recorded as an English ADR with a single decision each, see [`docs/adr/`](docs/adr/README.md).
- All documentation and code identifiers are in English; only user-facing messages are localized to Persian.

## MVP status

What the one-week MVP contains, in more detail in [MVP architecture](docs/architecture/mvp-architecture.md) and [MVP plan](docs/product/mvp-plan.md):

- **Built and running:** the four services with a database each; gRPC between them (`GetAd`, `LockAds`); Kafka events through a transactional outbox with idempotent consumers; the atomic exclusive lock in the Ad service; versioned approvals; the lock saga with a locker-fee deadline and compensation; Envoy with JWT validation and REST transcoding; seeded users and a dev token tool.
- **Verified by:** unit and integration tests per service and library (Go: 10 packages with tests across `libs/goplatform`, `src/ad` and `src/matching`; .NET: Platform 114, Negotiation 80, Swap 65 tests), `tests/e2e` (happy path, payment timeout, lock race, locker eligibility; run against the live stack), `scripts/demo.sh` (a recorded run is in the demo walkthrough), 7 fault-injection scenarios that all passed ([chaos test results](docs/testing/chaos-test-results.md)) and k6 load tests ([load test results](docs/testing/load-test-results.md)).
- **Load test headline (measured 2026-10-09, one run per setting):** the read path (`browse.js`) held p95 under 25 ms with 0% errors up to 200 virtual users (1,540.2 req/s, p95 16.2 ms), with the knee around 400 VUs (2,389.0 req/s, p95 87.3 ms) and a peak of 2,578.2 req/s at 800 VUs (p95 274.1 ms). The full write path (`negotiate.js`, publish, negotiate, agree, lock, close) completed every iteration up to 40 VUs (438.3 req/s, `time_to_ads_closed` p95 1,564 ms on the first sweep). After the pool and overload fixes, 160 VUs completed 943 of 943 iterations, while at 320 VUs only 107 of 995 iterations saw `AGREED` within 30 s: the asynchronous chain is the limit under heavy load. Hot-ad contention: exactly 10 negotiations (the cap) and exactly one swap not rejected or cancelled in every run. Caveats: a single Windows laptop, k6 on the same machine as the system under test, services in Development mode, no tuning, single runs, and the post-fix runs were on a wiped stack, so they are not comparable one-to-one with the first sweep. These numbers say nothing about production scale.
- **Mocked or cut** (list in the plan): Communication, Reputation, KYC, Report, locker partner (mocked in Swap), pricing, hotspots, TTL expiry, item-condition claims, Elasticsearch, Redis, Logstash.
- **Not done:** fault-injection scenarios beyond the seven recorded (matching down, the `AGREEMENT_PENDING` republish sweeper, a Kafka outage longer than the consumer session timeout, a crash between the Kafka produce and `published_at`), containerized deployment of the services, a real identity provider, per-service service-to-service authentication, repair of a swap stuck in `LOCKING`, a replay tool for the `dead_letter` table.

## Status and known limitations

Known and intentional for the current phase:

- The platform does not hold or move money and does not guarantee that an agreed price difference is paid; this is between the parties.
- A higher KYC level for locker delivery is assumed and mocked.
- Courier and in-person hand-over are not tracked or insured by the platform.
- The platform does not arbitrate item-condition disputes; it only records and forwards claims.
- Informal competitors (Telegram/Instagram swap groups, global swap apps) have not been analyzed yet.
- The system runs on a single node (one Postgres instance, one Kafka node, services as host processes); there is no fault tolerance against the loss of a node or of infrastructure components.
- Services do not have a per-service identity in this phase: the `system:swap` / `system:negotiation` identities are accepted only together with one shared secret (`INTERNAL_AUTH_TOKEN`, public dev default `dev-internal-token`, no rotation, plaintext gRPC), and ordinary user ids are believed as sent in `x-user-id`. The internal network is isolated and only the gateway should be reachable by clients, so it must not be exposed beyond the gateway; the gateway uses a committed dev signing key. Production target is mutual TLS between services and a real identity provider.
- Performance numbers exist only from the k6 runs recorded in [load test results](docs/testing/load-test-results.md): one laptop, k6 on the same machine, Development mode, single runs. Nothing is extrapolated to production scale.

Weaknesses found in the code are listed under [Known gaps](docs/architecture/mvp-architecture.md#known-gaps) in the MVP architecture document.

### What would change in production

- Every service and infrastructure component runs with multiple replicas; the message broker and the durable stores are replicated.
- Matching is sharded (the approach is `TBD`) and backed by a real search engine.
- The mocked providers (KYC, locker partner) are replaced by real integrations.
- Performance numbers are only stated where a load test measured them.

## Documentation

[`docs/README.md`](docs/README.md) is the entry point. Where things live:

| Folder | Contents |
|---|---|
| [`docs/product/`](docs/product) | Business case; the [MVP plan](docs/product/mvp-plan.md) with its status and the [REST demo walkthrough](docs/product/demo-walkthrough.md) |
| [`docs/domain/`](docs/domain) | Ubiquitous language, Event Storming output, bounded contexts, context map (not written yet) |
| [`docs/architecture/`](docs/architecture) | [MVP architecture](docs/architecture/mvp-architecture.md) (current state, known gaps), [design principles](docs/architecture/design-principles.md) |
| [`docs/adr/`](docs/adr/README.md) | One architecture decision per file; drafts in `docs/adr/drafts/` |
| [`docs/evidence/`](docs/evidence) | Experiments and reviews behind ADRs |
| [`docs/guidelines/`](docs/guidelines) | [Running locally](docs/guidelines/running-locally.md), [API conventions](docs/guidelines/api-conventions.md), [MVP service conventions](docs/guidelines/mvp-service-conventions.md) |
| [`docs/api/`](docs/api) | API documentation and event contracts (not written yet; the contracts are in `api/proto`) |
| [`docs/testing/`](docs/testing) | Recorded [load test results](docs/testing/load-test-results.md) and [chaos (fault-injection) results](docs/testing/chaos-test-results.md); a separate test strategy is not written |
| [`docs/process/`](docs/process) | How the team works (not written yet) |
| [`docs/get-started.md`](docs/get-started.md) | Developer onboarding |
| [`docs/open-items.md`](docs/open-items.md) | Undecided questions that feed new ADRs |

When the architecture documents and the plan disagree, the architecture document wins: it is derived from the code. Some folders are still empty.
