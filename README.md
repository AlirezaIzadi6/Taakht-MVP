# Taakht

**A goods-swap layer for a classifieds platform (designed around Divar), built as a microservices portfolio project.**

> **Status: design phase (draft).** The architecture is documented and the key decisions are recorded; implementation has not started. Sections marked `TBD` depend on decisions that are still open. See [Status and known limitations](#status-and-known-limitations).

## The idea

Many people hold small, low-value items they no longer use. Selling them for cash is unattractive (high friction, low price, no trust mechanism), and informal swap groups have no matching or safety net.

Taakht lets users declare what they have and what they want, matches them with each other (also while they are away from the app), and walks both sides through negotiation and delivery. Money never goes through the platform: any price difference is settled directly between the two parties.

Honest framing: this is a **calculated bet for a Growth/Experiments team**, not a claim about the future of the platform. It deliberately targets low-value goods and takes no cut of the exchange (the only revenue idea is paid matching priority), so direct revenue is capped; the case rests on secondary metrics (engagement and reactivation of idle inventory).

## How it works

1. **Browse** anonymously by category and neighborhood. Browsing is a stateless query; nothing is stored and requests cannot be sent.
2. **Publish an Ad** to become discoverable: at least one filter plus an explicit "show me to others" opt-in. An Ad is persistent, versioned, may be partially specified on both sides ("have" / "want"), and stays active in the matching engine after restarts. It can be hidden at any time; it is never deleted, and once a deal is agreed on it, the Ad is closed for good (still not deleted). The user gets a notification when a match appears.
3. **Request and negotiate.** A Swap Request opens a **Negotiation** right away, with a chat and a versioned **Proposal** (items, delivery method, any agreed price difference, locker cost split). Each of your Ads can have a limited number of open negotiations at a time (configurable, 5 as the example default); settle one before starting another beyond the limit. Every edit is a new Proposal version the other side must approve.
4. **Pin down the terms (optional).** Naming exact items is optional; the parties know what they are trading. To record exactly what was agreed, either side can edit their Ad, and the agreement is stored against that Ad version. Approvals are bound to the version of the terms they were given on.
5. **Lock.** When both sides approve the same Proposal, an agreement is reached, a **Swap** is created, an atomic exclusive lock is taken, competing requests are cancelled and their users notified, and the Ad leaves the index.
6. **Complete the swap.** The platform never touches money: any price difference is paid directly between the two parties, outside the system. The parties pick one of two delivery methods (a **locker**, or **in person / courier** arranged by themselves) and are responsible for checking the goods themselves: there is no post-delivery confirmation checklist, only the item-condition claim each side records beforehand.

## Architecture at a glance

A microservices system designed for the scale it would need to operate at (barter liquidity only exists on a large platform), run as a single-node demo. The distributed-systems problems (partial failure, eventual consistency, ordering, idempotency) are real in this demo, because they come from process and data boundaries, not from the number of machines. Scale itself is claimed only where it is measured.

Services are classified by how deep the implementation goes:

| Service | Responsibility | Class | Implementation |
|---|---|---|---|
| **Ad** | Owns Ads: creation, versioned edits, publish/hide, consumption by a Swap | Core | Full |
| **Matching** | Geo/category/value/reputation-aware candidate discovery (Criteria against the Ad index); two-sided sync and async matching | Core | Full |
| **Negotiation** | Concurrent Negotiations with versioned Proposals and chat, state machine up to agreement | Core | Full |
| **Settlement** | Exclusive lock, item-condition claims, tracking of both sides' obligations up to the final outcome of a Swap (no money involved) | Core | Full |
| **Communication** | Trade-scoped chat, system notifications (delivery mostly mocked), notifications to users watching an Ad | Supporting | Minimal |
| **Reputation** | Event-sourced trust score (recomputable when the formula changes), anti-collusion limits; Matching keeps a local copy | Supporting | Minimal |
| **Delivery Orchestration** | Locker delivery tracking, claim forwarding (no content arbitration); in-person/courier hand-over is arranged by the parties and not tracked | Supporting | Minimal |
| **Provider adapters** | Everything outside the swap domain (see below) | Generic | Mock |

### What is real and what is mocked

Every dependency outside the swap domain sits behind a small interface with a mock implementation. Swapping in a real implementation later is a wiring change, not a change to the core services.

| Interface | Phase 1 (this project) |
|---|---|
| `AuthProvider` | Issues a JWT for seeded users, no real password check |
| `AdRegistryProvider` | Seeded fake ads/users and a small fixed category/neighborhood taxonomy |
| `KYCProvider` | Always passes |
| `DeliveryProvider` | Simulated locker statuses (the only delivery method integrated with a partner) |
| `ReportProvider` | Records an abuse report (any user can be reported, not only a trade counterpart), no review workflow |

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

- **Languages:** Go 1.27 (high-load services), .NET 10
- **Async communication:** Apache Kafka (domain events published through a transactional outbox)
- **Sync communication:** gRPC between services, REST/JSON at the edge behind an API Gateway (load balancing, rate limiting, JWT validation); GraphQL is being evaluated as a complement
- **Storage:** PostgreSQL as the durable store, Elasticsearch for search and match scoring, Redis for geo queries and sorting
- **Observability:** Logstash (rest of the stack TBD)
- **Testing:** unit, integration and end-to-end tests; scripted load and stress tests kept in the repo, with results recorded

`TBD:` locking, real-time channel, service-to-service authentication (API keys proposed), the rest of the observability stack and load-testing tooling. Deployment topology and scaling approach are open as well.

## Quick start

### Developer setup

The repository is a monorepo of .NET 10 and Go 1.27 services. You need git, the .NET SDK and Go installed (`make setup` only checks for them, it never installs them).

```bash
git clone <repo-url>
cd Taakht
make setup
```

`make setup` verifies the prerequisites, installs `lefthook` and `golangci-lint` if missing, and registers the git hooks. Everyday commands:

| Command | What it does |
|---|---|
| `make fmt` | Auto-fixes formatting and style for .NET and Go |
| `make lint` | Checks everything without fixing (exactly what CI runs) |
| `make test` | Runs all .NET and Go tests (Go with `-race`) |

On Windows, run `make` from Git Bash. Prerequisites, hooks, CI and troubleshooting are in [Get started](docs/get-started.md).

### Running the demo

The demo is **API-driven**: no product UI (at most a minimal UI for testing), everything is exercised through API calls against seeded data.

Start the whole system (Postgres, Kafka and the four services) with one command; users `user-1`..`user-4` and the categories come from `config/eligibility.json`:

```bash
make dev          # start infra + all services in the background
make scenario     # scripted end-to-end walkthrough (make e2e runs the tests)
make dev-stop     # stop the services; make reset wipes all data
```

Prerequisites, ports, log locations, `grpcurl` examples and troubleshooting are in [Running locally](docs/guidelines/running-locally.md).

Then follow a scripted walkthrough of API calls: log in as a seeded user, publish an Ad, watch a match notification, send competing Swap Requests, negotiate Proposals, approve, lock, settle.

## Repository layout

```text
Taakht/
├── docs/            design documents (see Documentation below)
├── src/             one solution (.NET) or module (Go) per service
├── gateway/         Envoy edge gateway config (REST/JSON to gRPC)
├── Makefile         setup, fmt, lint, test
├── lefthook.yml     git hooks (pre-commit fixes, Conventional Commits check)
└── .github/         CI
```

Each .NET service has its own solution file and each Go service its own module. The Makefile and CI discover them automatically, so adding a service needs no configuration change.

## Contributing

- Commit messages and pull request titles follow [Conventional Commits](https://www.conventionalcommits.org), e.g. `feat(reputation): add score projection`. PRs are squash-merged, so the title becomes the commit message.
- Git hooks fix formatting on staged files; anything they cannot fix mechanically (nullable warnings, unchecked errors, missing `CancellationToken`/`context.Context`) must be fixed by hand.
- CI runs `ci-ok` as the single required check: format/lint, build and tests per solution or module, with unrelated stacks skipped.
- Every architecture decision is recorded as an English ADR with a single decision each, see [`docs/adr/`](docs/adr/README.md).
- All documentation and code identifiers are in English; only user-facing messages are localized to Persian.

## Status and known limitations

Known and intentional for the current phase:

- The platform does not hold or move money and does not guarantee that an agreed price difference is paid; this is between the parties.
- A higher KYC level for locker delivery is assumed and mocked.
- Courier and in-person hand-over are not tracked or insured by the platform.
- The platform does not arbitrate item-condition disputes; it only records and forwards claims.
- Informal competitors (Telegram/Instagram swap groups, global swap apps) have not been analyzed yet.
- The system runs on a single node; there is no fault tolerance against the loss of a node or of infrastructure components.
- Services do not authenticate each other in this phase: the internal network is isolated and only the gateway publishes a port, so it must not be exposed beyond the gateway. Production target is mutual TLS between services.

### What would change in production

- Every service and infrastructure component runs with multiple replicas; the message broker and the durable stores are replicated.
- Matching is sharded (the approach is `TBD`).
- The mocked providers (KYC, locker partner) are replaced by real integrations.
- Performance numbers are only stated where a load test measured them.

## Documentation

[`docs/README.md`](docs/README.md) is the entry point. Where things live:

| Folder | Contents |
|---|---|
| [`docs/product/`](docs/product) | Business case: business summary, business decisions log; later PRD, risk assessment, roadmap |
| [`docs/domain/`](docs/domain) | Ubiquitous language, Event Storming output, bounded contexts, context map |
| [`docs/architecture/`](docs/architecture) | Architecture overview (current state), chronological decisions log, diagrams |
| [`docs/adr/`](docs/adr/README.md) | One architecture decision per file, with the reasoning |
| [`docs/guidelines/`](docs/guidelines) | Conventions that follow from decisions (topic naming, retry/DLQ, code style) |
| [`docs/api/`](docs/api) | API documentation and event contracts |
| [`docs/testing/`](docs/testing) | Test strategy, load/stress scenarios and recorded results |
| [`docs/process/`](docs/process) | How the team works |
| [`docs/open-items.md`](docs/open-items.md) | Undecided questions that feed new ADRs |

When the architecture overview and the decisions log disagree, the overview wins. Some folders are still empty while the design is being migrated into the repository.
