# Open items

Decisions that are not yet recorded as ADRs, in the order we work through them. Rule: an item stays here until it is decided; only then does it become an ADR (see [ADR rules](adr/README.md)). ADR numbers are given in the order ADRs are accepted, not by the position in this list.

## How each item is handled

1. Discuss the question and the real options with the owner.
2. Gather evidence for the options (documentation, spike where it matters) and note what remains unverified.
3. The owner confirms the decision.
4. Write the ADR (rejected options and reasons included), give it the next number, add it to the [ADR index](adr/README.md), and remove or mark the item here.

## Status values

- **Open** - not decided.
- **Stack named** - the README names the choice, but the reasoning and rejected options are not recorded.
- **Draft** - a draft ADR exists in `adr/drafts/`; spike-verified drafts say so inside.
- **Done** - ADR accepted (link it).

## Order

| # | Topic | Status | Draft or note | Depends on |
|---|---|---|---|---|
| **Foundation** | | | | |
| 1 | Monorepo | Stack named | README, Makefile and CI already assume it | - |
| 2 | Microservice architecture | Draft | [use-microservices](adr/drafts/use-microservices.md); reasoning from published guidance, not an experiment; three drivers (product, demonstration, team and learning) | 1 |
| 3 | Service decomposition | Open | Working boundaries: Ad and Matching (Go), Deal = negotiation + settlement and Reputation (.NET); Communication and Delivery minimal or mocked. Needs the context map to confirm | 2 |
| 4 | Languages: Go and .NET, and the rule that splits services between them | Draft | [split-services-between-go-and-dotnet](adr/drafts/split-services-between-go-and-dotnet.md); no benchmark, rests on workload fit and learning | 3 |
| **Communication** | | | | |
| 5 | Sync versus async rule (gRPC for requests, events for state changes) | Stack named | README | 3 |
| 6 | Kafka | Stack named | README; project memory | 5 |
| 7 | API contract in Protobuf | Draft | [define-api-contract-in-protobuf](adr/drafts/define-api-contract-in-protobuf.md); spike-verified (Go and .NET); the OpenAPI-first alternative was argued, not tried | 1, 5 |
| 8 | Kafka event format (Protobuf, no registry for now) | Draft | [describe-kafka-events-in-protobuf](adr/drafts/describe-kafka-events-in-protobuf.md); spike-verified; registry options compared from documentation only | 6, 7 |
| 9 | Transactional outbox | Stack named | README | 6, 12 |
| 10 | Consistency and cross-service workflows (saga or process manager, idempotent consumers, the exclusive lock) | Open | README lists locking as TBD | 5, 6, 9 |
| **Data** | | | | |
| 11 | Data ownership and local read models (for example Matching's copy of Reputation) | Open | | 3, 10 |
| 12 | PostgreSQL | Stack named | README | 11 |
| 13 | Event sourcing in Reputation (including upcasting) | Stack named | README; project memory | 8, 12 |
| 14 | Search (Elasticsearch for search and match scoring) | Stack named | README | 3 |
| 15 | Geo index (Redis for geo queries and sorting) | Stack named | README | 14 |
| 16 | Matching sharding and scaling | Open | README: approach TBD, production concern | 14, 15 |
| **Edge and security** | | | | |
| 17 | Edge API style (REST at the edge, GraphQL as a possible complement) | Open | README: GraphQL being evaluated | 7 |
| 18 | Envoy as edge gateway and REST to gRPC transcoding (rate limiting as a section) | Draft | [use-envoy-for-rest-grpc-transcoding](adr/drafts/use-envoy-for-rest-grpc-transcoding.md); spike-verified; grpc-gateway compared in a spike | 7, 17 |
| 19 | JWT validation at the edge | Draft | [validate-jwt-at-the-edge](adr/drafts/validate-jwt-at-the-edge.md); spike-verified | 18 |
| 20 | Service-to-service authentication | Draft | [authenticate-service-to-service-calls](adr/drafts/authenticate-service-to-service-calls.md); reasoning only, no spike; may merge with 19 | 19 |
| 21 | Real-time channel (chat, notifications) | Open | README: TBD | 5 |
| **Operations** | | | | |
| 22 | Observability stack | Open | README: only Logstash named, rest TBD | - |
| 23 | Deployment topology and scaling | Open | README: open | 1, 2 |

## Not ADRs (go elsewhere)

| Topic | Where |
|---|---|
| Internal service structure (Clean Architecture, ports and adapters, CQS) | Guidelines |
| Identifier strategy | Guidelines (promote to an ADR if it turns out to be expensive to change) |
| Config and secrets handling | Guidelines |
| Retry, timeout and DLQ patterns | Guidelines |
| API versioning, errors, pagination, caller identity | [API conventions](guidelines/api-conventions.md) (draft) |
| External providers behind ports with mocks | README, "Key design decisions" |
| Privacy of location and user data | Product or domain docs |
| Media storage | Not in the README scope yet; add an item if images become part of it |
| Load and stress test tooling | Testing docs |
| Commit, PR and CI conventions | Process docs |

## Resolved or narrowed by the MVP build

The one-week MVP ([plan](product/mvp-plan.md), [architecture as built](architecture/mvp-architecture.md)) answered or narrowed these items in code. The ADRs below are drafts (status Proposed); the items stay in the table above until an ADR is accepted.

| Item | What the MVP answered | Left open | Where |
|---|---|---|---|
| 3 Service decomposition | Four services were built: Ad, Matching (Go), Negotiation, Swap (.NET). Negotiation and settlement are separate services; Reputation and Communication are not built | Confirmation against the context map; whether Negotiation and Swap should merge | [keep-negotiation-and-swap-as-separate-services](adr/drafts/keep-negotiation-and-swap-as-separate-services.md) |
| 9 Transactional outbox | Outbox table written in the business transaction plus a polling relay per service; at-least-once; `clock_timestamp()` keeps the order of events within a transaction | CDC (Debezium), pruning, several relay instances | [use-a-transactional-outbox-for-events](adr/drafts/use-a-transactional-outbox-for-events.md) |
| 10 Consistency and workflows | The exclusive lock is claimed in the Ad service in one transaction, idempotent by `swap_id`, driven by a saga in Swap. Consumers are idempotent through a processed-events table plus state checks | Timeouts and repair for a negotiation stuck in `AGREEMENT_PENDING`; DLQ | [take-the-exclusive-ad-lock-in-the-ad-service](adr/drafts/take-the-exclusive-ad-lock-in-the-ad-service.md), [make-consumers-idempotent-with-a-processed-events-table](adr/drafts/make-consumers-idempotent-with-a-processed-events-table.md) |
| Approval validity | Approvals are bound to ad versions and Proposal numbers; staleness is computed by comparison; the final approval re-checks versions synchronously with Ad, and `LockAds` is the authority | Showing staleness in the REST contract | [bind-approvals-to-versions](adr/drafts/bind-approvals-to-versions.md) |
| Consistency between lock and index (part of 10, 11) | Eventual, through events: Matching removes an ad when `AdLocked` arrives and re-adds it on `AdReleased`. Negotiation keeps a local copy of ad versions (`ad_ref`) fed by events and refreshed by synchronous `GetAd` | A locked ad can briefly appear in search results; the effect is only a refused request later | [MVP architecture](architecture/mvp-architecture.md) |
| 14 Search (partly) | For the MVP, search is plain SQL on a Postgres read model in Matching, scored in Go; no Elasticsearch | Whether Elasticsearch is needed at scale (not measured) | [MVP architecture](architecture/mvp-architecture.md), "Differences from the full design" |
| 11 Data ownership (partly) | Each service owns its database; copies are event-fed read models (`ad_index`, `ad_ref`) | Matching's copy of Reputation (no Reputation service yet) | [MVP architecture](architecture/mvp-architecture.md), "Data ownership" |
