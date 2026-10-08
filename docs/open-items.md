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
