# XXXX. Build the system as microservices

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): cross-cutting

## Context

The README describes a system of independently deployable services, designed for the scale the business needs and run as a single-node demo. The team is two people, each working in a different language (Go and .NET), and the project has to move fast.

Three drivers, kept separate on purpose:

- **Product:** matching quality and liquidity only exist on a large platform, so parts of the system (matching, search) are expected to scale and change differently from the rest. This is an expectation, not a measurement.
- **Demonstration:** the project is a portfolio piece meant to show how concurrency and eventual consistency are handled; those problems come from process and data boundaries, so the boundaries are part of the point.
- **Team and learning:** two people work in two languages, so a service boundary is also an ownership boundary, with the Protobuf contract between them. Learning distributed systems in practice is an explicit goal.

## Decision

We will build the system as independently deployable services, each owning its data and communicating over gRPC and Kafka events. The working boundaries are Ad and Matching (Go), Deal (negotiation and settlement together) and Reputation (.NET); Communication and Delivery Orchestration start as minimal or mocked services. These boundaries are confirmed in the service decomposition decision before each service is implemented.

## Options considered

1. **Microservices from the start (chosen)** - serves all three drivers; boundaries are enforced by processes, not by discipline; the two people can work in parallel. Cost: the "microservice premium" (automated deployment, monitoring, eventual consistency, failure handling) lands on two people, and a wrong boundary is expensive to move.
2. **Modular monolith with enforced module boundaries** - the lowest-risk and fastest start, and the common advice for new systems; services can be extracted later when measurements justify it. Rejected because it does not demonstrate the distributed problems the project is meant to show, and because a one-language deployable does not fit two people working in two languages.
3. **A few coarse services, split further as boundaries stabilise** - a middle path. The chosen option is already coarse (four services instead of the README's eight rows); going coarser would remove the Ad, Deal and Matching boundaries where the concurrency and consistency problems appear.

## Consequences

- Positive: independent deployment and scaling per service; firm boundaries; real concurrency and consistency problems; parallel work across the two languages.
- Negative / trade-offs we accept: slower start; operational overhead (CI per service and per stack, observability, deployment) for two people; cross-service changes need coordinated contracts; a wrong boundary means moving code between services, possibly across languages.
- Revisit when: services are repeatedly changed together (merge them); velocity is unacceptable; or measurements show no service needs independent scaling and the demonstration goal has changed.

## Known gaps

- No measurement shows that any service needs independent scaling; the product driver is an expectation.
- Service boundaries are not yet validated against the domain (open decision), which is the main risk of this option.

## Evidence

Reasoning from published guidance (Monolith First, Microservice Premium, the microservices article), not an experiment. The Go and .NET services were built from one contract behind one gateway in a spike. Details: [microservices evidence](../../evidence/microservices-vs-monolith.md), [contract evidence](../../evidence/api-contract-protobuf.md).

## References

- [Design principles](../../architecture/design-principles.md)
- [Use a single monorepo for all services](use-a-monorepo.md)
- [Split services between Go and .NET](split-services-between-go-and-dotnet.md)
- README, "Architecture at a glance"
