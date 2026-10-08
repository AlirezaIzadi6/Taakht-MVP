# Evidence: microservices versus monolith

Date: 2026-10-08. Backs the ADR "Build the system as microservices". This is a document review only; nothing was built or measured.

## Sources and findings

- Martin Fowler, "Monolith First": do not start a new project with microservices, even one expected to grow. Reasons: uncertain value and the need for speed; the microservice premium; service boundaries are hard to get right early and refactoring across services is much harder than inside a monolith. Two routes to services: peel services off gradually, or replace the monolith. A third option: start with a few coarse services and split them as boundaries stabilise. Caveats: not every monolith can be decomposed, a modular monolith needs real discipline, and the author notes the evidence is thin. The opposing view: starting with microservices teaches the working rhythm early and suits cases where boundaries are predictable, though the author still advises against it without experience.
- Martin Fowler, "Microservice Premium": microservices add cost and risk (automated deployment, monitoring, failure handling, eventual consistency). They pay off only when the system is too complex to manage as a monolith; drivers include large teams, business functions that evolve independently, and scaling needs.
- Fowler and Lewis, "Microservices": benefits are independent deployment and scaling, firm module boundaries, and per-service technology choice. Costs are consistency (eventual consistency, compensation), remote-call cost, operational complexity, and boundaries that are hard to get right and to refactor. Services that always change together should probably be merged; limit synchronous call chains.

## How this applies

The published advice favours a modular monolith for a two-person team with unvalidated boundaries. The ADR chooses microservices anyway, for two stated drivers: an expected scaling need that is not measured, and the project's goal of demonstrating distributed-systems problems. The ADR records the cost and the risk instead of arguing the advice away.

## Not verified

No prototype of the operational overhead (CI per service, observability, deployment), no measurement of scaling needs, and the service boundaries themselves.
