# Design principles

## Design for scale, run on one node: choose complexity by reversibility

The system is designed for the scale the business needs, but runs as a single-node demo. To decide how much complexity each choice deserves, ask: **how expensive is it to change this later?**

| Kind | Examples | Rule |
|---|---|---|
| **One-way door** (expensive to reverse) | Service boundaries, data ownership, API and event contracts (Protobuf), Kafka and async communication, idempotency, event sourcing in Reputation | Design for scale from the start, even though it runs on one node |
| **Two-way door** (cheap to reverse) | Gateway product, UI for API docs, replicas and HA, deployment topology, where JWTs are verified | Take the simplest option that does not lock us in; upgrade when a measurement says so |
| **The swap domain itself** | Matching and its geo index, the atomic lock | Build for real (see the README rule of thumb) |

Complexity enters only if at least one is true:

1. It is a one-way door and would be costly to retrofit.
2. It is needed to measure a scale claim (for example replicas for a load test).
3. It is part of the swap domain itself.

Otherwise the dependency is mocked, or the simplest option is used and the question stays in `open-items.md`.

Scale is claimed only where a test measured it. Every non-trivial choice is recorded as an ADR with the rejected options and the reason.
