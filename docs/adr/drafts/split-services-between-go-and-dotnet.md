# XXXX. Split services between Go and .NET by workload

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): cross-cutting

## Context

The README names Go 1.27 (high-load services) and .NET 10 (the rest). Two people work on the project, one in each language, and learning both stacks in a distributed setting is a goal. Service boundaries are also ownership boundaries, so the language rule has to be simple and have a stated reason.

## Decision

We will use Go for services whose main work is concurrency and throughput, and .NET for services whose main work is rich domain state. Working assignment: Go for Matching and Ad, .NET for Deal (negotiation and settlement) and Reputation. Services share only the Protobuf contracts in `api/`, never code (see the monorepo ADR).

## Options considered

1. **Split by workload (chosen)** - Matching (scanning and scoring candidates, geo queries) and Ad (the contended exclusive reservation) are concurrency-heavy; Deal (versioned proposals, approvals, obligations) and Reputation (event-sourced projections) are dominated by state machines and domain rules. Each person owns two services. Cost: two toolchains, two CI stacks, and common infrastructure code (interceptors, clients) written twice.
2. **Everything in .NET** - one stack, simplest to run and review, but gives up the second language the team wants to learn and the README's Go choice for high-load services.
3. **Everything in Go** - one stack, but the same trade-off in the other direction and a weaker fit for state-machine-heavy domain code.
4. **Choose per service by preference, with no stated rule** - maximum freedom, but the reasoning would be invisible to a reader.

## Consequences

- Positive: each language is used where its model fits; parallel ownership; the contract between the two people is the Protobuf API.
- Negative / trade-offs we accept: duplicated tooling and infrastructure libraries; two sets of dependencies and security updates; cross-language debugging.
- Revisit when: a service's workload changes class, ownership changes, or running two stacks costs more than it teaches.

## Known gaps

- No benchmark shows that Go beats .NET for Matching or Ad. Correctness of the exclusive lock does not depend on the language (it rests on database constraints); the split rests on workload fit and on the learning goal.
- The .NET 10 toolchain is unverified: the development machine had only SDK 8 and 9 and `global.json` asks for 10.0.400; the official SDK 10 image failed in a test (see the contract evidence).

## Evidence

Both languages produced services from one proto behind one gateway in a spike. Details: [evidence](../../evidence/api-contract-protobuf.md).

## References

- [Use a single monorepo for all services](use-a-monorepo.md)
- [Build the system as microservices](use-microservices.md)
- [Define the API contract in Protobuf](define-api-contract-in-protobuf.md)
