# XXXX. Use a single monorepo for all services

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): cross-cutting

## Context

The system is a set of services written in Go and .NET that share one API and event contract (Protobuf, see the API contract ADR). The team is two people. The repository already assumes a single repo: the Makefile discovers every `*.sln` and `go.mod`, and CI uses path filters, per-solution and per-module jobs and a single required check (`ci-ok`) so unrelated stacks are skipped.

## Decision

We will keep all services, the shared contracts (`api/`), the gateway configuration and the documentation in one repository. Each service is its own .NET solution or Go module, built and released independently. Services share only contracts, never code.

## Options considered

1. **Single monorepo (chosen)** - one change can update the contract and every affected service in one pull request (the spike generated a Go and a .NET service from one `.proto`); one CI, one set of conventions and tooling; least coordination for a two-person team. Cost: nothing in the repo structure enforces service independence, so it takes discipline; a broken commit is visible to everyone; CI time can grow with the number of services.
2. **One repository per service** - strong boundaries and independent permissions and release cadence, but every contract change becomes several coordinated pull requests, CI and tooling are duplicated, and shared configuration drifts. No benefit at the current team size.
3. **Monorepo for services with a separate repository for contracts** - decouples contract versions from code, but turns each contract change into two pull requests plus a version bump, and was already rejected for `api/` (see the API contract ADR).
4. **Monorepo with a build orchestration tool (Nx, Bazel)** - faster incremental builds at scale, but heavy to adopt and not needed while the native tooling and path filters suffice. Not evaluated beyond this reasoning; the other alternatives above were also argued, not tried.

## Consequences

- Positive: atomic cross-service changes; one CI definition; simple onboarding.
- Negative / trade-offs we accept: independence between services depends on rules we must keep (below); no per-service access control.
- Rules that go with this decision:
  - Services depend on each other only through contracts in `api/`, never by importing another service's code.
  - Each service has its own solution or module, test project and release pipeline.
  - CI keeps path filters and `ci-ok` as the single required check.
- Revisit when any of these happens: more than one team owns the code; services need very different release cadence or access control; CI time for a typical change becomes unacceptable.

## Known gaps

- CI has never run on a real service: the repository has no `.sln` or `go.mod` yet, so service discovery, the path filters and the `ci-ok` skip logic are untested in practice. This decision relies on them.
- The rule "services share only contracts" is not enforced by any tool; it relies on review.

## References

- [Define the API contract in Protobuf](define-api-contract-in-protobuf.md)
- README, "Repository layout"; `Makefile`; `.github/workflows/ci.yml`
