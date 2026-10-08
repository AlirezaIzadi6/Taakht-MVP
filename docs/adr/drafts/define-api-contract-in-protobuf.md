# XXXX. Define the API contract in Protobuf

- Status: Proposed (draft, not yet numbered; alternatives evaluated by reasoning, the chosen option verified in a spike)
- Date: 2026-10-05
- Deciders: TBD
- Bounded context(s): cross-cutting

## Context

Services are written in .NET and Go and talk to each other over gRPC. Clients reach the system through a REST/JSON edge (see the README, Technology section). We need one place that defines every API so that:

- Go and .NET services cannot drift apart on message shapes.
- The REST surface and its documentation cannot drift from what the services actually accept.
- Breaking changes are caught before they ship.

## Decision

We will define every service API in `.proto` files in a shared `api/` directory of this monorepo and treat them as the single source of truth. REST routes are declared in the same files with `google.api.http` annotations. Server code, the gateway descriptor and the OpenAPI document are all generated from these files, and `buf lint` and `buf breaking` run in CI.

## Options considered

1. **Protobuf as the source of truth (preferred; verified in a spike)** - one contract for both languages; REST mapping, docs and code are generated from it; `buf breaking` gives automatic compatibility checks. Cost: contributors must learn Protobuf and the `buf` toolchain, and generated OpenAPI is less hand-tunable.
2. **Code-first per language** (Huma/swaggo in Go, Swashbuckle or built-in OpenAPI in .NET) - familiar tooling, but the services speak gRPC, so there is no REST handler to generate a spec from, and two languages would produce two independently drifting descriptions.
3. **A separate repository or the Buf Schema Registry for the protos** - decouples the contract from code, but every change becomes two PRs with version coordination, and the registry adds an external dependency. Not needed while everything lives in one monorepo.
4. **OpenAPI-first, REST as the contract** - good for external consumers, but gRPC between services would still need its own contract, giving two sources of truth.

## Consequences

- Positive: one reviewed contract; generated, always-current docs; compatibility enforced mechanically in CI.
- Negative / trade-offs we accept: Protobuf and `buf` become required knowledge; REST-specific details (file upload, custom status codes) are limited by what the gateway can express.
- Follow-ups:
  - Create `api/` with `buf.yaml` and `buf.gen.yaml`.
  - Add `api/` to the CI path filters.
  - Vendor `google/api/annotations.proto` and `http.proto` for .NET and add them as `Protobuf` items (see evidence).
  - Versioning and error-model rules live in the [API conventions](../../guidelines/api-conventions.md) draft, not in an ADR.
  - Kafka events are covered by [Describe Kafka events in Protobuf](describe-kafka-events-in-protobuf.md).

## Evidence

One proto produced a Go service, a .NET service and the Envoy descriptor, and `buf breaking` rejected incompatible changes. The .NET build needs the Google API protos compiled explicitly and was only run on .NET 9. Details: [evidence](../../evidence/api-contract-protobuf.md).

## References

- [Use Envoy for REST to gRPC transcoding](use-envoy-for-rest-grpc-transcoding.md)
- README, Technology section
