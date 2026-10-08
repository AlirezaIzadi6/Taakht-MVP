# XXXX. Use Envoy as the edge gateway for REST to gRPC transcoding

- Status: Proposed (draft, not yet numbered)
- Date: 2026-10-05
- Deciders: TBD
- Bounded context(s): cross-cutting

## Context

External clients speak REST/JSON; services speak gRPC (.NET and Go). The README places a gateway at the edge for load balancing, rate limiting and JWT validation. We must choose what translates REST to gRPC.

What matters: the REST mapping comes from the Protobuf contract (`google.api.http`) and not from a second configuration; it works for both languages; JWT and rate limiting are covered; and replacing it later stays cheap (see [design principles](../../architecture/design-principles.md)).

## Decision

We will run Envoy as the single public entry point and use its `grpc_json_transcoder` filter, driven by a descriptor built from the shared `.proto` files. JWT validation and rate limiting are configured in the same Envoy. Only RPCs with an HTTP annotation are public (`auto_mapping: false`). The configuration lives in `gateway/envoy.yaml`.

## Options considered

1. **Envoy `grpc_json_transcoder` (preferred; behaviour verified in a spike, see below)** - one translation point for every service language. JWT validation (`jwt_authn`) and rate limiting (`local_ratelimit`) are configuration, not code we write. Cost: a 175 MB image, YAML to maintain, and a descriptor change needs an Envoy restart or xDS.
2. **Transcoding inside each service** (ASP.NET Core gRPC JSON transcoding) - runs in-process with no extra hop, but covers .NET only, so Go services would need a different mechanism. Microsoft's docs limit it to server streaming, and the `Grpc.Swagger` OpenAPI package is deprecated with no replacement.
3. **Standalone grpc-gateway proxy (Go)** - uses the same `google.api.http` annotations, so switching later is cheap, and the image is small (27 MB in the spike). It transcodes correctly, but JWT validation, rate limiting and claim forwarding are code we write and own. In the spike, 73 hand-written lines already contained a bug (the limiter ran before JWT validation, so rejected requests used up the quota). A proto change means regenerate, rebuild and redeploy.
4. **Other API gateways** - APISIX's `grpc-transcode` plugin does not use `google.api.http` annotations (the HTTP mapping is configured per route), which would create a second source of truth next to the proto. Kong's docs did not say whether it uses annotations; not evaluated further.
5. **Connect** - the docs we read list no .NET support, so it cannot serve both stacks.

## Consequences

- Positive: one place for translation and edge concerns; services stay pure gRPC; the gateway can be replaced cheaply.
- Negative / trade-offs we accept: Envoy configuration must be learned; changing the descriptor drops in-flight requests unless we adopt xDS; the transcoder does not validate field values, so validation belongs in services.
- Follow-ups:
  - Build `descriptor.binpb` and `openapi.yaml` from the same commit in CI.
  - [Validate JWTs at the Envoy edge](validate-jwt-at-the-edge.md).
  - Decide the gateway's deployment topology (open in the README).

## Evidence

Transcoding, error mapping, JWT and rate limiting worked in a spike; one setting (`match_incoming_request_route: true`) is mandatory. grpc-gateway was compared in the same spike. Details: [evidence](../../evidence/edge-gateway.md).

## References

- [Define the API contract in Protobuf](define-api-contract-in-protobuf.md)
- Envoy gRPC-JSON transcoder: https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/grpc_json_transcoder_filter
- ASP.NET Core gRPC JSON transcoding: https://learn.microsoft.com/en-us/aspnet/core/grpc/json-transcoding
- grpc-gateway: https://github.com/grpc-ecosystem/grpc-gateway
