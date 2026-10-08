# Evidence: edge gateway (Envoy, transcoding, JWT)

Dates: 2026-10-05 and 2026-10-06. Backs the ADRs "Use Envoy for REST to gRPC transcoding" and "Validate JWTs at the Envoy edge".

## Setup

Envoy v1.34, a Go gRPC service (grpc-go), `buf` for the descriptor and code generation, nginx with Scalar for the docs, an HS256 test token with a throwaway secret. A second variant replaced Envoy with a Go grpc-gateway proxy plus hand-written JWT and rate-limit middleware (73 lines).

## Envoy results

| Check | Result |
|---|---|
| GET with path parameter, POST with JSON body, from `google.api.http` only | Works |
| gRPC errors | NotFound gives 404 and InvalidArgument gives 400, with a JSON body (`convert_grpc_status`) |
| No token or expired token | 401 before the service |
| Claims to the service | Payload as base64url metadata (`forward_payload_header`); `claim_to_headers` (`sub` to `x-user-id`) also works |
| `local_ratelimit` per route | 429 after the bucket was empty |
| RPC without an HTTP annotation | Unreachable (no REST path; the native gRPC path has no route) |
| OpenAPI from the annotations | Both plugins work: gnostic `protoc-gen-openapi` gives 3.0.3, grpc-gateway `openapiv2` gives Swagger 2.0; unannotated RPCs are omitted; the gnostic output renders in Scalar |
| Descriptor change | Read at startup; restart took about 1.2 s until healthy and drops in-flight requests |

**Gotcha:** without `match_incoming_request_route: true` every transcoded call returned 404, because the route is re-matched on the rewritten `/pkg.Service/Method` path.

```yaml
- name: envoy.filters.http.grpc_json_transcoder
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.grpc_json_transcoder.v3.GrpcJsonTranscoder
    proto_descriptor: /etc/envoy/descriptor.binpb
    services: [taakht.hello.v1.HelloService]
    auto_mapping: false
    match_incoming_request_route: true
    convert_grpc_status: true
```

## Spoofing test (x-user-id)

With `claim_to_headers`, a valid token plus a forged `x-user-id: attacker` (sent once or twice) reached the service as the real `user-42`. A token without `sub` plus a forged header reached the service with no value. A forged header without a token got 401. Only requests matched by a `jwt_authn` rule were tested.

## grpc-gateway comparison

| | Envoy | grpc-gateway |
|---|---|---|
| Same tests (transcoding, errors, JWT, rate limit) | Pass | Pass |
| JWT and rate limit | Configuration | Hand-written Go (73 lines) |
| Image size | 175 MB | 27 MB |
| Idle memory | Not measured | 4.8 MiB |
| Restart | about 1.2 s | about 1.25 s; a proto change also needs regenerate, rebuild, redeploy |

The hand-written version first contained a bug: the rate limiter ran before JWT validation, so rejected requests used up the quota. The fix was not rebuilt and re-run because Docker ran out of disk. The unannotated RPC had no generated handler (0 mentions in the generated code) but was not tested over HTTP.

## Documentation findings

- Envoy transcoder: `proto_descriptor` is required; an empty `services` list disables the filter.
- Envoy `jwt_authn`: no revocation, no OpenID Connect discovery; checks issuer and audience only.
- ASP.NET Core gRPC JSON transcoding: runs in-process, .NET only, server streaming only; the `Grpc.Swagger` OpenAPI package is deprecated with no replacement.
- grpc-gateway: generates a Go reverse proxy usable with any backend language; `protoc-gen-openapiv2` is stable, `openapiv3` is alpha.
- APISIX `grpc-transcode` does not use `google.api.http` (mapping is per route). Kong's documentation did not say. Connect's documentation lists no .NET support.

## Not verified

Hot reload of the descriptor (xDS), Envoy memory, requests on routes without a `jwt_authn` rule, streaming and multipart, Kong's annotation support.
