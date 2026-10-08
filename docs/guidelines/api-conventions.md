# API conventions (draft)

Conventions that follow from the Protobuf API contract decision. Not an ADR; cheap to change.

## Versioning

- The major version is part of both the REST path and the proto package: `/v1/users/{id}` and `taakht.user.v1`.
- Compatible changes (new optional fields, new RPCs) stay in `v1`. A breaking change requires a new `v2` package and path, with both versions served during migration.
- `buf breaking` runs against `main` in CI and blocks accidental breaking changes.

## Errors

- Services return standard gRPC status codes with `google.rpc.Status` details.
- Envoy maps them to HTTP status codes and a JSON `google.rpc.Status` body (`convert_grpc_status`).
- Request validation errors use `google.rpc.BadRequest` details with field names.

## REST mapping

- Only RPCs with a `google.api.http` annotation are public; Envoy auto-mapping stays disabled.
- Each service owns one path prefix at the edge.

## Caller identity

- The user id reaches services as the `x-user-id` metadata key, set by Envoy from the verified JWT (`sub` claim). Services read it only through the shared interceptor and pass it unchanged on outgoing calls.
- Verified in a spike (2026-10-06): a client-sent `x-user-id` is overwritten or dropped on routes covered by a `jwt_authn` rule.
