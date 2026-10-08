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

### Path and verb conventions

- Resources are plural nouns under `/v1/`: `/v1/ads/{ad_id}`. The collection path lists (`GET /v1/ads` = the caller's own ads), `POST` on it creates.
- Standard methods use the HTTP verb: `GET` read, `POST` create, `PUT` full update (with the stale-version check in the body), no `DELETE` yet.
- State transitions are custom verbs on the resource: `POST /v1/<resource>/{id}:<verb>`, verb in lower-case kebab-case (`:publish`, `:approve-ad`, `:reject-proposal`). Their body is `*` when they carry arguments and empty otherwise.
- Sub-collections and searches hang off the owning prefix: `/v1/matching/search`, `/v1/matching/ads/{ad_id}/matches`.
- Dev-only mocks live under `/v1/dev/...` and must be dropped (annotation and route) before production.
- JSON field names are lowerCamelCase (`expectedVersion`), enums are their proto names, 64-bit integers are JSON strings. Query parameters carry request fields that are neither in the path nor the body (`?adId=`, `?version=`, `?limit=`).
- Errors: JSON `{"code": <grpc code>, "message": "..."}` with the HTTP status Envoy derives from the gRPC code (`NOT_FOUND` 404, `INVALID_ARGUMENT` 400, `ABORTED` 409, `FAILED_PRECONDITION` 400, `PERMISSION_DENIED` 403, `UNAUTHENTICATED` 401, `RESOURCE_EXHAUSTED` 429). A missing or invalid JWT is 401 from Envoy.
- Internal RPCs (`AdService.LockAds`) have no annotation and are unreachable from the edge.

### Edge routes (MVP)

| Prefix | Service | Method and path -> RPC |
|---|---|---|
| `/v1/ads` | ad :9001 | `POST /v1/ads` (body = ad spec) CreateAd; `GET /v1/ads` ListMyAds; `GET /v1/ads/{ad_id}?version=` GetAd; `PUT /v1/ads/{ad_id}` (body `{expectedVersion, spec}`) EditAd; `POST /v1/ads/{ad_id}:publish` PublishAd; `POST /v1/ads/{ad_id}:hide` HideAd |
| `/v1/matching` | matching :9002 | `POST /v1/matching/search` (body `{criteria, limit}`) Search; `GET /v1/matching/ads/{ad_id}/matches?limit=` FindMatches |
| `/v1/negotiations` | negotiation :9003 | `POST /v1/negotiations` OpenNegotiation; `GET /v1/negotiations?adId=` ListNegotiations; `GET /v1/negotiations/{negotiation_id}` GetNegotiation; `POST .../{id}:approve-ad` (`{adVersion}`); `:revise` (`{seenProposalNumber, terms}`); `:approve-proposal` and `:reject-proposal` (`{proposalNumber}`); `:close` |
| `/v1/swaps` | swap :9004 | `GET /v1/swaps` ListMySwaps; `GET /v1/swaps/{swap_id}` GetSwap |
| `/v1/dev/swaps` | swap :9004 | `POST /v1/dev/swaps/{swap_id}/locker-fee-paid:simulate` (`{userId}`) SimulateLockerFeePaid |

A path under a prefix that matches no annotation is passed to the service as a plain HTTP request and fails with 415 (it is not a JSON 404).

## Caller identity

- The user id reaches services as the `x-user-id` metadata key, set by Envoy from the verified JWT (`sub` claim). Services read it only through the shared interceptor and pass it unchanged on outgoing calls.
- Verified in a spike (2026-10-06): a client-sent `x-user-id` is overwritten or dropped on routes covered by a `jwt_authn` rule.
- A usable id is 1..64 characters without control characters; anything else is `UNAUTHENTICATED`.
- Service identities: service-to-service calls that have no user use reserved ids with the prefix `system:`. Today `system:swap` (Swap's `LockAds` call, the only caller `LockAds` accepts) and `system:negotiation` (Negotiation's `GetAd` calls, sent as explicit metadata). In `GetAd`, the owner and any `system:*` caller read any ad and version; everyone else gets only a `PUBLISHED` ad at its current version, otherwise `NOT_FOUND`.
- Trust assumption: the services believe the header. Envoy overwrites it from the JWT, so a client cannot choose it at the edge, but the JWT `sub` itself is not checked for the `system:` prefix. Real user ids must never start with `system:`, and service ports must not be reachable by clients. A production token issuer must refuse such ids; service-to-service authentication proper is a separate decision ([ADR draft](../adr/drafts/authenticate-service-to-service-calls.md)).
- Dev-only RPCs (`SimulateLockerFeePaid`) exist only with `ENABLE_DEV_ENDPOINTS=true` or the Development environment; otherwise the service answers `UNIMPLEMENTED`. gRPC reflection is off unless `TAAKHT_GRPC_REFLECTION=on` (Go) or the Development environment (.NET).
- Input limits are enforced in the services (for example ad title 1..120 characters, description at most 4000, lists at most 20 entries of at most 64 characters, price difference 0..10^12) and fail with `INVALID_ARGUMENT`.
