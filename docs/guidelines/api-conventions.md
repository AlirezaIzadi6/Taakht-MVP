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

## Pagination

List endpoints (`ListMyAds` `GET /v1/ads`, `ListNegotiations` `GET /v1/negotiations`, `ListMySwaps` `GET /v1/swaps`) are bounded and keyset-paginated. An unbounded list is unsafe at the edge: Envoy answers 500 when the transcoded response exceeds its buffer.

- Request: `page_size` (`pageSize` in the query; `<= 0` means 50, above 200 is clamped to 200) and `page_token` (`pageToken`; empty = first page).
- Response: the items plus `next_page_token` (`nextPageToken`), empty on the last page. Follow it until it is empty; the page may hold fewer items than requested only on the last page.
- Order is newest first (`created_at DESC`, then `id DESC` as the deterministic tie-breaker). The token is opaque: base64url of `created_at|id` of the last item returned (UTC, microseconds). Do not build or parse it; a malformed token is `INVALID_ARGUMENT` (HTTP 400).
- Implemented as `WHERE (created_at, id) < (cursor)` with `LIMIT page_size + 1` (the extra row only tells whether a next page exists), never `OFFSET`. Items created after the first page was read are newer than the cursor and are not part of that walk, so no item older than the cursor is duplicated or skipped; a new item shows only on a fresh first page.
- `ListNegotiations` keeps its `adId` filter on every page, and fills `requesterAd` / `targetAd` only for the returned page.

### Path and verb conventions

- Resources are plural nouns under `/v1/`: `/v1/ads/{ad_id}`. The collection path lists (`GET /v1/ads` = the caller's own ads), `POST` on it creates.
- Standard methods use the HTTP verb: `GET` read, `POST` create, `PUT` full update (with the stale-version check in the body), no `DELETE` yet.
- State transitions are custom verbs on the resource: `POST /v1/<resource>/{id}:<verb>`, verb in lower-case kebab-case (`:publish`, `:approve-ad`, `:reject-proposal`). Their body is `*` when they carry arguments and empty otherwise.
- Sub-collections and searches hang off the owning prefix: `/v1/matching/search`, `/v1/matching/ads/{ad_id}/matches`.
- Dev-only mocks live under `/v1/dev/...` and must be dropped (annotation and route) before production.
- JSON field names are lowerCamelCase (`expectedVersion`), enums are their proto names, 64-bit integers are JSON strings. Query parameters carry request fields that are neither in the path nor the body (`?adId=`, `?version=`, `?limit=`).
- The `Negotiation` message (every negotiation response) has, besides ids, status, `activeProposal` and `approvals`, two ads for the parties: `requesterAd` (field 12) and `targetAd` (field 13), each a full `taakht.ad.v1.Ad` (id, owner, status, current version, spec) as the Ad service shows it now. They are filled on `GET /v1/negotiations/{id}`, `GET /v1/negotiations` and every `POST` that returns a negotiation (open, `:approve-ad`, `:revise`, `:approve-proposal`, `:reject-proposal`, `:close`); only the two parties can read a negotiation at all. If an ad cannot be read at that moment the field is simply absent (the call still succeeds). Both ads are shown regardless of their status, so a party sees the counterpart's ad even while it is hidden or locked.
- Existence is not leaked: `GET /v1/ads/{id}` answers 404 `ad not found` for a missing ad and for a hidden/foreign one alike, and `POST /v1/negotiations` answers 404 `ad not available` for a missing ad, for a requester ad owned by someone else and for a target ad that is not published. `EditAd` with a spec equal to the current one returns the current ad (same `version`) without creating a version; a stale `expectedVersion` is still 409.
- Errors: JSON `{"code": <grpc code>, "message": "..."}` with the HTTP status Envoy derives from the gRPC code (`NOT_FOUND` 404, `INVALID_ARGUMENT` 400, `ABORTED` 409, `FAILED_PRECONDITION` 400, `PERMISSION_DENIED` 403, `UNAUTHENTICATED` 401, `RESOURCE_EXHAUSTED` 429). A missing or invalid JWT is 401 from Envoy.
- Internal RPCs (`AdService.LockAds`) have no annotation and are unreachable from the edge.

### Edge routes (MVP)

| Prefix | Service | Method and path -> RPC |
|---|---|---|
| `/v1/ads` | ad :9001 | `POST /v1/ads` (body = ad spec) CreateAd; `GET /v1/ads?pageSize=&pageToken=` ListMyAds; `GET /v1/ads/{ad_id}?version=` GetAd; `PUT /v1/ads/{ad_id}` (body `{expectedVersion, spec}`) EditAd; `POST /v1/ads/{ad_id}:publish` PublishAd; `POST /v1/ads/{ad_id}:hide` HideAd |
| `/v1/matching` | matching :9002 | `POST /v1/matching/search` (body `{criteria, limit}`) Search; `GET /v1/matching/ads/{ad_id}/matches?limit=` FindMatches |
| `/v1/negotiations` | negotiation :9003 | `POST /v1/negotiations` OpenNegotiation; `GET /v1/negotiations?adId=&pageSize=&pageToken=` ListNegotiations; `GET /v1/negotiations/{negotiation_id}` GetNegotiation; `POST .../{id}:approve-ad` (`{adVersion}`); `:revise` (`{seenProposalNumber, terms}`); `:approve-proposal` and `:reject-proposal` (`{proposalNumber}`); `:close` |
| `/v1/swaps` | swap :9004 | `GET /v1/swaps?pageSize=&pageToken=` ListMySwaps; `GET /v1/swaps/{swap_id}` GetSwap |
| `/v1/dev/swaps` | swap :9004 | `POST /v1/dev/swaps/{swap_id}/locker-fee-paid:simulate` (`{userId}`) SimulateLockerFeePaid |

A path under a prefix that matches no annotation is passed to the service as a plain HTTP request and fails with 415 (it is not a JSON 404).

## Caller identity

- The user id reaches services as the `x-user-id` metadata key, set by Envoy from the verified JWT (`sub` claim). Services read it only through the shared interceptor and pass it unchanged on outgoing calls.
- Verified in a spike (2026-10-06): a client-sent `x-user-id` is overwritten or dropped on routes covered by a `jwt_authn` rule.
- A usable id is an opaque token of 1..64 printable ASCII bytes (0x21-0x7E: no spaces, control characters or non-ASCII), identical in the Go and .NET libs; a repeated `x-user-id` or `x-internal-token` header is rejected too; anything else is `UNAUTHENTICATED`. Ids are opaque ASCII tokens in this MVP.
- Service identities: service-to-service calls that have no user use reserved ids with the prefix `system:`. Today `system:swap` (Swap's `LockAds` call, the only caller `LockAds` accepts) and `system:negotiation` (Negotiation's `GetAd` calls, sent as explicit metadata). In `GetAd`, the owner and any `system:*` caller read any ad and version; everyone else gets only a `PUBLISHED` ad at its current version, otherwise `NOT_FOUND`.
- Proof of a service identity: a `system:` id counts only together with the metadata `x-internal-token`, which must equal the shared secret `INTERNAL_AUTH_TOKEN` (compared in constant time). The identity interceptor of every service answers `UNAUTHENTICATED` to a `system:` `x-user-id` without a valid token; it is never silently treated as a normal user. The client interceptors of both platform libs add the token to every outgoing call whose `x-user-id` starts with `system:`. Locally the secret defaults to `dev-internal-token` (DEV ONLY, public in the repo; each service logs a warning at startup while the default is in use); set `INTERNAL_AUTH_TOKEN` in any shared environment.
- Trust assumption: for normal users the services believe the header. Envoy overwrites it from the JWT, so a client cannot choose it at the edge. Envoy removes client-sent `x-user-id` and `x-internal-token` before authentication (a first Lua filter, so a signed token without `sub` cannot keep a client-chosen id), requires the audience `taakht-api`, and its `rbac` filter denies (403) a `/v1/` request whose `x-user-id` is absent or starts with `system:` (case-insensitive); `tools/devtoken` refuses to mint such ids. Only the exact ids `system:swap` and `system:negotiation` are service identities (case-sensitive); any other `system:`-prefixed id (any case) is `UNAUTHENTICATED`. Even without those edge rules, a minted `system:` token reaches the services without the internal secret and is rejected. What remains: the secret is a single shared value (no per-service identity, no rotation), so anyone who holds it or can read service traffic can act as any `system:` caller; real service-to-service authentication is a separate decision ([ADR draft](../adr/drafts/authenticate-service-to-service-calls.md)).
- Dev-only RPCs (`SimulateLockerFeePaid`) exist only with `ENABLE_DEV_ENDPOINTS=true` or the Development environment; otherwise the service answers `UNIMPLEMENTED`. gRPC reflection is off unless `TAAKHT_GRPC_REFLECTION=on` (Go) or the Development environment (.NET).
- Input limits are enforced in the services (for example ad title 1..120 characters, description at most 4000, lists at most 20 entries of at most 64 characters, price difference 0..10^12) and fail with `INVALID_ARGUMENT`.
