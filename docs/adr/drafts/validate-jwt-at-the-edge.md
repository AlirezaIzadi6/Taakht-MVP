# XXXX. Validate JWTs at the Envoy edge

- Status: Proposed (draft, not yet numbered)
- Date: 2026-10-06
- Deciders: TBD
- Bounded context(s): cross-cutting

## Context

Clients authenticate with JWTs (in phase 1 `AuthProvider` is a mock that issues a token for seeded users). The README places JWT validation in the API gateway. Services are written in .NET and Go. We must decide where tokens are verified and how services learn who the caller is, including when one service calls another on behalf of a user.

## Decision

We will verify JWTs (signature, expiry, issuer, audience) in Envoy with the `jwt_authn` filter and reject invalid requests before they reach a service. Envoy forwards only the claims services need (initially the user id as `x-user-id`) as gRPC metadata, using `claim_to_headers`. Services read the user identity only through a shared interceptor and propagate it unchanged on outgoing calls. Tokens are short-lived because `jwt_authn` has no revocation.

## Options considered

1. **Verify at Envoy, forward only needed claims (chosen)** - one verification point for both languages, invalid requests dropped early, services stay simple. Cost: services trust the metadata, so its trustworthiness depends on how callers are authenticated (see the service-to-service ADR).
2. **Forward the whole JWT and verify it again in every service** - independent of the network and safer if a service is reached directly, but duplicates token and JWKS handling in Go and .NET and lets every service drift.
3. **Verify only in the services** - no edge dependency, but unauthenticated traffic reaches every service and the README's gateway responsibility is lost.

## Consequences

- Positive: consistent authentication in one place; cheaper services.
- Negative / trade-offs we accept: a service reachable without passing through Envoy would accept forged identity metadata; no token revocation (mitigated by short expiry); authorization decisions still belong to each service because `jwt_authn` only checks issuer and audience.
- Follow-ups:
  - Keep every service route covered by a `jwt_authn` rule: the header protection was verified only for requests matched by a rule.
  - Trust in the metadata is covered by [Authenticate service-to-service calls](authenticate-service-to-service-calls.md).
  - Decide the identity provider, key rotation and token lifetime.

## Evidence

`jwt_authn` rejected missing and expired tokens with 401, and with `claim_to_headers` a client-forged `x-user-id` never reached the service. Only routes covered by a rule were tested. Details: [evidence](../../evidence/edge-gateway.md).

## References

- [Use Envoy for REST to gRPC transcoding](use-envoy-for-rest-grpc-transcoding.md)
- Envoy `jwt_authn` filter documentation
- README, Technology section
