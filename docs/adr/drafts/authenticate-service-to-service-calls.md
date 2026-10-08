# XXXX. Authenticate service-to-service calls: isolated network now, mTLS in production

- Status: Proposed (draft, not yet numbered)
- Date: 2026-10-06
- Deciders: TBD
- Bounded context(s): cross-cutting

## Context

Services trust the user identity that Envoy forwards (see the JWT ADR). That trust is only sound if nothing but Envoy and the other services can reach them. The README lists service-to-service authentication as TBD with API keys proposed. Phase 1 is a single-node demo with a mock `AuthProvider`; production must run with replicas and real isolation.

Facts that shape the choice (from the gRPC and ASP.NET Core docs):

- gRPC treats TLS as the foundation for service-to-service traffic, with client certificates for mutual authentication.
- In .NET, `CallCredentials` (bearer tokens, and therefore API keys) are only applied on a TLS-secured channel unless explicitly overridden.
- So token or API-key schemes need TLS anyway; mTLS adds only a client certificate on top of that.

## Decision

In phase 1 we will not enforce service identity between services. The internal network is isolated (only Envoy publishes a port), every channel and interceptor is created through a shared library per language so authentication can be added in one place, and the accepted risk is documented in the README limitations. The production target is mutual TLS between services; the mechanism for issuing and rotating certificates (scripted, a service mesh, or SPIFFE/SPIRE) is decided when production topology is decided.

## Options considered

1. **Trust the isolated network (phase 1, chosen)** - no cost now and no wasted work, because the shared-library seam keeps the later change small. Risk: a compromised container or an exposed port allows forged identity.
2. **mTLS from day one with scripted certificates** - strongest and demonstrable, but every Go and .NET service needs certificate plumbing before any domain logic exists, which the complexity rule does not justify yet.
3. **API keys over TLS (the README's proposal)** - needs TLS like mTLS does, plus a shared secret with manual rotation and coarse identity. Not simpler than mTLS and weaker.
4. **Internal token issuer (client-credentials style)** - allows fine-grained claims, but adds a security-critical component to build and run.

## Consequences

- Positive: no early plumbing; a clear upgrade path; the risk is explicit.
- Negative / trade-offs we accept: phase 1 offers no defence if the internal network is breached; the demo must not be exposed beyond the gateway.
- Follow-ups:
  - Create the shared channel and interceptor library for Go and .NET (convention doc).
  - Add the risk to the README "Known limitations".
  - Decide the certificate mechanism with the deployment topology; then write the mTLS ADR (this one would be superseded).
  - Decide Kafka client authentication separately.

## References

- [Validate JWTs at the Envoy edge](validate-jwt-at-the-edge.md)
- gRPC authentication guide; ASP.NET Core gRPC authentication and authorization; SPIFFE overview
