# XXXX. Describe Kafka events in Protobuf, without a schema registry for now

- Status: Proposed (draft, not yet numbered; options compared from documentation, chosen option verified in a spike)
- Date: 2026-10-06
- Deciders: TBD
- Bounded context(s): cross-cutting

## Context

Services exchange domain events through Kafka (published with a transactional outbox), and Reputation is event-sourced, so events live for years and are replayed. Producers and consumers are written in Go and .NET. Event schemas are therefore a one-way door: a format chosen now is hard to change later. The REST/gRPC contract is already defined in Protobuf (see the API contract ADR). Phase 1 is a single-node demo run by one team in one monorepo with one CI.

## Decision

We will define Kafka event payloads as Protobuf messages in `api/`, separate from the API messages, and write them to Kafka as plain Protobuf bytes with no schema registry. Each record carries `message-type` and `schema-version` Kafka headers. `buf breaking` runs in CI on the event protos and is a required check. We will reconsider a registry when producers or consumers exist outside this repository's CI.

## Options considered

1. **Protobuf, no registry (chosen)** - one schema language for APIs and events; compatibility enforced at merge time by `buf breaking`; no extra component to run. Cost: nothing stops an incompatible change at runtime if it bypasses CI (see spike); tools need the `.proto` files to decode messages.
2. **Protobuf with a schema registry** - the registry rejects an incompatible schema when a producer registers it, and tools can discover schemas. Cost: another service to run; every message gets a framing prefix (magic byte and schema id) that ties all producers and consumers to registry-aware libraries; adding it later means migrating the framing of existing messages. Confluent Schema Registry's license allows our use (the restriction is on offering a competing hosted service). Not run in the spike.
3. **Avro with a registry** - the most common Kafka setup, but introduces a second schema language next to Protobuf. Not run.
4. **JSON** - readable and simple, but field renames break consumers (per the Protobuf guidance on JSON) and nothing enforces compatibility. Not run.

## Consequences

- Positive: one toolchain; no extra infrastructure; headers leave room to add a registry later in a controlled way.
- Negative / trade-offs we accept: the only guard against a breaking change is CI; a producer deployed outside CI could corrupt the meaning of events silently.
- Follow-ups:
  - Write the rules in the conventions: never reuse or retype a field number, use `reserved`, events are separate messages from API messages (Protobuf guidance for long-term storage), a consumer must treat required-in-practice fields as invalid when empty.
  - Decide topic layout (one message type per topic, or a type header) and Kafka UI configuration per topic.
  - Decide how Reputation events are upcast when the formula or schema changes (separate ADR).
  - Revisit a registry if producers appear outside this repository's CI.

## Evidence

Go to .NET works with plain Protobuf. An incompatible type change (v3) raised no error and silently emptied a field, while `buf breaking` rejected it. A registry was not tested. Details: [evidence](../../evidence/kafka-events.md).

## References

- [Define the API contract in Protobuf](define-api-contract-in-protobuf.md)
- Protobuf guidance on safe changes and long-term storage (protobuf.dev best practices)
- Confluent Protobuf serializer documentation; Confluent Community License FAQ
