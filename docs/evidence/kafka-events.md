# Evidence: Kafka events in Protobuf

Date: 2026-10-06. Backs the ADR "Describe Kafka events in Protobuf".

## Setup

Kafka (KRaft, single node), a Go producer (franz-go, pure Go, no cgo), .NET consumers (Confluent.Kafka), Protobuf bytes with `message-type` and `schema-version` headers. Three schema versions of one event: v1 has three fields, v2 adds a string field, v3 changes `owner_id` from string to int64.

## Results

| Check | Result |
|---|---|
| Go producer to .NET consumer with plain Protobuf | Works |
| v1 consumer reading v2 events | Correct; the unknown field is ignored |
| v2 consumer reading old v1 events | Correct; the new field has its default |
| **v1 or v2 consumer reading v3 events** | **No error; `owner_id` silently empty** (same in Kafka UI) |
| `buf breaking` v1 to v2 | Accepted |
| `buf breaking` v1 to v3 | Rejected: `Field "2" with name "owner_id" ... changed type from "string" to "int64"` |
| Kafka UI | Decodes the events when given the `.proto` directory (`protobufFilesDir`) and a message name; the single-file form (`protobufFile`) failed to start with "message type not found"; one message name applies per topic |

## Documentation findings

- Protobuf guidance: adding fields is safe, removing needs `reserved`, never reuse or retype a number; keep storage messages separate from API messages; avoid text formats for interchange.
- Confluent Protobuf serializer: each message gets a magic byte and schema id, so a registry changes the message framing; the documentation covers Java only. A Go serializer exists (confluent-kafka-go v2.15.1); the .NET package page could not be read.
- Confluent Community License allows self-hosting Schema Registry for internal use; the restriction is on offering a competing hosted service. Apicurio Registry is Apache 2.0.

## Not verified

A schema registry (and that it would have rejected v3 at produce time), the .NET registry serializer, the transactional outbox, Avro and JSON options.
