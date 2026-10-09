# XXXX. Use a transactional outbox for events

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): cross-cutting

## Context

Services change their own database and must tell other services through Kafka. Writing the row and producing the event are two systems; a crash between them loses the event or announces a change that was rolled back. The lock saga depends on events arriving (`AgreementReached` to Swap, `ExclusiveLockAcquired` back to Negotiation, `SwapCompleted` to Ad). The README lists the outbox as a stack choice without recorded reasoning ([open items](../../open-items.md), item 9).

## Decision

We will write every event into an `outbox` table in the same database transaction as the business change, and publish it to Kafka from a polling relay inside the service. Delivery is at-least-once. Rows are inserted with `created_at = clock_timestamp()` so events written in one transaction keep their order, and the relay reads `ORDER BY created_at` with `FOR UPDATE SKIP LOCKED`.

## Options considered

1. **Outbox table plus a polling relay in each service (chosen)** - atomic with the business change, no extra infrastructure, easy to test with a plain database. Cost: up to one poll interval (200 ms) of latency, a table that grows, duplicates after a crash, and a relay to write twice (Go and .NET).
2. **Produce to Kafka directly after the commit (dual write)** - least code. Rejected: a crash or Kafka outage between commit and produce loses the event with no record that it was owed.
3. **Produce first, then write the row** - the reverse failure: announces changes that never happened. Rejected.
4. **Outbox with change data capture (Debezium)** - no polling, lower latency, ordering from the log. Deferred: it adds Kafka Connect and a connector per database to a one-week build; the table and the row format stay the same, so the relay can be replaced later.
5. **One transaction spanning Postgres and Kafka** - not available; Postgres cannot take part in a Kafka transaction.

## Consequences

- Positive: a committed change always has its event recorded; Kafka outages delay events but do not break writes; the outbox rows are an inspectable record of what was published.
- Negative / trade-offs we accept: at-least-once delivery (consumers must be idempotent, see [processed events](make-consumers-idempotent-with-a-processed-events-table.md)); 200 ms polling latency per hop (the lock saga crosses three hops); extra writes and storage.
- Published rows are pruned after `OUTBOX_RETENTION` (default 24h) by the housekeeping job; unpublished rows are never deleted.
- Follow-ups: revisit CDC when polling load or latency matters.

## Known gaps

- `outbox` rows are never deleted.
- Order is guaranteed per Kafka key (the aggregate id) from one relay. With several relay instances, `SKIP LOCKED` lets them publish different batches concurrently and global order is not guaranteed. The MVP runs one instance per service.
- A crash after the broker acknowledgement and before `published_at` is committed publishes the batch again.
- When Kafka is down, the Go and .NET relays log the failure and retry on the next poll. Nothing alerts on a growing backlog.
- No latency or throughput was measured.

## Evidence

- Code: `libs/goplatform/outbox/outbox.go` (`Add`, `RunRelay`), `libs/dotnet/Taakht.Platform/Outbox.cs` and `OutboxRelay.cs`; the same table definition in each service's `migrations/001_init.sql`.
- Tests: `libs/goplatform/integration_test.go` (`TestOutboxKafkaConsume`, needs `TEST_DATABASE_URL` and `KAFKA_BROKERS`), `libs/dotnet/Taakht.Platform.Tests/PipelineIntegrationTests.cs` (outbox to Kafka to consumer), `OutboxAndIdentityTests.cs`; the lock and approval paths write their events inside the tested transaction (for example `NegotiationPostgresTests.ApprovalFlowReachesAgreementAndWritesOutbox`).
- Convention: [MVP service conventions](../../guidelines/mvp-service-conventions.md), "Events".
- Overview: [MVP architecture](../../architecture/mvp-architecture.md).

## References

- [Make consumers idempotent with a processed-events table](make-consumers-idempotent-with-a-processed-events-table.md)
- [Describe Kafka events in Protobuf](describe-kafka-events-in-protobuf.md)
- [Open items](../../open-items.md), item 9
