# XXXX. Make consumers idempotent with a processed-events table

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): cross-cutting

## Context

The outbox delivers at-least-once ([outbox ADR](use-a-transactional-outbox-for-events.md)), and consumers also see redelivery after a rebalance or a failed offset commit. Handlers change state (close ads, cancel negotiations, create swaps), so applying an event twice must be harmless. We also cannot assume a global order: events from different topics can arrive in any order relative to each other.

## Decision

We will give every consuming service a `processed_events (consumer, event_id)` table. For each event the consumer inserts the pair with `ON CONFLICT DO NOTHING` and runs the handler in the same database transaction; if the insert added nothing the event is skipped. The Kafka offset is committed after the transaction. Handlers additionally check current state, so late or reordered events are ignored rather than applied blindly.

## Options considered

1. **Dedupe table in the same transaction as the handler (chosen)** - exactly-once effect for the handler's database writes, simple, testable without Kafka (`consume.Process`, `EventConsumer.ProcessAsync`). Cost: one insert per event, and a table that grows.
2. **Naturally idempotent handlers only** - no extra table. Rejected as the sole mechanism: some handlers emit new events or create rows (swap creation, `AdClosed`), where replay would duplicate output and a mistake is silent. We still do state checks as a second layer.
3. **Kafka exactly-once (transactions, read-process-write)** - covers Kafka-to-Kafka only; the handlers write to Postgres. Rejected.
4. **Version or offset checks in each aggregate** - cheap for snapshot events (Matching does this for ad versions) but does not generalise to commands such as "close these ads".

## Consequences

- Positive: replays are absorbed uniformly in both languages; a handler failure rolls back the dedupe row, so a retry runs the handler again.
- Negative / trade-offs we accept: a failing handler is retried with backoff (200 ms doubling to 5 s) and blocks its partition, unless it marks the failure permanent. A handler can mark a failure permanent (`consume.Permanent` in Go, `PermanentEventException` in .NET; also used for payloads that do not decode and for a `LockAds` refusal that rejects the swap): the event is then logged at error level, recorded in `processed_events` with the handler's writes rolled back, stored in a per-service `dead_letter` table (envelope and error, pruned after `DEAD_LETTER_RETENTION`) in the same statement, and skipped, so a poison message does not block the partition. Only transient errors retry forever. Unknown envelope types and envelopes that do not decode are skipped without a dedupe row. A handler that calls another service runs inside the open dedupe transaction (Swap's `AgreementReached` handler holds it during `LockAds`, up to 10 s).
- Follow-ups: a replay tool and alerting for `dead_letter` (today an operator reads the table and replays by hand).

## Known gaps

- `processed_events` rows are pruned after `PROCESSED_EVENTS_RETENTION` (default 7 days, see the service conventions). A redelivery of an event older than that would be handled again, so the retention must stay well above any realistic redelivery window.
- A skipped permanent failure is recorded as processed and its payload is not kept, so the event cannot be replayed without re-publishing it. Deciding what is permanent is done by each handler; a wrong classification drops an event silently apart from the log.
- The .NET consumer logs and backs off on `KafkaException` (consume and commit); the Go consumer logs fetch errors. Neither alerts.
- The dedupe key is `(consumer, event_id)`; if a producer re-emitted the same fact with a new `event_id` (for example after a rebuild), it would be applied again. The handlers' state checks are the protection.
- No test kills a consumer between the handler commit and the offset commit; redelivery is simulated by calling the handler twice. The chaos scenario 6 ([results](../../testing/chaos-test-results.md)) re-published `AgreementReached` with the same `event_id` (absorbed: no new rows) and with a fresh `event_id` (not deduplicated by this table, one extra `processed_events` row each, but the state-checking handler did nothing, exactly one swap).
- A killed consumer does not leave its group, so a restarted service waits for the session timeout (~45 s) before it gets partitions again; handlers are idle during that time.
- `dead_letter` has no replay tool and nothing alerts on it; a transient error that never clears still blocks a partition.

## Evidence

- Code: `libs/goplatform/consume/consume.go` (`Process`, `Run`), `libs/dotnet/Taakht.Platform/EventConsumer.cs` (`ProcessAsync`).
- Tests: `src/matching/internal/handlers/handlers_test.go` (`TestProcessedEventIsIdempotent`, `TestOutOfOrderEdit`), `src/ad/internal/ad/service_test.go` (`TestSwapEventConsumers`), `NegotiationPostgresTests.AdEditedEventMakesApprovalStaleAndIgnoresLateDuplicates` and `ExclusiveLockAgreesWinnerCancelsCompetitorsAndIsIdempotent`, `WorkflowDatabaseTests.Duplicate_agreement_creates_one_swap_and_locks_once`, the permanent-failure cases in `libs/goplatform` and `libs/dotnet/Taakht.Platform.Tests`, `PipelineIntegrationTests.Outbox_to_kafka_to_consumer_is_idempotent`.
- Convention: [MVP service conventions](../../guidelines/mvp-service-conventions.md), "Events".
- Overview: [MVP architecture](../../architecture/mvp-architecture.md), "Idempotency and outbox mechanics".

## References

- [Use a transactional outbox for events](use-a-transactional-outbox-for-events.md)
- [Describe Kafka events in Protobuf](describe-kafka-events-in-protobuf.md)
