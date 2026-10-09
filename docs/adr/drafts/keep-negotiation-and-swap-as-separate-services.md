# XXXX. Keep Negotiation and Swap as separate services

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): Negotiation, Swap

## Context

The design groups negotiation and settlement as one "Deal" context. The MVP implements them as two services (both .NET): Negotiation owns the non-binding phase (Proposals, four approvals); Swap owns what happens after an agreement (the lock saga, locker fees with a deadline, the final outcome). The architecture notes had already renamed Settlement to Swap once money handling was removed. The earlier ADR drafts name "Deal" as one service; this ADR records what was built and why.

## Decision

We will keep Negotiation and Swap as two services with separate databases. Negotiation ends at `AgreementReached`; Swap starts from it, takes the lock, tracks payment and publishes the outcome. They communicate only through Kafka events (`AgreementReached` one way; `ExclusiveLockAcquired`, `SwapRejected` and `SwapCancelled` back). Swap calls the Ad service; Negotiation does not call Swap.

## Options considered

1. **Two services (chosen)** - the two parts have different lifecycles and invariants: Negotiation is interactive, versioned and cancellable; Swap is a short, mostly automatic saga with deadlines and an irreversible outcome. The boundary makes the saga and its failure handling explicit and gives the demo a real cross-service flow. Cost: an extra service, database and Kafka hop (a Swap outage leaves negotiations `AGREEMENT_PENDING` until the sweeper republishes the agreement and, after three periods, cancels them), and agreement that is eventual rather than immediate.
2. **One Deal service** - fewer moving parts. Rejected for the MVP: the lock lives in the Ad service anyway, so a single service would still have a cross-service step, and it would hide the saga the project wants to show.
3. **Swap as a module inside Negotiation** - same deployable, separate code. Not tried; the benefits of a boundary (separate data, separate failure) would be lost, and it contradicts the "service boundary equals ownership boundary" reasoning in [use microservices](use-microservices.md).

## Consequences

- Positive: Negotiation has no timers or partner calls; Swap owns the deadline sweeper and the locker mock; the agreement event is a stable contract; each side's state machine is pure code and tested alone.
- Negative / trade-offs we accept: agreement is asynchronous (the final approval returns `AGREEMENT_PENDING`); Negotiation learns the outcome only through `ExclusiveLockAcquired`, `SwapRejected` and `SwapCancelled`; the agreed terms are copied into the event.
- Follow-ups: Negotiation consumes `SwapCancelled` (an `AGREED` negotiation becomes `CANCELLED`, named by the `negotiation_id` the event carries; the ad pair is only a fallback for events without it) but ignores `SwapCompleted`; decide whether a completed swap needs a negotiation status. If the two always change together, merge them.

## Known gaps

- The boundary was chosen while building, not validated against the domain context map. The open decomposition item ([open items](../../open-items.md), item 3) still needs that confirmation.
- The revisit trigger is a judgement: no measurement of change coupling exists.
- Fault injection ([chaos results](../../testing/chaos-test-results.md)) covers swap killed and restarted (scenario 1), negotiation killed with the agreement in its outbox (5) and a payment deadline passing while Ad was down (7); all passed. After a restart the consumer-group rebalance adds up to ~45 s before the other side sees progress. Not run: the `AGREEMENT_PENDING` republish sweeper and `ExclusiveLockAcquired` waiting while negotiation is down.

## Evidence

- Code: `src/negotiation/Taakht.Negotiation` (`Domain/SwapNegotiation.cs`, `Application/NegotiationService.cs`, `Application/EventHandlers.cs`), `src/swap/Taakht.Swap` (`Domain/SwapMachine.cs`, `Application/SwapWorkflow.cs`, `Application/PaymentDeadlineSweeper.cs`).
- Tests: `SwapNegotiationTests` and `NegotiationPostgresTests`; `SwapMachineTests` and `WorkflowDatabaseTests`.
- Overview: [MVP architecture](../../architecture/mvp-architecture.md).

## References

- [Build the system as microservices](use-microservices.md)
- [Take the exclusive ad lock in the Ad service](take-the-exclusive-ad-lock-in-the-ad-service.md)
- [Open items](../../open-items.md), item 3
