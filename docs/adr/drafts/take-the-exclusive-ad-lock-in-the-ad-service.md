# XXXX. Take the exclusive ad lock in the Ad service

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): Ad, Swap, Negotiation

## Context

When two parties agree to swap, both ads must become unavailable to every other negotiation, and exactly one of several competing agreements on the same ad may win. The earlier design removed the "one approval per ad" rule, so exclusivity can only come from a lock. Where the lock lives was an open question ([open items](../../open-items.md), item 10); the proposal on record was the Ad service. The lock also has to survive duplicated and retried requests.

## Decision

We will take the lock in the Ad service, which owns the ad rows: `LockAds(swap_id, [(ad_id, version)] x2)` claims both ads in one database transaction, or neither. It is idempotent by `swap_id`. `LockAds` accepts only the service identity `system:swap`, proven by the shared internal token (`x-internal-token`). The Swap service drives it as a saga step after `AgreementReached`; the Ad service releases or closes the ads when it consumes `SwapCancelled` or `SwapCompleted`.

## Options considered

1. **Lock in the Ad service, called by Swap (chosen)** - the claim and the data it protects share one transaction, so atomicity and the version check need no distributed lock. The winner is whoever commits first. Cost: Swap depends synchronously on Ad for this step, and the outcome reaches Negotiation only through events.
2. **Lock in Negotiation** - it already holds the approvals. Rejected: it would have to reach into Ad data it does not own, or keep a copy that can be stale; the version check would race with ad edits.
3. **Lock in Swap with its own table of locked ads** - Swap would be the authority on ad availability while Ad could still edit, hide or publish a locked ad. Rejected: two owners of one fact.
4. **Distributed lock (Redis, advisory locks across services)** - adds a component and still needs the Ad row to reflect the state. Rejected.
5. **Optimistic version bump on each ad, no separate lock** - gives no way to claim two ads atomically or to record who holds them.

## Consequences

- Positive: all-or-nothing claim with row locks taken in id order (no deadlock between swaps that share an ad); the agreed version is re-checked at the authority; retries and redelivery are safe through the `ad_lock (swap_id, ad_id)` key.
- Negative / trade-offs we accept: `hidden` ads can also be locked; a refused lock (`FAILED_PRECONDITION`, `NOT_FOUND`, `INVALID_ARGUMENT` or `PERMISSION_DENIED`) ends the negotiation (`SwapRejected` leads to `CANCELLED`) instead of retrying; the other services (matching index, negotiation status) learn about the lock eventually, through `AdLocked` and `ExclusiveLockAcquired`.
- Follow-ups: reconcile a swap that takes the lock after the negotiation was cancelled for timing out (see the architecture document); a repair for a swap stuck in `LOCKING`.

## Known gaps

- Implemented with `SELECT ... FOR UPDATE` on both ad rows plus checks in code, not the single conditional `UPDATE` that the plan describes. The guarantee is the same.
- Stuck negotiations: a sweeper in Negotiation republishes `AgreementReached` up to three times (every `AGREEMENT_PENDING_TIMEOUT`, default 10 minutes) and then cancels the negotiation; a swap that stays `LOCKING` is only retried through those republished events. If the swap locks after the cancellation, `ExclusiveLockAcquired` for a `CANCELLED` negotiation is ignored and logged, so the swap can complete while the negotiation shows `CANCELLED`.
- Negotiation consumes `SwapCancelled` and cancels the `AGREED` negotiation named by the event's `negotiation_id` (the ad pair is used only for events without that field). It acts on `ExclusiveLockAcquired` only for an existing `AGREEMENT_PENDING` or `AGREED` winner whose ads match the event. It does not consume `SwapCompleted`, so a completed swap leaves it `AGREED`. Competitors cancelled at lock time stay cancelled.
- The caller check on `LockAds` requires `x-user-id: system:swap` together with the shared `INTERNAL_AUTH_TOKEN` (`x-internal-token`); a bare `system:swap` is `UNAUTHENTICATED`. It is one secret for all services, not a per-service cryptographic identity (see [authenticate service-to-service calls](authenticate-service-to-service-calls.md)).
- Consistency between the lock and the search index is eventual: Matching removes an ad when `AdLocked` arrives, so a locked ad can briefly appear in `Search` results. A user acting on such a result is stopped at `OpenNegotiation` (the target must be published) or at `LockAds`.
- Unit-level concurrency is tested in one process (concurrent goroutines). Across processes the race was exercised by `tests/load/contention.js` through the full stack (10 final approvals on one hot ad at the same instant, 40 to 200 requesters): in every run exactly one swap for the ad was not `REJECTED`/`CANCELLED` and the ad ended `CLOSED` ([load test results](../../testing/load-test-results.md)). One run per setting on one laptop.
- Fault injection ([chaos results](../../testing/chaos-test-results.md), scenario 2): with Ad killed during the lock the swap stayed `LOCKING`, the handler was retried with backoff and `LockAds` succeeded once Ad was back. The exact crash window between the lock commit and the swap update was not injected.
- The consumer dedupe transaction is open during the `LockAds` call (up to the 10 s timeout); see known gap 6 in the architecture document.

## Evidence

- Code: `LockAds` in `src/ad/internal/ad/service.go`; `ad_lock` in `src/ad/migrations/001_init.sql`; `SwapWorkflow.HandleAgreementReachedAsync` and `SwapMachine` in `src/swap/Taakht.Swap`; `EventHandlers.OnExclusiveLockAsync` in `src/negotiation/Taakht.Negotiation/Application/EventHandlers.cs`.
- Tests: `TestLockAdsRulesAndIdempotency`, `TestLockAdsConcurrentOverlap`, `TestSwapEventConsumers` in `src/ad/internal/ad/service_test.go`; `WorkflowDatabaseTests` (duplicate agreement, rejected lock, retry after a transient failure); `NegotiationPostgresTests.ExclusiveLockAgreesWinnerCancelsCompetitorsAndIsIdempotent` and `SwapRejectedCancelsThePendingNegotiation`.
- Overview: [MVP architecture](../../architecture/mvp-architecture.md), "The lock saga".

## References

- [Keep Negotiation and Swap as separate services](keep-negotiation-and-swap-as-separate-services.md)
- [Bind approvals to versions](bind-approvals-to-versions.md)
- [Authenticate service-to-service calls](authenticate-service-to-service-calls.md)
- [Use a transactional outbox for events](use-a-transactional-outbox-for-events.md)
- [Open items](../../open-items.md), item 10
