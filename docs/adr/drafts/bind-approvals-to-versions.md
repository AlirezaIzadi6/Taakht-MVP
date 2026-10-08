# XXXX. Bind approvals to versions

- Status: Proposed (draft, not yet numbered; for review)
- Date: 2026-10-08
- Deciders: @AlirezaIzadi6, @ArmanIzadi99
- Bounded context(s): Negotiation, Ad

## Context

A swap is agreed on specific content: a version of each ad and a version of the Proposal (terms). Ads can be edited and Proposals revised while the other side is deciding. An approval must not silently apply to content its giver never saw. Ad edits reach Negotiation as events, so its picture of an ad's version can lag.

## Decision

We will make each approval record its target (an ad version, or a Proposal number) and keep it. Validity is computed when read: an approval is valid only while its target equals the current version. We will not delete or invalidate approvals when something changes. At the final approval, Negotiation checks both ads' versions synchronously with the Ad service before it writes `AgreementReached`; the Ad service's `LockAds` checks the agreed versions again.

## Options considered

1. **Approvals bound to versions, staleness by comparison (chosen)** - no cascade on edit and no write per open negotiation when an ad changes (an ad may have up to 10 open negotiations); restoring a previous Proposal makes its approval valid again by itself. Cost: every read recomputes validity, and clients must understand `valid = false`.
2. **Invalidate approvals when an ad is edited or a Proposal is revised** - an `AdEdited` handler would update every open negotiation of that ad; duplicates and ordering make it error-prone, and it would still need the final version check.
3. **Block ad edits while any approval is active** - simple for Negotiation, but an owner could not fix a typo while talking to several people, and Ad would need to know about negotiations. Rejected.
4. **Approvals not bound to versions (approve the ad, not a version)** - a seller could change the item after being approved. Rejected.

## Consequences

- Positive: a stale approval cannot become an agreement; the rule is a pure function in the domain (`SwapNegotiation.IsValid`, `HasAllApprovals`) and is unit-tested; edits need no fan-out.
- Negative / trade-offs we accept: the local version (`ad_ref`) is eventually consistent, so a client may see an approval as valid until `AdEdited` arrives; the final check then answers `ABORTED` and the negotiation stays `OPEN`. Rejecting a Proposal restores only one previous level. The final check and the lock are not one atomic step: an edit between them is caught by `LockAds`, which ends the negotiation as `CANCELLED`.
- Follow-ups: surface staleness to clients in the REST contract.

## Known gaps

- Approval history is not kept (a new approval overwrites the previous one of the same user and kind).
- No test combines ad edits with approvals under real concurrency.
- The design keeps the active, previous and agreed Proposal; the MVP keeps the active and previous ones, and the agreed terms travel inside `AgreementReached`.

## Evidence

- Code: `src/negotiation/Taakht.Negotiation/Domain/SwapNegotiation.cs` (`IsValid`, `HasAllApprovals`, `ReachAgreement`), `Application/NegotiationService.cs` (`TryReachAgreementAsync`), `ad_ref` in `migrations/001_init.sql`; the version check in `LockAds` in `src/ad/internal/ad/service.go`.
- Tests: `SwapNegotiationTests` (domain), `NegotiationPostgresTests.ApproveAdWithStaleVersionAborts`, `FinalSyncCheckAbortsWhenAnAdMovedBeforeItsEventArrived`, `AdEditedEventMakesApprovalStaleAndIgnoresLateDuplicates`.
- Overview: [MVP architecture](../../architecture/mvp-architecture.md), "Approval validity, versions and staleness".

## References

- [Take the exclusive ad lock in the Ad service](take-the-exclusive-ad-lock-in-the-ad-service.md)
- [Keep Negotiation and Swap as separate services](keep-negotiation-and-swap-as-separate-services.md)
