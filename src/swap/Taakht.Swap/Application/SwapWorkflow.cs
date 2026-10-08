using Microsoft.Extensions.Logging;
using Taakht.Negotiation.V1;
using Taakht.Platform;
using Taakht.Swap.Domain;
using Taakht.Swap.Infrastructure;
using DomainDelivery = Taakht.Swap.Domain.DeliveryMethod;
using PbDelivery = Taakht.Negotiation.V1.DeliveryMethod;

namespace Taakht.Swap.Application;

public sealed record SwapOptions(TimeSpan PaymentDeadline);

/// <summary>Orchestrates the domain state machine with the store, the Ad service and the locker partner.</summary>
public sealed partial class SwapWorkflow(
    SwapStore store,
    IAdClient ads,
    IDeliveryProvider delivery,
    SwapOptions options,
    TimeProvider clock,
    ILogger<SwapWorkflow> logger)
{
    public const string SystemUser = SystemIdentities.Swap;

    /// <summary>
    /// Handles AgreementReached. Safe to re-run at every step: the swap row is created once per negotiation,
    /// the Ad-side lock is idempotent by swap id, and each state change is a state-checked short transaction.
    /// Transient Ad failures propagate so the consumer retries.
    /// </summary>
    public async Task HandleAgreementReachedAsync(AgreementReached msg, CancellationToken ct)
    {
        if (msg.AdA is null || msg.AdB is null || msg.NegotiationId.Length == 0)
        {
            LogMalformed(msg.NegotiationId);
            return;
        }

        var terms = msg.Terms ?? new Terms();
        var draft = SwapModel.Create(
            Guid.NewGuid(),
            msg.NegotiationId,
            msg.AdA.AdId,
            msg.AdA.Version,
            msg.AdB.AdId,
            msg.AdB.Version,
            new SwapLeg(msg.AdA.OwnerId, ToDomain(terms.LegA), false),
            new SwapLeg(msg.AdB.OwnerId, ToDomain(terms.LegB), false),
            clock.GetUtcNow());

        var swap = await store.InsertIfAbsentAsync(draft, ct);
        if (swap.Status != SwapStatus.Locking)
        {
            return;
        }

        LockOutcome outcome;
        using (UserContext.Use(SystemUser))
        {
            outcome = await ads.LockAdsAsync(
                swap.Id,
                [new AdVersionRef(swap.AdAId, swap.AdAVersion), new AdVersionRef(swap.AdBId, swap.AdBVersion)],
                ct);
        }

        var result = outcome switch
        {
            LockOutcome.Rejected rejected => await store.ApplyAsync(swap.Id, s => SwapMachine.LockRejected(s, rejected.Reason), ct),
            _ => await store.ApplyAsync(swap.Id, s => SwapMachine.LockAcquired(s, clock.GetUtcNow(), options.PaymentDeadline), ct),
        };
        LogProgress(swap.Id, swap.NegotiationId, result?.Swap.Status);
    }

    /// <summary>Records a locker fee payment (mock partner webhook).</summary>
    public async Task<SwapModel> RecordFeePaidAsync(Guid swapId, string userId, CancellationToken ct)
    {
        var result = await store.ApplyAsync(swapId, s => SwapMachine.FeePaid(s, userId), ct)
            ?? throw new KeyNotFoundException("swap not found");
        return result.Swap;
    }

    /// <summary>Cancels swaps whose payment deadline passed with fees still unpaid. Returns the number cancelled.</summary>
    public async Task<int> SweepOverdueAsync(CancellationToken ct)
    {
        var cancelled = 0;
        foreach (var id in await store.ListOverdueAsync(clock.GetUtcNow(), ct))
        {
            var swap = await store.GetAsync(id, ct);
            if (swap is null)
            {
                continue;
            }

            // Ask the partner before giving up: a payment may have been made that we have not recorded yet.
            var paidUsers = new List<string>();
            foreach (var leg in new[] { swap.LegA, swap.LegB }.Where(l => l.OwesFee))
            {
                if (await delivery.GetPaymentStatusAsync(id, leg.OwnerUserId, ct))
                {
                    paidUsers.Add(leg.OwnerUserId);
                }
            }

            var result = await store.ApplyAsync(id, s => TimeoutAfterPartnerCheck(s, paidUsers), ct);
            if (result is { Changed: true, Swap.Status: SwapStatus.Cancelled })
            {
                cancelled++;
                LogCancelled(id, result.Swap.CancelReason);
            }
        }

        return cancelled;
    }

    private Transition TimeoutAfterPartnerCheck(SwapModel swap, List<string> paidUsers)
    {
        if (swap.Status != SwapStatus.AwaitingPayment)
        {
            return Transition.NoChange(swap);
        }

        var current = swap;
        var events = new List<SwapEvent>();
        var changed = false;
        foreach (var user in paidUsers)
        {
            var t = SwapMachine.FeePaid(current, user);
            current = t.Swap;
            events.AddRange(t.Events);
            changed |= t.Changed;
        }

        var timeout = SwapMachine.PaymentTimedOut(current, clock.GetUtcNow());
        events.AddRange(timeout.Events);
        return new Transition(timeout.Swap, events, changed || timeout.Changed);
    }

    private static DomainDelivery ToDomain(PbDelivery method) =>
        method == PbDelivery.Locker ? DomainDelivery.Locker : DomainDelivery.InPerson;

    [LoggerMessage(Level = LogLevel.Warning, Message = "Ignoring malformed AgreementReached for negotiation '{NegotiationId}'")]
    private partial void LogMalformed(string negotiationId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Swap {SwapId} for negotiation {NegotiationId} is now {Status}")]
    private partial void LogProgress(Guid swapId, string negotiationId, SwapStatus? status);

    [LoggerMessage(Level = LogLevel.Information, Message = "Swap {SwapId} cancelled: {Reason}")]
    private partial void LogCancelled(Guid swapId, string reason);
}
