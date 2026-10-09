using Microsoft.Extensions.Logging;
using Npgsql;
using Taakht.Ad.V1;
using Taakht.Common.V1;
using Taakht.Negotiation.Domain;
using Taakht.Negotiation.Infrastructure;
using Taakht.Swap.V1;

namespace Taakht.Negotiation.Application;

/// <summary>
/// Consumers of ad.events and swap.events. They run inside the platform consumer's transaction
/// (dedupe row + these writes commit together) and check state, because events can arrive late or twice.
/// </summary>
public sealed class EventHandlers(TimeProvider clock, ILogger<EventHandlers> logger)
{
    public const string AdTopic = "ad.events";
    public const string SwapTopic = "swap.events";
    public const string CancelReasonLockedElsewhere = "ad locked by another swap";

    public IReadOnlyDictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>> Build() =>
        new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>
        {
            [AdPublished.Descriptor.FullName] = (c, t, e) => UpsertAdAsync(c, t, AdPublished.Parser.ParseFrom(e.Payload).Ad),
            [AdEdited.Descriptor.FullName] = (c, t, e) => UpsertAdAsync(c, t, AdEdited.Parser.ParseFrom(e.Payload).Ad),
            [AdReleased.Descriptor.FullName] = (c, t, e) => UpsertAdAsync(c, t, AdReleased.Parser.ParseFrom(e.Payload).Ad),
            [ExclusiveLockAcquired.Descriptor.FullName] = (c, t, e) =>
                OnExclusiveLockAsync(c, t, ExclusiveLockAcquired.Parser.ParseFrom(e.Payload)),
            [SwapRejected.Descriptor.FullName] = (c, t, e) => OnSwapRejectedAsync(c, t, SwapRejected.Parser.ParseFrom(e.Payload)),
            [SwapCancelled.Descriptor.FullName] = (c, t, e) => OnSwapCancelledAsync(c, t, SwapCancelled.Parser.ParseFrom(e.Payload)),
        };

    internal static Task UpsertAdAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Taakht.Ad.V1.Ad? ad) =>
        ad is null || string.IsNullOrEmpty(ad.Id)
            ? Task.CompletedTask
            : NegotiationRepository.UpsertAdRefAsync(
                conn, tx, new AdSnapshot(ad.Id, ad.OwnerId, ad.Version, GrpcAdClient.Map(ad.Status)), CancellationToken.None);

    /// <summary>
    /// The swap holds the lock on the winner's ads. The winner named by the event must exist, concern exactly those ads
    /// and still be AGREEMENT_PENDING (or already AGREED); otherwise the event is stale (an old swap, a negotiation that was
    /// cancelled since) and is ignored, in particular it must not cancel anybody else's negotiation.
    /// </summary>
    internal async Task OnExclusiveLockAsync(NpgsqlConnection conn, NpgsqlTransaction tx, ExclusiveLockAcquired e)
    {
        var now = clock.GetUtcNow();
        if (!Guid.TryParse(e.NegotiationId, out var id))
        {
            logger.LogError("ExclusiveLockAcquired {SwapId} has no valid negotiation id; ignored", e.SwapId);
            return;
        }

        var winner = await NegotiationRepository.LoadAsync(conn, tx, id, true, CancellationToken.None);
        if (winner is null)
        {
            logger.LogWarning("ExclusiveLockAcquired {SwapId} names unknown negotiation {NegotiationId}; ignored as stale", e.SwapId, id);
            return;
        }

        if (!IsPair(winner, e.AdAId, e.AdBId))
        {
            logger.LogError(
                "ExclusiveLockAcquired {SwapId} names ads ({AdA}, {AdB}) that do not belong to negotiation {NegotiationId}; ignored",
                e.SwapId, e.AdAId, e.AdBId, id);
            return;
        }

        if (winner.Status is not (NegotiationStatus.AgreementPending or NegotiationStatus.Agreed))
        {
            if (winner.Status == NegotiationStatus.Cancelled && winner.CancelReason == NegotiationService.AgreementTimedOutReason)
            {
                // Residual risk (see the architecture doc): the sweeper gave up on a swap that was only slow.
                logger.LogError(
                    "Swap {SwapId} holds the ads of negotiation {NegotiationId}, which the sweeper already cancelled ('{Reason}'); needs a manual look",
                    e.SwapId, id, winner.CancelReason);
            }
            else
            {
                logger.LogWarning(
                    "ExclusiveLockAcquired {SwapId} for negotiation {NegotiationId} which is {Status}; ignored as stale",
                    e.SwapId, id, winner.Status);
            }

            return;
        }

        if (winner.MarkAgreed(now))
        {
            await NegotiationRepository.SaveAsync(conn, tx, winner, CancellationToken.None);
        }

        // The winner's ads are locked: every other live negotiation that involves either of them is dead.
        var competitors = await NegotiationRepository.LockLiveInvolvingAsync(
            conn, tx, [winner.RequesterAdId, winner.TargetAdId], id, CancellationToken.None);
        foreach (var other in await NegotiationRepository.LoadManyAsync(conn, tx, competitors, true, CancellationToken.None))
        {
            if (!other.Cancel(CancelReasonLockedElsewhere, now))
            {
                continue;
            }

            await NegotiationRepository.SaveAsync(conn, tx, other, CancellationToken.None);
            await NegotiationService.AddClosedEventAsync(
                conn, tx, other, NegotiationStatus.Cancelled, CancelReasonLockedElsewhere, CancellationToken.None);
        }
    }

    private static bool IsPair(SwapNegotiation n, string adAId, string adBId) =>
        (n.RequesterAdId == adAId && n.TargetAdId == adBId) || (n.RequesterAdId == adBId && n.TargetAdId == adAId);

    internal async Task OnSwapRejectedAsync(NpgsqlConnection conn, NpgsqlTransaction tx, SwapRejected e)
    {
        if (!Guid.TryParse(e.NegotiationId, out var id))
        {
            return;
        }

        var n = await NegotiationRepository.LoadAsync(conn, tx, id, true, CancellationToken.None);
        if (n is null)
        {
            logger.LogWarning("SwapRejected for unknown negotiation {NegotiationId}", id);
            return;
        }

        // Only a negotiation waiting for the lock can be rejected by the swap; an agreed one stays agreed.
        if (n.Status != NegotiationStatus.AgreementPending)
        {
            return;
        }

        n.Cancel(e.Reason, clock.GetUtcNow());
        await NegotiationRepository.SaveAsync(conn, tx, n, CancellationToken.None);
        await NegotiationService.AddClosedEventAsync(conn, tx, n, NegotiationStatus.Cancelled, e.Reason, CancellationToken.None);
    }

    /// <summary>
    /// The swap behind an agreed negotiation was cancelled. The event names the negotiation; only an event without
    /// that field (published before it existed, still in flight) falls back to finding the AGREED negotiation by its ad
    /// pair (the oldest if a stale duplicate exists). Competitors cancelled when the lock was taken stay cancelled.
    /// </summary>
    internal async Task OnSwapCancelledAsync(NpgsqlConnection conn, NpgsqlTransaction tx, SwapCancelled e)
    {
        Guid id;
        if (e.NegotiationId.Length > 0)
        {
            if (!Guid.TryParse(e.NegotiationId, out id))
            {
                logger.LogError("SwapCancelled {SwapId} has an invalid negotiation id '{NegotiationId}'; ignored", e.SwapId, e.NegotiationId);
                return;
            }
        }
        else
        {
            if (string.IsNullOrEmpty(e.AdAId) || string.IsNullOrEmpty(e.AdBId))
            {
                return;
            }

            var ids = await NegotiationRepository.LockAgreedForPairAsync(conn, tx, e.AdAId, e.AdBId, CancellationToken.None);
            if (ids.Count == 0)
            {
                logger.LogWarning("SwapCancelled {SwapId} has no negotiation id and matches no agreed negotiation, ignoring", e.SwapId);
                return;
            }

            id = ids[0];
        }

        var n = await NegotiationRepository.LoadAsync(conn, tx, id, true, CancellationToken.None);
        if (n is null)
        {
            logger.LogWarning("SwapCancelled {SwapId} names unknown negotiation {NegotiationId}, ignoring", e.SwapId, id);
            return;
        }

        if (!n.CancelAgreed(e.Reason, clock.GetUtcNow()))
        {
            return;
        }

        await NegotiationRepository.SaveAsync(conn, tx, n, CancellationToken.None);
        await NegotiationService.AddClosedEventAsync(conn, tx, n, NegotiationStatus.Cancelled, e.Reason, CancellationToken.None);
    }
}
