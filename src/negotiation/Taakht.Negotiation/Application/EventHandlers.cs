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
        };

    internal static Task UpsertAdAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Taakht.Ad.V1.Ad? ad) =>
        ad is null || string.IsNullOrEmpty(ad.Id)
            ? Task.CompletedTask
            : NegotiationRepository.UpsertAdRefAsync(
                conn, tx, new AdSnapshot(ad.Id, ad.OwnerId, ad.Version, GrpcAdClient.Map(ad.Status)), CancellationToken.None);

    internal async Task OnExclusiveLockAsync(NpgsqlConnection conn, NpgsqlTransaction tx, ExclusiveLockAcquired e)
    {
        var now = clock.GetUtcNow();
        if (!Guid.TryParse(e.NegotiationId, out var id))
        {
            logger.LogWarning("ExclusiveLockAcquired {SwapId} has no valid negotiation id", e.SwapId);
            return;
        }

        var agreed = await NegotiationRepository.LoadAsync(conn, tx, id, true, CancellationToken.None);
        if (agreed is null)
        {
            logger.LogWarning("ExclusiveLockAcquired for unknown negotiation {NegotiationId}", id);
        }
        else if (agreed.MarkAgreed(now))
        {
            await NegotiationRepository.SaveAsync(conn, tx, agreed, CancellationToken.None);
        }

        // The ads are locked whatever happened to the winning negotiation: every competitor is dead.
        var competitors = await NegotiationRepository.LockLiveInvolvingAsync(conn, tx, [e.AdAId, e.AdBId], id, CancellationToken.None);
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
}
