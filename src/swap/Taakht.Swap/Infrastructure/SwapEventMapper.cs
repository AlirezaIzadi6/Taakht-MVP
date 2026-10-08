using Google.Protobuf;
using Taakht.Swap.Domain;
using SwapPb = Taakht.Swap.V1;

namespace Taakht.Swap.Infrastructure;

public static class SwapEventMapper
{
    public const string Topic = "swap.events";

    public static IMessage ToMessage(SwapModel swap, SwapEvent ev) => ev switch
    {
        ExclusiveLockAcquiredEvent => new SwapPb.ExclusiveLockAcquired
        {
            SwapId = swap.Id.ToString(),
            NegotiationId = swap.NegotiationId,
            AdAId = swap.AdAId,
            AdBId = swap.AdBId,
        },
        SwapRejectedEvent r => new SwapPb.SwapRejected
        {
            SwapId = swap.Id.ToString(),
            NegotiationId = swap.NegotiationId,
            Reason = r.Reason,
        },
        SwapCompletedEvent => new SwapPb.SwapCompleted
        {
            SwapId = swap.Id.ToString(),
            AdAId = swap.AdAId,
            AdBId = swap.AdBId,
        },
        SwapCancelledEvent c => new SwapPb.SwapCancelled
        {
            SwapId = swap.Id.ToString(),
            AdAId = swap.AdAId,
            AdBId = swap.AdBId,
            Reason = c.Reason,
            DefaultingUserId = c.DefaultingUserId,
        },
        _ => throw new ArgumentOutOfRangeException(nameof(ev), ev, "unknown swap event"),
    };
}
