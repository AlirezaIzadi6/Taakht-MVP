namespace Taakht.Swap.Domain;

public enum SwapStatus
{
    Locking,
    Rejected,
    AwaitingPayment,
    Completed,
    Cancelled,
}

public enum DeliveryMethod
{
    InPerson,
    Locker,
}

public sealed record SwapLeg(string OwnerUserId, DeliveryMethod Method, bool FeePaid)
{
    public bool NeedsFee => Method == DeliveryMethod.Locker;

    public bool OwesFee => NeedsFee && !FeePaid;
}

public sealed record SwapModel(
    Guid Id,
    string NegotiationId,
    string AdAId,
    int AdAVersion,
    string AdBId,
    int AdBVersion,
    SwapStatus Status,
    SwapLeg LegA,
    SwapLeg LegB,
    DateTimeOffset? PaymentDeadline,
    string CancelReason,
    DateTimeOffset CreatedAt)
{
    public static SwapModel Create(
        Guid id, string negotiationId, string adAId, int adAVersion, string adBId, int adBVersion,
        SwapLeg legA, SwapLeg legB, DateTimeOffset now) =>
        new(id, negotiationId, adAId, adAVersion, adBId, adBVersion, SwapStatus.Locking, legA, legB, null, "", now);

    public bool IsParty(string userId) => LegA.OwnerUserId == userId || LegB.OwnerUserId == userId;
}

/// <summary>Domain events produced by state transitions; mapped to protobuf messages at the edge.</summary>
public abstract record SwapEvent;

public sealed record ExclusiveLockAcquiredEvent : SwapEvent;

public sealed record SwapRejectedEvent(string Reason) : SwapEvent;

public sealed record SwapCompletedEvent : SwapEvent;

public sealed record SwapCancelledEvent(string Reason, string DefaultingUserId) : SwapEvent;

/// <summary>Result of a transition. <see cref="Changed"/> is false when the call was a no-op replay.</summary>
public sealed record Transition(SwapModel Swap, IReadOnlyList<SwapEvent> Events, bool Changed)
{
    public static Transition NoChange(SwapModel swap) => new(swap, [], false);
}

public enum DomainErrorKind
{
    FailedPrecondition,
}

public sealed class SwapDomainException(DomainErrorKind kind, string message) : Exception(message)
{
    public DomainErrorKind Kind { get; } = kind;
}
