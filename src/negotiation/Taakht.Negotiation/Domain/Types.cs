namespace Taakht.Negotiation.Domain;

public enum DeliveryMethod
{
    InPerson = 1,
    Locker = 2,
}

public enum NegotiationStatus
{
    Open = 1,
    AgreementPending = 2,
    Agreed = 3,
    Declined = 4,
    Withdrawn = 5,
    Cancelled = 6,
}

public enum ApprovalKind
{
    Ad = 1,
    Terms = 2,
}

public enum AdStatus
{
    Published = 1,
    Hidden = 2,
    Locked = 3,
    Closed = 4,
}

/// <summary>Local view of an ad: what the negotiation needs to know to validate it.</summary>
public sealed record AdSnapshot(string AdId, string OwnerId, int Version, AdStatus Status)
{
    public bool IsLockable => Status is AdStatus.Published or AdStatus.Hidden;
}

/// <summary>Current versions of the two ads of a negotiation.</summary>
public sealed record AdVersions(int RequesterAdVersion, int TargetAdVersion);

public sealed record Terms(DeliveryMethod LegA, DeliveryMethod LegB, long PriceDifference, string PayerUserId)
{
    public static Terms Default { get; } = new(DeliveryMethod.InPerson, DeliveryMethod.InPerson, 0, string.Empty);
}

public sealed record Proposal(Guid Id, int Number, Terms Terms, string? AuthorUserId);

public sealed record Approval(string UserId, ApprovalKind Kind, int Target);

public sealed record ApprovalView(Approval Approval, bool Valid);
