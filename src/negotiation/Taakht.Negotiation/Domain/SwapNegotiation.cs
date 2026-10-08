namespace Taakht.Negotiation.Domain;

/// <summary>
/// Aggregate root. Pure state machine: no I/O. Callers pass in the facts they fetched
/// (current ad snapshots) and persist the result.
/// </summary>
public sealed class SwapNegotiation
{
    private readonly List<Approval> _approvals;

    private SwapNegotiation(
        Guid id,
        string requesterUserId,
        string requesterAdId,
        string targetUserId,
        string targetAdId,
        NegotiationStatus status,
        Proposal active,
        Proposal? previous,
        int lastProposalNumber,
        string cancelReason,
        int version,
        DateTimeOffset createdAt,
        DateTimeOffset updatedAt,
        IEnumerable<Approval> approvals)
    {
        Id = id;
        RequesterUserId = requesterUserId;
        RequesterAdId = requesterAdId;
        TargetUserId = targetUserId;
        TargetAdId = targetAdId;
        Status = status;
        ActiveProposal = active;
        PreviousProposal = previous;
        LastProposalNumber = lastProposalNumber;
        CancelReason = cancelReason;
        Version = version;
        CreatedAt = createdAt;
        UpdatedAt = updatedAt;
        _approvals = [.. approvals];
    }

    public Guid Id { get; }

    public string RequesterUserId { get; }

    public string RequesterAdId { get; }

    public string TargetUserId { get; }

    public string TargetAdId { get; }

    public NegotiationStatus Status { get; private set; }

    public Proposal ActiveProposal { get; private set; }

    /// <summary>The proposal restored if the active one is rejected. Only one level is kept.</summary>
    public Proposal? PreviousProposal { get; private set; }

    public int LastProposalNumber { get; private set; }

    public string CancelReason { get; private set; }

    public int Version { get; private set; }

    public DateTimeOffset CreatedAt { get; }

    public DateTimeOffset UpdatedAt { get; private set; }

    public IReadOnlyList<Approval> Approvals => _approvals;

    public static SwapNegotiation Restore(
        Guid id,
        string requesterUserId,
        string requesterAdId,
        string targetUserId,
        string targetAdId,
        NegotiationStatus status,
        Proposal active,
        Proposal? previous,
        int lastProposalNumber,
        string cancelReason,
        int version,
        DateTimeOffset createdAt,
        DateTimeOffset updatedAt,
        IEnumerable<Approval> approvals) =>
        new(id, requesterUserId, requesterAdId, targetUserId, targetAdId, status, active, previous,
            lastProposalNumber, cancelReason, version, createdAt, updatedAt, approvals);

    /// <summary>
    /// Opens a negotiation: proposal 1 and the requester's approval of the target ad's current version.
    /// </summary>
    public static SwapNegotiation Open(Guid id, AdSnapshot requesterAd, AdSnapshot targetAd, string requesterUserId, DateTimeOffset now)
    {
        if (requesterAd.AdId == targetAd.AdId)
        {
            throw new DomainException(DomainError.InvalidArgument, "an ad cannot negotiate with itself");
        }

        if (requesterAd.OwnerId != requesterUserId)
        {
            throw new DomainException(DomainError.PermissionDenied, "caller does not own the requester ad");
        }

        if (requesterAd.OwnerId == targetAd.OwnerId)
        {
            throw new DomainException(DomainError.FailedPrecondition, "both ads have the same owner");
        }

        if (!requesterAd.IsLockable)
        {
            throw new DomainException(DomainError.FailedPrecondition, "the requester ad must be published or hidden");
        }

        if (targetAd.Status != AdStatus.Published)
        {
            throw new DomainException(DomainError.FailedPrecondition, "the target ad must be published");
        }

        var first = new Proposal(Guid.NewGuid(), 1, Terms.Default, null);
        return new SwapNegotiation(
            id, requesterUserId, requesterAd.AdId, targetAd.OwnerId, targetAd.AdId,
            NegotiationStatus.Open, first, null, 1, string.Empty, 1, now, now,
            [new Approval(requesterUserId, ApprovalKind.Ad, targetAd.Version)]);
    }

    public static bool IsOpenLike(NegotiationStatus status) =>
        status is NegotiationStatus.Open or NegotiationStatus.AgreementPending;

    public bool IsParty(string userId) => userId == RequesterUserId || userId == TargetUserId;

    /// <summary>Id of the ad the given party approves (the other side's ad).</summary>
    public string AdApprovedBy(string userId) => userId == RequesterUserId ? TargetAdId : RequesterAdId;

    /// <summary>Owners of the legs that use a locker. Leg A belongs to the requester, leg B to the target.</summary>
    public IReadOnlyList<string> LockerLegOwners(Terms terms)
    {
        var owners = new List<string>(2);
        if (terms.LegA == DeliveryMethod.Locker)
        {
            owners.Add(RequesterUserId);
        }

        if (terms.LegB == DeliveryMethod.Locker)
        {
            owners.Add(TargetUserId);
        }

        return owners;
    }

    public bool IsValid(Approval approval, AdVersions versions) => approval.Kind switch
    {
        ApprovalKind.Terms => approval.Target == ActiveProposal.Number,
        ApprovalKind.Ad => approval.UserId == RequesterUserId
            ? approval.Target == versions.TargetAdVersion
            : approval.Target == versions.RequesterAdVersion,
        _ => false,
    };

    public IReadOnlyList<ApprovalView> ApprovalViews(AdVersions versions) =>
        [.. _approvals.Select(a => new ApprovalView(a, IsValid(a, versions)))];

    /// <summary>True when each party has a valid AD approval and a valid TERMS approval.</summary>
    public bool HasAllApprovals(AdVersions versions)
    {
        foreach (var user in new[] { RequesterUserId, TargetUserId })
        {
            foreach (var kind in new[] { ApprovalKind.Ad, ApprovalKind.Terms })
            {
                var approval = _approvals.Find(a => a.UserId == user && a.Kind == kind);
                if (approval is null || !IsValid(approval, versions))
                {
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// The caller approves the other side's ad at the version they saw.
    /// <paramref name="otherAd"/> is the current state of that ad, fetched synchronously.
    /// </summary>
    public void ApproveAd(string userId, int adVersion, AdSnapshot otherAd, DateTimeOffset now)
    {
        RequireOpenParty(userId);
        if (otherAd.AdId != AdApprovedBy(userId))
        {
            throw new DomainException(DomainError.InvalidArgument, "snapshot is not the other side's ad");
        }

        if (adVersion != otherAd.Version)
        {
            throw new DomainException(DomainError.Aborted, $"ad version {adVersion} is not current ({otherAd.Version}); re-read the ad");
        }

        if (!otherAd.IsLockable)
        {
            throw new DomainException(DomainError.FailedPrecondition, "the ad is not available for a swap");
        }

        SetApproval(new Approval(userId, ApprovalKind.Ad, adVersion));
        Touch(now);
    }

    /// <summary>Creates a new active proposal. The author's TERMS approval is implicit.</summary>
    public void Revise(string userId, int seenProposalNumber, Terms terms, DateTimeOffset now)
    {
        RequireOpenParty(userId);
        RequireActiveNumber(seenProposalNumber);
        ValidateTerms(terms);

        var number = LastProposalNumber + 1;
        PreviousProposal = ActiveProposal;
        ActiveProposal = new Proposal(Guid.NewGuid(), number, terms, userId);
        LastProposalNumber = number;
        SetApproval(new Approval(userId, ApprovalKind.Terms, number));
        Touch(now);
    }

    public void ApproveProposal(string userId, int proposalNumber, DateTimeOffset now)
    {
        RequireOpenParty(userId);
        RequireActiveNumber(proposalNumber);
        SetApproval(new Approval(userId, ApprovalKind.Terms, proposalNumber));
        Touch(now);
    }

    /// <summary>
    /// Rejects the active proposal and restores the previous one. Either party may do it:
    /// the author withdrawing their own revision has the same effect.
    /// </summary>
    public void RejectProposal(string userId, int proposalNumber, DateTimeOffset now)
    {
        RequireOpenParty(userId);
        RequireActiveNumber(proposalNumber);
        if (PreviousProposal is null)
        {
            throw new DomainException(DomainError.FailedPrecondition, "there is no earlier proposal to restore");
        }

        ActiveProposal = PreviousProposal;
        PreviousProposal = null;
        Touch(now);
    }

    /// <summary>Target owner declines, requester withdraws. Only from Open.</summary>
    public NegotiationStatus Close(string userId, DateTimeOffset now)
    {
        if (!IsParty(userId))
        {
            throw new DomainException(DomainError.PermissionDenied, "caller is not a party of this negotiation");
        }

        if (Status != NegotiationStatus.Open)
        {
            throw new DomainException(DomainError.FailedPrecondition, $"negotiation is {Status}, only an open negotiation can be closed");
        }

        Status = userId == TargetUserId ? NegotiationStatus.Declined : NegotiationStatus.Withdrawn;
        Touch(now);
        return Status;
    }

    /// <summary>
    /// All four approvals are valid against the freshly synced versions: move to AgreementPending.
    /// </summary>
    public void ReachAgreement(AdVersions synced, DateTimeOffset now)
    {
        if (Status != NegotiationStatus.Open)
        {
            throw new DomainException(DomainError.FailedPrecondition, $"negotiation is {Status}");
        }

        if (!HasAllApprovals(synced))
        {
            throw new DomainException(DomainError.Aborted, "approvals changed; re-read the negotiation");
        }

        Status = NegotiationStatus.AgreementPending;
        Touch(now);
    }

    /// <summary>The swap holds the exclusive lock. Returns false when the event is irrelevant (idempotent).</summary>
    public bool MarkAgreed(DateTimeOffset now)
    {
        if (!IsOpenLike(Status))
        {
            return false;
        }

        Status = NegotiationStatus.Agreed;
        Touch(now);
        return true;
    }

    /// <summary>System cancellation. Returns false when the negotiation is already closed.</summary>
    public bool Cancel(string reason, DateTimeOffset now)
    {
        if (!IsOpenLike(Status))
        {
            return false;
        }

        Status = NegotiationStatus.Cancelled;
        CancelReason = reason;
        Touch(now);
        return true;
    }

    private void ValidateTerms(Terms terms)
    {
        if (!Enum.IsDefined(terms.LegA) || !Enum.IsDefined(terms.LegB))
        {
            throw new DomainException(DomainError.InvalidArgument, "both delivery methods are required");
        }

        if (terms.PriceDifference < 0)
        {
            throw new DomainException(DomainError.InvalidArgument, "price difference cannot be negative");
        }

        if (terms.PriceDifference == 0)
        {
            if (terms.PayerUserId.Length != 0)
            {
                throw new DomainException(DomainError.InvalidArgument, "payer must be empty when there is no price difference");
            }

            return;
        }

        if (!IsParty(terms.PayerUserId))
        {
            throw new DomainException(DomainError.InvalidArgument, "the payer must be one of the parties");
        }
    }

    private void RequireOpenParty(string userId)
    {
        if (!IsParty(userId))
        {
            throw new DomainException(DomainError.PermissionDenied, "caller is not a party of this negotiation");
        }

        if (Status != NegotiationStatus.Open)
        {
            throw new DomainException(DomainError.FailedPrecondition, $"negotiation is {Status}");
        }
    }

    private void RequireActiveNumber(int number)
    {
        if (number != ActiveProposal.Number)
        {
            throw new DomainException(DomainError.Aborted, $"proposal {number} is not active (active is {ActiveProposal.Number}); re-read the negotiation");
        }
    }

    private void SetApproval(Approval approval)
    {
        _approvals.RemoveAll(a => a.UserId == approval.UserId && a.Kind == approval.Kind);
        _approvals.Add(approval);
    }

    private void Touch(DateTimeOffset now)
    {
        Version++;
        UpdatedAt = now;
    }
}
