using Taakht.Negotiation.Domain;

namespace Taakht.Negotiation.Tests.Domain;

public class SwapNegotiationTests
{
    private const string _requester = "user-1";
    private const string _target = "user-2";
    private static readonly DateTimeOffset _now = new(2026, 10, 8, 12, 0, 0, TimeSpan.Zero);

    private static readonly AdSnapshot _requesterAd = new("ad-a", _requester, 1, AdStatus.Published);
    private static readonly AdSnapshot _targetAd = new("ad-b", _target, 3, AdStatus.Published);
    private static readonly AdVersions _versions = new(1, 3);

    private static SwapNegotiation Open() =>
        SwapNegotiation.Open(Guid.NewGuid(), _requesterAd, _targetAd, _requester, _now);

    private static SwapNegotiation AllApproved()
    {
        var n = Open();
        n.ApproveAd(_target, 1, _requesterAd, _now);
        n.ApproveProposal(_requester, 1, _now);
        n.ApproveProposal(_target, 1, _now);
        return n;
    }

    private static DomainError ErrorOf(Action act) => Assert.Throws<DomainException>(act).Error;

    [Fact]
    public void Open_CreatesProposalOneAndRequesterAdApproval()
    {
        var n = Open();

        Assert.Equal(NegotiationStatus.Open, n.Status);
        Assert.Equal(1, n.ActiveProposal.Number);
        Assert.Equal(Terms.Default, n.ActiveProposal.Terms);
        Assert.Null(n.ActiveProposal.AuthorUserId);
        Assert.Equal(_target, n.TargetUserId);
        var approval = Assert.Single(n.Approvals);
        Assert.Equal(new Approval(_requester, ApprovalKind.Ad, 3), approval);
    }

    [Fact]
    public void Open_AllowsHiddenRequesterAd()
    {
        var hidden = _requesterAd with { Status = AdStatus.Hidden };
        var n = SwapNegotiation.Open(Guid.NewGuid(), hidden, _targetAd, _requester, _now);
        Assert.Equal(NegotiationStatus.Open, n.Status);
    }

    [Theory]
    [InlineData(AdStatus.Locked)]
    [InlineData(AdStatus.Closed)]
    public void Open_RejectsUnavailableRequesterAd(AdStatus status) =>
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() =>
            SwapNegotiation.Open(Guid.NewGuid(), _requesterAd with { Status = status }, _targetAd, _requester, _now)));

    [Theory]
    [InlineData(AdStatus.Hidden)]
    [InlineData(AdStatus.Locked)]
    [InlineData(AdStatus.Closed)]
    public void Open_RequiresTargetPublished_AndAnswersLikeAMissingAd(AdStatus status)
    {
        var ex = Assert.Throws<DomainException>(() =>
            SwapNegotiation.Open(Guid.NewGuid(), _requesterAd, _targetAd with { Status = status }, _requester, _now));
        Assert.Equal(DomainError.NotFound, ex.Error);
        Assert.Equal(SwapNegotiation.AdNotAvailable, ex.Message);
    }

    [Fact]
    public void Open_RequiresCallerToOwnRequesterAd_AndAnswersLikeAMissingAd()
    {
        var ex = Assert.Throws<DomainException>(() =>
            SwapNegotiation.Open(Guid.NewGuid(), _requesterAd, _targetAd, "user-3", _now));
        Assert.Equal(DomainError.NotFound, ex.Error);
        Assert.Equal(SwapNegotiation.AdNotAvailable, ex.Message);
    }

    [Fact]
    public void Open_RejectsSameOwner() =>
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() =>
            SwapNegotiation.Open(Guid.NewGuid(), _requesterAd, _targetAd with { OwnerId = _requester }, _requester, _now)));

    [Fact]
    public void Open_RejectsSameAd() =>
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() =>
            SwapNegotiation.Open(Guid.NewGuid(), _requesterAd, _requesterAd, _requester, _now)));

    [Fact]
    public void ApproveAd_ByTargetOwner_RecordsApprovalOnRequesterAd()
    {
        var n = Open();

        n.ApproveAd(_target, 1, _requesterAd, _now);

        Assert.Contains(new Approval(_target, ApprovalKind.Ad, 1), n.Approvals);
    }

    [Fact]
    public void ApproveAd_StaleVersion_Aborts()
    {
        var n = Open();
        Assert.Equal(DomainError.Aborted, ErrorOf(() => n.ApproveAd(_target, 1, _requesterAd with { Version = 2 }, _now)));
    }

    [Fact]
    public void ApproveAd_NotAParty_PermissionDenied()
    {
        var n = Open();
        Assert.Equal(DomainError.PermissionDenied, ErrorOf(() => n.ApproveAd("user-3", 1, _requesterAd, _now)));
    }

    [Fact]
    public void ApproveAd_AdNotLockable_FailedPrecondition()
    {
        var n = Open();
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() =>
            n.ApproveAd(_target, 1, _requesterAd with { Status = AdStatus.Locked }, _now)));
    }

    [Fact]
    public void ApproveAd_WrongAdSnapshot_InvalidArgument()
    {
        var n = Open();
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() => n.ApproveAd(_target, 3, _targetAd, _now)));
    }

    [Fact]
    public void ApproveAd_Again_OverwritesWithoutDuplicates()
    {
        var n = Open();
        n.ApproveAd(_requester, 4, _targetAd with { Version = 4 }, _now);

        Assert.Single(n.Approvals, a => a.UserId == _requester && a.Kind == ApprovalKind.Ad);
        Assert.Contains(new Approval(_requester, ApprovalKind.Ad, 4), n.Approvals);
    }

    [Fact]
    public void Revise_CreatesNewActiveProposal_KeepsPrevious_AuthorApprovesImplicitly()
    {
        var n = Open();
        var terms = new Terms(DeliveryMethod.Locker, DeliveryMethod.InPerson, 500_000, _requester);

        n.Revise(_target, 1, terms, _now);

        Assert.Equal(2, n.ActiveProposal.Number);
        Assert.Equal(_target, n.ActiveProposal.AuthorUserId);
        Assert.Equal(terms, n.ActiveProposal.Terms);
        Assert.Equal(1, n.PreviousProposal?.Number);
        Assert.Contains(new Approval(_target, ApprovalKind.Terms, 2), n.Approvals);
    }

    [Fact]
    public void Revise_MakesOtherSidesTermsApprovalStale()
    {
        var n = Open();
        n.ApproveProposal(_requester, 1, _now);

        n.Revise(_target, 1, Terms.Default, _now);

        var views = n.ApprovalViews(_versions);
        Assert.False(views.Single(v => v.Approval.UserId == _requester && v.Approval.Kind == ApprovalKind.Terms).Valid);
        Assert.True(views.Single(v => v.Approval.UserId == _target && v.Approval.Kind == ApprovalKind.Terms).Valid);
    }

    [Fact]
    public void Revise_StaleSeenNumber_Aborts()
    {
        var n = Open();
        n.Revise(_target, 1, Terms.Default, _now);
        Assert.Equal(DomainError.Aborted, ErrorOf(() => n.Revise(_requester, 1, Terms.Default, _now)));
    }

    [Fact]
    public void Revise_PayerMustBeAParty()
    {
        var n = Open();
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() =>
            n.Revise(_target, 1, new Terms(DeliveryMethod.InPerson, DeliveryMethod.InPerson, 10, "user-3"), _now)));
    }

    [Fact]
    public void Revise_PriceRequiresPayer()
    {
        var n = Open();
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() =>
            n.Revise(_target, 1, new Terms(DeliveryMethod.InPerson, DeliveryMethod.InPerson, 10, string.Empty), _now)));
    }

    [Fact]
    public void Revise_PayerWithoutPrice_Invalid()
    {
        var n = Open();
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() =>
            n.Revise(_target, 1, new Terms(DeliveryMethod.InPerson, DeliveryMethod.InPerson, 0, _target), _now)));
    }

    [Fact]
    public void Revise_NegativePrice_Invalid()
    {
        var n = Open();
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() =>
            n.Revise(_target, 1, new Terms(DeliveryMethod.InPerson, DeliveryMethod.InPerson, -1, _target), _now)));
    }

    [Fact]
    public void Revise_PriceUpperBound()
    {
        var n = Open();
        n.Revise(_target, 1, new Terms(DeliveryMethod.InPerson, DeliveryMethod.InPerson, SwapNegotiation.MaxPriceDifference, _target), _now);
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() =>
            n.Revise(_requester, 2, new Terms(DeliveryMethod.InPerson, DeliveryMethod.InPerson, SwapNegotiation.MaxPriceDifference + 1, _target), _now)));
    }

    [Fact]
    public void CancelAgreed_OnlyFromAgreed()
    {
        var n = AllApproved();
        Assert.False(n.CancelAgreed("late", _now));
        n.ReachAgreement(_versions, _now);
        Assert.False(n.CancelAgreed("not yet locked", _now));
        Assert.True(n.MarkAgreed(_now));
        Assert.True(n.CancelAgreed("fee not paid", _now));
        Assert.Equal(NegotiationStatus.Cancelled, n.Status);
        Assert.Equal("fee not paid", n.CancelReason);
        Assert.False(n.CancelAgreed("again", _now));
    }

    [Fact]
    public void Revise_UnspecifiedDelivery_Invalid()
    {
        var n = Open();
        Assert.Equal(DomainError.InvalidArgument, ErrorOf(() =>
            n.Revise(_target, 1, new Terms(0, DeliveryMethod.InPerson, 0, string.Empty), _now)));
    }

    [Fact]
    public void LockerLegOwners_LegAIsRequester_LegBIsTarget()
    {
        var n = Open();

        Assert.Equal([_requester], n.LockerLegOwners(new Terms(DeliveryMethod.Locker, DeliveryMethod.InPerson, 0, string.Empty)));
        Assert.Equal([_target], n.LockerLegOwners(new Terms(DeliveryMethod.InPerson, DeliveryMethod.Locker, 0, string.Empty)));
        Assert.Equal([_requester, _target], n.LockerLegOwners(new Terms(DeliveryMethod.Locker, DeliveryMethod.Locker, 0, string.Empty)));
        Assert.Empty(n.LockerLegOwners(Terms.Default));
    }

    [Fact]
    public void ApproveProposal_StaleNumber_Aborts()
    {
        var n = Open();
        n.Revise(_target, 1, Terms.Default, _now);
        Assert.Equal(DomainError.Aborted, ErrorOf(() => n.ApproveProposal(_requester, 1, _now)));
    }

    [Fact]
    public void ApproveProposal_RecordsApprovalOnActiveNumber()
    {
        var n = Open();
        n.Revise(_target, 1, Terms.Default, _now);

        n.ApproveProposal(_requester, 2, _now);

        Assert.Contains(new Approval(_requester, ApprovalKind.Terms, 2), n.Approvals);
    }

    [Fact]
    public void RejectProposal_RestoresPrevious()
    {
        var n = Open();
        n.Revise(_target, 1, new Terms(DeliveryMethod.Locker, DeliveryMethod.Locker, 0, string.Empty), _now);

        n.RejectProposal(_requester, 2, _now);

        Assert.Equal(1, n.ActiveProposal.Number);
        Assert.Null(n.PreviousProposal);
    }

    [Fact]
    public void RejectProposal_ByAuthor_AlsoRestores()
    {
        var n = Open();
        n.Revise(_target, 1, Terms.Default, _now);

        n.RejectProposal(_target, 2, _now);

        Assert.Equal(1, n.ActiveProposal.Number);
    }

    [Fact]
    public void RejectProposal_OnProposalOne_FailedPrecondition()
    {
        var n = Open();
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() => n.RejectProposal(_target, 1, _now)));
    }

    [Fact]
    public void RejectProposal_StaleNumber_Aborts()
    {
        var n = Open();
        n.Revise(_target, 1, Terms.Default, _now);
        Assert.Equal(DomainError.Aborted, ErrorOf(() => n.RejectProposal(_requester, 1, _now)));
    }

    [Fact]
    public void Revise_AfterReject_UsesFreshNumber()
    {
        var n = Open();
        n.Revise(_target, 1, Terms.Default, _now);
        n.RejectProposal(_requester, 2, _now);

        n.Revise(_requester, 1, Terms.Default, _now);

        Assert.Equal(3, n.ActiveProposal.Number);
        Assert.Equal(1, n.PreviousProposal?.Number);
    }

    [Fact]
    public void ApprovalOfRestoredProposal_BecomesValidAgain()
    {
        var n = Open();
        n.ApproveProposal(_requester, 1, _now);
        n.Revise(_target, 1, Terms.Default, _now);
        Assert.False(n.IsValid(new Approval(_requester, ApprovalKind.Terms, 1), _versions));

        n.RejectProposal(_requester, 2, _now);

        Assert.True(n.IsValid(new Approval(_requester, ApprovalKind.Terms, 1), _versions));
    }

    [Fact]
    public void AdApproval_BecomesStaleWhenAdVersionMoves()
    {
        var n = Open();

        var views = n.ApprovalViews(new AdVersions(1, 4));

        Assert.False(Assert.Single(views).Valid);
    }

    [Fact]
    public void AdApproval_ValidityUsesTheOtherSidesAd()
    {
        var n = Open();
        n.ApproveAd(_target, 1, _requesterAd, _now);

        var views = n.ApprovalViews(new AdVersions(2, 3));

        Assert.True(views.Single(v => v.Approval.UserId == _requester).Valid);
        Assert.False(views.Single(v => v.Approval.UserId == _target).Valid);
    }

    [Fact]
    public void HasAllApprovals_FalseUntilFourValid()
    {
        var n = Open();
        Assert.False(n.HasAllApprovals(_versions));

        n.ApproveAd(_target, 1, _requesterAd, _now);
        n.ApproveProposal(_requester, 1, _now);
        Assert.False(n.HasAllApprovals(_versions));

        n.ApproveProposal(_target, 1, _now);
        Assert.True(n.HasAllApprovals(_versions));
    }

    [Fact]
    public void HasAllApprovals_FalseWhenAnAdMoved()
    {
        var n = AllApproved();
        Assert.False(n.HasAllApprovals(new AdVersions(2, 3)));
        Assert.False(n.HasAllApprovals(new AdVersions(1, 4)));
    }

    [Fact]
    public void ReachAgreement_MovesToPending()
    {
        var n = AllApproved();

        n.ReachAgreement(_versions, _now);

        Assert.Equal(NegotiationStatus.AgreementPending, n.Status);
    }

    [Fact]
    public void ReachAgreement_WithStaleApprovals_Aborts()
    {
        var n = AllApproved();
        Assert.Equal(DomainError.Aborted, ErrorOf(() => n.ReachAgreement(new AdVersions(1, 9), _now)));
        Assert.Equal(NegotiationStatus.Open, n.Status);
    }

    [Fact]
    public void ReachAgreement_TwiceFails()
    {
        var n = AllApproved();
        n.ReachAgreement(_versions, _now);
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() => n.ReachAgreement(_versions, _now)));
    }

    [Fact]
    public void AfterAgreementPending_NoMoreChanges()
    {
        var n = AllApproved();
        n.ReachAgreement(_versions, _now);

        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() => n.Revise(_target, 1, Terms.Default, _now)));
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() => n.ApproveProposal(_target, 1, _now)));
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() => n.ApproveAd(_target, 1, _requesterAd, _now)));
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() => n.Close(_target, _now)));
    }

    [Fact]
    public void Close_ByTarget_Declines()
    {
        var n = Open();
        Assert.Equal(NegotiationStatus.Declined, n.Close(_target, _now));
        Assert.Equal(NegotiationStatus.Declined, n.Status);
    }

    [Fact]
    public void Close_ByRequester_Withdraws() =>
        Assert.Equal(NegotiationStatus.Withdrawn, Open().Close(_requester, _now));

    [Fact]
    public void Close_ByStranger_PermissionDenied() =>
        Assert.Equal(DomainError.PermissionDenied, ErrorOf(() => Open().Close("user-3", _now)));

    [Fact]
    public void Close_Twice_FailedPrecondition()
    {
        var n = Open();
        n.Close(_target, _now);
        Assert.Equal(DomainError.FailedPrecondition, ErrorOf(() => n.Close(_requester, _now)));
    }

    [Fact]
    public void MarkAgreed_FromPending()
    {
        var n = AllApproved();
        n.ReachAgreement(_versions, _now);

        Assert.True(n.MarkAgreed(_now));
        Assert.Equal(NegotiationStatus.Agreed, n.Status);
        Assert.False(n.MarkAgreed(_now));
    }

    [Fact]
    public void Cancel_RecordsReason_AndIsIdempotent()
    {
        var n = Open();

        Assert.True(n.Cancel("ad locked by another swap", _now));
        Assert.Equal(NegotiationStatus.Cancelled, n.Status);
        Assert.Equal("ad locked by another swap", n.CancelReason);
        Assert.False(n.Cancel("again", _now));
        Assert.Equal("ad locked by another swap", n.CancelReason);
    }

    [Fact]
    public void Cancel_DoesNotTouchAgreedOrDeclined()
    {
        var declined = Open();
        declined.Close(_target, _now);
        Assert.False(declined.Cancel("x", _now));
        Assert.Equal(NegotiationStatus.Declined, declined.Status);

        var agreed = AllApproved();
        agreed.ReachAgreement(_versions, _now);
        agreed.MarkAgreed(_now);
        Assert.False(agreed.Cancel("x", _now));
        Assert.Equal(NegotiationStatus.Agreed, agreed.Status);
    }

    [Fact]
    public void ReopenCancelledAsAgreed_OnlyForExactlyTheExpectedReason()
    {
        var timedOut = AllApproved();
        timedOut.ReachAgreement(_versions, _now);
        timedOut.Cancel("agreement timed out", _now);
        var version = timedOut.Version;

        Assert.False(timedOut.ReopenCancelledAsAgreed("something else", _now));
        Assert.Equal(NegotiationStatus.Cancelled, timedOut.Status);

        Assert.True(timedOut.ReopenCancelledAsAgreed("agreement timed out", _now));
        Assert.Equal(NegotiationStatus.Agreed, timedOut.Status);
        Assert.Equal(string.Empty, timedOut.CancelReason);
        Assert.Equal(version + 1, timedOut.Version);
        Assert.False(timedOut.ReopenCancelledAsAgreed("agreement timed out", _now)); // idempotent

        var rejected = AllApproved();
        rejected.ReachAgreement(_versions, _now);
        rejected.Cancel("ad locked by another swap", _now);
        Assert.False(rejected.ReopenCancelledAsAgreed("agreement timed out", _now));
        Assert.Equal(NegotiationStatus.Cancelled, rejected.Status);
        Assert.Equal("ad locked by another swap", rejected.CancelReason);

        var open = Open();
        Assert.False(open.ReopenCancelledAsAgreed("agreement timed out", _now));
        Assert.Equal(NegotiationStatus.Open, open.Status);
    }

    [Fact]
    public void Mutations_BumpVersion()
    {
        var n = Open();
        var v = n.Version;
        n.ApproveProposal(_target, 1, _now);
        Assert.Equal(v + 1, n.Version);
    }
}
