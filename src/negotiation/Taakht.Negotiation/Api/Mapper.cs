using Google.Protobuf.WellKnownTypes;
using Taakht.Negotiation.Application;
using Taakht.Negotiation.Domain;
using NegV1 = Taakht.Negotiation.V1;

namespace Taakht.Negotiation.Api;

public static class Mapper
{
    public static NegV1.Negotiation ToProto(NegotiationView view)
    {
        var n = view.Negotiation;
        var result = new NegV1.Negotiation
        {
            Id = n.Id.ToString(),
            RequesterUserId = n.RequesterUserId,
            RequesterAdId = n.RequesterAdId,
            TargetUserId = n.TargetUserId,
            TargetAdId = n.TargetAdId,
            Status = ToProto(n.Status),
            ActiveProposal = ToProto(n.ActiveProposal),
            CancelReason = n.CancelReason,
            CreatedAt = Timestamp.FromDateTimeOffset(n.CreatedAt),
            UpdatedAt = Timestamp.FromDateTimeOffset(n.UpdatedAt),
            RequesterAd = view.RequesterAd,
            TargetAd = view.TargetAd,
        };
        foreach (var v in n.ApprovalViews(view.Versions))
        {
            result.Approvals.Add(new NegV1.Approval
            {
                UserId = v.Approval.UserId,
                Kind = v.Approval.Kind == ApprovalKind.Ad ? NegV1.ApprovalKind.Ad : NegV1.ApprovalKind.Terms,
                Target = v.Approval.Target,
                Valid = v.Valid,
            });
        }

        return result;
    }

    public static NegV1.Proposal ToProto(Proposal p) => new()
    {
        Id = p.Id.ToString(),
        Number = p.Number,
        Terms = ToProto(p.Terms),
        AuthorUserId = p.AuthorUserId ?? string.Empty,
    };

    public static NegV1.Terms ToProto(Terms t) => new()
    {
        LegA = ToProto(t.LegA),
        LegB = ToProto(t.LegB),
        PriceDifference = t.PriceDifference,
        PayerUserId = t.PayerUserId,
    };

    public static Terms FromProto(NegV1.Terms? t) => t is null
        ? throw new DomainException(DomainError.InvalidArgument, "terms are required")
        : new Terms(FromProto(t.LegA), FromProto(t.LegB), t.PriceDifference, t.PayerUserId);

    public static NegV1.NegotiationStatus ToProto(NegotiationStatus status) => status switch
    {
        NegotiationStatus.Open => NegV1.NegotiationStatus.Open,
        NegotiationStatus.AgreementPending => NegV1.NegotiationStatus.AgreementPending,
        NegotiationStatus.Agreed => NegV1.NegotiationStatus.Agreed,
        NegotiationStatus.Declined => NegV1.NegotiationStatus.Declined,
        NegotiationStatus.Withdrawn => NegV1.NegotiationStatus.Withdrawn,
        NegotiationStatus.Cancelled => NegV1.NegotiationStatus.Cancelled,
        _ => NegV1.NegotiationStatus.Unspecified,
    };

    private static NegV1.DeliveryMethod ToProto(DeliveryMethod m) =>
        m == DeliveryMethod.Locker ? NegV1.DeliveryMethod.Locker : NegV1.DeliveryMethod.InPerson;

    // Unspecified maps to an undefined domain value so that Revise rejects it as INVALID_ARGUMENT.
    private static DeliveryMethod FromProto(NegV1.DeliveryMethod m) => m switch
    {
        NegV1.DeliveryMethod.InPerson => DeliveryMethod.InPerson,
        NegV1.DeliveryMethod.Locker => DeliveryMethod.Locker,
        _ => 0,
    };
}
