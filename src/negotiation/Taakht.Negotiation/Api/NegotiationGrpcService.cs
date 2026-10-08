using Grpc.Core;
using Taakht.Negotiation.Application;
using Taakht.Negotiation.Domain;
using Taakht.Platform;
using NegV1 = Taakht.Negotiation.V1;

namespace Taakht.Negotiation.Api;

public sealed class NegotiationGrpcService(NegotiationService service) : NegV1.NegotiationService.NegotiationServiceBase
{
    public override async Task<NegV1.Negotiation> OpenNegotiation(NegV1.OpenNegotiationRequest request, ServerCallContext context) =>
        Mapper.ToProto(await service.OpenAsync(
            CurrentUser.Id(context), request.RequesterAdId, request.TargetAdId, context.CancellationToken));

    public override async Task<NegV1.Negotiation> ApproveAd(NegV1.ApproveAdRequest request, ServerCallContext context) =>
        Mapper.ToProto(await service.ApproveAdAsync(
            CurrentUser.Id(context), ParseId(request.NegotiationId), request.AdVersion, context.CancellationToken));

    public override async Task<NegV1.Negotiation> ReviseProposal(NegV1.ReviseProposalRequest request, ServerCallContext context) =>
        Mapper.ToProto(await service.ReviseProposalAsync(
            CurrentUser.Id(context), ParseId(request.NegotiationId), request.SeenProposalNumber,
            Mapper.FromProto(request.Terms), context.CancellationToken));

    public override async Task<NegV1.Negotiation> ApproveProposal(NegV1.ProposalRefRequest request, ServerCallContext context) =>
        Mapper.ToProto(await service.ApproveProposalAsync(
            CurrentUser.Id(context), ParseId(request.NegotiationId), request.ProposalNumber, context.CancellationToken));

    public override async Task<NegV1.Negotiation> RejectProposal(NegV1.ProposalRefRequest request, ServerCallContext context) =>
        Mapper.ToProto(await service.RejectProposalAsync(
            CurrentUser.Id(context), ParseId(request.NegotiationId), request.ProposalNumber, context.CancellationToken));

    public override async Task<NegV1.Negotiation> CloseNegotiation(NegV1.CloseNegotiationRequest request, ServerCallContext context) =>
        Mapper.ToProto(await service.CloseAsync(
            CurrentUser.Id(context), ParseId(request.NegotiationId), context.CancellationToken));

    public override async Task<NegV1.Negotiation> GetNegotiation(NegV1.NegotiationIdRequest request, ServerCallContext context) =>
        Mapper.ToProto(await service.GetAsync(
            CurrentUser.Id(context), ParseId(request.NegotiationId), context.CancellationToken));

    public override async Task<NegV1.ListNegotiationsResponse> ListNegotiations(NegV1.ListNegotiationsRequest request, ServerCallContext context)
    {
        var views = await service.ListAsync(CurrentUser.Id(context), request.AdId, context.CancellationToken);
        var response = new NegV1.ListNegotiationsResponse();
        response.Negotiations.AddRange(views.Select(Mapper.ToProto));
        return response;
    }

    private static Guid ParseId(string id) =>
        Guid.TryParse(id, out var guid) ? guid : throw new DomainException(DomainError.InvalidArgument, "invalid negotiation_id");
}
