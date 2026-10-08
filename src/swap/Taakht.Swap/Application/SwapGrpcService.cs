using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Taakht.Platform;
using Taakht.Swap.Domain;
using Taakht.Swap.Infrastructure;
using SwapPb = Taakht.Swap.V1;

namespace Taakht.Swap.Application;

/// <summary>Gates the mock-partner endpoints (ENABLE_DEV_ENDPOINTS=true or the Development environment).</summary>
public sealed record SwapApiOptions(bool DevEndpointsEnabled);

public sealed class SwapGrpcService(SwapStore store, SwapWorkflow workflow, SwapApiOptions api) : SwapPb.SwapService.SwapServiceBase
{
    public override async Task<SwapPb.Swap> GetSwap(SwapPb.SwapIdRequest request, ServerCallContext context)
    {
        var user = CurrentUser.Id(context);
        var swap = await store.GetAsync(ParseId(request.SwapId), context.CancellationToken)
            ?? throw new RpcException(new Status(StatusCode.NotFound, "swap not found"));
        if (!swap.IsParty(user))
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, "not a party of this swap"));
        }

        return ToProto(swap);
    }

    public override async Task<SwapPb.ListMySwapsResponse> ListMySwaps(Empty request, ServerCallContext context)
    {
        var swaps = await store.ListForUserAsync(CurrentUser.Id(context), context.CancellationToken);
        var response = new SwapPb.ListMySwapsResponse();
        response.Swaps.AddRange(swaps.Select(ToProto));
        return response;
    }

    public override async Task<SwapPb.Swap> SimulateLockerFeePaid(SwapPb.SimulateLockerFeePaidRequest request, ServerCallContext context)
    {
        if (!api.DevEndpointsEnabled)
        {
            throw new RpcException(new Status(StatusCode.Unimplemented, "SimulateLockerFeePaid is only available when dev endpoints are enabled"));
        }

        var caller = CurrentUser.Id(context);
        if (string.IsNullOrWhiteSpace(request.UserId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "user_id is required"));
        }

        if (!string.Equals(caller, request.UserId, StringComparison.Ordinal))
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, "you can only pay your own leg"));
        }

        try
        {
            var swap = await store.GetAsync(ParseId(request.SwapId), context.CancellationToken)
                ?? throw new RpcException(new Status(StatusCode.NotFound, "swap not found"));
            if (!swap.IsParty(caller))
            {
                throw new RpcException(new Status(StatusCode.PermissionDenied, "not a party of this swap"));
            }

            return ToProto(await workflow.RecordFeePaidAsync(ParseId(request.SwapId), request.UserId, context.CancellationToken));
        }
        catch (KeyNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (SwapDomainException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }

    internal static SwapPb.Swap ToProto(SwapModel s) => new()
    {
        Id = s.Id.ToString(),
        NegotiationId = s.NegotiationId,
        AdAId = s.AdAId,
        AdBId = s.AdBId,
        Status = s.Status switch
        {
            SwapStatus.Locking => SwapPb.SwapStatus.Locking,
            SwapStatus.Rejected => SwapPb.SwapStatus.Rejected,
            SwapStatus.AwaitingPayment => SwapPb.SwapStatus.AwaitingPayment,
            SwapStatus.Completed => SwapPb.SwapStatus.Completed,
            SwapStatus.Cancelled => SwapPb.SwapStatus.Cancelled,
            _ => SwapPb.SwapStatus.Unspecified,
        },
        LegA = ToProto(s.LegA),
        LegB = ToProto(s.LegB),
        PaymentDeadline = s.PaymentDeadline is { } d ? Timestamp.FromDateTimeOffset(d) : null,
        CancelReason = s.CancelReason,
        CreatedAt = Timestamp.FromDateTimeOffset(s.CreatedAt),
    };

    private static SwapPb.Leg ToProto(SwapLeg leg) => new()
    {
        OwnerUserId = leg.OwnerUserId,
        Method = leg.Method == Domain.DeliveryMethod.Locker
            ? Taakht.Negotiation.V1.DeliveryMethod.Locker
            : Taakht.Negotiation.V1.DeliveryMethod.InPerson,
        FeePaid = leg.FeePaid,
    };

    private static Guid ParseId(string id) =>
        Guid.TryParse(id, out var parsed)
            ? parsed
            : throw new RpcException(new Status(StatusCode.InvalidArgument, "invalid swap_id"));
}
