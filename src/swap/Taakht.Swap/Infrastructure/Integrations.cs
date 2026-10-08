using Grpc.Core;
using Taakht.Ad.V1;

namespace Taakht.Swap.Infrastructure;

public readonly record struct AdVersionRef(string AdId, int Version);

public abstract record LockOutcome
{
    public sealed record Locked : LockOutcome;

    public sealed record Rejected(string Reason) : LockOutcome;
}

/// <summary>Port to the Ad service. Transient failures surface as exceptions so the caller can retry.</summary>
public interface IAdClient
{
    Task<LockOutcome> LockAdsAsync(Guid swapId, IReadOnlyList<AdVersionRef> ads, CancellationToken ct);
}

public sealed class GrpcAdClient(AdService.AdServiceClient client) : IAdClient
{
    private static readonly TimeSpan _callTimeout = TimeSpan.FromSeconds(10);

    public async Task<LockOutcome> LockAdsAsync(Guid swapId, IReadOnlyList<AdVersionRef> ads, CancellationToken ct)
    {
        var request = new LockAdsRequest { SwapId = swapId.ToString() };
        request.Ads.AddRange(ads.Select(a => new AdRef { AdId = a.AdId, Version = a.Version }));
        try
        {
            await client.LockAdsAsync(request, deadline: DateTime.UtcNow + _callTimeout, cancellationToken: ct);
            return new LockOutcome.Locked();
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.FailedPrecondition or StatusCode.NotFound)
        {
            return new LockOutcome.Rejected(ex.Status.Detail.Length > 0 ? ex.Status.Detail : "ads could not be locked");
        }
    }
}

/// <summary>Port to the locker partner (payment status query).</summary>
public interface IDeliveryProvider
{
    Task<bool> GetPaymentStatusAsync(Guid swapId, string userId, CancellationToken ct);
}

/// <summary>Mock partner: reports whatever payments were recorded through SimulateLockerFeePaid.</summary>
public sealed class MockDeliveryProvider(SwapStore store) : IDeliveryProvider
{
    public async Task<bool> GetPaymentStatusAsync(Guid swapId, string userId, CancellationToken ct)
    {
        var swap = await store.GetAsync(swapId, ct);
        return swap is not null
            && ((swap.LegA.OwnerUserId == userId && swap.LegA.FeePaid) || (swap.LegB.OwnerUserId == userId && swap.LegB.FeePaid));
    }
}
