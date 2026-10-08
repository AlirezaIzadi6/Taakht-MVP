using Grpc.Core;
using Taakht.Ad.V1;
using Taakht.Negotiation.Application;
using DomainAdStatus = Taakht.Negotiation.Domain.AdStatus;
using DomainSnapshot = Taakht.Negotiation.Domain.AdSnapshot;

namespace Taakht.Negotiation.Infrastructure;

/// <summary>Calls ad.GetAd; the caller's x-user-id is forwarded by the platform client interceptor.</summary>
public sealed class GrpcAdClient(AdService.AdServiceClient client) : IAdClient
{
    public async Task<DomainSnapshot?> GetAdAsync(string adId, CancellationToken ct)
    {
        try
        {
            var ad = await client.GetAdAsync(
                new GetAdRequest { AdId = adId },
                deadline: DateTime.UtcNow.AddSeconds(5),
                cancellationToken: ct);
            return new DomainSnapshot(ad.Id, ad.OwnerId, ad.Version, Map(ad.Status));
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            return null;
        }
    }

    internal static DomainAdStatus Map(Taakht.Ad.V1.AdStatus status) => status switch
    {
        Taakht.Ad.V1.AdStatus.Published => DomainAdStatus.Published,
        Taakht.Ad.V1.AdStatus.Hidden => DomainAdStatus.Hidden,
        Taakht.Ad.V1.AdStatus.Locked => DomainAdStatus.Locked,
        _ => DomainAdStatus.Closed,
    };
}
