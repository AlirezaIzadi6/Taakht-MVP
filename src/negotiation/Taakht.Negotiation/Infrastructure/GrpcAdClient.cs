using Grpc.Core;
using Taakht.Ad.V1;
using Taakht.Negotiation.Application;
using Taakht.Platform;
using DomainAdStatus = Taakht.Negotiation.Domain.AdStatus;
using DomainSnapshot = Taakht.Negotiation.Domain.AdSnapshot;

namespace Taakht.Negotiation.Infrastructure;

/// <summary>
/// Calls ad.GetAd as the system identity (system:negotiation): the Ad service shows unpublished ads and old versions
/// only to owners and system callers, and negotiation authorizes the real caller itself. The explicit header wins over
/// the ambient user the platform client interceptor would otherwise forward.
/// </summary>
public sealed class GrpcAdClient(AdService.AdServiceClient client) : IAdClient
{
    public async Task<DomainSnapshot?> GetAdAsync(string adId, CancellationToken ct)
    {
        try
        {
            var ad = await client.GetAdAsync(
                new GetAdRequest { AdId = adId },
                headers: new Metadata { { IdentityConstants.UserIdHeader, SystemIdentities.Negotiation } },
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
