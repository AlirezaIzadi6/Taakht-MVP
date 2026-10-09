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
/// the ambient user the platform client interceptor would otherwise forward; that interceptor adds x-internal-token to the call.
/// </summary>
public sealed class GrpcAdClient(AdService.AdServiceClient client) : IAdClient
{
    public async Task<DomainSnapshot?> GetAdAsync(string adId, CancellationToken ct)
    {
        var ad = await FetchAsync(adId, TimeSpan.FromSeconds(5), ct);
        return ad is null ? null : new DomainSnapshot(ad.Id, ad.OwnerId, ad.Version, Map(ad.Status));
    }

    public Task<Taakht.Ad.V1.Ad?> GetAdDetailsAsync(string adId, CancellationToken ct) =>
        FetchAsync(adId, TimeSpan.FromSeconds(3), ct);

    private async Task<Taakht.Ad.V1.Ad?> FetchAsync(string adId, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            return await client.GetAdAsync(
                new GetAdRequest { AdId = adId },
                headers: new Metadata { { IdentityConstants.UserIdHeader, SystemIdentities.Negotiation } },
                deadline: DateTime.UtcNow.Add(timeout),
                cancellationToken: ct);
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
