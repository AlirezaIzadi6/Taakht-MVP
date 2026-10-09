using Taakht.Negotiation.Domain;

namespace Taakht.Negotiation.Application;

/// <summary>Synchronous view of the Ad service (current version of an ad).</summary>
public interface IAdClient
{
    /// <summary>Returns the current state of the ad, or null when it does not exist.</summary>
    Task<AdSnapshot?> GetAdAsync(string adId, CancellationToken ct);

    /// <summary>Returns the full current ad (spec included) for display, or null when it does not exist.</summary>
    Task<Taakht.Ad.V1.Ad?> GetAdDetailsAsync(string adId, CancellationToken ct);
}

/// <summary>Port for the (mocked) locker partner / KYC check of a leg owner.</summary>
public interface ILockerEligibility
{
    Task<bool> IsEligibleAsync(string userId, CancellationToken ct);
}

/// <summary>Mock adapter: everybody is eligible except one seeded user, to show the error path.</summary>
public sealed class MockLockerEligibility : ILockerEligibility
{
    public const string IneligibleUserId = "user-4";

    public Task<bool> IsEligibleAsync(string userId, CancellationToken ct) =>
        Task.FromResult(userId != IneligibleUserId);
}

public sealed record NegotiationOptions(int Cap = 10)
{
    public static readonly TimeSpan MinAgreementPendingTimeout = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan MaxAgreementPendingTimeout = TimeSpan.FromDays(30);

    /// <summary>How long a negotiation may wait in AGREEMENT_PENDING before each recovery step of the sweeper.</summary>
    public TimeSpan AgreementPendingTimeout { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>How often AgreementReached is published again before the negotiation is given up.</summary>
    public int MaxRepublishes { get; init; } = 3;

    public TimeSpan SweepInterval { get; init; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// A negotiation together with the current versions of its ads (needed to judge approval validity) and, when the
/// Ad service could be read, the two ads themselves (shown to the parties).
/// </summary>
public sealed record NegotiationView(
    SwapNegotiation Negotiation,
    AdVersions Versions,
    Taakht.Ad.V1.Ad? RequesterAd = null,
    Taakht.Ad.V1.Ad? TargetAd = null);
