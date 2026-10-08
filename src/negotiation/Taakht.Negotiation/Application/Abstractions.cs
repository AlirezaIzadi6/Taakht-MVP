using Taakht.Negotiation.Domain;

namespace Taakht.Negotiation.Application;

/// <summary>Synchronous view of the Ad service (current version of an ad).</summary>
public interface IAdClient
{
    /// <summary>Returns the current state of the ad, or null when it does not exist.</summary>
    Task<AdSnapshot?> GetAdAsync(string adId, CancellationToken ct);
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

public sealed record NegotiationOptions(int Cap = 10);

/// <summary>A negotiation together with the current versions of its ads (needed to judge approval validity).</summary>
public sealed record NegotiationView(SwapNegotiation Negotiation, AdVersions Versions);
