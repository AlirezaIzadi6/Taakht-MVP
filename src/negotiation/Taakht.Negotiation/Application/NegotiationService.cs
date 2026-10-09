using Microsoft.Extensions.Logging;
using Npgsql;
using Taakht.Negotiation.Api;
using Taakht.Negotiation.Domain;
using Taakht.Negotiation.Infrastructure;
using Taakht.Platform;
using NegV1 = Taakht.Negotiation.V1;

namespace Taakht.Negotiation.Application;

/// <summary>
/// Use cases. Each one: load, call the pure domain, save and write outbox rows in one transaction.
/// Calls to the Ad service happen outside transactions.
/// </summary>
public sealed class NegotiationService(
    NpgsqlDataSource dataSource,
    IAdClient ads,
    ILockerEligibility lockerEligibility,
    NegotiationOptions options,
    TimeProvider clock,
    ILogger<NegotiationService> logger)
{
    public const string Topic = "negotiation.events";
    public const string AgreementTimedOutReason = "agreement timed out";

    private const string _pgUniqueViolation = "23505";

    public async Task<NegotiationView> OpenAsync(string userId, string requesterAdId, string targetAdId, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(requesterAdId) || string.IsNullOrWhiteSpace(targetAdId))
        {
            throw new DomainException(DomainError.InvalidArgument, "requester_ad_id and target_ad_id are required");
        }

        var requesterAd = await ads.GetAdAsync(requesterAdId, ct)
            ?? throw new DomainException(DomainError.NotFound, SwapNegotiation.AdNotAvailable);
        var targetAd = await ads.GetAdAsync(targetAdId, ct)
            ?? throw new DomainException(DomainError.NotFound, SwapNegotiation.AdNotAvailable);

        var negotiation = SwapNegotiation.Open(Guid.NewGuid(), requesterAd, targetAd, userId, clock.GetUtcNow());

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Upsert and lock both ad rows in a fixed order: the cap check below is then serialized per ad.
        foreach (var ad in new[] { requesterAd, targetAd }.OrderBy(a => a.AdId, StringComparer.Ordinal))
        {
            await NegotiationRepository.UpsertAdRefAsync(conn, tx, ad, ct);
        }

        await NegotiationRepository.LockAdRefsAsync(conn, tx, [requesterAd.AdId, targetAd.AdId], ct);

        if (await NegotiationRepository.LivePairExistsAsync(conn, tx, requesterAd.AdId, targetAd.AdId, ct))
        {
            throw new DomainException(DomainError.AlreadyExists, "an open negotiation already exists for this pair of ads");
        }

        foreach (var ad in new[] { requesterAd, targetAd })
        {
            var live = await NegotiationRepository.CountLiveAsync(conn, tx, ad.AdId, ct);
            if (live >= options.Cap)
            {
                throw new DomainException(DomainError.ResourceExhausted, $"ad {ad.AdId} already has {options.Cap} open negotiations");
            }
        }

        try
        {
            await NegotiationRepository.InsertAsync(conn, tx, negotiation, ct);
        }
        catch (PostgresException ex) when (ex.SqlState == _pgUniqueViolation)
        {
            throw new DomainException(DomainError.AlreadyExists, "an open negotiation already exists for this pair of ads");
        }

        await Outbox.AddAsync(conn, tx, Topic, negotiation.Id.ToString(), new NegV1.NegotiationOpened
        {
            NegotiationId = negotiation.Id.ToString(),
            RequesterAdId = negotiation.RequesterAdId,
            TargetAdId = negotiation.TargetAdId,
        }, ct);
        await tx.CommitAsync(ct);

        var versions = await VersionsAsync(conn, null, negotiation, ct);
        return await WithAdsAsync(new NegotiationView(negotiation, versions), ct);
    }

    public async Task<NegotiationView> ApproveAdAsync(string userId, Guid negotiationId, int adVersion, CancellationToken ct)
    {
        var peek = await LoadViewAsync(userId, negotiationId, ct);
        var otherAdId = peek.Negotiation.AdApprovedBy(userId);
        var otherAd = await ads.GetAdAsync(otherAdId, ct)
            ?? throw new DomainException(DomainError.NotFound, "ad not found");

        await MutateAsync(userId, negotiationId, async (n, conn, tx) =>
        {
            n.ApproveAd(userId, adVersion, otherAd, clock.GetUtcNow());
            await NegotiationRepository.UpsertAdRefAsync(conn, tx, otherAd, ct);
        }, ct);

        return await WithAdsAsync(await TryReachAgreementAsync(negotiationId, ct), ct);
    }

    public async Task<NegotiationView> ReviseProposalAsync(
        string userId, Guid negotiationId, int seenProposalNumber, Terms terms, CancellationToken ct)
    {
        var peek = await LoadViewAsync(userId, negotiationId, ct);
        foreach (var owner in peek.Negotiation.LockerLegOwners(terms))
        {
            if (!await lockerEligibility.IsEligibleAsync(owner, ct))
            {
                throw new DomainException(DomainError.FailedPrecondition, $"user {owner} is not eligible for locker delivery");
            }
        }

        await MutateAsync(userId, negotiationId, (n, _, _) =>
        {
            n.Revise(userId, seenProposalNumber, terms, clock.GetUtcNow());
            return Task.CompletedTask;
        }, ct);

        return await GetAsync(userId, negotiationId, ct);
    }

    public async Task<NegotiationView> ApproveProposalAsync(string userId, Guid negotiationId, int proposalNumber, CancellationToken ct)
    {
        await MutateAsync(userId, negotiationId, (n, _, _) =>
        {
            n.ApproveProposal(userId, proposalNumber, clock.GetUtcNow());
            return Task.CompletedTask;
        }, ct);

        return await WithAdsAsync(await TryReachAgreementAsync(negotiationId, ct), ct);
    }

    public async Task<NegotiationView> RejectProposalAsync(string userId, Guid negotiationId, int proposalNumber, CancellationToken ct)
    {
        await MutateAsync(userId, negotiationId, (n, _, _) =>
        {
            n.RejectProposal(userId, proposalNumber, clock.GetUtcNow());
            return Task.CompletedTask;
        }, ct);

        return await GetAsync(userId, negotiationId, ct);
    }

    public async Task<NegotiationView> CloseAsync(string userId, Guid negotiationId, CancellationToken ct)
    {
        await MutateAsync(userId, negotiationId, async (n, conn, tx) =>
        {
            var status = n.Close(userId, clock.GetUtcNow());
            await AddClosedEventAsync(conn, tx, n, status, string.Empty, ct);
        }, ct);

        return await GetAsync(userId, negotiationId, ct);
    }

    public async Task<NegotiationView> GetAsync(string userId, Guid negotiationId, CancellationToken ct) =>
        await WithAdsAsync(await LoadViewAsync(userId, negotiationId, ct), ct);

    private async Task<NegotiationView> LoadViewAsync(string userId, Guid negotiationId, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var n = await NegotiationRepository.LoadAsync(conn, null, negotiationId, false, ct)
            ?? throw new DomainException(DomainError.NotFound, "negotiation not found");
        if (!n.IsParty(userId))
        {
            throw new DomainException(DomainError.PermissionDenied, "caller is not a party of this negotiation");
        }

        return new NegotiationView(n, await VersionsAsync(conn, null, n, ct));
    }

    public async Task<IReadOnlyList<NegotiationView>> ListAsync(string userId, string? adId, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var ids = await NegotiationRepository.ListIdsAsync(conn, userId, adId, ct);
        var negotiations = await NegotiationRepository.LoadManyAsync(conn, null, ids, false, ct);
        var versions = await NegotiationRepository.AdVersionsAsync(
            conn, null, [.. negotiations.SelectMany(n => new[] { n.RequesterAdId, n.TargetAdId }).Distinct()], ct);
        return await WithAdsAsync([.. negotiations.Select(n => new NegotiationView(n, ToVersions(n, versions)))], ct);
    }

    /// <summary>
    /// Counterpart visibility: the two ads of a negotiation, as the Ad service shows them now. Only the parties get here.
    /// A failing or missing ad leaves its field empty (logged); it never fails the call.
    /// </summary>
    private async Task<NegotiationView> WithAdsAsync(NegotiationView view, CancellationToken ct) =>
        (await WithAdsAsync([view], ct))[0];

    private async Task<IReadOnlyList<NegotiationView>> WithAdsAsync(IReadOnlyList<NegotiationView> views, CancellationToken ct)
    {
        if (views.Count == 0)
        {
            return views;
        }

        // One fetch per distinct ad within this request, a few at a time.
        using var gate = new SemaphoreSlim(8);
        var fetches = new Dictionary<string, Task<Taakht.Ad.V1.Ad?>>(StringComparer.Ordinal);
        foreach (var adId in views.SelectMany(v => new[] { v.Negotiation.RequesterAdId, v.Negotiation.TargetAdId }))
        {
            if (!fetches.ContainsKey(adId))
            {
                fetches[adId] = FetchAdAsync(adId, gate, ct);
            }
        }

        await Task.WhenAll(fetches.Values);
        var result = new List<NegotiationView>(views.Count);
        foreach (var v in views)
        {
            result.Add(v with
            {
                RequesterAd = await fetches[v.Negotiation.RequesterAdId],
                TargetAd = await fetches[v.Negotiation.TargetAdId],
            });
        }

        return result;
    }

    private async Task<Taakht.Ad.V1.Ad?> FetchAdAsync(string adId, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        try
        {
            return await ads.GetAdDetailsAsync(adId, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Could not read ad {AdId} for a negotiation response; returning it without the ad", adId);
            return null;
        }
        finally
        {
            gate.Release();
        }
    }

    private static AdVersions ToVersions(SwapNegotiation n, Dictionary<string, int> versions) =>
        new(versions.GetValueOrDefault(n.RequesterAdId), versions.GetValueOrDefault(n.TargetAdId));

    private static async Task<AdVersions> VersionsAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, SwapNegotiation n, CancellationToken ct) =>
        ToVersions(n, await NegotiationRepository.AdVersionsAsync(conn, tx, [n.RequesterAdId, n.TargetAdId], ct));

    private async Task<SwapNegotiation> MutateAsync(
        string userId,
        Guid negotiationId,
        Func<SwapNegotiation, NpgsqlConnection, NpgsqlTransaction, Task> apply,
        CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        var n = await NegotiationRepository.LoadAsync(conn, tx, negotiationId, true, ct)
            ?? throw new DomainException(DomainError.NotFound, "negotiation not found");
        if (!n.IsParty(userId))
        {
            throw new DomainException(DomainError.PermissionDenied, "caller is not a party of this negotiation");
        }

        await apply(n, conn, tx);
        await NegotiationRepository.SaveAsync(conn, tx, n, ct);
        await tx.CommitAsync(ct);
        return n;
    }

    /// <summary>
    /// When all four approvals look valid locally, re-check both ads synchronously and, if they still match,
    /// move to AgreementPending and write AgreementReached in the same transaction.
    /// </summary>
    private async Task<NegotiationView> TryReachAgreementAsync(Guid negotiationId, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var n = await NegotiationRepository.LoadAsync(conn, null, negotiationId, false, ct)
            ?? throw new DomainException(DomainError.NotFound, "negotiation not found");
        var local = await VersionsAsync(conn, null, n, ct);

        if (n.Status != NegotiationStatus.Open || !n.HasAllApprovals(local))
        {
            return new NegotiationView(n, local);
        }

        var requesterAd = await ads.GetAdAsync(n.RequesterAdId, ct)
            ?? throw new DomainException(DomainError.Aborted, "the requester ad no longer exists");
        var targetAd = await ads.GetAdAsync(n.TargetAdId, ct)
            ?? throw new DomainException(DomainError.Aborted, "the target ad no longer exists");

        await using var tx = await conn.BeginTransactionAsync(ct);
        await NegotiationRepository.UpsertAdRefAsync(conn, tx, requesterAd, ct);
        await NegotiationRepository.UpsertAdRefAsync(conn, tx, targetAd, ct);

        var synced = new AdVersions(requesterAd.Version, targetAd.Version);
        if (!requesterAd.IsLockable || !targetAd.IsLockable || !n.HasAllApprovals(synced))
        {
            await tx.CommitAsync(ct);
            throw new DomainException(DomainError.Aborted, "an ad changed or is no longer available; the approvals are stale, re-read the negotiation");
        }

        var locked = await NegotiationRepository.LoadAsync(conn, tx, negotiationId, true, ct);
        if (locked is null || locked.Status != NegotiationStatus.Open)
        {
            await tx.CommitAsync(ct);
            return await GetViewAsync(negotiationId, ct);
        }

        if (!locked.HasAllApprovals(synced))
        {
            await tx.CommitAsync(ct);
            throw new DomainException(DomainError.Aborted, "approvals changed concurrently; re-read the negotiation");
        }

        locked.ReachAgreement(synced, clock.GetUtcNow());
        await NegotiationRepository.SaveAsync(conn, tx, locked, ct);
        await NegotiationRepository.SetAgreedVersionsAsync(conn, tx, locked.Id, synced.RequesterAdVersion, synced.TargetAdVersion, ct);
        await Outbox.AddAsync(conn, tx, Topic, locked.Id.ToString(), BuildAgreement(locked, synced), ct);
        await tx.CommitAsync(ct);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Agreement reached for negotiation {NegotiationId}", locked.Id);
        }

        return new NegotiationView(locked, synced);
    }

    private async Task<NegotiationView> GetViewAsync(Guid negotiationId, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var n = await NegotiationRepository.LoadAsync(conn, null, negotiationId, false, ct)
            ?? throw new DomainException(DomainError.NotFound, "negotiation not found");
        return new NegotiationView(n, await VersionsAsync(conn, null, n, ct));
    }

    private static NegV1.AgreementReached BuildAgreement(SwapNegotiation n, AdVersions versions) => new()
    {
        NegotiationId = n.Id.ToString(),
        AdA = new NegV1.AgreedAd { AdId = n.RequesterAdId, OwnerId = n.RequesterUserId, Version = versions.RequesterAdVersion },
        AdB = new NegV1.AgreedAd { AdId = n.TargetAdId, OwnerId = n.TargetUserId, Version = versions.TargetAdVersion },
        Terms = Mapper.ToProto(n.ActiveProposal.Terms),
    };

    /// <summary>
    /// Recovery for negotiations stuck in AGREEMENT_PENDING (the swap never answered). Each negotiation moves through
    /// periods of <see cref="NegotiationOptions.AgreementPendingTimeout"/>: at the end of a period AgreementReached is
    /// written to the outbox again (the swap service is idempotent by negotiation id) up to
    /// <see cref="NegotiationOptions.MaxRepublishes"/> times; if it is still pending after the last period it is
    /// CANCELLED ("agreement timed out"). It is never cancelled earlier because the swap may be mid-flight.
    /// Returns the number of negotiations acted on.
    /// </summary>
    public async Task<int> SweepAgreementPendingAsync(CancellationToken ct)
    {
        var now = clock.GetUtcNow();
        var cutoff = now - options.AgreementPendingTimeout;
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var due = await NegotiationRepository.ListDuePendingAsync(conn, cutoff, 100, ct);
        var acted = 0;
        foreach (var id in due)
        {
            await using var tx = await conn.BeginTransactionAsync(ct);
            var state = await NegotiationRepository.LockDuePendingAsync(conn, tx, id, cutoff, ct);
            var n = state is null ? null : await NegotiationRepository.LoadAsync(conn, tx, id, true, ct);
            if (state is null || n is null)
            {
                await tx.RollbackAsync(ct); // answered or handled by someone else in the meantime
                continue;
            }

            if (state.RepublishCount >= options.MaxRepublishes)
            {
                n.Cancel(AgreementTimedOutReason, now);
                await NegotiationRepository.SaveAsync(conn, tx, n, ct);
                await AddClosedEventAsync(conn, tx, n, NegotiationStatus.Cancelled, AgreementTimedOutReason, ct);
                logger.LogWarning(
                    "Negotiation {NegotiationId} stayed AGREEMENT_PENDING through {Republishes} republishes; cancelled",
                    id, state.RepublishCount);
            }
            else
            {
                var versions = state.RequesterVersion is { } r && state.TargetVersion is { } t
                    ? new AdVersions(r, t)
                    : await VersionsAsync(conn, tx, n, ct);
                await Outbox.AddAsync(conn, tx, Topic, id.ToString(), BuildAgreement(n, versions), ct);
                await NegotiationRepository.MarkRepublishedAsync(conn, tx, id, now, ct);
                logger.LogWarning(
                    "Negotiation {NegotiationId} is still AGREEMENT_PENDING; AgreementReached published again ({Attempt}/{Max})",
                    id, state.RepublishCount + 1, options.MaxRepublishes);
            }

            await tx.CommitAsync(ct);
            acted++;
        }

        return acted;
    }

    internal static Task AddClosedEventAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, SwapNegotiation n, NegotiationStatus status, string reason, CancellationToken ct) =>
        Outbox.AddAsync(conn, tx, Topic, n.Id.ToString(), new NegV1.NegotiationClosed
        {
            NegotiationId = n.Id.ToString(),
            Status = Mapper.ToProto(status),
            Reason = reason,
        }, ct);
}
