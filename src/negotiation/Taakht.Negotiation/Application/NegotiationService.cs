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

    private const string _pgUniqueViolation = "23505";

    public async Task<NegotiationView> OpenAsync(string userId, string requesterAdId, string targetAdId, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(requesterAdId) || string.IsNullOrWhiteSpace(targetAdId))
        {
            throw new DomainException(DomainError.InvalidArgument, "requester_ad_id and target_ad_id are required");
        }

        var requesterAd = await ads.GetAdAsync(requesterAdId, ct)
            ?? throw new DomainException(DomainError.NotFound, "requester ad not found");
        var targetAd = await ads.GetAdAsync(targetAdId, ct)
            ?? throw new DomainException(DomainError.NotFound, "target ad not found");

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
        return new NegotiationView(negotiation, versions);
    }

    public async Task<NegotiationView> ApproveAdAsync(string userId, Guid negotiationId, int adVersion, CancellationToken ct)
    {
        var peek = await GetAsync(userId, negotiationId, ct);
        var otherAdId = peek.Negotiation.AdApprovedBy(userId);
        var otherAd = await ads.GetAdAsync(otherAdId, ct)
            ?? throw new DomainException(DomainError.NotFound, "ad not found");

        await MutateAsync(userId, negotiationId, async (n, conn, tx) =>
        {
            n.ApproveAd(userId, adVersion, otherAd, clock.GetUtcNow());
            await NegotiationRepository.UpsertAdRefAsync(conn, tx, otherAd, ct);
        }, ct);

        return await TryReachAgreementAsync(negotiationId, ct);
    }

    public async Task<NegotiationView> ReviseProposalAsync(
        string userId, Guid negotiationId, int seenProposalNumber, Terms terms, CancellationToken ct)
    {
        var peek = await GetAsync(userId, negotiationId, ct);
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

        return await TryReachAgreementAsync(negotiationId, ct);
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

    public async Task<NegotiationView> GetAsync(string userId, Guid negotiationId, CancellationToken ct)
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
        return [.. negotiations.Select(n => new NegotiationView(n, ToVersions(n, versions)))];
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
        await Outbox.AddAsync(conn, tx, Topic, locked.Id.ToString(), new NegV1.AgreementReached
        {
            NegotiationId = locked.Id.ToString(),
            AdA = new NegV1.AgreedAd { AdId = locked.RequesterAdId, OwnerId = locked.RequesterUserId, Version = synced.RequesterAdVersion },
            AdB = new NegV1.AgreedAd { AdId = locked.TargetAdId, OwnerId = locked.TargetUserId, Version = synced.TargetAdVersion },
            Terms = Mapper.ToProto(locked.ActiveProposal.Terms),
        }, ct);
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

    internal static Task AddClosedEventAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, SwapNegotiation n, NegotiationStatus status, string reason, CancellationToken ct) =>
        Outbox.AddAsync(conn, tx, Topic, n.Id.ToString(), new NegV1.NegotiationClosed
        {
            NegotiationId = n.Id.ToString(),
            Status = Mapper.ToProto(status),
            Reason = reason,
        }, ct);
}
