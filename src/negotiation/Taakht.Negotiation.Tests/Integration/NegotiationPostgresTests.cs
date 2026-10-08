using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Taakht.Common.V1;
using Taakht.Negotiation.Application;
using Taakht.Negotiation.Domain;
using Taakht.Negotiation.Infrastructure;
using NegV1 = Taakht.Negotiation.V1;

namespace Taakht.Negotiation.Tests.Integration;

public sealed class NegotiationPostgresTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private sealed class FakeAdClient : IAdClient
    {
        private readonly Dictionary<string, AdSnapshot> _ads = [];
        private readonly object _gate = new();

        public void Set(AdSnapshot ad)
        {
            lock (_gate)
            {
                _ads[ad.AdId] = ad;
            }
        }

        public Task<AdSnapshot?> GetAdAsync(string adId, CancellationToken ct)
        {
            lock (_gate)
            {
                return Task.FromResult(_ads.GetValueOrDefault(adId));
            }
        }
    }

    private sealed record Harness(FakeAdClient Ads, NegotiationService Service, EventHandlers Events, NpgsqlDataSource Db)
    {
        public void SetAll(params AdSnapshot[] ads)
        {
            foreach (var ad in ads)
            {
                Ads.Set(ad);
            }
        }

        public async Task<List<Envelope>> OutboxAsync(Guid negotiationId)
        {
            await using var conn = await Db.OpenConnectionAsync();
            var rows = await conn.QueryAsync<byte[]>(
                "SELECT envelope FROM outbox WHERE key = @k ORDER BY created_at", new { k = negotiationId.ToString() });
            return [.. rows.Select(Envelope.Parser.ParseFrom)];
        }
    }

    private static AdSnapshot NewAd(string owner, int version = 1, AdStatus status = AdStatus.Published) =>
        new(Guid.NewGuid().ToString(), owner, version, status);

    // Returns null (the test body then does nothing) when TEST_DATABASE_URL is not set.
    private Harness? Create(int cap = 10)
    {
        if (db.DataSource is null)
        {
            return null;
        }

        var ads = new FakeAdClient();
        var service = new NegotiationService(
            db.DataSource, ads, new MockLockerEligibility(), new NegotiationOptions(cap), TimeProvider.System,
            NullLogger<NegotiationService>.Instance);
        return new Harness(ads, service, new EventHandlers(TimeProvider.System, NullLogger<EventHandlers>.Instance), db.DataSource);
    }

    [Fact]
    public async Task ApprovalFlowReachesAgreementAndWritesOutbox()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var adA = NewAd("user-1");
        var adB = NewAd("user-2", 3);
        h.SetAll(adA, adB);

        var opened = await h.Service.OpenAsync("user-1", adA.AdId, adB.AdId, default);
        Assert.Equal(NegotiationStatus.Open, opened.Negotiation.Status);
        var id = opened.Negotiation.Id;

        await h.Service.ApproveAdAsync("user-2", id, adA.Version, default);
        await h.Service.ApproveProposalAsync("user-1", id, 1, default);
        var revised = await h.Service.ReviseProposalAsync(
            "user-2", id, 1, new Terms(DeliveryMethod.Locker, DeliveryMethod.InPerson, 100, "user-1"), default);
        Assert.Equal(2, revised.Negotiation.ActiveProposal.Number);
        var views = revised.Negotiation.ApprovalViews(revised.Versions);
        Assert.False(views.Single(v => v.Approval.UserId == "user-1" && v.Approval.Kind == ApprovalKind.Terms).Valid);

        var stale = await Assert.ThrowsAsync<DomainException>(() => h.Service.ApproveProposalAsync("user-1", id, 1, default));
        Assert.Equal(DomainError.Aborted, stale.Error);
        var agreed = await h.Service.ApproveProposalAsync("user-1", id, 2, default);

        Assert.Equal(NegotiationStatus.AgreementPending, agreed.Negotiation.Status);
        var outbox = await h.OutboxAsync(id);
        var agreement = Assert.Single(outbox, e => e.Type == NegV1.AgreementReached.Descriptor.FullName);
        var payload = NegV1.AgreementReached.Parser.ParseFrom(agreement.Payload);
        Assert.Equal(id.ToString(), payload.NegotiationId);
        Assert.Equal(adA.AdId, payload.AdA.AdId);
        Assert.Equal("user-1", payload.AdA.OwnerId);
        Assert.Equal(1, payload.AdA.Version);
        Assert.Equal(adB.AdId, payload.AdB.AdId);
        Assert.Equal(3, payload.AdB.Version);
        Assert.Equal(100, payload.Terms.PriceDifference);
        Assert.Contains(outbox, e => e.Type == NegV1.NegotiationOpened.Descriptor.FullName);
    }

    [Fact]
    public async Task FinalSyncCheckAbortsWhenAnAdMovedBeforeItsEventArrived()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var adA = NewAd("user-1");
        var adB = NewAd("user-2");
        h.SetAll(adA, adB);
        var id = (await h.Service.OpenAsync("user-1", adA.AdId, adB.AdId, default)).Negotiation.Id;
        await h.Service.ApproveProposalAsync("user-1", id, 1, default);
        await h.Service.ApproveProposalAsync("user-2", id, 1, default);

        h.Ads.Set(adB with { Version = 2 });

        var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.ApproveAdAsync("user-2", id, 1, default));
        Assert.Equal(DomainError.Aborted, ex.Error);
        var view = await h.Service.GetAsync("user-1", id, default);
        Assert.Equal(NegotiationStatus.Open, view.Negotiation.Status);
    }

    [Fact]
    public async Task ApproveAdWithStaleVersionAborts()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var adA = NewAd("user-1", 2);
        var adB = NewAd("user-2");
        h.SetAll(adA, adB);
        var id = (await h.Service.OpenAsync("user-1", adA.AdId, adB.AdId, default)).Negotiation.Id;

        var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.ApproveAdAsync("user-2", id, 1, default));
        Assert.Equal(DomainError.Aborted, ex.Error);
    }

    [Fact]
    public async Task AdEditedEventMakesApprovalStaleAndIgnoresLateDuplicates()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var adA = NewAd("user-1");
        var adB = NewAd("user-2");
        h.SetAll(adA, adB);
        var id = (await h.Service.OpenAsync("user-1", adA.AdId, adB.AdId, default)).Negotiation.Id;

        await using (var conn = await h.Db.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await EventHandlers.UpsertAdAsync(conn, tx, new Taakht.Ad.V1.Ad { Id = adB.AdId, OwnerId = "user-2", Version = 2, Status = Taakht.Ad.V1.AdStatus.Published });
            await EventHandlers.UpsertAdAsync(conn, tx, new Taakht.Ad.V1.Ad { Id = adB.AdId, OwnerId = "user-2", Version = 1, Status = Taakht.Ad.V1.AdStatus.Published });
            await tx.CommitAsync();
        }

        var view = await h.Service.GetAsync("user-1", id, default);
        Assert.Equal(2, view.Versions.TargetAdVersion);
        Assert.False(view.Negotiation.ApprovalViews(view.Versions).Single().Valid);
    }

    [Fact]
    public async Task OpenWithDuplicatePairIsAlreadyExists()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var adA = NewAd("user-1");
        var adB = NewAd("user-2");
        h.SetAll(adA, adB);
        await h.Service.OpenAsync("user-1", adA.AdId, adB.AdId, default);

        var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.OpenAsync("user-1", adA.AdId, adB.AdId, default));
        Assert.Equal(DomainError.AlreadyExists, ex.Error);
    }

    [Fact]
    public async Task OpenCapIsEnforcedAtomicallyUnderConcurrency()
    {
        if (Create(cap: 10) is not { } h)
        {
            return;
        }

        var target = NewAd("user-2");
        h.SetAll(target);
        var requesters = Enumerable.Range(0, 20).Select(i => NewAd($"requester-{i}")).ToList();
        h.SetAll([.. requesters]);

        var results = await Task.WhenAll(requesters.Select(async r =>
        {
            try
            {
                await h.Service.OpenAsync(r.OwnerId, r.AdId, target.AdId, default);
                return (DomainError?)null;
            }
            catch (DomainException ex)
            {
                return ex.Error;
            }
        }));

        Assert.Equal(10, results.Count(r => r is null));
        Assert.Equal(10, results.Count(r => r == DomainError.ResourceExhausted));
        await using var conn = await h.Db.OpenConnectionAsync();
        Assert.Equal(10, await conn.ExecuteScalarAsync<int>(
            "SELECT count(*)::int FROM negotiation WHERE target_ad_id = @id", new { id = target.AdId }));
    }

    [Fact]
    public async Task OpenCapAppliesToTheRequesterAdToo()
    {
        if (Create(cap: 2) is not { } h)
        {
            return;
        }

        var mine = NewAd("user-1");
        var targets = Enumerable.Range(0, 3).Select(_ => NewAd("user-2")).ToList();
        h.SetAll(mine);
        h.SetAll([.. targets]);

        await h.Service.OpenAsync("user-1", mine.AdId, targets[0].AdId, default);
        await h.Service.OpenAsync("user-1", mine.AdId, targets[1].AdId, default);
        var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.OpenAsync("user-1", mine.AdId, targets[2].AdId, default));
        Assert.Equal(DomainError.ResourceExhausted, ex.Error);
    }

    [Fact]
    public async Task ExclusiveLockAgreesWinnerCancelsCompetitorsAndIsIdempotent()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var target = NewAd("user-1");
        var b = NewAd("user-2");
        var c = NewAd("user-3");
        h.SetAll(target, b, c);

        var winner = (await h.Service.OpenAsync("user-2", b.AdId, target.AdId, default)).Negotiation.Id;
        var loser = (await h.Service.OpenAsync("user-3", c.AdId, target.AdId, default)).Negotiation.Id;
        await h.Service.ApproveAdAsync("user-1", winner, b.Version, default);
        await h.Service.ApproveProposalAsync("user-1", winner, 1, default);
        var pending = await h.Service.ApproveProposalAsync("user-2", winner, 1, default);
        Assert.Equal(NegotiationStatus.AgreementPending, pending.Negotiation.Status);

        var lockEvent = new Taakht.Swap.V1.ExclusiveLockAcquired
        {
            SwapId = Guid.NewGuid().ToString(),
            NegotiationId = winner.ToString(),
            AdAId = b.AdId,
            AdBId = target.AdId,
        };
        for (var i = 0; i < 2; i++)
        {
            await using var conn = await h.Db.OpenConnectionAsync();
            await using var tx = await conn.BeginTransactionAsync();
            await h.Events.OnExclusiveLockAsync(conn, tx, lockEvent);
            await tx.CommitAsync();
        }

        Assert.Equal(NegotiationStatus.Agreed, (await h.Service.GetAsync("user-1", winner, default)).Negotiation.Status);
        var cancelled = (await h.Service.GetAsync("user-3", loser, default)).Negotiation;
        Assert.Equal(NegotiationStatus.Cancelled, cancelled.Status);
        Assert.Equal(EventHandlers.CancelReasonLockedElsewhere, cancelled.CancelReason);

        var closed = (await h.OutboxAsync(loser)).Where(e => e.Type == NegV1.NegotiationClosed.Descriptor.FullName).ToList();
        var single = Assert.Single(closed);
        Assert.Equal(NegV1.NegotiationStatus.Cancelled, NegV1.NegotiationClosed.Parser.ParseFrom(single.Payload).Status);
    }

    [Fact]
    public async Task SwapRejectedCancelsThePendingNegotiation()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var a = NewAd("user-1");
        var b = NewAd("user-2");
        h.SetAll(a, b);
        var id = (await h.Service.OpenAsync("user-1", a.AdId, b.AdId, default)).Negotiation.Id;
        await h.Service.ApproveAdAsync("user-2", id, 1, default);
        await h.Service.ApproveProposalAsync("user-1", id, 1, default);
        await h.Service.ApproveProposalAsync("user-2", id, 1, default);

        await using (var conn = await h.Db.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await h.Events.OnSwapRejectedAsync(conn, tx, new Taakht.Swap.V1.SwapRejected { NegotiationId = id.ToString(), Reason = "ad not lockable" });
            await tx.CommitAsync();
        }

        var n = (await h.Service.GetAsync("user-1", id, default)).Negotiation;
        Assert.Equal(NegotiationStatus.Cancelled, n.Status);
        Assert.Equal("ad not lockable", n.CancelReason);
    }

    [Fact]
    public async Task GetAndListAreRestrictedToPartiesNewestFirst()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var (u1, u2, u3) = (Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
        var mine = NewAd(u1);
        var t1 = NewAd(u2);
        var t2 = NewAd(u3);
        h.SetAll(mine, t1, t2);

        var first = (await h.Service.OpenAsync(u1, mine.AdId, t1.AdId, default)).Negotiation.Id;
        var second = (await h.Service.OpenAsync(u1, mine.AdId, t2.AdId, default)).Negotiation.Id;

        var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.GetAsync(u3, first, default));
        Assert.Equal(DomainError.PermissionDenied, ex.Error);
        var list = await h.Service.ListAsync(u1, mine.AdId, default);
        Assert.Equal([second, first], list.Select(v => v.Negotiation.Id));
        Assert.Single(await h.Service.ListAsync(u2, null, default));
        Assert.Empty(await h.Service.ListAsync(u2, t2.AdId, default));
    }

    [Fact]
    public async Task CloseByTargetDeclinesAndEmitsClosedEvent()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var a = NewAd("user-1");
        var b = NewAd("user-2");
        h.SetAll(a, b);
        var id = (await h.Service.OpenAsync("user-1", a.AdId, b.AdId, default)).Negotiation.Id;

        var closed = await h.Service.CloseAsync("user-2", id, default);

        Assert.Equal(NegotiationStatus.Declined, closed.Negotiation.Status);
        Assert.Contains(await h.OutboxAsync(id), e => e.Type == NegV1.NegotiationClosed.Descriptor.FullName);
    }

    [Fact]
    public async Task ReviseWithLockerLegOfIneligibleOwnerIsFailedPrecondition()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var a = NewAd("user-1");
        var b = NewAd("user-4");
        h.SetAll(a, b);
        var id = (await h.Service.OpenAsync("user-1", a.AdId, b.AdId, default)).Negotiation.Id;

        var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.ReviseProposalAsync(
            "user-1", id, 1, new Terms(DeliveryMethod.InPerson, DeliveryMethod.Locker, 0, string.Empty), default));
        Assert.Equal(DomainError.FailedPrecondition, ex.Error);

        var ok = await h.Service.ReviseProposalAsync(
            "user-1", id, 1, new Terms(DeliveryMethod.Locker, DeliveryMethod.InPerson, 0, string.Empty), default);
        Assert.Equal(2, ok.Negotiation.ActiveProposal.Number);
    }
}
