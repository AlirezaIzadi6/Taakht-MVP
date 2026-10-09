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

        public HashSet<string> BrokenDetails { get; } = [];

        public List<string> DetailCalls { get; } = [];

        public Task<AdSnapshot?> GetAdAsync(string adId, CancellationToken ct)
        {
            lock (_gate)
            {
                return Task.FromResult(_ads.GetValueOrDefault(adId));
            }
        }

        public Task<Taakht.Ad.V1.Ad?> GetAdDetailsAsync(string adId, CancellationToken ct)
        {
            lock (_gate)
            {
                DetailCalls.Add(adId);
                if (BrokenDetails.Contains(adId))
                {
                    throw new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.Unavailable, "ad down"));
                }

                return Task.FromResult(_ads.TryGetValue(adId, out var ad)
                    ? new Taakht.Ad.V1.Ad
                    {
                        Id = ad.AdId,
                        OwnerId = ad.OwnerId,
                        Version = ad.Version,
                        Status = Taakht.Ad.V1.AdStatus.Published,
                        Spec = new Taakht.Ad.V1.AdSpec { Title = "title-" + ad.AdId },
                    }
                    : null);
            }
        }
    }

    private sealed class FakeClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public void Advance(TimeSpan by) => _now += by;

        public override DateTimeOffset GetUtcNow() => _now;
    }

    private sealed record Harness(FakeAdClient Ads, NegotiationService Service, EventHandlers Events, NpgsqlDataSource Db, FakeClock Clock)
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
        var clock = new FakeClock(DateTimeOffset.UtcNow);
        var service = new NegotiationService(
            db.DataSource, ads, new MockLockerEligibility(),
            new NegotiationOptions(cap) { AgreementPendingTimeout = _pendingTimeout }, clock, NullLogger<NegotiationService>.Instance);
        return new Harness(ads, service, new EventHandlers(clock, NullLogger<EventHandlers>.Instance), db.DataSource, clock);
    }

    private static readonly TimeSpan _pendingTimeout = TimeSpan.FromMinutes(10);

    // Two users, two published ads, all four approvals: the negotiation ends AGREEMENT_PENDING.
    private static async Task<(Guid Id, AdSnapshot RequesterAd, AdSnapshot TargetAd)> PendingAsync(Harness h, string requester, string target)
    {
        var a = NewAd(requester);
        var b = NewAd(target);
        h.SetAll(a, b);
        var id = (await h.Service.OpenAsync(requester, a.AdId, b.AdId, default)).Negotiation.Id;
        await h.Service.ApproveAdAsync(target, id, 1, default);
        await h.Service.ApproveProposalAsync(requester, id, 1, default);
        var view = await h.Service.ApproveProposalAsync(target, id, 1, default);
        Assert.Equal(NegotiationStatus.AgreementPending, view.Negotiation.Status);
        return (id, a, b);
    }

    // The database is shared by all tests of the class and the sweeper looks at every pending row: start from none.
    private static async Task ClearPendingAsync(Harness h)
    {
        await using var conn = await h.Db.OpenConnectionAsync();
        await conn.ExecuteAsync("UPDATE negotiation SET status = 'CANCELLED' WHERE status = 'AGREEMENT_PENDING'");
    }

    private static Taakht.Swap.V1.ExclusiveLockAcquired LockEvent(Guid winner, string adA, string adB) => new()
    {
        SwapId = Guid.NewGuid().ToString(),
        NegotiationId = winner.ToString(),
        AdAId = adA,
        AdBId = adB,
    };

    private static async Task ApplyLockAsync(Harness h, Taakht.Swap.V1.ExclusiveLockAcquired e)
    {
        await using var conn = await h.Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await h.Events.OnExclusiveLockAsync(conn, tx, e);
        await tx.CommitAsync();
    }

    private static async Task<NegotiationStatus> StatusOfAsync(Harness h, Guid id, string user) =>
        (await h.Service.GetAsync(user, id, default)).Negotiation.Status;

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
    public async Task SwapCancelledCancelsTheAgreedNegotiationOfThatAdPairAndIsStateChecked()
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
        var cancelled = new Taakht.Swap.V1.SwapCancelled { SwapId = Guid.NewGuid().ToString(), AdAId = a.AdId, AdBId = b.AdId, Reason = "locker fee not paid in time" };

        // Not agreed yet (still waiting for the lock): nothing to cancel.
        await ApplyCancelledAsync(h, cancelled);
        Assert.Equal(NegotiationStatus.AgreementPending, (await h.Service.GetAsync("user-1", id, default)).Negotiation.Status);

        await using (var conn = await h.Db.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await h.Events.OnExclusiveLockAsync(conn, tx, new Taakht.Swap.V1.ExclusiveLockAcquired { SwapId = cancelled.SwapId, NegotiationId = id.ToString(), AdAId = a.AdId, AdBId = b.AdId });
            await tx.CommitAsync();
        }

        // Reversed ad order and a duplicate delivery are both fine.
        await ApplyCancelledAsync(h, new Taakht.Swap.V1.SwapCancelled { SwapId = cancelled.SwapId, AdAId = b.AdId, AdBId = a.AdId, Reason = cancelled.Reason });
        await ApplyCancelledAsync(h, cancelled);

        var n = (await h.Service.GetAsync("user-1", id, default)).Negotiation;
        Assert.Equal(NegotiationStatus.Cancelled, n.Status);
        Assert.Equal("locker fee not paid in time", n.CancelReason);
        var closed = (await h.OutboxAsync(id)).Where(e => e.Type == NegV1.NegotiationClosed.Descriptor.FullName).ToList();
        Assert.Single(closed);

        // Unknown pair: ignored.
        await ApplyCancelledAsync(h, new Taakht.Swap.V1.SwapCancelled { SwapId = "x", AdAId = Guid.NewGuid().ToString(), AdBId = a.AdId, Reason = "r" });
    }

    private static async Task ApplyCancelledAsync(Harness h, Taakht.Swap.V1.SwapCancelled e)
    {
        await using var conn = await h.Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await h.Events.OnSwapCancelledAsync(conn, tx, e);
        await tx.CommitAsync();
    }

    [Fact]
    public async Task AdRefUpsertNeverMovesBackwardsAndHasNoStatusColumn()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var ad = NewAd("user-1", 3);
        await using var conn = await h.Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await NegotiationRepository.UpsertAdRefAsync(conn, tx, ad, default);
        await NegotiationRepository.UpsertAdRefAsync(conn, tx, ad with { Version = 2 }, default);
        await NegotiationRepository.UpsertAdRefAsync(conn, tx, ad with { Version = 3, OwnerId = "someone-else" }, default);
        Assert.Equal(3, await conn.ExecuteScalarAsync<int>("SELECT current_version FROM ad_ref WHERE ad_id = @id", new { id = ad.AdId }, tx));
        Assert.Equal("user-1", await conn.ExecuteScalarAsync<string>("SELECT owner_id FROM ad_ref WHERE ad_id = @id", new { id = ad.AdId }, tx));
        Assert.Equal(0, await conn.ExecuteScalarAsync<int>(
            "SELECT count(*)::int FROM information_schema.columns WHERE table_name = 'ad_ref' AND column_name = 'status'", transaction: tx));
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
        h.Clock.Advance(TimeSpan.FromSeconds(1));
        var second = (await h.Service.OpenAsync(u1, mine.AdId, t2.AdId, default)).Negotiation.Id;

        var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.GetAsync(u3, first, default));
        Assert.Equal(DomainError.PermissionDenied, ex.Error);
        var list = await h.Service.ListAsync(u1, mine.AdId, 0, null, default);
        Assert.Equal([second, first], list.Items.Select(v => v.Negotiation.Id));
        Assert.Equal(string.Empty, list.NextPageToken);
        Assert.Single((await h.Service.ListAsync(u2, null, 0, null, default)).Items);
        Assert.Empty((await h.Service.ListAsync(u2, t2.AdId, 0, null, default)).Items);
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

    [Fact]
    public async Task StaleLockEventOfACancelledWinnerDoesNotCancelANewPendingNegotiation()
    {
        if (Create() is not { } h)
        {
            return;
        }

        // N3 won ads (A, C) once (swap3) and was later cancelled with its swap; N1 on (A, B) is pending now.
        var adA = NewAd("user-1");
        var adB = NewAd("user-2");
        var adC = NewAd("user-3");
        h.SetAll(adA, adB, adC);
        var n3 = (await h.Service.OpenAsync("user-3", adC.AdId, adA.AdId, default)).Negotiation.Id;
        await h.Service.ApproveAdAsync("user-1", n3, 1, default);
        await h.Service.ApproveProposalAsync("user-1", n3, 1, default);
        await h.Service.ApproveProposalAsync("user-3", n3, 1, default);
        var swap3 = LockEvent(n3, adC.AdId, adA.AdId);
        await ApplyLockAsync(h, swap3);
        await ApplyCancelledAsync(h, new Taakht.Swap.V1.SwapCancelled { SwapId = swap3.SwapId, NegotiationId = n3.ToString(), Reason = "locker fee not paid in time" });
        Assert.Equal(NegotiationStatus.Cancelled, await StatusOfAsync(h, n3, "user-1"));

        var n1 = (await h.Service.OpenAsync("user-2", adB.AdId, adA.AdId, default)).Negotiation.Id;
        await h.Service.ApproveAdAsync("user-1", n1, 1, default);
        await h.Service.ApproveProposalAsync("user-1", n1, 1, default);
        await h.Service.ApproveProposalAsync("user-2", n1, 1, default);
        Assert.Equal(NegotiationStatus.AgreementPending, await StatusOfAsync(h, n1, "user-1"));

        // The old lock event for swap3 is delivered again (replay / late consumer).
        await ApplyLockAsync(h, swap3);

        Assert.Equal(NegotiationStatus.AgreementPending, await StatusOfAsync(h, n1, "user-1"));
        Assert.Equal(NegotiationStatus.Cancelled, await StatusOfAsync(h, n3, "user-1"));
        Assert.DoesNotContain(await h.OutboxAsync(n1), e => e.Type == NegV1.NegotiationClosed.Descriptor.FullName);
    }

    [Fact]
    public async Task LockEventForUnknownOrMismatchedWinnerIsIgnored()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var (winner, a, b) = await PendingAsync(h, "user-1", "user-2");
        var c = NewAd("user-3");
        h.SetAll(c);
        var other = (await h.Service.OpenAsync("user-3", c.AdId, b.AdId, default)).Negotiation.Id;

        await ApplyLockAsync(h, LockEvent(Guid.NewGuid(), a.AdId, b.AdId)); // unknown winner
        await ApplyLockAsync(h, LockEvent(winner, a.AdId, c.AdId)); // ads that are not the winner's
        Assert.Equal(NegotiationStatus.AgreementPending, await StatusOfAsync(h, winner, "user-1"));
        Assert.Equal(NegotiationStatus.Open, await StatusOfAsync(h, other, "user-3"));

        await ApplyLockAsync(h, LockEvent(winner, b.AdId, a.AdId)); // the right pair, either order
        Assert.Equal(NegotiationStatus.Agreed, await StatusOfAsync(h, winner, "user-1"));
        Assert.Equal(NegotiationStatus.Cancelled, await StatusOfAsync(h, other, "user-3"));
    }

    [Fact]
    public async Task SwapCancelledUsesTheNegotiationIdAndFallsBackToTheAdPairOnlyWhenItIsEmpty()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var (n1, a, b) = await PendingAsync(h, "user-1", "user-2");
        await ApplyLockAsync(h, LockEvent(n1, a.AdId, b.AdId));
        Assert.Equal(NegotiationStatus.Agreed, await StatusOfAsync(h, n1, "user-1"));

        // Another agreed negotiation must be untouched when the event names n1.
        var (n2, a2, b2) = await PendingAsync(h, "user-3", "user-4");
        await ApplyLockAsync(h, LockEvent(n2, a2.AdId, b2.AdId));
        Assert.Equal(NegotiationStatus.Agreed, await StatusOfAsync(h, n2, "user-3"));

        await ApplyCancelledAsync(h, new Taakht.Swap.V1.SwapCancelled
        {
            SwapId = "s",
            NegotiationId = n1.ToString(),
            AdAId = "ignored",
            AdBId = "ignored",
            Reason = "r1",
        });
        Assert.Equal(NegotiationStatus.Cancelled, await StatusOfAsync(h, n1, "user-1"));
        Assert.Equal(NegotiationStatus.Agreed, await StatusOfAsync(h, n2, "user-3"));

        // A malformed id is an error, not a fallback to the ad pair.
        await ApplyCancelledAsync(h, new Taakht.Swap.V1.SwapCancelled { SwapId = "s", NegotiationId = "nope", AdAId = a2.AdId, AdBId = b2.AdId });
        Assert.Equal(NegotiationStatus.Agreed, await StatusOfAsync(h, n2, "user-3"));

        // An unknown negotiation id does nothing either.
        await ApplyCancelledAsync(h, new Taakht.Swap.V1.SwapCancelled { SwapId = "s", NegotiationId = Guid.NewGuid().ToString() });
        Assert.Equal(NegotiationStatus.Agreed, await StatusOfAsync(h, n2, "user-3"));
    }

    [Fact]
    public async Task SweeperRepublishesThreeTimesThenCancelsAndLateEventsAreHandledSafely()
    {
        if (Create() is not { } h)
        {
            return;
        }

        await ClearPendingAsync(h);
        var (id, a, b) = await PendingAsync(h, "user-1", "user-2");
        var competitor = NewAd("user-3");
        h.SetAll(competitor);
        var other = (await h.Service.OpenAsync("user-3", competitor.AdId, b.AdId, default)).Negotiation.Id;

        static int Count(IEnumerable<Envelope> events) => events.Count(e => e.Type == NegV1.AgreementReached.Descriptor.FullName);
        Assert.Equal(1, Count(await h.OutboxAsync(id)));

        // Not due yet: nothing happens, in particular no cancel.
        h.Clock.Advance(_pendingTimeout - TimeSpan.FromSeconds(1));
        Assert.Equal(0, await h.Service.SweepAgreementPendingAsync(default));

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            h.Clock.Advance(TimeSpan.FromSeconds(attempt == 1 ? 2 : _pendingTimeout.TotalSeconds));
            Assert.Equal(1, await h.Service.SweepAgreementPendingAsync(default));
            Assert.Equal(0, await h.Service.SweepAgreementPendingAsync(default)); // once per period
            Assert.Equal(NegotiationStatus.AgreementPending, await StatusOfAsync(h, id, "user-1"));
            Assert.Equal(1 + attempt, Count(await h.OutboxAsync(id)));
        }

        // Every republished event carries the original agreed versions and a fresh event id.
        var events = (await h.OutboxAsync(id)).Where(e => e.Type == NegV1.AgreementReached.Descriptor.FullName).ToList();
        Assert.Equal(4, events.Select(e => e.EventId).Distinct().Count());
        foreach (var e in events)
        {
            var payload = NegV1.AgreementReached.Parser.ParseFrom(e.Payload);
            Assert.Equal(id.ToString(), payload.NegotiationId);
            Assert.Equal((a.AdId, 1), (payload.AdA.AdId, payload.AdA.Version));
            Assert.Equal((b.AdId, 1), (payload.AdB.AdId, payload.AdB.Version));
        }

        // The ad_ref moving on does not change what is republished (the agreed versions are stored).
        h.Clock.Advance(_pendingTimeout + TimeSpan.FromSeconds(1));
        Assert.Equal(1, await h.Service.SweepAgreementPendingAsync(default));
        var cancelled = (await h.Service.GetAsync("user-1", id, default)).Negotiation;
        Assert.Equal(NegotiationStatus.Cancelled, cancelled.Status);
        Assert.Equal(NegotiationService.AgreementTimedOutReason, cancelled.CancelReason);
        Assert.Equal(4, Count(await h.OutboxAsync(id)));
        var closed = Assert.Single(await h.OutboxAsync(id), e => e.Type == NegV1.NegotiationClosed.Descriptor.FullName);
        Assert.Equal(NegV1.NegotiationStatus.Cancelled, NegV1.NegotiationClosed.Parser.ParseFrom(closed.Payload).Status);
        Assert.Equal(0, await h.Service.SweepAgreementPendingAsync(default));

        // A late SwapRejected is ignored (not pending any more); so is a late lock event, which must not cancel
        // anybody: that is the documented residual risk, logged for an operator.
        await using (var conn = await h.Db.OpenConnectionAsync())
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await h.Events.OnSwapRejectedAsync(conn, tx, new Taakht.Swap.V1.SwapRejected { NegotiationId = id.ToString(), Reason = "late" });
            await tx.CommitAsync();
        }

        await ApplyLockAsync(h, LockEvent(id, a.AdId, b.AdId));
        Assert.Equal(NegotiationStatus.Cancelled, await StatusOfAsync(h, id, "user-1"));
        Assert.Equal(NegotiationService.AgreementTimedOutReason, (await h.Service.GetAsync("user-1", id, default)).Negotiation.CancelReason);
        Assert.Equal(NegotiationStatus.Open, await StatusOfAsync(h, other, "user-3"));
    }

    [Fact]
    public async Task SweeperLeavesAnAnsweredNegotiationAlone()
    {
        if (Create() is not { } h)
        {
            return;
        }

        await ClearPendingAsync(h);
        var (id, a, b) = await PendingAsync(h, "user-1", "user-2");
        await ApplyLockAsync(h, LockEvent(id, a.AdId, b.AdId));
        h.Clock.Advance(_pendingTimeout * 5);

        Assert.Equal(0, await h.Service.SweepAgreementPendingAsync(default));
        Assert.Equal(NegotiationStatus.Agreed, await StatusOfAsync(h, id, "user-1"));
    }

    [Fact]
    public async Task OpenAnswersLikeAMissingAdForForeignRequesterAdsAndUnpublishedTargets()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var foreign = NewAd("user-9");
        var hiddenTarget = NewAd("user-2", status: AdStatus.Hidden);
        var publishedTarget = NewAd("user-2");
        var mine = NewAd("user-1");
        h.SetAll(foreign, hiddenTarget, publishedTarget, mine);
        var missing = Guid.NewGuid().ToString();

        var errors = new List<DomainException>
        {
            await Assert.ThrowsAsync<DomainException>(() => h.Service.OpenAsync("user-1", missing, publishedTarget.AdId, default)),
            await Assert.ThrowsAsync<DomainException>(() => h.Service.OpenAsync("user-1", foreign.AdId, publishedTarget.AdId, default)),
            await Assert.ThrowsAsync<DomainException>(() => h.Service.OpenAsync("user-1", mine.AdId, missing, default)),
            await Assert.ThrowsAsync<DomainException>(() => h.Service.OpenAsync("user-1", mine.AdId, hiddenTarget.AdId, default)),
        };
        Assert.All(errors, e =>
        {
            Assert.Equal(DomainError.NotFound, e.Error);
            Assert.Equal("ad not available", e.Message);
        });
    }

    [Fact]
    public async Task ResponsesCarryBothAdsForThePartiesAndAFailingAdFetchNeverFailsTheCall()
    {
        if (Create() is not { } h)
        {
            return;
        }

        var (u1, u2, u3) = (Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
        var a = NewAd(u1);
        var b = NewAd(u2);
        var c = NewAd(u3);
        h.SetAll(a, b, c);

        var opened = await h.Service.OpenAsync(u1, a.AdId, b.AdId, default);
        Assert.Equal(a.AdId, opened.RequesterAd?.Id);
        Assert.Equal(b.AdId, opened.TargetAd?.Id);
        Assert.Equal("title-" + b.AdId, opened.TargetAd?.Spec.Title);
        var id = opened.Negotiation.Id;
        var approved = await h.Service.ApproveAdAsync(u2, id, 1, default);
        Assert.NotNull(approved.RequesterAd);
        Assert.NotNull((await h.Service.ApproveProposalAsync(u1, id, 1, default)).TargetAd);
        Assert.NotNull((await h.Service.CloseAsync(u1, id, default)).RequesterAd);

        // Mapped to the wire message too.
        var wire = Api.Mapper.ToProto(await h.Service.GetAsync(u2, id, default));
        Assert.Equal(a.AdId, wire.RequesterAd.Id);
        Assert.Equal(b.AdId, wire.TargetAd.Id);

        // Only parties: a stranger gets PERMISSION_DENIED and no ad is fetched for them.
        h.Ads.DetailCalls.Clear();
        await Assert.ThrowsAsync<DomainException>(() => h.Service.GetAsync(u3, id, default));
        Assert.Empty(h.Ads.DetailCalls);

        // A list shares one fetch per distinct ad.
        var d = NewAd(u2);
        h.SetAll(d);
        await h.Service.OpenAsync(u1, a.AdId, d.AdId, default);
        h.Ads.DetailCalls.Clear();
        var list = (await h.Service.ListAsync(u1, null, 0, null, default)).Items;
        Assert.Equal(2, list.Count);
        Assert.All(list, v => Assert.NotNull(v.RequesterAd));
        Assert.Equal(3, h.Ads.DetailCalls.Count); // a, b, d: ad a appears in both negotiations but is fetched once
        Assert.Equal(3, h.Ads.DetailCalls.Distinct().Count());

        // Ad down for one ad: that field is empty, the rest of the call works.
        h.Ads.BrokenDetails.Add(b.AdId);
        var degraded = await h.Service.GetAsync(u1, id, default);
        Assert.NotNull(degraded.RequesterAd);
        Assert.Null(degraded.TargetAd);
        Assert.Equal(NegotiationStatus.Withdrawn, degraded.Negotiation.Status);
        var degradedList = (await h.Service.ListAsync(u1, null, 0, null, default)).Items;
        Assert.Equal(2, degradedList.Count);
        var wireDegraded = Api.Mapper.ToProto(degraded);
        Assert.Null(wireDegraded.TargetAd);
    }

    // Opens n negotiations for one user (all at the same fake-clock instant unless advance is set), newest last.
    private static async Task<List<Guid>> OpenManyAsync(Harness h, string requester, int n, bool advance)
    {
        var mine = NewAd(requester);
        h.SetAll(mine);
        var ids = new List<Guid>();
        for (var i = 0; i < n; i++)
        {
            var target = NewAd(Guid.NewGuid().ToString());
            h.Ads.Set(target);
            if (advance)
            {
                h.Clock.Advance(TimeSpan.FromSeconds(1));
            }

            ids.Add((await h.Service.OpenAsync(requester, mine.AdId, target.AdId, default)).Negotiation.Id);
        }

        return ids;
    }

    private static async Task<List<Guid>> WalkAsync(Harness h, string user, string? adId, int size)
    {
        var seen = new List<Guid>();
        string? token = null;
        var pages = 0;
        do
        {
            var page = await h.Service.ListAsync(user, adId, size, token, default);
            seen.AddRange(page.Items.Select(v => v.Negotiation.Id));
            token = page.NextPageToken;
            Assert.True(++pages < 100);
        }
        while (token != string.Empty);
        return seen;
    }

    [Fact]
    public async Task ListPagesWalkEverythingOnceWithTiedTimestamps()
    {
        var h = Create(cap: 1000);
        if (h is null)
        {
            return;
        }

        var u1 = Guid.NewGuid().ToString();
        var ids = await OpenManyAsync(h, u1, 7, advance: false); // identical created_at: only the id breaks ties

        var first = await h.Service.ListAsync(u1, null, 3, null, default);
        Assert.Equal(3, first.Items.Count);
        Assert.NotEmpty(first.NextPageToken);

        var seen = await WalkAsync(h, u1, null, 3);
        Assert.Equal(7, seen.Count);
        Assert.Equal(ids.OrderByDescending(x => x.ToString(), StringComparer.Ordinal), seen.AsEnumerable()); // created_at DESC, id DESC
        Assert.Equal(seen, await WalkAsync(h, u1, null, 200));

        var exact = await h.Service.ListAsync(u1, null, 7, null, default);
        Assert.Equal(7, exact.Items.Count);
        Assert.Equal(string.Empty, exact.NextPageToken);

        Assert.Empty((await h.Service.ListAsync(Guid.NewGuid().ToString(), null, 0, null, default)).Items);
    }

    [Fact]
    public async Task ListPageSizeDefaultsAndIsClamped()
    {
        var h = Create(cap: 1000);
        if (h is null)
        {
            return;
        }

        var u1 = Guid.NewGuid().ToString();
        await OpenManyAsync(h, u1, 205, advance: true);

        Assert.Equal(50, (await h.Service.ListAsync(u1, null, 0, null, default)).Items.Count);
        Assert.Equal(50, (await h.Service.ListAsync(u1, null, -3, null, default)).Items.Count);
        var big = await h.Service.ListAsync(u1, null, 100000, null, default);
        Assert.Equal(200, big.Items.Count);
        Assert.NotEmpty(big.NextPageToken);
        var rest = await h.Service.ListAsync(u1, null, 100000, big.NextPageToken, default);
        Assert.Equal(5, rest.Items.Count);
        Assert.Equal(string.Empty, rest.NextPageToken);
    }

    [Fact]
    public async Task ListPageEnrichesOnlyTheReturnedPage()
    {
        var h = Create(cap: 1000);
        if (h is null)
        {
            return;
        }

        var u1 = Guid.NewGuid().ToString();
        await OpenManyAsync(h, u1, 6, advance: true);
        h.Ads.DetailCalls.Clear();

        var page = await h.Service.ListAsync(u1, null, 2, null, default);

        Assert.Equal(2, page.Items.Count);
        Assert.Equal(3, h.Ads.DetailCalls.Distinct().Count()); // own ad once + two counterpart ads
        Assert.Equal(3, h.Ads.DetailCalls.Count);
    }

    [Fact]
    public async Task ListInvalidTokenIsInvalidArgument()
    {
        var h = Create();
        if (h is null)
        {
            return;
        }

        foreach (var token in new[] { "!!!", "abc", "bm9waXBl" })
        {
            var ex = await Assert.ThrowsAsync<DomainException>(() => h.Service.ListAsync("user-1", null, 10, token, default));
            Assert.Equal(DomainError.InvalidArgument, ex.Error);
        }
    }

    [Fact]
    public async Task ListInsertBetweenPagesDoesNotDuplicateOrSkipOlderItems()
    {
        var h = Create(cap: 1000);
        if (h is null)
        {
            return;
        }

        var u1 = Guid.NewGuid().ToString();
        var ids = await OpenManyAsync(h, u1, 6, advance: true);
        var p1 = await h.Service.ListAsync(u1, null, 3, null, default);

        var late = NewAd(Guid.NewGuid().ToString());
        h.Ads.Set(late);
        h.Clock.Advance(TimeSpan.FromSeconds(1));
        var mine = p1.Items[0].Negotiation.RequesterAdId;
        await h.Service.OpenAsync(u1, mine, late.AdId, default);

        var p2 = await h.Service.ListAsync(u1, null, 10, p1.NextPageToken, default);

        Assert.Equal(string.Empty, p2.NextPageToken);
        var all = p1.Items.Concat(p2.Items).Select(v => v.Negotiation.Id).ToList();
        Assert.Equal(6, all.Count);
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Equal(ids.OrderByDescending(x => ids.IndexOf(x)), all);
    }

    [Fact]
    public async Task ListAdFilterIsKeptWhilePaging()
    {
        var h = Create(cap: 1000);
        if (h is null)
        {
            return;
        }

        var u1 = Guid.NewGuid().ToString();
        var a = NewAd(u1);
        var b = NewAd(u1);
        h.SetAll(a, b);
        for (var i = 0; i < 3; i++)
        {
            var t = NewAd(Guid.NewGuid().ToString());
            h.Ads.Set(t);
            h.Clock.Advance(TimeSpan.FromSeconds(1));
            await h.Service.OpenAsync(u1, a.AdId, t.AdId, default);
        }

        var tb = NewAd(Guid.NewGuid().ToString());
        h.Ads.Set(tb);
        await h.Service.OpenAsync(u1, b.AdId, tb.AdId, default);

        Assert.Equal(3, (await WalkAsync(h, u1, a.AdId, 2)).Count);
        Assert.Equal(4, (await WalkAsync(h, u1, null, 2)).Count);
    }
}
