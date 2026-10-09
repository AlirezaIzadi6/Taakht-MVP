using Dapper;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Taakht.Common.V1;
using Taakht.Negotiation.V1;
using Taakht.Platform;
using Taakht.Swap.Application;
using Taakht.Swap.Domain;
using Taakht.Swap.Infrastructure;
using DomainDelivery = Taakht.Swap.Domain.DeliveryMethod;
using PbDelivery = Taakht.Negotiation.V1.DeliveryMethod;
using SwapPb = Taakht.Swap.V1;

namespace Taakht.Swap.Tests;

/// <summary>Runs only when TEST_DATABASE_URL points at a Postgres server where databases can be created.</summary>
public sealed class DbFactAttribute : FactAttribute
{
    public DbFactAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("TEST_DATABASE_URL")))
        {
            Skip = "TEST_DATABASE_URL is not set";
        }
    }
}

public sealed class TestDatabase : IAsyncLifetime
{
    private readonly string _name = $"swap_test_{Guid.NewGuid():N}";
    private string _adminConnectionString = "";

    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var url = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");
        if (string.IsNullOrEmpty(url))
        {
            return;
        }

        _adminConnectionString = DatabaseUrl.ToConnectionString(url);
        await using (var admin = new NpgsqlConnection(_adminConnectionString))
        {
            await admin.ExecuteAsync($"CREATE DATABASE {_name}");
        }

        var builder = new NpgsqlConnectionStringBuilder(_adminConnectionString) { Database = _name };
        DataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        await Migrator.MigrateAsync(DataSource, typeof(SwapWorkflow).Assembly);
    }

    public async Task DisposeAsync()
    {
        if (DataSource is null)
        {
            return;
        }

        await DataSource.DisposeAsync();
        NpgsqlConnection.ClearAllPools();
        await using var admin = new NpgsqlConnection(_adminConnectionString);
        await admin.ExecuteAsync($"DROP DATABASE IF EXISTS {_name} WITH (FORCE)");
    }
}

public sealed class FakeAdClient : IAdClient
{
    private readonly Queue<Func<LockOutcome>> _script = new();

    public List<Guid> Calls { get; } = [];

    public FakeAdClient Then(Func<LockOutcome> step)
    {
        _script.Enqueue(step);
        return this;
    }

    public Task<LockOutcome> LockAdsAsync(Guid swapId, IReadOnlyList<AdVersionRef> ads, CancellationToken ct)
    {
        Calls.Add(swapId);
        return Task.FromResult(_script.Count > 0 ? _script.Dequeue()() : new LockOutcome.Locked());
    }
}

public sealed class FakeClock(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public void Advance(TimeSpan by) => _now += by;

    public override DateTimeOffset GetUtcNow() => _now;
}

public class WorkflowDatabaseTests(TestDatabase db) : IClassFixture<TestDatabase>
{
    private static readonly TimeSpan _window = TimeSpan.FromMinutes(2);

    private static AgreementReached Agreement(string negotiationId, PbDelivery legA = PbDelivery.Locker, PbDelivery legB = PbDelivery.Locker) => new()
    {
        NegotiationId = negotiationId,
        AdA = new AgreedAd { AdId = $"ad-a-{negotiationId}", OwnerId = "user-1", Version = 3 },
        AdB = new AgreedAd { AdId = $"ad-b-{negotiationId}", OwnerId = "user-2", Version = 5 },
        Terms = new Terms { LegA = legA, LegB = legB },
    };

    private (SwapWorkflow Workflow, SwapStore Store, FakeClock Clock) Build(FakeAdClient ads)
    {
        var store = new SwapStore(db.DataSource);
        var clock = new FakeClock(DateTimeOffset.UtcNow);
        var workflow = new SwapWorkflow(
            store, ads, new MockDeliveryProvider(store), new SwapOptions(_window), clock, NullLogger<SwapWorkflow>.Instance);
        return (workflow, store, clock);
    }

    private async Task<List<string>> OutboxTypesAsync(Guid swapId)
    {
        await using var conn = await db.DataSource.OpenConnectionAsync();
        var rows = await conn.QueryAsync<byte[]>("SELECT envelope FROM outbox WHERE key = @k ORDER BY created_at, id", new { k = swapId.ToString() });
        return [.. rows.Select(r => Envelope.Parser.ParseFrom(r).Type)];
    }

    private async Task<SwapModel> SwapOfAsync(SwapStore store, string negotiationId)
    {
        await using var conn = await db.DataSource.OpenConnectionAsync();
        var id = await conn.QuerySingleAsync<Guid>("SELECT id FROM swap WHERE negotiation_id = @n", new { n = negotiationId });
        return (await store.GetAsync(id, CancellationToken.None))!;
    }

    [DbFact]
    public async Task Duplicate_agreement_creates_one_swap_and_locks_once()
    {
        var ads = new FakeAdClient();
        var (workflow, store, _) = Build(ads);
        var msg = Agreement("neg-dup");

        await workflow.HandleAgreementReachedAsync(msg, CancellationToken.None);
        await workflow.HandleAgreementReachedAsync(msg, CancellationToken.None);

        await using var conn = await db.DataSource.OpenConnectionAsync();
        Assert.Equal(1, await conn.QuerySingleAsync<int>("SELECT count(*) FROM swap WHERE negotiation_id = 'neg-dup'"));
        var swap = await SwapOfAsync(store, "neg-dup");
        Assert.Equal(SwapStatus.AwaitingPayment, swap.Status);
        Assert.NotNull(swap.PaymentDeadline);
        Assert.Single(ads.Calls);
        Assert.Equal(["taakht.swap.v1.ExclusiveLockAcquired"], await OutboxTypesAsync(swap.Id));
    }

    [DbFact]
    public async Task Rejected_lock_marks_swap_rejected_and_emits_event()
    {
        var ads = new FakeAdClient().Then(() => new LockOutcome.Rejected("ad version changed"));
        var (workflow, store, _) = Build(ads);

        await workflow.HandleAgreementReachedAsync(Agreement("neg-rej"), CancellationToken.None);

        var swap = await SwapOfAsync(store, "neg-rej");
        Assert.Equal(SwapStatus.Rejected, swap.Status);
        Assert.Equal("ad version changed", swap.CancelReason);
        Assert.Equal(["taakht.swap.v1.SwapRejected"], await OutboxTypesAsync(swap.Id));
    }

    [DbFact]
    public async Task Retry_after_transient_failure_succeeds_without_duplicating_the_swap()
    {
        var ads = new FakeAdClient()
            .Then(() => throw new RpcException(new Status(StatusCode.Unavailable, "ad down")));
        var (workflow, store, _) = Build(ads);
        var msg = Agreement("neg-retry");

        await Assert.ThrowsAsync<RpcException>(() => workflow.HandleAgreementReachedAsync(msg, CancellationToken.None));
        Assert.Equal(SwapStatus.Locking, (await SwapOfAsync(store, "neg-retry")).Status);

        await workflow.HandleAgreementReachedAsync(msg, CancellationToken.None);

        var swap = await SwapOfAsync(store, "neg-retry");
        Assert.Equal(SwapStatus.AwaitingPayment, swap.Status);
        Assert.Equal(2, ads.Calls.Count);
        Assert.Equal(ads.Calls[0], ads.Calls[1]);
        Assert.Equal(["taakht.swap.v1.ExclusiveLockAcquired"], await OutboxTypesAsync(swap.Id));
    }

    [DbFact]
    public async Task Agreement_without_locker_legs_completes_immediately()
    {
        var (workflow, store, _) = Build(new FakeAdClient());

        await workflow.HandleAgreementReachedAsync(Agreement("neg-nolocker", PbDelivery.InPerson, PbDelivery.InPerson), CancellationToken.None);

        var swap = await SwapOfAsync(store, "neg-nolocker");
        Assert.Equal(SwapStatus.Completed, swap.Status);
        Assert.Equal(
            ["taakht.swap.v1.ExclusiveLockAcquired", "taakht.swap.v1.SwapCompleted"],
            await OutboxTypesAsync(swap.Id));
    }

    [DbFact]
    public async Task Fee_payments_complete_the_swap_and_replays_are_idempotent()
    {
        var (workflow, store, _) = Build(new FakeAdClient());
        await workflow.HandleAgreementReachedAsync(Agreement("neg-pay"), CancellationToken.None);
        var swap = await SwapOfAsync(store, "neg-pay");

        var afterFirst = await workflow.RecordFeePaidAsync(swap.Id, "user-1", CancellationToken.None);
        await workflow.RecordFeePaidAsync(swap.Id, "user-1", CancellationToken.None);
        Assert.Equal(SwapStatus.AwaitingPayment, afterFirst.Status);

        var done = await workflow.RecordFeePaidAsync(swap.Id, "user-2", CancellationToken.None);
        await workflow.RecordFeePaidAsync(swap.Id, "user-2", CancellationToken.None);

        Assert.Equal(SwapStatus.Completed, done.Status);
        Assert.Equal(
            ["taakht.swap.v1.ExclusiveLockAcquired", "taakht.swap.v1.SwapCompleted"],
            await OutboxTypesAsync(swap.Id));
    }

    [DbFact]
    public async Task Fee_paid_by_a_stranger_is_a_failed_precondition()
    {
        var (workflow, store, _) = Build(new FakeAdClient());
        await workflow.HandleAgreementReachedAsync(Agreement("neg-stranger"), CancellationToken.None);
        var swap = await SwapOfAsync(store, "neg-stranger");

        await Assert.ThrowsAsync<SwapDomainException>(() => workflow.RecordFeePaidAsync(swap.Id, "user-9", CancellationToken.None));
    }

    [DbFact]
    public async Task Sweeper_cancels_unpaid_swap_after_deadline_and_blames_first_unpaid_leg()
    {
        var (workflow, store, clock) = Build(new FakeAdClient());
        await workflow.HandleAgreementReachedAsync(Agreement("neg-timeout"), CancellationToken.None);
        var swap = await SwapOfAsync(store, "neg-timeout");
        await workflow.RecordFeePaidAsync(swap.Id, "user-1", CancellationToken.None);

        Assert.Equal(0, await workflow.SweepOverdueAsync(CancellationToken.None));
        clock.Advance(_window + TimeSpan.FromSeconds(1));
        Assert.True(await workflow.SweepOverdueAsync(CancellationToken.None) >= 1);

        var cancelled = await SwapOfAsync(store, "neg-timeout");
        Assert.Equal(SwapStatus.Cancelled, cancelled.Status);
        Assert.Equal("locker fee not paid in time", cancelled.CancelReason);
        Assert.Equal(
            ["taakht.swap.v1.ExclusiveLockAcquired", "taakht.swap.v1.SwapCancelled"],
            await OutboxTypesAsync(swap.Id));
        await Assert.ThrowsAsync<SwapDomainException>(() => workflow.RecordFeePaidAsync(swap.Id, "user-2", CancellationToken.None));
    }

    [DbFact]
    public async Task Payment_racing_the_sweeper_yields_exactly_one_terminal_event()
    {
        var (workflow, store, clock) = Build(new FakeAdClient());
        for (var i = 0; i < 10; i++)
        {
            var negotiationId = $"neg-race-{i}";
            await workflow.HandleAgreementReachedAsync(Agreement(negotiationId, PbDelivery.Locker, PbDelivery.InPerson), CancellationToken.None);
            var swap = await SwapOfAsync(store, negotiationId);
            clock.Advance(_window + TimeSpan.FromSeconds(1));

            var pay = Task.Run(async () =>
            {
                try
                {
                    await workflow.RecordFeePaidAsync(swap.Id, "user-1", CancellationToken.None);
                }
                catch (SwapDomainException)
                {
                    // The sweeper won.
                }
            });
            var sweep = Task.Run(() => workflow.SweepOverdueAsync(CancellationToken.None));
            await Task.WhenAll(pay, sweep);

            var final = await SwapOfAsync(store, negotiationId);
            var terminal = (await OutboxTypesAsync(swap.Id))
                .Count(t => t is "taakht.swap.v1.SwapCompleted" or "taakht.swap.v1.SwapCancelled");
            Assert.Contains(final.Status, new[] { SwapStatus.Completed, SwapStatus.Cancelled });
            Assert.Equal(1, terminal);
        }
    }

    [DbFact]
    public async Task Only_parties_see_a_swap_in_listings()
    {
        var (workflow, store, _) = Build(new FakeAdClient());
        await workflow.HandleAgreementReachedAsync(Agreement("neg-list"), CancellationToken.None);
        var swap = await SwapOfAsync(store, "neg-list");

        Assert.Contains((await store.ListForUserAsync("user-1", 200, null, CancellationToken.None)).Items, s => s.Id == swap.Id);
        Assert.Contains((await store.ListForUserAsync("user-2", 200, null, CancellationToken.None)).Items, s => s.Id == swap.Id);
        Assert.DoesNotContain((await store.ListForUserAsync("user-3", 200, null, CancellationToken.None)).Items, s => s.Id == swap.Id);
        Assert.False(swap.IsParty("user-3"));
        Assert.Equal(DomainDelivery.Locker, swap.LegA.Method);
    }

    private static AgreementReached AgreementFor(string negotiationId, string owner, string other) => new()
    {
        NegotiationId = negotiationId,
        AdA = new AgreedAd { AdId = $"ad-a-{negotiationId}", OwnerId = owner, Version = 1 },
        AdB = new AgreedAd { AdId = $"ad-b-{negotiationId}", OwnerId = other, Version = 1 },
        Terms = new Terms { LegA = PbDelivery.Locker, LegB = PbDelivery.Locker },
    };

    private async Task<(SwapStore Store, string Owner, List<Guid> Ids)> SeedSwapsAsync(int count, bool advance)
    {
        var (workflow, store, clock) = Build(new FakeAdClient());
        var owner = $"owner-{Guid.NewGuid():N}";
        var tag = Guid.NewGuid().ToString("N");
        var ids = new List<Guid>();
        for (var i = 0; i < count; i++)
        {
            if (advance)
            {
                clock.Advance(TimeSpan.FromSeconds(1));
            }

            // Alternate the user between the two legs: both owner columns must be paged together.
            var msg = i % 2 == 0 ? AgreementFor($"pg-{tag}-{i}", owner, "other") : AgreementFor($"pg-{tag}-{i}", "other", owner);
            await workflow.HandleAgreementReachedAsync(msg, CancellationToken.None);
            ids.Add((await SwapOfAsync(store, msg.NegotiationId)).Id);
        }

        return (store, owner, ids);
    }

    private static async Task<List<Guid>> WalkAsync(SwapStore store, string owner, int size)
    {
        var seen = new List<Guid>();
        var token = (string?)null;
        var pages = 0;
        do
        {
            var page = await store.ListForUserAsync(owner, size, token, CancellationToken.None);
            seen.AddRange(page.Items.Select(s => s.Id));
            token = page.NextPageToken;
            Assert.True(++pages < 100);
        }
        while (token != string.Empty);
        return seen;
    }

    [DbFact]
    public async Task Listing_pages_walk_every_swap_once_even_with_tied_timestamps()
    {
        var (store, owner, ids) = await SeedSwapsAsync(7, advance: false); // identical created_at: the id breaks ties

        var first = await store.ListForUserAsync(owner, 3, null, CancellationToken.None);
        Assert.Equal(3, first.Items.Count);
        Assert.NotEmpty(first.NextPageToken);

        var seen = await WalkAsync(store, owner, 3);
        Assert.Equal(ids.OrderByDescending(x => x.ToString(), StringComparer.Ordinal), seen);
        Assert.Equal(seen, await WalkAsync(store, owner, 200));

        var exact = await store.ListForUserAsync(owner, 7, null, CancellationToken.None);
        Assert.Equal(7, exact.Items.Count);
        Assert.Equal(string.Empty, exact.NextPageToken);
        Assert.Empty((await store.ListForUserAsync($"nobody-{Guid.NewGuid():N}", 0, null, CancellationToken.None)).Items);
    }

    [DbFact]
    public async Task Listing_is_newest_first_and_page_size_is_defaulted_and_clamped()
    {
        var (store, owner, ids) = await SeedSwapsAsync(205, advance: true);

        var def = await store.ListForUserAsync(owner, 0, null, CancellationToken.None);
        Assert.Equal(50, def.Items.Count);
        Assert.Equal(ids[^1], def.Items[0].Id); // newest first
        Assert.Equal(50, (await store.ListForUserAsync(owner, -1, null, CancellationToken.None)).Items.Count);
        var big = await store.ListForUserAsync(owner, 100000, null, CancellationToken.None);
        Assert.Equal(200, big.Items.Count);
        var rest = await store.ListForUserAsync(owner, 100000, big.NextPageToken, CancellationToken.None);
        Assert.Equal(5, rest.Items.Count);
        Assert.Equal(string.Empty, rest.NextPageToken);
    }

    [DbFact]
    public async Task Listing_rejects_a_malformed_token()
    {
        var (_, store, _) = Build(new FakeAdClient());
        foreach (var token in new[] { "!!!", "abc", "bm9waXBl" })
        {
            await Assert.ThrowsAsync<InvalidPageTokenException>(() => store.ListForUserAsync("user-1", 10, token, CancellationToken.None));
        }
    }

    [DbFact]
    public async Task Listing_insert_between_pages_does_not_duplicate_or_skip_older_swaps()
    {
        var (store, owner, ids) = await SeedSwapsAsync(6, advance: true);
        var p1 = await store.ListForUserAsync(owner, 3, null, CancellationToken.None);

        var (workflow, _, clock) = Build(new FakeAdClient());
        clock.Advance(TimeSpan.FromDays(1));
        await workflow.HandleAgreementReachedAsync(AgreementFor($"pg-late-{Guid.NewGuid():N}", owner, "other"), CancellationToken.None);

        var p2 = await store.ListForUserAsync(owner, 10, p1.NextPageToken, CancellationToken.None);
        Assert.Equal(string.Empty, p2.NextPageToken);
        var all = p1.Items.Concat(p2.Items).Select(s => s.Id).ToList();
        Assert.Equal(6, all.Count);
        Assert.Equal(all.Count, all.Distinct().Count());
        Assert.Equal(Enumerable.Reverse(ids), all);
    }

    [DbFact]
    public async Task Malformed_agreement_with_a_negotiation_id_is_rejected_and_announced()
    {
        var ads = new FakeAdClient();
        var (workflow, store, _) = Build(ads);
        var broken = new AgreementReached { NegotiationId = "neg-malformed", AdA = new AgreedAd { AdId = "x", OwnerId = "user-1", Version = 1 } };

        await workflow.HandleAgreementReachedAsync(broken, CancellationToken.None);
        await workflow.HandleAgreementReachedAsync(broken, CancellationToken.None); // redelivery changes nothing

        var swap = await SwapOfAsync(store, "neg-malformed");
        Assert.Equal(SwapStatus.Rejected, swap.Status);
        Assert.Equal(SwapWorkflow.MalformedReason, swap.CancelReason);
        Assert.Empty(ads.Calls);
        await using var conn = await db.DataSource.OpenConnectionAsync();
        var envelopes = (await conn.QueryAsync<byte[]>(
            "SELECT envelope FROM outbox WHERE key = @k", new { k = swap.Id.ToString() })).Select(Envelope.Parser.ParseFrom).ToList();
        var rejected = Assert.Single(envelopes);
        var payload = SwapPb.SwapRejected.Parser.ParseFrom(rejected.Payload);
        Assert.Equal("neg-malformed", payload.NegotiationId);
        Assert.Equal("malformed agreement", payload.Reason);
    }

    [DbFact]
    public async Task Malformed_agreement_without_a_negotiation_id_creates_nothing()
    {
        var (workflow, _, _) = Build(new FakeAdClient());
        await using var conn = await db.DataSource.OpenConnectionAsync();
        var before = await conn.QuerySingleAsync<int>("SELECT count(*) FROM swap");

        await workflow.HandleAgreementReachedAsync(new AgreementReached { AdA = new AgreedAd { AdId = "a" } }, CancellationToken.None);

        Assert.Equal(before, await conn.QuerySingleAsync<int>("SELECT count(*) FROM swap"));
    }

    [DbFact]
    public async Task Malformed_duplicate_does_not_reject_a_wellformed_swap()
    {
        var (workflow, store, _) = Build(new FakeAdClient());
        await workflow.HandleAgreementReachedAsync(Agreement("neg-good-then-bad"), CancellationToken.None);

        await workflow.HandleAgreementReachedAsync(new AgreementReached { NegotiationId = "neg-good-then-bad" }, CancellationToken.None);

        Assert.Equal(SwapStatus.AwaitingPayment, (await SwapOfAsync(store, "neg-good-then-bad")).Status);
    }

    [DbFact]
    public async Task Completed_and_cancelled_events_carry_the_negotiation_id()
    {
        var (workflow, store, clock) = Build(new FakeAdClient());
        await workflow.HandleAgreementReachedAsync(Agreement("neg-ev-done", PbDelivery.InPerson, PbDelivery.InPerson), CancellationToken.None);
        await workflow.HandleAgreementReachedAsync(Agreement("neg-ev-cancel"), CancellationToken.None);
        clock.Advance(_window + TimeSpan.FromSeconds(1));
        await workflow.SweepOverdueAsync(CancellationToken.None);

        var done = await SwapOfAsync(store, "neg-ev-done");
        var cancelled = await SwapOfAsync(store, "neg-ev-cancel");
        await using var conn = await db.DataSource.OpenConnectionAsync();
        async Task<Envelope> OfTypeAsync(Guid swapId, string type) =>
            (await conn.QueryAsync<byte[]>("SELECT envelope FROM outbox WHERE key = @k", new { k = swapId.ToString() }))
                .Select(Envelope.Parser.ParseFrom).Single(e => e.Type == type);

        var completedEvent = SwapPb.SwapCompleted.Parser.ParseFrom((await OfTypeAsync(done.Id, SwapPb.SwapCompleted.Descriptor.FullName)).Payload);
        Assert.Equal("neg-ev-done", completedEvent.NegotiationId);
        var cancelledEvent = SwapPb.SwapCancelled.Parser.ParseFrom((await OfTypeAsync(cancelled.Id, SwapPb.SwapCancelled.Descriptor.FullName)).Payload);
        Assert.Equal("neg-ev-cancel", cancelledEvent.NegotiationId);
    }
}
