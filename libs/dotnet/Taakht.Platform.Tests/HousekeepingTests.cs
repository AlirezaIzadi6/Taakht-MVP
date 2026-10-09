using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Taakht.Platform.Tests;

public class HousekeepingTests
{
    private static readonly string? _dbUrl = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");

    [Theory]
    [InlineData("24h", 24 * 3600)]
    [InlineData("7d", 7 * 86400)]
    [InlineData("1d12h", 36 * 3600)]
    [InlineData("10m", 600)]
    [InlineData("90s", 90)]
    [InlineData("1h30m", 5400)]
    [InlineData("01:30:00", 5400)]
    public void ParseDuration_accepts_units_and_days(string input, int seconds)
        => Assert.Equal(TimeSpan.FromSeconds(seconds), HousekeepingOptions.ParseDuration(input));

    [Theory]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("7x")]
    [InlineData("1dfoo")]
    public void ParseDuration_rejects_garbage(string input)
        => Assert.ThrowsAny<Exception>(() => HousekeepingOptions.ParseDuration(input));

    [Fact]
    public void FromConfiguration_uses_defaults_and_overrides()
    {
        var defaults = HousekeepingOptions.FromValues(_ => null);
        Assert.Equal(TimeSpan.FromHours(24), defaults.OutboxRetention);
        Assert.Equal(TimeSpan.FromDays(7), defaults.ProcessedEventsRetention);
        Assert.Equal(TimeSpan.FromMinutes(10), defaults.Interval);

        var config = new Dictionary<string, string?>
        {
            ["OUTBOX_RETENTION"] = "2d",
            ["PROCESSED_EVENTS_RETENTION"] = "30d",
            ["PRUNE_INTERVAL"] = "5m",
        };
        var o = HousekeepingOptions.FromValues(k => config.GetValueOrDefault(k));
        Assert.Equal(TimeSpan.FromDays(2), o.OutboxRetention);
        Assert.Equal(TimeSpan.FromDays(30), o.ProcessedEventsRetention);
        Assert.Equal(TimeSpan.FromMinutes(5), o.Interval);
    }

    [Theory]
    [InlineData("PRUNE_INTERVAL", "30s")]
    [InlineData("OUTBOX_RETENTION", "0s")]
    [InlineData("PROCESSED_EVENTS_RETENTION", "59s")]
    public void FromConfiguration_rejects_values_below_one_minute(string key, string value)
    {
        var config = new Dictionary<string, string?> { [key] = value };
        Assert.Throws<ArgumentOutOfRangeException>(() => HousekeepingOptions.FromValues(k => config.GetValueOrDefault(k)));
    }

    [Fact]
    public void FromConfiguration_rejects_unparseable_values()
    {
        var config = new Dictionary<string, string?> { ["PRUNE_INTERVAL"] = "soon" };
        Assert.Throws<FormatException>(() => HousekeepingOptions.FromValues(k => config.GetValueOrDefault(k)));
    }

    [Fact]
    public async Task PruneOnce_deletes_only_old_published_outbox_and_old_processed_events_in_batches()
    {
        if (string.IsNullOrEmpty(_dbUrl))
        {
            return;
        }

        var dbName = "platform_hk_" + Guid.NewGuid().ToString("N")[..10];
        var adminCs = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString(_dbUrl));
        await using var admin = NpgsqlDataSource.Create(adminCs.ConnectionString);
        await using (var create = admin.CreateCommand($"CREATE DATABASE {dbName}"))
        {
            await create.ExecuteNonQueryAsync();
        }

        adminCs.Database = dbName;
        adminCs.Pooling = false;
        try
        {
            await using var db = NpgsqlDataSource.Create(adminCs.ConnectionString);
            await PlatformSchema.EnsureTablesAsync(db);
            await using (var seed = db.CreateCommand(
                """
                INSERT INTO outbox (id, topic, key, envelope, created_at, published_at)
                SELECT gen_random_uuid(), 't', 'k', '\x00', now() - interval '3 days', now() - interval '2 days' FROM generate_series(1, 25);
                INSERT INTO outbox (id, topic, key, envelope, created_at, published_at)
                SELECT gen_random_uuid(), 't', 'k', '\x00', now(), now() FROM generate_series(1, 5);
                INSERT INTO outbox (id, topic, key, envelope, created_at)
                SELECT gen_random_uuid(), 't', 'k', '\x00', now() - interval '30 days' FROM generate_series(1, 5);
                INSERT INTO outbox (id, topic, key, envelope, created_at)
                SELECT gen_random_uuid(), 't', 'k', '\x00', now() FROM generate_series(1, 5);
                INSERT INTO processed_events (consumer, event_id, processed_at)
                SELECT 'c', gen_random_uuid(), now() - interval '10 days' FROM generate_series(1, 12);
                INSERT INTO processed_events (consumer, event_id, processed_at)
                SELECT 'c', gen_random_uuid(), now() - interval '1 day' FROM generate_series(1, 4);
                """))
            {
                await seed.ExecuteNonQueryAsync();
            }

            var service = new HousekeepingService(db, new HousekeepingOptions { BatchSize = 10 }, NullLogger<HousekeepingService>.Instance);
            var (outbox, processed) = await service.PruneOnceAsync();

            Assert.Equal(25, outbox);
            Assert.Equal(12, processed);
            Assert.Equal(5, await CountAsync(db, "SELECT count(*) FROM outbox WHERE published_at IS NOT NULL"));
            Assert.Equal(10, await CountAsync(db, "SELECT count(*) FROM outbox WHERE published_at IS NULL"));
            Assert.Equal(4, await CountAsync(db, "SELECT count(*) FROM processed_events"));

            Assert.Equal((0L, 0L), await service.PruneOnceAsync());
        }
        finally
        {
            await using var drop = admin.CreateCommand($"DROP DATABASE IF EXISTS {dbName} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
    }

    private static async Task<long> CountAsync(NpgsqlDataSource db, string sql)
    {
        await using var cmd = db.CreateCommand(sql);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}
