using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform.Tests;

public class EventConsumerTests
{
    private static readonly string? _dbUrl = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");

    [Fact]
    public async Task Handled_type_with_invalid_event_id_is_logged_as_error()
    {
        await using var db = NpgsqlDataSource.Create("Host=localhost;Database=none");
        var logger = new CapturingLogger<EventConsumer>();
        var consumer = new EventConsumer(db, new KafkaOptions("localhost:1"), "g", ["t"], Handlers(), logger);

        var env = Outbox.Wrap("agg", new StringValue { Value = "x" }, Guid.NewGuid());
        env.EventId = "not-a-guid";
        await consumer.ProcessAsync(env.ToByteArray());

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("invalid event id", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Unknown_type_is_skipped_quietly_and_garbage_is_an_error()
    {
        await using var db = NpgsqlDataSource.Create("Host=localhost;Database=none");
        var logger = new CapturingLogger<EventConsumer>();
        var consumer = new EventConsumer(db, new KafkaOptions("localhost:1"), "g", ["t"], Handlers(), logger);

        await consumer.ProcessAsync(Outbox.Wrap("agg", new Int32Value { Value = 1 }, Guid.NewGuid()).ToByteArray());
        Assert.Empty(logger.Entries);

        await consumer.ProcessAsync([0xff, 0xff, 0xff]);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public async Task Permanent_skip_writes_processed_event_and_dead_letter_together()
    {
        if (string.IsNullOrEmpty(_dbUrl))
        {
            return;
        }

        var dbName = "platform_dl_" + Guid.NewGuid().ToString("N")[..10];
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
            var consumer = new EventConsumer(db, new KafkaOptions("localhost:1"), "grp", ["t"], Handlers(), new CapturingLogger<EventConsumer>());

            var env = Outbox.Wrap("agg", new StringValue { Value = "x" }, Guid.NewGuid());
            await consumer.SkipPermanentlyAsync(env.ToByteArray(), "some.topic", "bad input");
            await consumer.SkipPermanentlyAsync(env.ToByteArray(), "some.topic", "bad input"); // idempotent

            await using var cmd = db.CreateCommand(
                "SELECT (SELECT count(*) FROM processed_events), (SELECT count(*) FROM dead_letter), (SELECT error FROM dead_letter), (SELECT topic FROM dead_letter)");
            await using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.Equal(1L, reader.GetInt64(1));
            Assert.Equal("bad input", reader.GetString(2));
            Assert.Equal("some.topic", reader.GetString(3));
        }
        finally
        {
            await using var drop = admin.CreateCommand($"DROP DATABASE IF EXISTS {dbName} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
    }

    private static Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>> Handlers() => new()
    {
        ["google.protobuf.StringValue"] = (_, _, _) => Task.CompletedTask,
    };

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception)));
    }
}
