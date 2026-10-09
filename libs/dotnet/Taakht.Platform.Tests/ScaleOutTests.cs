using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform.Tests;

/// <summary>
/// Consumer concurrency, handlers outside the dedupe transaction, and several relay instances on one database.
/// Run only when TEST_DATABASE_URL and KAFKA_BROKERS are set (make up).
/// </summary>
public class ScaleOutTests
{
    private static readonly string? _dbUrl = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");
    private static readonly string? _brokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS");

    private static bool Enabled => !string.IsNullOrEmpty(_dbUrl) && !string.IsNullOrEmpty(_brokers);

    [Fact]
    public async Task Concurrent_consumer_keeps_per_key_order_and_runs_partitions_together()
    {
        if (!Enabled)
        {
            return;
        }

        await using var env = await TestEnv.CreateAsync(partitions: 6);
        var keys = Enumerable.Range(0, 24).Select(i => $"key-{i:D2}").ToList();
        const int perKey = 25;
        await env.ProduceAsync(keys, perKey);

        var running = 0;
        var maxRunning = 0;
        var handlers = new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>
        {
            [Int32Value.Descriptor.FullName] = async (conn, tx, e) =>
            {
                var now = Interlocked.Increment(ref running);
                InterlockedMax(ref maxRunning, now);
                try
                {
                    await Task.Delay(3);
                    await TestEnv.RecordAsync(conn, tx, e);
                }
                finally
                {
                    Interlocked.Decrement(ref running);
                }
            },
        };

        using var consumer = new EventConsumer(env.Db, new KafkaOptions(_brokers!), env.Group, [env.Topic], handlers, NullLogger<EventConsumer>.Instance)
        {
            Concurrency = 4,
        };
        await consumer.StartAsync(CancellationToken.None);
        try
        {
            await WaitAsync(async () => await env.HandledCountAsync() >= keys.Count * perKey);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }

        await env.AssertExactlyOnceInOrderAsync(keys, perKey);
        Assert.True(maxRunning >= 2, $"max concurrent handlers was {maxRunning}");

        // Offsets were committed: a new consumer in the same group has nothing left to do.
        var before = await env.ProcessedCountAsync();
        using var again = new EventConsumer(env.Db, new KafkaOptions(_brokers!), env.Group, [env.Topic], handlers, NullLogger<EventConsumer>.Instance);
        await again.StartAsync(CancellationToken.None);
        await Task.Delay(3000);
        await again.StopAsync(CancellationToken.None);
        Assert.Equal(before, await env.ProcessedCountAsync());
    }

    [Fact]
    public async Task A_stuck_partition_does_not_block_the_others_and_catches_up_in_order()
    {
        if (!Enabled)
        {
            return;
        }

        await using var env = await TestEnv.CreateAsync(partitions: 6);
        var keys = Enumerable.Range(0, 20).Select(i => $"key-{i:D2}").ToList();
        const int perKey = 5;
        var partitionOf = await env.ProduceAsync(keys, perKey);
        var stuck = keys[0];
        var other = keys.First(k => partitionOf[k] != partitionOf[stuck]);

        var release = false;
        var handlers = new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>
        {
            [Int32Value.Descriptor.FullName] = async (conn, tx, e) =>
            {
                if (e.AggregateId == stuck && Int32Value.Parser.ParseFrom(e.Payload).Value == 2 && !Volatile.Read(ref release))
                {
                    throw new InvalidOperationException("transient: dependency down");
                }

                await TestEnv.RecordAsync(conn, tx, e);
            },
        };

        using var consumer = new EventConsumer(env.Db, new KafkaOptions(_brokers!), env.Group, [env.Topic], handlers, NullLogger<EventConsumer>.Instance)
        {
            Concurrency = 3,
        };
        await consumer.StartAsync(CancellationToken.None);
        try
        {
            await WaitAsync(async () => await env.HandledCountAsync(other) == perKey);
            Assert.Equal(1, await env.HandledCountAsync(stuck));

            Volatile.Write(ref release, true);
            await WaitAsync(async () => await env.HandledCountAsync() == keys.Count * perKey);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }

        await env.AssertExactlyOnceInOrderAsync(keys, perKey);
    }

    [Fact]
    public async Task At_least_once_handler_runs_outside_a_transaction_and_records_only_after_success()
    {
        if (!Enabled)
        {
            return;
        }

        await using var env = await TestEnv.CreateAsync(partitions: 1);
        var key = "agg";
        var calls = 0;
        var openTransactions = -1L;
        var rowBeforeSuccess = true;

        async Task Handler(Envelope e, CancellationToken ct)
        {
            calls++;
            await using var cmd = env.Db.CreateCommand(
                "SELECT (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'idle in transaction')," +
                " (SELECT count(*) FROM processed_events WHERE event_id = @e)");
            cmd.Parameters.AddWithValue("e", Guid.Parse(e.EventId));
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            openTransactions = reader.GetInt64(0);
            rowBeforeSuccess = reader.GetInt64(1) != 0;
            if (calls == 1)
            {
                throw new InvalidOperationException("transient");
            }
        }

        var atLeastOnce = new Dictionary<string, Func<Envelope, CancellationToken, Task>> { [Int32Value.Descriptor.FullName] = Handler };
        using var consumer = new EventConsumer(
            env.Db, new KafkaOptions(_brokers!), env.Group, [env.Topic],
            new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>(), NullLogger<EventConsumer>.Instance, atLeastOnce);
        var message = Outbox.Wrap(key, new Int32Value { Value = 1 });
        var bytes = message.ToByteArray();

        await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.ProcessAsync(bytes));
        Assert.Equal(0, await env.ProcessedCountAsync()); // a failed handler leaves no row: the event is retried
        await consumer.ProcessAsync(bytes);
        Assert.Equal(1, await env.ProcessedCountAsync());
        Assert.Equal(2, calls);
        Assert.Equal(0, openTransactions);
        Assert.False(rowBeforeSuccess);

        await consumer.ProcessAsync(bytes); // redelivery after success skips the handler
        Assert.Equal(2, calls);
        Assert.Equal(1, await env.ProcessedCountAsync());
    }

    [Fact]
    public async Task At_least_once_handler_through_kafka_retries_transients_and_parks_poison()
    {
        if (!Enabled)
        {
            return;
        }

        await using var env = await TestEnv.CreateAsync(partitions: 1);
        var attempts = 0;
        async Task Handler(Envelope e, CancellationToken ct)
        {
            var seq = Int32Value.Parser.ParseFrom(e.Payload).Value;
            if (seq == 2)
            {
                throw new PermanentEventException("poison");
            }

            if (seq == 3 && Interlocked.Increment(ref attempts) < 3)
            {
                throw new InvalidOperationException("transient");
            }

            await using var conn = await env.Db.OpenConnectionAsync(ct);
            await TestEnv.RecordAsync(conn, null, e);
        }

        await env.ProduceAsync(["agg"], 4);
        using var consumer = new EventConsumer(
            env.Db, new KafkaOptions(_brokers!), env.Group, [env.Topic],
            new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>(), NullLogger<EventConsumer>.Instance,
            new Dictionary<string, Func<Envelope, CancellationToken, Task>> { [Int32Value.Descriptor.FullName] = Handler })
        {
            MaxBackoff = TimeSpan.FromMilliseconds(400),
        };
        await consumer.StartAsync(CancellationToken.None);
        try
        {
            await WaitAsync(async () => await env.HandledCountAsync() == 3);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }

        Assert.Equal(3, attempts);
        Assert.Equal(4, await env.ProcessedCountAsync());
        await using var dead = env.Db.CreateCommand("SELECT count(*) FROM dead_letter");
        Assert.Equal(1L, await dead.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Two_relays_publish_each_row_once_in_order_and_the_standby_takes_over()
    {
        if (!Enabled)
        {
            return;
        }

        await using var env = await TestEnv.CreateAsync(partitions: 3);
        var keys = Enumerable.Range(0, 10).Select(i => $"key-{i:D2}").ToList();
        await env.AddOutboxAsync(keys, 1, 40);

        var kafka = new KafkaOptions(_brokers!);
        using var relay1 = new OutboxRelay(env.Db, kafka, NullLogger<OutboxRelay>.Instance) { StandbyRetry = TimeSpan.FromMilliseconds(500) };
        using var relay2 = new OutboxRelay(env.Db, kafka, NullLogger<OutboxRelay>.Instance) { StandbyRetry = TimeSpan.FromMilliseconds(500) };
        await relay1.StartAsync(CancellationToken.None);
        await WaitAsync(async () => await env.AdvisoryLocksAsync() == 1);
        await relay2.StartAsync(CancellationToken.None);
        try
        {
            await WaitAsync(async () => await env.UnpublishedAsync() == 0);
            await Task.Delay(1500);
            Assert.Equal(1, await env.AdvisoryLocksAsync());

            await relay1.StopAsync(CancellationToken.None);
            await env.AddOutboxAsync(keys, 41, 45);
            await WaitAsync(async () => await env.UnpublishedAsync() == 0);
        }
        finally
        {
            await relay1.StopAsync(CancellationToken.None);
            await relay2.StopAsync(CancellationToken.None);
        }

        var seen = env.ReadAll(keys.Count * 45);
        Assert.Equal(keys.Count * 45, seen.Count);
        var last = new Dictionary<string, int>();
        foreach (var (key, seq) in seen)
        {
            Assert.Equal(last.GetValueOrDefault(key) + 1, seq);
            last[key] = seq;
        }
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        do
        {
            current = Volatile.Read(ref target);
        }
        while (value > current && Interlocked.CompareExchange(ref target, value, current) != current);
    }

    private static async Task WaitAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException("condition not met");
    }

    /// <summary>A throwaway database (outbox, processed_events, dead_letter, handled) and a topic of N partitions.</summary>
    private sealed class TestEnv : IAsyncDisposable
    {
        private readonly NpgsqlDataSource _admin;
        private readonly string _dbName;

        private TestEnv(NpgsqlDataSource admin, NpgsqlDataSource db, string dbName, string topic)
        {
            _admin = admin;
            Db = db;
            _dbName = dbName;
            Topic = topic;
        }

        public NpgsqlDataSource Db { get; }

        public string Topic { get; }

        public string Group { get; } = "scale-" + Guid.NewGuid().ToString("N")[..10];

        public static async Task<TestEnv> CreateAsync(int partitions)
        {
            var suffix = Guid.NewGuid().ToString("N")[..10];
            var dbName = "platform_scale_" + suffix;
            var adminCs = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString(_dbUrl!));
            var admin = NpgsqlDataSource.Create(adminCs.ConnectionString);
            await using (var create = admin.CreateCommand($"CREATE DATABASE {dbName}"))
            {
                await create.ExecuteNonQueryAsync();
            }

            adminCs.Database = dbName;
            adminCs.MaxPoolSize = 40;
            var db = NpgsqlDataSource.Create(adminCs.ConnectionString);
            await PlatformSchema.EnsureTablesAsync(db);
            await using (var cmd = db.CreateCommand(
                "CREATE TABLE handled (n bigserial PRIMARY KEY, key text NOT NULL, seq int NOT NULL, event_id uuid NOT NULL)"))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            var topic = "platform.scale." + suffix;
            using var kafkaAdmin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _brokers }).Build();
            await kafkaAdmin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }]);
            return new TestEnv(admin, db, dbName, topic);
        }

        public async Task<Dictionary<string, int>> ProduceAsync(List<string> keys, int perKey)
        {
            using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = _brokers }).Build();
            var partitions = new Dictionary<string, int>();
            for (var seq = 1; seq <= perKey; seq++)
            {
                foreach (var key in keys)
                {
                    var result = await producer.ProduceAsync(
                        Topic, new Message<string, byte[]> { Key = key, Value = Outbox.Wrap(key, new Int32Value { Value = seq }).ToByteArray() });
                    partitions[key] = result.Partition.Value;
                }
            }

            return partitions;
        }

        public static async Task RecordAsync(NpgsqlConnection conn, NpgsqlTransaction? tx, Envelope e)
        {
            await using var cmd = new NpgsqlCommand("INSERT INTO handled (key, seq, event_id) VALUES (@k, @s, @e)", conn, tx);
            cmd.Parameters.AddWithValue("k", e.AggregateId);
            cmd.Parameters.AddWithValue("s", Int32Value.Parser.ParseFrom(e.Payload).Value);
            cmd.Parameters.AddWithValue("e", Guid.Parse(e.EventId));
            await cmd.ExecuteNonQueryAsync();
        }

        public Task<long> HandledCountAsync(string? key = null) =>
            ScalarAsync(key is null ? "SELECT count(*) FROM handled" : $"SELECT count(*) FROM handled WHERE key = '{key}'");

        public Task<long> ProcessedCountAsync() => ScalarAsync("SELECT count(*) FROM processed_events");

        public Task<long> UnpublishedAsync() => ScalarAsync("SELECT count(*) FROM outbox WHERE published_at IS NULL");

        public Task<long> AdvisoryLocksAsync() => ScalarAsync(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())");

        public async Task AssertExactlyOnceInOrderAsync(List<string> keys, int perKey)
        {
            await using var cmd = Db.CreateCommand("SELECT key, seq FROM handled ORDER BY n");
            await using var reader = await cmd.ExecuteReaderAsync();
            var last = new Dictionary<string, int>();
            var total = 0;
            while (await reader.ReadAsync())
            {
                var key = reader.GetString(0);
                var seq = reader.GetInt32(1);
                Assert.Equal(last.GetValueOrDefault(key) + 1, seq);
                last[key] = seq;
                total++;
            }

            Assert.Equal(keys.Count * perKey, total);
        }

        public async Task AddOutboxAsync(List<string> keys, int from, int to)
        {
            await using var conn = await Db.OpenConnectionAsync();
            await using var tx = await conn.BeginTransactionAsync();
            for (var seq = from; seq <= to; seq++)
            {
                foreach (var key in keys)
                {
                    await Outbox.AddAsync(conn, tx, Topic, key, new Int32Value { Value = seq });
                }
            }

            await tx.CommitAsync();
        }

        /// <summary>Reads the topic from the start until <paramref name="want"/> messages arrived and then a little longer, to catch duplicates.</summary>
        public List<(string Key, int Seq)> ReadAll(int want)
        {
            using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = _brokers,
                GroupId = "reader-" + Guid.NewGuid().ToString("N"),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            }).Build();
            consumer.Subscribe(Topic);
            var result = new List<(string, int)>();
            var deadline = DateTime.UtcNow.AddSeconds(30);
            var quietUntil = DateTime.MaxValue;
            while (DateTime.UtcNow < deadline && DateTime.UtcNow < quietUntil)
            {
                var message = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (message?.Message is null)
                {
                    continue;
                }

                var env = Envelope.Parser.ParseFrom(message.Message.Value);
                result.Add((env.AggregateId, Int32Value.Parser.ParseFrom(env.Payload).Value));
                if (result.Count >= want && quietUntil == DateTime.MaxValue)
                {
                    quietUntil = DateTime.UtcNow.AddSeconds(2);
                }
            }

            consumer.Close();
            return result;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                using var kafkaAdmin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _brokers }).Build();
                await kafkaAdmin.DeleteTopicsAsync([Topic]);
            }
            catch (DeleteTopicsException)
            {
            }

            await Db.DisposeAsync();
            NpgsqlConnection.ClearAllPools();
            await using var drop = _admin.CreateCommand($"DROP DATABASE IF EXISTS {_dbName} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
            await _admin.DisposeAsync();
        }

        private async Task<long> ScalarAsync(string sql)
        {
            await using var cmd = Db.CreateCommand(sql);
            return (long)(await cmd.ExecuteScalarAsync())!;
        }
    }
}
