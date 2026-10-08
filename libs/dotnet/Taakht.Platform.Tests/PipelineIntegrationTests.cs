using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform.Tests;

/// <summary>Runs only when TEST_DATABASE_URL and KAFKA_BROKERS are set (make up).</summary>
public class PipelineIntegrationTests
{
    private static readonly string? _dbUrl = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");
    private static readonly string? _brokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS");

    [Fact]
    public async Task Outbox_to_kafka_to_consumer_is_idempotent()
    {
        if (string.IsNullOrEmpty(_dbUrl) || string.IsNullOrEmpty(_brokers))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..10];
        var dbName = "platform_test_" + suffix;
        var topic = "platform.test." + suffix;
        var group = "platform-test-" + suffix;

        var adminCs = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString(_dbUrl));
        await using var admin = NpgsqlDataSource.Create(adminCs.ConnectionString);
        await using (var create = admin.CreateCommand($"CREATE DATABASE {dbName}"))
        {
            await create.ExecuteNonQueryAsync();
        }

        adminCs.Database = dbName;
        adminCs.Pooling = false;
        await using var db = NpgsqlDataSource.Create(adminCs.ConnectionString);
        using var kafkaAdmin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _brokers }).Build();
        try
        {
            await PlatformSchema.EnsureTablesAsync(db);
            await PlatformSchema.EnsureTablesAsync(db); // idempotent
            await kafkaAdmin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);

            var kafka = new KafkaOptions(_brokers);
            var handled = new List<string>();
            var handlers = new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>
            {
                ["google.protobuf.StringValue"] = (_, _, env) =>
                {
                    lock (handled)
                    {
                        handled.Add(StringValue.Parser.ParseFrom(env.Payload).Value);
                    }

                    return Task.CompletedTask;
                },
            };

            await using (var conn = await db.OpenConnectionAsync())
            await using (var tx = await conn.BeginTransactionAsync())
            {
                await Outbox.AddAsync(conn, tx, topic, "agg-1", new StringValue { Value = "first" });
                await Outbox.AddAsync(conn, tx, topic, "agg-1", new Int32Value { Value = 7 }); // unknown to the consumer
                await tx.CommitAsync();
            }

            // A redelivered copy of the first event (same event_id) must be skipped.
            Envelope duplicate;
            await using (var cmd = db.CreateCommand("SELECT envelope FROM outbox ORDER BY created_at LIMIT 1"))
            {
                duplicate = Envelope.Parser.ParseFrom((byte[])(await cmd.ExecuteScalarAsync())!);
            }

            using var relay = new OutboxRelay(db, kafka, NullLogger<OutboxRelay>.Instance);
            using var consumer = new EventConsumer(db, kafka, group, [topic], handlers, NullLogger<EventConsumer>.Instance);
            await relay.StartAsync(CancellationToken.None);
            await consumer.StartAsync(CancellationToken.None);
            try
            {
                using (var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = _brokers }).Build())
                {
                    await producer.ProduceAsync(topic, new Message<string, byte[]> { Key = "agg-1", Value = duplicate.ToByteArray() });
                }

                await WaitAsync(async () =>
                {
                    await using var cmd = db.CreateCommand("SELECT count(*) FROM outbox WHERE published_at IS NULL");
                    var pending = (long)(await cmd.ExecuteScalarAsync())!;
                    await using var cmd2 = db.CreateCommand("SELECT count(*) FROM processed_events");
                    return pending == 0 && (long)(await cmd2.ExecuteScalarAsync())! >= 1;
                });

                await Task.Delay(2000); // give the duplicate time to be consumed and skipped
            }
            finally
            {
                await relay.StopAsync(CancellationToken.None);
                await consumer.StopAsync(CancellationToken.None);
            }

            Assert.Equal(["first"], handled);
        }
        finally
        {
            try
            {
                await kafkaAdmin.DeleteTopicsAsync([topic]);
            }
            catch (DeleteTopicsException)
            {
            }

            await using var drop = admin.CreateCommand($"DROP DATABASE IF EXISTS {dbName} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Permanent_failures_do_not_block_the_partition()
    {
        if (string.IsNullOrEmpty(_dbUrl) || string.IsNullOrEmpty(_brokers))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..10];
        var dbName = "platform_perm_" + suffix;
        var topic = "platform.perm." + suffix;
        var group = "platform-perm-" + suffix;

        var adminCs = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString(_dbUrl));
        await using var admin = NpgsqlDataSource.Create(adminCs.ConnectionString);
        await using (var create = admin.CreateCommand($"CREATE DATABASE {dbName}"))
        {
            await create.ExecuteNonQueryAsync();
        }

        adminCs.Database = dbName;
        adminCs.Pooling = false;
        await using var db = NpgsqlDataSource.Create(adminCs.ConnectionString);
        using var kafkaAdmin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _brokers }).Build();
        try
        {
            await PlatformSchema.EnsureTablesAsync(db);
            await kafkaAdmin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);

            var handled = new List<string>();
            var handlers = new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>
            {
                ["google.protobuf.Int32Value"] = (_, _, _) => throw new PermanentEventException("poison"),
                ["google.protobuf.StringValue"] = (_, _, env) =>
                {
                    lock (handled)
                    {
                        handled.Add(StringValue.Parser.ParseFrom(env.Payload).Value);
                    }

                    return Task.CompletedTask;
                },
            };

            var explicitPoison = Outbox.Wrap("agg", new Int32Value { Value = 1 }, Guid.NewGuid());
            var undecodable = Outbox.Wrap("agg", new StringValue { Value = "x" }, Guid.NewGuid());
            undecodable.Payload = ByteString.CopyFrom(0xff, 0xff, 0xff);
            var good = Outbox.Wrap("agg", new StringValue { Value = "good" }, Guid.NewGuid());

            using (var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = _brokers }).Build())
            {
                foreach (var env in new[] { explicitPoison, undecodable, good })
                {
                    await producer.ProduceAsync(topic, new Message<string, byte[]> { Key = "agg", Value = env.ToByteArray() });
                }
            }

            using var consumer = new EventConsumer(db, new KafkaOptions(_brokers), group, [topic], handlers, NullLogger<EventConsumer>.Instance);
            await consumer.StartAsync(CancellationToken.None);
            try
            {
                await WaitAsync(async () =>
                {
                    await using var cmd = db.CreateCommand("SELECT count(*) FROM processed_events");
                    return (long)(await cmd.ExecuteScalarAsync())! == 3;
                });
            }
            finally
            {
                await consumer.StopAsync(CancellationToken.None);
            }

            Assert.Equal(["good"], handled);
        }
        finally
        {
            try
            {
                await kafkaAdmin.DeleteTopicsAsync([topic]);
            }
            catch (DeleteTopicsException)
            {
            }

            await using var drop = admin.CreateCommand($"DROP DATABASE IF EXISTS {dbName} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
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

            await Task.Delay(250);
        }

        throw new TimeoutException("condition not met");
    }
}
