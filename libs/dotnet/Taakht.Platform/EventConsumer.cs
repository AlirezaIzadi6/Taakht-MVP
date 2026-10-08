using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform;

/// <summary>
/// Kafka consumer: the processed_events dedupe row and the handler's writes share one transaction and the
/// offset is committed after it. Unknown envelope types are skipped. Failed handlers are retried with backoff.
/// </summary>
public sealed class EventConsumer(
    NpgsqlDataSource dataSource,
    KafkaOptions kafka,
    string group,
    IReadOnlyCollection<string> topics,
    IReadOnlyDictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>> handlers,
    ILogger<EventConsumer> logger) : BackgroundService
{
    public TimeSpan MaxBackoff { get; init; } = TimeSpan.FromSeconds(5);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(() => RunAsync(stoppingToken), CancellationToken.None);

    private async Task RunAsync(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.Brokers,
            GroupId = group,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe(topics);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                }
                catch (ConsumeException ex)
                {
                    logger.LogWarning(ex, "Consume error");
                    continue;
                }

                if (result?.Message is null)
                {
                    continue;
                }

                if (!await HandleWithRetryAsync(result.Message.Value, result.TopicPartitionOffset, ct))
                {
                    break;
                }

                consumer.Commit(result);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task<bool> HandleWithRetryAsync(byte[] value, TopicPartitionOffset position, CancellationToken ct)
    {
        var delay = TimeSpan.FromMilliseconds(200);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await ProcessAsync(value, ct);
                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Handler failed at {Position}; retrying in {Delay}", position, delay);
                try
                {
                    await Task.Delay(delay, ct);
                }
                catch (OperationCanceledException)
                {
                    return false;
                }

                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxBackoff.Ticks));
            }
        }

        return false;
    }

    /// <summary>Processes one message value in a single transaction. Public for tests.</summary>
    public async Task ProcessAsync(byte[] value, CancellationToken ct = default)
    {
        Envelope env;
        try
        {
            env = Envelope.Parser.ParseFrom(value);
        }
        catch (InvalidProtocolBufferException ex)
        {
            logger.LogWarning(ex, "Skipping undecodable message");
            return;
        }

        if (!handlers.TryGetValue(env.Type, out var handler) || !Guid.TryParse(env.EventId, out var eventId))
        {
            return;
        }

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using (var cmd = new NpgsqlCommand(
            "INSERT INTO processed_events (consumer, event_id) VALUES (@c, @e) ON CONFLICT DO NOTHING", conn, tx))
        {
            cmd.Parameters.AddWithValue("c", group);
            cmd.Parameters.AddWithValue("e", eventId);
            if (await cmd.ExecuteNonQueryAsync(ct) == 0)
            {
                return;
            }
        }

        await handler(conn, tx, env);
        await tx.CommitAsync(ct);
    }
}
