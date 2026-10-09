using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Taakht.Platform;

/// <summary>Polls the outbox table and produces rows to Kafka (at-least-once).</summary>
public sealed class OutboxRelay(
    NpgsqlDataSource dataSource, KafkaOptions kafka, ILogger<OutboxRelay> logger) : BackgroundService
{
    private const int _batchSize = 100;

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = kafka.Brokers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 30_000,
        }).Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            var published = 0;
            try
            {
                published = await RelayBatchAsync(producer, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox relay failed; retrying");
            }

            if (published < _batchSize)
            {
                try
                {
                    await Task.Delay(PollInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    private async Task<int> RelayBatchAsync(IProducer<string, byte[]> producer, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var rows = new List<(Guid Id, string Topic, string Key, byte[] Envelope)>();
        await using (var cmd = new NpgsqlCommand(
            "SELECT id, topic, key, envelope FROM outbox WHERE published_at IS NULL ORDER BY created_at, id LIMIT @n FOR UPDATE SKIP LOCKED",
            conn, tx))
        {
            cmd.Parameters.AddWithValue("n", _batchSize);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rows.Add((reader.GetGuid(0), reader.GetString(1), reader.GetString(2), (byte[])reader[3]));
            }
        }

        // Sequential awaited produce keeps per-key order and the ack guarantee simple.
        foreach (var row in rows)
        {
            await producer.ProduceAsync(row.Topic, new Message<string, byte[]> { Key = row.Key, Value = row.Envelope }, ct);
            await using var upd = new NpgsqlCommand("UPDATE outbox SET published_at = now() WHERE id = @id", conn, tx);
            upd.Parameters.AddWithValue("id", row.Id);
            await upd.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
        return rows.Count;
    }
}
