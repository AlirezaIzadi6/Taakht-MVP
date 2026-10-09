using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Taakht.Platform;

/// <summary>
/// Polls the outbox table and produces rows to Kafka (at-least-once). Only one instance per database relays at a time:
/// the active one holds a session-level advisory lock (<c>pg_try_advisory_lock(hashtext(current_database() || '-outbox-relay'))</c>)
/// on a dedicated connection, which Postgres releases if the instance or its connection dies. The others stand by,
/// retry the lock every <see cref="StandbyRetry"/> and take over when the holder is gone. A batch is produced concurrently
/// (the idempotent producer keeps the order per partition, so per key) and marked published in one statement.
/// </summary>
public sealed class OutboxRelay(
    NpgsqlDataSource dataSource, KafkaOptions kafka, ILogger<OutboxRelay> logger) : BackgroundService
{
    private const int _batchSize = 100;
    private const string _lockSql = "SELECT pg_try_advisory_lock(hashtext(current_database() || '-outbox-relay'))";
    private const string _unlockSql = "SELECT pg_advisory_unlock(hashtext(current_database() || '-outbox-relay'))";

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    public TimeSpan StandbyRetry { get; init; } = TimeSpan.FromSeconds(3);

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
            NpgsqlConnection? lockConn = null;
            try
            {
                lockConn = await TryTakeLockAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox relay cannot check the relay lock; retrying");
            }

            if (lockConn is null)
            {
                if (!await DelayAsync(StandbyRetry, stoppingToken))
                {
                    break;
                }

                continue;
            }

            logger.LogInformation("Outbox relay lock acquired; relaying");
            try
            {
                await RelayWhileLockedAsync(producer, lockConn, stoppingToken);
            }
            finally
            {
                await ReleaseLockAsync(lockConn);
            }
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, ct);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private async Task<NpgsqlConnection?> TryTakeLockAsync(CancellationToken ct)
    {
        var conn = await dataSource.OpenConnectionAsync(ct);
        try
        {
            await using var cmd = new NpgsqlCommand(_lockSql, conn);
            if ((bool)(await cmd.ExecuteScalarAsync(ct))!)
            {
                return conn;
            }
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }

        await conn.DisposeAsync();
        return null;
    }

    private static async Task ReleaseLockAsync(NpgsqlConnection lockConn)
    {
        try
        {
            // Unlock explicitly: disposing returns a pooled connection, which would keep the session (and the lock) alive.
            await using var cmd = new NpgsqlCommand(_unlockSql, lockConn);
            await cmd.ExecuteScalarAsync(CancellationToken.None);
        }
        catch (NpgsqlException)
        {
            // the connection is gone, and with it the lock
        }
        finally
        {
            await lockConn.DisposeAsync();
        }
    }

    private async Task RelayWhileLockedAsync(IProducer<string, byte[]> producer, NpgsqlConnection lockConn, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // A dead lock connection means the lock is gone and another instance may be relaying.
            try
            {
                await using var ping = new NpgsqlCommand("SELECT 1", lockConn);
                await ping.ExecuteScalarAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox relay lost its lock connection; standing by");
                return;
            }

            var published = 0;
            try
            {
                published = await RelayBatchAsync(producer, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox relay failed; retrying");
            }

            if (published < _batchSize && !await DelayAsync(PollInterval, ct))
            {
                return;
            }
        }
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

        if (rows.Count == 0)
        {
            return 0;
        }

        // ProduceAsync queues the message before it returns, so the calls enqueue in row order; the idempotent producer
        // keeps that order per partition across retries. Any failure fails the whole batch (nothing is marked, all rows are
        // tried again, consumers deduplicate).
        var acks = new List<Task<DeliveryResult<string, byte[]>>>(rows.Count);
        foreach (var row in rows)
        {
            acks.Add(producer.ProduceAsync(row.Topic, new Message<string, byte[]> { Key = row.Key, Value = row.Envelope }, ct));
        }

        await Task.WhenAll(acks);

        await using (var upd = new NpgsqlCommand("UPDATE outbox SET published_at = now() WHERE id = ANY(@ids)", conn, tx))
        {
            upd.Parameters.AddWithValue("ids", rows.Select(r => r.Id).ToArray());
            await upd.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
        PlatformMetrics.OutboxPublished.Inc(rows.Count);
        return rows.Count;
    }
}
