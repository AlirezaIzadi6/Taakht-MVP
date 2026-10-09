using System.Collections.Concurrent;
using System.Globalization;
using System.Threading.Channels;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform;

/// <summary>
/// Thrown by a handler for an event that can never succeed (rejected input, undecodable payload). The consumer
/// logs it, records the event as processed (and in dead_letter), commits the offset and moves on instead of retrying forever.
/// </summary>
public sealed class PermanentEventException : Exception
{
    public PermanentEventException()
    {
    }

    public PermanentEventException(string message)
        : base(message)
    {
    }

    public PermanentEventException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Kafka consumer: the processed_events dedupe row and the handler's writes share one transaction and the
/// offset is committed after it. Unknown envelope types are skipped (a handled type with an unreadable event id is logged at error level). Failed handlers are retried with backoff
/// (blocking their partition) unless they throw <see cref="PermanentEventException"/> or the payload cannot be decoded;
/// those events are stored in dead_letter.
/// <para>
/// <b>Concurrency.</b> With <see cref="Concurrency"/> of 1 (the default, or CONSUMER_CONCURRENCY) one loop handles every
/// partition in turn. With more, every assigned partition gets its own worker: records of one partition (so of one Kafka key) stay in
/// order, partitions run in parallel, at most <see cref="Concurrency"/> handlers at once. Offsets are committed by the polling
/// thread shortly after the handler finished; a crash can redeliver a few handled events, which processed_events absorbs.
/// </para>
/// <para>
/// <b>At-least-once handlers.</b> A handler listed in <c>atLeastOnceHandlers</c> (opt-in, keyed by envelope type) runs FIRST, with no
/// transaction or connection of the consumer open, and the processed_events row is inserted afterwards in its own short
/// transaction, only after the handler succeeded. Use it for a handler that makes long remote calls. The handler must be
/// idempotent and own its transactions: a crash (or a failed insert) after success runs it again, and during a rebalance two
/// instances may run it for the same event at the same time.
/// </para>
/// </summary>
public sealed class EventConsumer(
    NpgsqlDataSource dataSource,
    KafkaOptions kafka,
    string group,
    IReadOnlyCollection<string> topics,
    IReadOnlyDictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>> handlers,
    ILogger<EventConsumer> logger,
    IReadOnlyDictionary<string, Func<Envelope, CancellationToken, Task>>? atLeastOnceHandlers = null) : BackgroundService
{
    private const int _queueHighWater = 1000;
    private const int _queueLowWater = 250;

    public TimeSpan MaxBackoff { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>The consumer group (the service name).</summary>
    public string Group => group;

    /// <summary>Partitions held and last poll, read by the readiness check (see <see cref="ConsumerHealthState"/>).</summary>
    public ConsumerHealthState Health { get; } = new();

    /// <summary>Partitions handled at the same time; 1 is strictly sequential. Defaults to CONSUMER_CONCURRENCY, else 1.</summary>
    public int Concurrency { get; init; } = ConcurrencyFromEnvironment();

    /// <summary>Reads CONSUMER_CONCURRENCY: an integer of at least 1, anything else counts as 1.</summary>
    public static int ConcurrencyFromEnvironment()
    {
        var v = Environment.GetEnvironmentVariable("CONSUMER_CONCURRENCY");
        return int.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n >= 1 ? n : 1;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(() => RunAsync(stoppingToken), CancellationToken.None);

    private ConsumerConfig BuildConfig() => new()
    {
        BootstrapServers = kafka.Brokers,
        GroupId = group,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
    };

    private async Task RunAsync(CancellationToken ct)
    {
        if (Concurrency > 1)
        {
            await RunParallelAsync(ct);
            return;
        }

        using var consumer = new ConsumerBuilder<string, byte[]>(BuildConfig()).Build();
        consumer.Subscribe(topics);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    Health.Polled(consumer.Assignment.Count);
                }
                catch (KafkaException ex)
                {
                    logger.LogWarning(ex, "Kafka consume error; backing off");
                    if (!await BackoffAsync(ct))
                    {
                        break;
                    }

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

                try
                {
                    consumer.Commit(result);
                }
                catch (KafkaException ex)
                {
                    // The event is already recorded in processed_events, so a redelivery is harmless.
                    logger.LogWarning(ex, "Kafka commit failed at {Position}; continuing", result.TopicPartitionOffset);
                    if (!await BackoffAsync(ct))
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// One polling thread (the only one that touches the Kafka consumer) dispatches records to a worker per assigned
    /// partition; workers handle their partition in order and report the next offset to commit, which the polling thread commits.
    /// </summary>
    private async Task RunParallelAsync(CancellationToken ct)
    {
        var workers = new Dictionary<TopicPartition, PartitionWorker>();
        var paused = new HashSet<TopicPartition>();
        var done = new ConcurrentDictionary<TopicPartition, long>(); // next offset to commit per partition
        using var gate = new SemaphoreSlim(Concurrency);

        void StopWorkers(IEnumerable<TopicPartition> revoked, IConsumer<string, byte[]> c, bool commit)
        {
            var stopping = new List<PartitionWorker>();
            foreach (var tp in revoked)
            {
                if (workers.Remove(tp, out var w))
                {
                    stopping.Add(w);
                }

                paused.Remove(tp);
                if (!commit)
                {
                    done.TryRemove(tp, out _); // not ours any more: committing it would only fail
                }
            }

            foreach (var w in stopping)
            {
                w.Stop();
            }

            // A worker that is mid-handler has been cancelled; the event is redelivered to the new owner and deduplicated.
            Task.WaitAll(stopping.Select(w => w.Completion).ToArray(), TimeSpan.FromSeconds(15));
            if (commit)
            {
                CommitDone(c, done);
            }
        }

        using var consumer = new ConsumerBuilder<string, byte[]>(BuildConfig())
            .SetPartitionsRevokedHandler((c, parts) => StopWorkers(parts.Select(p => p.TopicPartition), c, true))
            .SetPartitionsLostHandler((c, parts) => StopWorkers(parts.Select(p => p.TopicPartition), c, false))
            .Build();
        consumer.Subscribe(topics);

        var lastCommit = DateTime.UtcNow;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromMilliseconds(200));
                    Health.Polled(consumer.Assignment.Count);
                }
                catch (KafkaException ex)
                {
                    logger.LogWarning(ex, "Kafka consume error; backing off");
                    if (!await BackoffAsync(ct))
                    {
                        break;
                    }

                    continue;
                }

                if (result?.Message is not null)
                {
                    var tp = result.TopicPartition;
                    if (!workers.TryGetValue(tp, out var worker))
                    {
                        worker = new PartitionWorker(this, tp, gate, done, ct);
                        workers[tp] = worker;
                    }

                    worker.Enqueue(result);
                    if (worker.Queued >= _queueHighWater && paused.Add(tp))
                    {
                        consumer.Pause([tp]);
                    }
                }

                if (paused.Count > 0)
                {
                    foreach (var tp in paused.Where(tp => workers.TryGetValue(tp, out var w) && w.Queued <= _queueLowWater).ToList())
                    {
                        consumer.Resume([tp]);
                        paused.Remove(tp);
                    }
                }

                if (DateTime.UtcNow - lastCommit >= TimeSpan.FromMilliseconds(500))
                {
                    CommitDone(consumer, done);
                    lastCommit = DateTime.UtcNow;
                }
            }
        }
        finally
        {
            foreach (var w in workers.Values)
            {
                w.Stop();
            }

            Task.WaitAll(workers.Values.Select(w => w.Completion).ToArray(), TimeSpan.FromSeconds(15));
            CommitDone(consumer, done);
            consumer.Close();
        }
    }

    private void CommitDone(IConsumer<string, byte[]> consumer, ConcurrentDictionary<TopicPartition, long> done)
    {
        var offsets = new List<TopicPartitionOffset>();
        foreach (var (tp, next) in done.ToArray())
        {
            if (done.TryRemove(new KeyValuePair<TopicPartition, long>(tp, next)))
            {
                offsets.Add(new TopicPartitionOffset(tp, next));
            }
        }

        if (offsets.Count == 0)
        {
            return;
        }

        try
        {
            consumer.Commit(offsets);
        }
        catch (KafkaException ex)
        {
            // The events are already recorded in processed_events, so a redelivery is harmless.
            logger.LogWarning(ex, "Kafka commit failed for {Count} partitions; continuing", offsets.Count);
        }
    }

    private sealed class PartitionWorker
    {
        private readonly Channel<ConsumeResult<string, byte[]>> _queue = Channel.CreateUnbounded<ConsumeResult<string, byte[]>>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        private readonly CancellationTokenSource _cts;
        private int _queued;

        public PartitionWorker(
            EventConsumer owner, TopicPartition partition, SemaphoreSlim gate, ConcurrentDictionary<TopicPartition, long> done, CancellationToken ct)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Completion = Task.Run(() => RunAsync(owner, partition, gate, done, _cts.Token), CancellationToken.None);
        }

        public Task Completion { get; }

        public int Queued => Volatile.Read(ref _queued);

        public void Enqueue(ConsumeResult<string, byte[]> result)
        {
            Interlocked.Increment(ref _queued);
            _queue.Writer.TryWrite(result);
        }

        public void Stop() => _cts.Cancel();

        private async Task RunAsync(
            EventConsumer owner, TopicPartition partition, SemaphoreSlim gate, ConcurrentDictionary<TopicPartition, long> done, CancellationToken ct)
        {
            try
            {
                await foreach (var result in _queue.Reader.ReadAllAsync(ct))
                {
                    await gate.WaitAsync(ct);
                    bool ok;
                    try
                    {
                        ok = await owner.HandleWithRetryAsync(result.Message.Value, result.TopicPartitionOffset, ct);
                    }
                    finally
                    {
                        gate.Release();
                    }

                    if (!ok)
                    {
                        return;
                    }

                    done[partition] = result.Offset.Value + 1;
                    Interlocked.Decrement(ref _queued);
                }
            }
            catch (OperationCanceledException)
            {
                // stopping or partition revoked
            }
        }
    }

    private static async Task<bool> BackoffAsync(CancellationToken ct)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private async Task<bool> HandleWithRetryAsync(byte[] value, TopicPartitionOffset position, CancellationToken ct)
    {
        var delay = TimeSpan.FromMilliseconds(200);
        var envelope = TryParse(value);
        using var requestScope = RequestContext.Use(envelope?.RequestId); // retries and failures are logged under the request id
        using var logScope = logger.BeginScope(new RequestLogScope(RequestContext.Current, null));
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        while (!ct.IsCancellationRequested)
        {
            try
            {
                try
                {
                    await ProcessAsync(value, ct);
                    if (envelope is not null && IsHandled(envelope.Type))
                    {
                        PlatformMetrics.ConsumerEvents.WithLabels("handled").Inc();
                        Health.EventProcessed();
                        var millis = Math.Round(System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds, 3);
                        if (logger.IsEnabled(LogLevel.Information))
                        {
                            logger.LogInformation("consume: event processed {Type} {EventId} {DurationMs}", envelope.Type, envelope.EventId, millis);
                        }
                    }
                }
                catch (PermanentEventException ex)
                {
                    PlatformMetrics.ConsumerEvents.WithLabels("permanent").Inc();
                    logger.LogError(ex, "Permanent handler failure at {Position}; skipping the event", position);
                    await RecordSkippedAsync(value, position.Topic, ex.Message, ct);
                }

                return true;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                PlatformMetrics.ConsumerEvents.WithLabels("failed").Inc();
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

    private static Envelope? TryParse(byte[] value)
    {
        try
        {
            return Envelope.Parser.ParseFrom(value);
        }
        catch (InvalidProtocolBufferException)
        {
            return null;
        }
    }

    private bool IsHandled(string type) => handlers.ContainsKey(type) || (atLeastOnceHandlers?.ContainsKey(type) ?? false);

    /// <summary>
    /// Records a permanently failed event as processed and keeps its envelope and the error in dead_letter,
    /// in one statement so both rows exist or neither does.
    /// </summary>
    private async Task RecordSkippedAsync(byte[] value, string topic, string error, CancellationToken ct)
    {
        Envelope env;
        try
        {
            env = Envelope.Parser.ParseFrom(value);
        }
        catch (InvalidProtocolBufferException ex)
        {
            logger.LogError(ex, "Skipped message is not a readable envelope; it cannot be recorded");
            return;
        }

        if (!Guid.TryParse(env.EventId, out var eventId))
        {
            logger.LogError("Skipped {Type} event has no valid event id ({EventId}); it cannot be recorded", env.Type, env.EventId);
            return;
        }

        await using var cmd = dataSource.CreateCommand(
            """
            WITH p AS (
              INSERT INTO processed_events (consumer, event_id) VALUES (@c, @e) ON CONFLICT DO NOTHING
            )
            INSERT INTO dead_letter (consumer, event_id, topic, payload, error) VALUES (@c, @e, @t, @p, @err)
            ON CONFLICT DO NOTHING
            """);
        cmd.Parameters.AddWithValue("c", group);
        cmd.Parameters.AddWithValue("e", eventId);
        cmd.Parameters.AddWithValue("t", topic);
        cmd.Parameters.AddWithValue("p", value);
        cmd.Parameters.AddWithValue("err", error);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The permanent-skip statement on its own, for tests.</summary>
    public Task SkipPermanentlyAsync(byte[] value, string topic, string error, CancellationToken ct = default)
        => RecordSkippedAsync(value, topic, error, ct);

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
            logger.LogError(ex, "Skipping undecodable message");
            return;
        }

        using var requestScope = RequestContext.Use(env.RequestId); // handlers (and the events they write) run under the id of the original call
        if (atLeastOnceHandlers is not null && atLeastOnceHandlers.TryGetValue(env.Type, out var external))
        {
            await ProcessAtLeastOnceAsync(env, external, ct);
            return;
        }

        if (!handlers.TryGetValue(env.Type, out var handler))
        {
            // Unknown types are expected: a consumer ignores what it does not handle.
            return;
        }

        if (!Guid.TryParse(env.EventId, out var eventId))
        {
            logger.LogError("Skipping {Type} event with invalid event id '{EventId}'", env.Type, env.EventId);
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

        try
        {
            await handler(conn, tx, env);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new PermanentEventException($"cannot decode {env.Type}: {ex.Message}", ex);
        }

        await tx.CommitAsync(ct);
    }

    private async Task ProcessAtLeastOnceAsync(Envelope env, Func<Envelope, CancellationToken, Task> handler, CancellationToken ct)
    {
        if (!Guid.TryParse(env.EventId, out var eventId))
        {
            logger.LogError("Skipping {Type} event with invalid event id '{EventId}'", env.Type, env.EventId);
            return;
        }

        await using (var check = dataSource.CreateCommand("SELECT 1 FROM processed_events WHERE consumer = @c AND event_id = @e"))
        {
            check.Parameters.AddWithValue("c", group);
            check.Parameters.AddWithValue("e", eventId);
            if (await check.ExecuteScalarAsync(ct) is not null)
            {
                return;
            }
        }

        try
        {
            await handler(env, ct);
        }
        catch (InvalidProtocolBufferException ex)
        {
            throw new PermanentEventException($"cannot decode {env.Type}: {ex.Message}", ex);
        }

        await using var insert = dataSource.CreateCommand(
            "INSERT INTO processed_events (consumer, event_id) VALUES (@c, @e) ON CONFLICT DO NOTHING");
        insert.Parameters.AddWithValue("c", group);
        insert.Parameters.AddWithValue("e", eventId);
        await insert.ExecuteNonQueryAsync(ct);
    }
}
