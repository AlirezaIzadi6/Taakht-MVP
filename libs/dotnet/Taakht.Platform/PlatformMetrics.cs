using Grpc.Core;
using Npgsql;
using Prometheus;

namespace Taakht.Platform;

/// <summary>
/// Prometheus metrics shared by all services. Names and labels match libs/goplatform/observe (the dashboards in
/// deploy/observability rely on that). Labels stay low-cardinality: gRPC method and status code, never ids.
/// </summary>
public static class PlatformMetrics
{
    public static readonly Counter GrpcRequests = Metrics.CreateCounter(
        "taakht_grpc_server_requests_total",
        "Unary gRPC calls handled, by full method and status code.",
        new CounterConfiguration { LabelNames = ["method", "code"] });

    public static readonly Histogram GrpcDuration = Metrics.CreateHistogram(
        "taakht_grpc_server_request_duration_seconds",
        "Unary gRPC handling time by full method.",
        new HistogramConfiguration
        {
            LabelNames = ["method"],
            Buckets = [.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10],
        });

    public static readonly Counter ConsumerEvents = Metrics.CreateCounter(
        "taakht_consumer_events_total",
        "Events processed by the consumer, by result (handled, failed, permanent).",
        new CounterConfiguration { LabelNames = ["result"] });

    public static readonly Counter OutboxPublished = Metrics.CreateCounter(
        "taakht_outbox_published_total",
        "Outbox rows published to Kafka.");

    public static readonly Counter HousekeepingDeleted = Metrics.CreateCounter(
        "taakht_housekeeping_deleted_total",
        "Rows deleted by housekeeping, by table.",
        new CounterConfiguration { LabelNames = ["table"] });

    public static readonly Gauge ConsumerJoined = Metrics.CreateGauge(
        "taakht_consumer_joined",
        "1 while the Kafka consumer holds partitions of its group.");

    public static readonly Gauge ConsumerLastEvent = Metrics.CreateGauge(
        "taakht_consumer_last_event_timestamp_seconds",
        "Unix time of the last event the consumer processed (0 if none yet).");

    public static readonly Gauge DbPoolConnections = Metrics.CreateGauge(
        "taakht_db_pool_connections",
        "Database pool connections by state (acquired, idle, total, max).",
        new GaugeConfiguration { LabelNames = ["state"] });

    public static readonly Gauge OutboxUnpublished = Metrics.CreateGauge(
        "taakht_outbox_unpublished",
        "Outbox rows not yet published to Kafka.");

    public static readonly Gauge OutboxOldestAge = Metrics.CreateGauge(
        "taakht_outbox_oldest_unpublished_age_seconds",
        "Age of the oldest unpublished outbox row (0 when none).");

    static PlatformMetrics()
    {
        foreach (var result in new[] { "handled", "failed", "permanent" })
        {
            ConsumerEvents.WithLabels(result);
        }

        foreach (var table in new[] { "outbox", "processed_events", "dead_letter" })
        {
            HousekeepingDeleted.WithLabels(table);
        }

        ConsumerJoined.Set(0);
        ConsumerLastEvent.Set(0);
    }

    /// <summary>Status code name as the Go library prints it ("Canceled", not "Cancelled").</summary>
    public static string CodeName(StatusCode code) => code == StatusCode.Cancelled ? "Canceled" : code.ToString();

    /// <summary>
    /// Refreshes the pool and outbox gauges when Prometheus scrapes (the outbox numbers come from the database, using the
    /// partial index outbox_unpublished).
    /// </summary>
    internal static void RegisterDatabase(NpgsqlDataSource dataSource, DatabasePoolSettings pool)
    {
        var poolStats = new NpgsqlPoolStats();
        Metrics.DefaultRegistry.AddBeforeCollectCallback(async ct =>
        {
            poolStats.Refresh();
            var used = poolStats.Get("used");
            var idle = poolStats.Get("idle");
            DbPoolConnections.WithLabels("acquired").Set(used);
            DbPoolConnections.WithLabels("idle").Set(idle);
            DbPoolConnections.WithLabels("total").Set(used + idle);
            DbPoolConnections.WithLabels("max").Set(pool.MaxConnections);
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromSeconds(2));
                var (count, age) = await OutboxBacklog.ReadAsync(dataSource, cts.Token);
                OutboxUnpublished.Set(count);
                OutboxOldestAge.Set(age.TotalSeconds);
            }
            catch (Exception ex) when (ex is NpgsqlException or TimeoutException or OperationCanceledException or InvalidOperationException)
            {
                // The gauges keep their last values; /readyz reports the database problem.
            }
        });
    }
}

/// <summary>The unpublished part of the outbox: how many rows and how old the oldest is.</summary>
public static class OutboxBacklog
{
    public static async Task<(long Count, TimeSpan Oldest)> ReadAsync(NpgsqlDataSource dataSource, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        await using var cmd = dataSource.CreateCommand(
            "SELECT count(*), COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(created_at)), 0)::float8 FROM outbox WHERE published_at IS NULL");
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (reader.GetInt64(0), TimeSpan.FromSeconds(reader.GetDouble(1)));
    }
}

/// <summary>
/// Reads the connection counts Npgsql publishes as the OpenTelemetry metric db.client.connection.count (tag
/// db.client.connection.state: idle or used); NpgsqlDataSource itself exposes no public pool statistics. One data source
/// per process, so the values are not split by pool name.
/// </summary>
internal sealed class NpgsqlPoolStats : IDisposable
{
    private readonly System.Diagnostics.Metrics.MeterListener _listener = new();
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _values = new();

    public NpgsqlPoolStats()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Npgsql" && instrument.Name == "db.client.connection.count")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<int>((_, value, tags, _) => Record(value, tags));
        _listener.SetMeasurementEventCallback<long>((_, value, tags, _) => Record(value, tags));
        _listener.Start();
    }

    public long Get(string state) => _values.TryGetValue(state, out var v) ? v : 0;

    public void Refresh() => _listener.RecordObservableInstruments();

    public void Dispose() => _listener.Dispose();

    private void Record(long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "db.client.connection.state" && tag.Value is string state)
            {
                _values[state] = value;
            }
        }
    }
}
