using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace Taakht.Platform;

/// <summary>
/// Liveness of one Kafka consumer, written by <see cref="EventConsumer"/> on every poll (about every 500 ms) and read by
/// the readiness check and the metrics.
/// </summary>
public sealed class ConsumerHealthState
{
    private long _lastPollTicks;
    private int _assigned;
    private long _lastEventTicks;

    public int AssignedPartitions => Volatile.Read(ref _assigned);

    public DateTime? LastPollUtc => _lastPollTicks == 0 ? null : new DateTime(Interlocked.Read(ref _lastPollTicks), DateTimeKind.Utc);

    public void Polled(int assignedPartitions, DateTime? now = null)
    {
        Volatile.Write(ref _assigned, assignedPartitions);
        Interlocked.Exchange(ref _lastPollTicks, (now ?? DateTime.UtcNow).Ticks);
        PlatformMetrics.ConsumerJoined.Set(assignedPartitions > 0 ? 1 : 0);
    }

    public void EventProcessed()
    {
        Interlocked.Exchange(ref _lastEventTicks, DateTime.UtcNow.Ticks);
        PlatformMetrics.ConsumerLastEvent.Set(new DateTimeOffset(new DateTime(_lastEventTicks, DateTimeKind.Utc)).ToUnixTimeMilliseconds() / 1000.0);
    }
}

/// <summary>Ready only while every <see cref="EventConsumer"/> holds partitions and polled within the staleness limit.</summary>
internal sealed class ConsumerHealthCheck(IServiceProvider services, TimeSpan maxStale) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        foreach (var consumer in services.GetServices<IHostedService>().OfType<EventConsumer>())
        {
            var result = Evaluate(consumer.Group, consumer.Health, DateTime.UtcNow, maxStale);
            if (result.Status != HealthStatus.Healthy)
            {
                return Task.FromResult(result);
            }
        }

        return Task.FromResult(HealthCheckResult.Healthy());
    }

    internal static HealthCheckResult Evaluate(string group, ConsumerHealthState state, DateTime now, TimeSpan maxStale)
    {
        if (state.AssignedPartitions <= 0)
        {
            return HealthCheckResult.Unhealthy($"consumer {group}: no partitions assigned (group not joined)");
        }

        if (state.LastPollUtc is not { } last || now - last > maxStale)
        {
            var age = state.LastPollUtc is { } l ? $"{(now - l).TotalSeconds:F0}s ago" : "never";
            return HealthCheckResult.Unhealthy($"consumer {group}: last poll {age} (limit {maxStale.TotalSeconds:F0}s)");
        }

        return HealthCheckResult.Healthy();
    }
}

/// <summary>SELECT 1 within 2 s.</summary>
internal sealed class DatabaseHealthCheck(NpgsqlDataSource dataSource) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            await using var cmd = dataSource.CreateCommand("SELECT 1");
            await cmd.ExecuteScalarAsync(cts.Token);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex) when (ex is NpgsqlException or TimeoutException or OperationCanceledException or InvalidOperationException)
        {
            return HealthCheckResult.Unhealthy($"database: {ex.GetType().Name}: {ex.Message}");
        }
    }
}

/// <summary>Fails when the oldest unpublished outbox row is older than the limit: the relay is stuck or Kafka is unreachable.</summary>
internal sealed class OutboxHealthCheck(NpgsqlDataSource dataSource, TimeSpan maxAge) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            var (_, oldest) = await OutboxBacklog.ReadAsync(dataSource, cts.Token);
            return Evaluate(oldest, maxAge);
        }
        catch (Exception ex) when (ex is NpgsqlException or TimeoutException or OperationCanceledException or InvalidOperationException)
        {
            return HealthCheckResult.Unhealthy($"outbox: {ex.GetType().Name}: {ex.Message}");
        }
    }

    internal static HealthCheckResult Evaluate(TimeSpan oldest, TimeSpan maxAge) =>
        oldest > maxAge
            ? HealthCheckResult.Unhealthy($"outbox: oldest unpublished event is {oldest.TotalSeconds:F0}s old (limit {maxAge.TotalSeconds:F0}s)")
            : HealthCheckResult.Healthy();
}

/// <summary>Settings of the readiness checks.</summary>
internal static class ReadinessSettings
{
    public const string OutboxAgeKey = "READY_OUTBOX_AGE";
    public const string ConsumerStaleKey = "READY_CONSUMER_STALE";

    public static TimeSpan OutboxAge(IConfiguration c) => Read(c, OutboxAgeKey, TimeSpan.FromSeconds(60));

    public static TimeSpan ConsumerStale(IConfiguration c) => Read(c, ConsumerStaleKey, TimeSpan.FromSeconds(60));

    private static TimeSpan Read(IConfiguration c, string key, TimeSpan fallback) =>
        c[key] is { Length: > 0 } raw
            ? HousekeepingOptions.ParseBounded(key, raw, TimeSpan.FromSeconds(1), TimeSpan.FromDays(1))
            : fallback;

}
