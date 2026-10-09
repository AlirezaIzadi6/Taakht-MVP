using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Taakht.Platform;

/// <summary>
/// Retention settings for <see cref="HousekeepingService"/>. OutboxRetention applies to rows that were already
/// published; unpublished rows are never deleted. ProcessedEventsRetention is the idempotency window of the
/// consumers: once a processed_events row is gone, a redelivery of that event is handled a second time, so it must
/// be much larger than any realistic redelivery window (topic retention, offset resets, replays).
/// </summary>
public sealed record HousekeepingOptions
{
    public const string OutboxRetentionKey = "OUTBOX_RETENTION";
    public const string ProcessedEventsRetentionKey = "PROCESSED_EVENTS_RETENTION";
    public const string DeadLetterRetentionKey = "DEAD_LETTER_RETENTION";
    public const string PruneIntervalKey = "PRUNE_INTERVAL";

    /// <summary>Upper bound for the day component; keeps durations far from TimeSpan overflow.</summary>
    public const int MaxDays = 36500;

    public static readonly TimeSpan MinDuration = TimeSpan.FromMinutes(1);

    private static readonly Regex _dayPrefix = new(@"^(\d+)d(.*)$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public TimeSpan OutboxRetention { get; init; } = TimeSpan.FromHours(24);

    public TimeSpan ProcessedEventsRetention { get; init; } = TimeSpan.FromDays(7);

    public TimeSpan DeadLetterRetention { get; init; } = TimeSpan.FromDays(30);

    public TimeSpan Interval { get; init; } = TimeSpan.FromMinutes(10);

    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(30);

    public int BatchSize { get; init; } = 1000;

    /// <summary>Reads OUTBOX_RETENTION, PROCESSED_EVENTS_RETENTION, DEAD_LETTER_RETENTION and PRUNE_INTERVAL (e.g. 24h, 7d, 10m) and validates.</summary>
    public static HousekeepingOptions FromConfiguration(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        return FromValues(key => configuration[key]);
    }

    /// <summary>Same as <see cref="FromConfiguration"/> over an arbitrary key lookup.</summary>
    public static HousekeepingOptions FromValues(Func<string, string?> lookup)
    {
        ArgumentNullException.ThrowIfNull(lookup);
        var defaults = new HousekeepingOptions();
        var options = defaults with
        {
            OutboxRetention = Read(lookup, OutboxRetentionKey, defaults.OutboxRetention),
            ProcessedEventsRetention = Read(lookup, ProcessedEventsRetentionKey, defaults.ProcessedEventsRetention),
            DeadLetterRetention = Read(lookup, DeadLetterRetentionKey, defaults.DeadLetterRetention),
            Interval = Read(lookup, PruneIntervalKey, defaults.Interval),
        };
        options.Validate();
        return options;
    }

    /// <summary>Parses "90s", "10m", "24h", "7d", "1d12h" or a TimeSpan literal ("01:30:00").</summary>
    public static TimeSpan ParseDuration(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var text = value.Trim();
        var total = TimeSpan.Zero;
        var m = _dayPrefix.Match(text);
        if (m.Success)
        {
            if (!long.TryParse(m.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var days) || days > MaxDays)
            {
                throw new FormatException($"Invalid duration '{value}': more than {MaxDays} days.");
            }

            total = TimeSpan.FromDays(days);
            text = m.Groups[2].Value;
            if (text.Length == 0)
            {
                return total;
            }
        }

        var matches = Regex.Matches(text, @"(\d+)(ms|h|m|s)", RegexOptions.CultureInvariant);
        if (matches.Count > 0 && string.Concat(matches.Select(x => x.Value)) == text)
        {
            foreach (Match part in matches)
            {
                if (!long.TryParse(part.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var n) || n > MaxDays * 24L * 3600)
                {
                    throw new FormatException($"Invalid duration '{value}': number too large.");
                }

                total += part.Groups[2].Value switch
                {
                    "ms" => TimeSpan.FromMilliseconds(n),
                    "s" => TimeSpan.FromSeconds(n),
                    "m" => TimeSpan.FromMinutes(n),
                    _ => TimeSpan.FromHours(n),
                };
            }

            return total;
        }

        if (!m.Success && TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var span))
        {
            return span;
        }

        throw new FormatException($"Invalid duration '{value}'; use e.g. 90s, 10m, 24h, 7d or 1d12h.");
    }

    /// <summary>
    /// Parses a duration setting (<see cref="ParseDuration"/> syntax) and requires <paramref name="min"/> &lt;= value &lt;= <paramref name="max"/>.
    /// The error names the setting, so a bad value fails startup with a clear message.
    /// </summary>
    public static TimeSpan ParseBounded(string name, string value, TimeSpan min, TimeSpan max)
    {
        TimeSpan parsed;
        try
        {
            parsed = ParseDuration(value);
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException or OverflowException)
        {
            throw new FormatException($"{name}: {ex.Message}", ex);
        }

        if (parsed < min || parsed > max)
        {
            throw new ArgumentOutOfRangeException(name, parsed, $"{name}={value} must be between {min} and {max}.");
        }

        return parsed;
    }

    /// <summary>Throws when a retention or the interval is below one minute.</summary>
    public void Validate()
    {
        Require(OutboxRetentionKey, OutboxRetention);
        Require(ProcessedEventsRetentionKey, ProcessedEventsRetention);
        Require(DeadLetterRetentionKey, DeadLetterRetention);
        Require(PruneIntervalKey, Interval);
        if (BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(BatchSize), BatchSize, "BatchSize must be positive.");
        }

        static void Require(string name, TimeSpan value)
        {
            if (value < MinDuration)
            {
                throw new ArgumentOutOfRangeException(name, value, $"{name} must be at least {MinDuration}.");
            }
        }
    }

    private static TimeSpan Read(Func<string, string?> lookup, string key, TimeSpan fallback)
    {
        var raw = lookup(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        try
        {
            return ParseDuration(raw);
        }
        catch (FormatException ex)
        {
            throw new FormatException($"{key}: {ex.Message}", ex);
        }
    }
}

/// <summary>
/// Prunes the outbox (published rows older than the retention; unpublished rows are never touched),
/// processed_events and dead_letter tables in batches, once shortly after startup and then every interval.
/// </summary>
public sealed class HousekeepingService(
    NpgsqlDataSource dataSource, HousekeepingOptions options, ILogger<HousekeepingService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        options.Validate();
        try
        {
            await Task.Delay(options.InitialDelay, stoppingToken);
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await PruneOnceAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Housekeeping prune failed; retrying at the next interval");
                }

                await Task.Delay(options.Interval, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }

    /// <summary>Runs one sweep and returns the number of deleted (outbox, processed_events, dead_letter) rows. Public for tests.</summary>
    public async Task<(long Outbox, long ProcessedEvents, long DeadLetter)> PruneOnceAsync(CancellationToken ct = default)
    {
        var outbox = await DeleteBatchesAsync(
            """
            DELETE FROM outbox WHERE id IN (
              SELECT id FROM outbox
              WHERE published_at IS NOT NULL AND published_at < now() - make_interval(secs => @secs)
              LIMIT @n FOR UPDATE SKIP LOCKED)
            """,
            options.OutboxRetention,
            ct);
        var processed = await DeleteBatchesAsync(
            """
            DELETE FROM processed_events WHERE (consumer, event_id) IN (
              SELECT consumer, event_id FROM processed_events
              WHERE processed_at < now() - make_interval(secs => @secs)
              LIMIT @n FOR UPDATE SKIP LOCKED)
            """,
            options.ProcessedEventsRetention,
            ct);
        var deadLetter = await DeleteBatchesAsync(
            """
            DELETE FROM dead_letter WHERE (consumer, event_id) IN (
              SELECT consumer, event_id FROM dead_letter
              WHERE created_at < now() - make_interval(secs => @secs)
              LIMIT @n FOR UPDATE SKIP LOCKED)
            """,
            options.DeadLetterRetention,
            ct);
        if ((outbox > 0 || processed > 0 || deadLetter > 0) && logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation(
                "Housekeeping pruned {OutboxDeleted} outbox, {ProcessedEventsDeleted} processed_events and {DeadLetterDeleted} dead_letter rows",
                outbox,
                processed,
                deadLetter);
        }

        return (outbox, processed, deadLetter);
    }

    private async Task<long> DeleteBatchesAsync(string sql, TimeSpan retention, CancellationToken ct)
    {
        long total = 0;
        while (true)
        {
            await using var cmd = dataSource.CreateCommand(sql);
            cmd.Parameters.AddWithValue("secs", retention.TotalSeconds);
            cmd.Parameters.AddWithValue("n", options.BatchSize);
            var n = await cmd.ExecuteNonQueryAsync(ct);
            total += n;
            if (n < options.BatchSize)
            {
                return total;
            }

            ct.ThrowIfCancellationRequested();
        }
    }
}
