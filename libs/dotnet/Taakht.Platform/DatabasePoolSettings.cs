using System.Globalization;
using Microsoft.Extensions.Configuration;

namespace Taakht.Platform;

/// <summary>
/// Bounds of the Npgsql pool of one service. Four services share one Postgres (max_connections=200 in
/// deploy/docker-compose.yml), and Npgsql's own default (100 per data source) would let one service use all of it.
/// Configured by DB_MAX_CONNS (default 20), DB_MIN_CONNS (default 5, so that bursts do not pay for opening connections) and DB_ACQUIRE_TIMEOUT (default 10s, how long a
/// request waits for a free pooled connection before it fails with UNAVAILABLE). An invalid value fails startup.
/// </summary>
public sealed record DatabasePoolSettings
{
    public const string MaxKey = "DB_MAX_CONNS";
    public const string MinKey = "DB_MIN_CONNS";
    public const string AcquireTimeoutKey = "DB_ACQUIRE_TIMEOUT";

    public static readonly DatabasePoolSettings Default = new();

    public int MaxConnections { get; init; } = 20;

    public int MinConnections { get; init; } = 5;

    public TimeSpan AcquireTimeout { get; init; } = TimeSpan.FromSeconds(10);

    public static DatabasePoolSettings FromConfiguration(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        return FromLookup(key => configuration[key]);
    }

    /// <summary>Same as <see cref="FromConfiguration"/> over any key lookup (null or empty means not set).</summary>
    public static DatabasePoolSettings FromLookup(Func<string, string?> get)
    {
        ArgumentNullException.ThrowIfNull(get);
        var max = ParseInt(MaxKey, get(MaxKey), 1) ?? Default.MaxConnections;
        var minConfigured = ParseInt(MinKey, get(MinKey), 0);
        if (minConfigured > max)
        {
            throw new FormatException($"{MinKey} ({minConfigured}) exceeds {MaxKey} ({max}).");
        }

        var timeout = Default.AcquireTimeout;
        if (get(AcquireTimeoutKey) is { Length: > 0 } raw)
        {
            timeout = HousekeepingOptions.ParseBounded(AcquireTimeoutKey, raw, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
        }

        return new DatabasePoolSettings
        {
            MaxConnections = max,
            MinConnections = minConfigured ?? Math.Min(Default.MinConnections, max),
            AcquireTimeout = timeout,
        };
    }

    private static int? ParseInt(string key, string? value, int min)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var n) || n < min)
        {
            throw new FormatException($"{key} must be an integer >= {min}, got '{value}'.");
        }

        return n;
    }
}
