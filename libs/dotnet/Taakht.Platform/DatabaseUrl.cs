using System.Globalization;
using Npgsql;

namespace Taakht.Platform;

/// <summary>Converts a <c>postgres://</c> URL (the DATABASE_URL convention) into Npgsql types.</summary>
public static class DatabaseUrl
{
    public static string ToConnectionString(string url)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(url);

        // Already a key=value connection string.
        if (!url.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase)
            && !url.StartsWith("postgresql://", StringComparison.OrdinalIgnoreCase))
        {
            return url.Contains("localhost", StringComparison.OrdinalIgnoreCase)
                ? UseLoopbackAddress(new NpgsqlConnectionStringBuilder(url)).ConnectionString
                : url;
        }

        var uri = new Uri(url);
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = LoopbackAddress(uri.Host),
            Port = uri.Port > 0 ? uri.Port : 5432,
            Database = Uri.UnescapeDataString(uri.AbsolutePath.TrimStart('/')),
        };

        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var parts = uri.UserInfo.Split(':', 2);
            builder.Username = Uri.UnescapeDataString(parts[0]);
            if (parts.Length > 1)
            {
                builder.Password = Uri.UnescapeDataString(parts[1]);
            }
        }

        foreach (var pair in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = pair.Split('=', 2);
            var key = Uri.UnescapeDataString(kv[0]);
            var value = kv.Length > 1 ? Uri.UnescapeDataString(kv[1]) : string.Empty;
            if (key.Equals("sslmode", StringComparison.OrdinalIgnoreCase))
            {
                builder.SslMode = value.ToLower(CultureInfo.InvariantCulture) switch
                {
                    "disable" => SslMode.Disable,
                    "allow" => SslMode.Allow,
                    "prefer" => SslMode.Prefer,
                    "require" => SslMode.Require,
                    "verify-ca" => SslMode.VerifyCA,
                    "verify-full" => SslMode.VerifyFull,
                    _ => throw new FormatException($"Unsupported sslmode '{value}'."),
                };
            }
            else
            {
                builder[key] = value;
            }
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Connection string with the pool bounds applied. Settings written in the URL / connection string itself
    /// (<c>Maximum Pool Size</c>, <c>Minimum Pool Size</c>, <c>Timeout</c>) win; the others take the values of
    /// <paramref name="pool"/>.
    /// </summary>
    public static string ToConnectionString(string url, DatabasePoolSettings pool)
    {
        ArgumentNullException.ThrowIfNull(pool);
        var builder = new NpgsqlConnectionStringBuilder(ToConnectionString(url));
        if (!builder.ShouldSerialize("Maximum Pool Size"))
        {
            builder.MaxPoolSize = pool.MaxConnections;
        }

        if (!builder.ShouldSerialize("Minimum Pool Size"))
        {
            builder.MinPoolSize = Math.Min(pool.MinConnections, builder.MaxPoolSize);
        }

        if (!builder.ShouldSerialize("Timeout"))
        {
            // Also bounds the wait for a pooled connection: an exhausted pool fails fast instead of hanging.
            builder.Timeout = (int)Math.Ceiling(pool.AcquireTimeout.TotalSeconds);
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// A host of exactly <c>localhost</c> becomes <c>127.0.0.1</c>. On Windows a new connection to <c>localhost</c> tries
    /// ::1 first, Docker publishes Postgres on 127.0.0.1 only, and the refused IPv6 attempt costs about 4 s for every new
    /// physical connection (measured: ~4.03 s against ~15 ms), which shows up whenever a pool has to grow.
    /// </summary>
    public static string LoopbackAddress(string host) =>
        string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase) ? "127.0.0.1" : host;

    private static NpgsqlConnectionStringBuilder UseLoopbackAddress(NpgsqlConnectionStringBuilder builder)
    {
        builder.Host = LoopbackAddress(builder.Host ?? string.Empty);
        return builder;
    }

    public static NpgsqlDataSource CreateDataSource(string url, DatabasePoolSettings? pool = null) =>
        NpgsqlDataSource.Create(ToConnectionString(url, pool ?? DatabasePoolSettings.Default));
}
