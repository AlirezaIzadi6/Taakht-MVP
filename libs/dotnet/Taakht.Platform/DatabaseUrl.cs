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
            return url;
        }

        var uri = new Uri(url);
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = uri.Host,
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

    public static NpgsqlDataSource CreateDataSource(string url) => NpgsqlDataSource.Create(ToConnectionString(url));
}
