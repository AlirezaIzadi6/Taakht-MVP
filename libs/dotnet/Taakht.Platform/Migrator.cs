using System.Reflection;
using Npgsql;

namespace Taakht.Platform;

/// <summary>Applies embedded <c>.sql</c> resources in name order and records them in schema_migrations.</summary>
public static class Migrator
{
    // Arbitrary constant so concurrent starters serialize.
    private const long _advisoryLockKey = 7_424_001;

    /// <summary>
    /// Applies every embedded resource ending in ".sql" (preferring those under a "migrations" folder),
    /// ordered by file name. The version is the file name without extension, e.g. "001_init".
    /// </summary>
    public static async Task MigrateAsync(NpgsqlDataSource dataSource, Assembly assembly, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(assembly);

        var migrations = ReadMigrations(assembly);

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await Exec(conn, "SELECT pg_advisory_lock(@k)", ct, ("k", _advisoryLockKey));
        try
        {
            await Exec(conn, "CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())", ct);

            var applied = new HashSet<string>(StringComparer.Ordinal);
            await using (var cmd = new NpgsqlCommand("SELECT version FROM schema_migrations", conn))
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    applied.Add(reader.GetString(0));
                }
            }

            foreach (var (version, sql) in migrations)
            {
                if (applied.Contains(version))
                {
                    continue;
                }

                await using var tx = await conn.BeginTransactionAsync(ct);
                await using (var cmd = new NpgsqlCommand(sql, conn, tx))
                {
                    await cmd.ExecuteNonQueryAsync(ct);
                }

                await using (var cmd = new NpgsqlCommand("INSERT INTO schema_migrations (version) VALUES (@v)", conn, tx))
                {
                    cmd.Parameters.AddWithValue("v", version);
                    await cmd.ExecuteNonQueryAsync(ct);
                }

                await tx.CommitAsync(ct);
            }
        }
        finally
        {
            await Exec(conn, "SELECT pg_advisory_unlock(@k)", CancellationToken.None, ("k", _advisoryLockKey));
        }
    }

    internal static List<(string Version, string Sql)> ReadMigrations(Assembly assembly)
    {
        var names = assembly.GetManifestResourceNames()
            .Where(n => n.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            .ToList();
        var inMigrations = names.Where(n => n.Contains(".migrations.", StringComparison.OrdinalIgnoreCase)).ToList();
        if (inMigrations.Count > 0)
        {
            names = inMigrations;
        }

        var result = new List<(string Version, string Sql)>();
        foreach (var name in names)
        {
            using var stream = assembly.GetManifestResourceStream(name)!;
            using var reader = new StreamReader(stream);
            result.Add((VersionOf(name), reader.ReadToEnd()));
        }

        return [.. result.OrderBy(m => m.Version, StringComparer.Ordinal)];
    }

    // "Svc.migrations.001_init.sql" -> "001_init"
    internal static string VersionOf(string resourceName)
    {
        var withoutExt = resourceName[..^4];
        var lastDot = withoutExt.LastIndexOf('.');
        return lastDot >= 0 ? withoutExt[(lastDot + 1)..] : withoutExt;
    }

    private static async Task Exec(NpgsqlConnection conn, string sql, CancellationToken ct, params (string Name, object Value)[] args)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var (name, value) in args)
        {
            cmd.Parameters.AddWithValue(name, value);
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }
}
