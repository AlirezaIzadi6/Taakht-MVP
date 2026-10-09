using Npgsql;

namespace Taakht.Platform;

/// <summary>The outbox, processed_events and dead_letter DDL from the service conventions.</summary>
public static class PlatformSchema
{
    public const string OutboxDdl = """
        CREATE TABLE IF NOT EXISTS outbox (
          id           uuid PRIMARY KEY,
          topic        text        NOT NULL,
          key          text        NOT NULL,
          envelope     bytea       NOT NULL,
          created_at   timestamptz NOT NULL DEFAULT now(),
          published_at timestamptz
        );
        CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
        """;

    public const string ProcessedEventsDdl = """
        CREATE TABLE IF NOT EXISTS processed_events (
          consumer     text        NOT NULL,
          event_id     uuid        NOT NULL,
          processed_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (consumer, event_id)
        );
        """;

    public const string DeadLetterDdl = """
        CREATE TABLE IF NOT EXISTS dead_letter (
          consumer   text        NOT NULL,
          event_id   uuid        NOT NULL,
          topic      text        NOT NULL,
          payload    bytea       NOT NULL,
          error      text        NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (consumer, event_id)
        );
        """;

    /// <summary>Idempotently creates the tables. Handy for tests; services normally ship the DDL in a migration.</summary>
    public static async Task EnsureTablesAsync(NpgsqlDataSource dataSource, CancellationToken ct = default)
    {
        await using var cmd = dataSource.CreateCommand(OutboxDdl + "\n" + ProcessedEventsDdl + "\n" + DeadLetterDdl);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
