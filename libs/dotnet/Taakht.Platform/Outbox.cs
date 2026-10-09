using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform;

public static class Outbox
{
    /// <summary>Wraps a payload in an Envelope (type = full proto name, aggregate_id = key).</summary>
    public static Envelope Wrap(string key, IMessage message, Guid? eventId = null, DateTime? occurredAtUtc = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(message);
        return new Envelope
        {
            EventId = (eventId ?? Guid.NewGuid()).ToString(),
            Type = message.Descriptor.FullName,
            AggregateId = key,
            OccurredAt = Timestamp.FromDateTime(occurredAtUtc ?? DateTime.UtcNow),
            Payload = message.ToByteString(),
            RequestId = RequestContext.Current ?? string.Empty, // the ambient request id, so consumers log under the same id
        };
    }

    /// <summary>Writes an outbox row inside the caller's transaction; the relay publishes it later.</summary>
    public static async Task AddAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string topic, string key, IMessage message,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentException.ThrowIfNullOrEmpty(topic);

        var envelope = Wrap(key, message);
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO outbox (id, topic, key, envelope, created_at) VALUES (@id, @topic, @key, @env, clock_timestamp())", connection, transaction);
        cmd.Parameters.AddWithValue("id", Guid.Parse(envelope.EventId));
        cmd.Parameters.AddWithValue("topic", topic);
        cmd.Parameters.AddWithValue("key", key);
        cmd.Parameters.AddWithValue("env", envelope.ToByteArray());
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
