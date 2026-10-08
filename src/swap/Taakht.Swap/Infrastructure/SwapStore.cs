using Dapper;
using Google.Protobuf;
using Npgsql;
using Taakht.Platform;
using Taakht.Swap.Domain;

namespace Taakht.Swap.Infrastructure;

/// <summary>Plain-SQL persistence for swaps. Every state change goes through <see cref="ApplyAsync"/>.</summary>
public sealed class SwapStore(NpgsqlDataSource dataSource)
{
    private const string _columns = """
        id, negotiation_id, ad_a_id, ad_a_version, ad_b_id, ad_b_version, status,
        leg_a_owner, leg_a_method, leg_a_fee_paid, leg_b_owner, leg_b_method, leg_b_fee_paid,
        payment_deadline, cancel_reason, created_at
        """;

    static SwapStore() => DefaultTypeMap.MatchNamesWithUnderscores = true;

    /// <summary>Inserts the swap unless its negotiation already has one; returns the stored swap either way.</summary>
    public async Task<SwapModel> InsertIfAbsentAsync(SwapModel swap, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO swap (id, negotiation_id, ad_a_id, ad_a_version, ad_b_id, ad_b_version, status,
                              leg_a_owner, leg_a_method, leg_a_fee_paid, leg_b_owner, leg_b_method, leg_b_fee_paid,
                              payment_deadline, cancel_reason, created_at, updated_at)
            VALUES (@Id, @NegotiationId, @AdAId, @AdAVersion, @AdBId, @AdBVersion, @Status,
                    @LegAOwner, @LegAMethod, @LegAFeePaid, @LegBOwner, @LegBMethod, @LegBFeePaid,
                    @PaymentDeadline, @CancelReason, @CreatedAt, @CreatedAt)
            ON CONFLICT (negotiation_id) DO NOTHING
            """,
            ToParameters(swap),
            cancellationToken: ct));
        var stored = await conn.QuerySingleAsync<SwapRow>(new CommandDefinition(
            $"SELECT {_columns} FROM swap WHERE negotiation_id = @n", new { n = swap.NegotiationId }, cancellationToken: ct));
        return stored.ToModel();
    }

    public async Task<SwapModel?> GetAsync(Guid id, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<SwapRow>(new CommandDefinition(
            $"SELECT {_columns} FROM swap WHERE id = @id", new { id }, cancellationToken: ct));
        return row?.ToModel();
    }

    public async Task<IReadOnlyList<SwapModel>> ListForUserAsync(string userId, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<SwapRow>(new CommandDefinition(
            $"SELECT {_columns} FROM swap WHERE leg_a_owner = @userId OR leg_b_owner = @userId ORDER BY created_at DESC",
            new { userId },
            cancellationToken: ct));
        return [.. rows.Select(r => r.ToModel())];
    }

    public async Task<IReadOnlyList<Guid>> ListOverdueAsync(DateTimeOffset now, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var ids = await conn.QueryAsync<Guid>(new CommandDefinition(
            "SELECT id FROM swap WHERE status = 'AWAITING_PAYMENT' AND payment_deadline <= @now ORDER BY payment_deadline",
            new { now = now.UtcDateTime },
            cancellationToken: ct));
        return [.. ids];
    }

    /// <summary>
    /// Locks the row, runs the domain transition and persists it together with its events (outbox) in one short
    /// transaction. The UPDATE is conditional on the status that was read, so racing writers cannot both win.
    /// Returns null when the swap does not exist.
    /// </summary>
    public async Task<Transition?> ApplyAsync(Guid id, Func<SwapModel, Transition> transition, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var row = await conn.QuerySingleOrDefaultAsync<SwapRow>(new CommandDefinition(
            $"SELECT {_columns} FROM swap WHERE id = @id FOR UPDATE", new { id }, tx, cancellationToken: ct));
        if (row is null)
        {
            return null;
        }

        var before = row.ToModel();
        var result = transition(before);
        if (!result.Changed)
        {
            return result;
        }

        var updated = await conn.ExecuteAsync(new CommandDefinition(
            """
            UPDATE swap
               SET status = @Status, leg_a_fee_paid = @LegAFeePaid, leg_b_fee_paid = @LegBFeePaid,
                   payment_deadline = @PaymentDeadline, cancel_reason = @CancelReason, updated_at = now()
             WHERE id = @Id AND status = @ExpectedStatus
            """,
            new
            {
                Id = id,
                ExpectedStatus = StatusToDb(before.Status),
                Status = StatusToDb(result.Swap.Status),
                LegAFeePaid = result.Swap.LegA.FeePaid,
                LegBFeePaid = result.Swap.LegB.FeePaid,
                PaymentDeadline = result.Swap.PaymentDeadline?.UtcDateTime,
                result.Swap.CancelReason,
            },
            tx,
            cancellationToken: ct));
        if (updated != 1)
        {
            throw new InvalidOperationException($"swap {id} changed concurrently");
        }

        foreach (var ev in result.Events)
        {
            // clock_timestamp() instead of the transaction's now(): events of one transition must keep their order in the relay.
            var envelope = Outbox.Wrap(id.ToString(), SwapEventMapper.ToMessage(result.Swap, ev));
            await conn.ExecuteAsync(new CommandDefinition(
                "INSERT INTO outbox (id, topic, key, envelope, created_at) VALUES (@Id, @Topic, @Key, @Envelope, clock_timestamp())",
                new { Id = Guid.Parse(envelope.EventId), Topic = SwapEventMapper.Topic, Key = id.ToString(), Envelope = envelope.ToByteArray() },
                tx,
                cancellationToken: ct));
        }

        await tx.CommitAsync(ct);
        return result;
    }

    internal static string StatusToDb(SwapStatus status) => status switch
    {
        SwapStatus.Locking => "LOCKING",
        SwapStatus.Rejected => "REJECTED",
        SwapStatus.AwaitingPayment => "AWAITING_PAYMENT",
        SwapStatus.Completed => "COMPLETED",
        SwapStatus.Cancelled => "CANCELLED",
        _ => throw new ArgumentOutOfRangeException(nameof(status)),
    };

    private static SwapStatus StatusFromDb(string status) => status switch
    {
        "LOCKING" => SwapStatus.Locking,
        "REJECTED" => SwapStatus.Rejected,
        "AWAITING_PAYMENT" => SwapStatus.AwaitingPayment,
        "COMPLETED" => SwapStatus.Completed,
        "CANCELLED" => SwapStatus.Cancelled,
        _ => throw new InvalidOperationException($"unknown swap status '{status}'"),
    };

    private static object ToParameters(SwapModel s) => new
    {
        s.Id,
        s.NegotiationId,
        s.AdAId,
        s.AdAVersion,
        s.AdBId,
        s.AdBVersion,
        Status = StatusToDb(s.Status),
        LegAOwner = s.LegA.OwnerUserId,
        LegAMethod = s.LegA.Method.ToString(),
        LegAFeePaid = s.LegA.FeePaid,
        LegBOwner = s.LegB.OwnerUserId,
        LegBMethod = s.LegB.Method.ToString(),
        LegBFeePaid = s.LegB.FeePaid,
        PaymentDeadline = s.PaymentDeadline?.UtcDateTime,
        s.CancelReason,
        CreatedAt = s.CreatedAt.UtcDateTime,
    };

    private sealed class SwapRow
    {
        public Guid Id { get; set; }

        public string NegotiationId { get; set; } = "";

        public string AdAId { get; set; } = "";

        public int AdAVersion { get; set; }

        public string AdBId { get; set; } = "";

        public int AdBVersion { get; set; }

        public string Status { get; set; } = "";

        public string LegAOwner { get; set; } = "";

        public string LegAMethod { get; set; } = "";

        public bool LegAFeePaid { get; set; }

        public string LegBOwner { get; set; } = "";

        public string LegBMethod { get; set; } = "";

        public bool LegBFeePaid { get; set; }

        public DateTime? PaymentDeadline { get; set; }

        public string CancelReason { get; set; } = "";

        public DateTime CreatedAt { get; set; }

        public SwapModel ToModel() => new(
            Id,
            NegotiationId,
            AdAId,
            AdAVersion,
            AdBId,
            AdBVersion,
            StatusFromDb(Status),
            new SwapLeg(LegAOwner, Enum.Parse<DeliveryMethod>(LegAMethod), LegAFeePaid),
            new SwapLeg(LegBOwner, Enum.Parse<DeliveryMethod>(LegBMethod), LegBFeePaid),
            PaymentDeadline is { } d ? new DateTimeOffset(DateTime.SpecifyKind(d, DateTimeKind.Utc)) : null,
            CancelReason,
            new DateTimeOffset(DateTime.SpecifyKind(CreatedAt, DateTimeKind.Utc)));
    }
}
