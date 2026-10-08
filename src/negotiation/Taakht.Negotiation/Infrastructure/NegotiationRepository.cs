using Dapper;
using Npgsql;
using Taakht.Negotiation.Domain;

namespace Taakht.Negotiation.Infrastructure;

/// <summary>Plain-SQL persistence. Callers own the connection and transaction.</summary>
public static class NegotiationRepository
{
    private const string _negotiationColumns = """
        id AS Id, requester_user_id AS RequesterUserId, requester_ad_id AS RequesterAdId,
        target_user_id AS TargetUserId, target_ad_id AS TargetAdId, status AS Status,
        active_proposal_number AS ActiveProposalNumber, previous_proposal_number AS PreviousProposalNumber,
        last_proposal_number AS LastProposalNumber, cancel_reason AS CancelReason, version AS Version,
        created_at AS CreatedAt, updated_at AS UpdatedAt
        """;

    public static string StatusToDb(NegotiationStatus status) => status switch
    {
        NegotiationStatus.Open => "OPEN",
        NegotiationStatus.AgreementPending => "AGREEMENT_PENDING",
        NegotiationStatus.Agreed => "AGREED",
        NegotiationStatus.Declined => "DECLINED",
        NegotiationStatus.Withdrawn => "WITHDRAWN",
        NegotiationStatus.Cancelled => "CANCELLED",
        _ => throw new ArgumentOutOfRangeException(nameof(status)),
    };

    public static NegotiationStatus StatusFromDb(string value) => value switch
    {
        "OPEN" => NegotiationStatus.Open,
        "AGREEMENT_PENDING" => NegotiationStatus.AgreementPending,
        "AGREED" => NegotiationStatus.Agreed,
        "DECLINED" => NegotiationStatus.Declined,
        "WITHDRAWN" => NegotiationStatus.Withdrawn,
        "CANCELLED" => NegotiationStatus.Cancelled,
        _ => throw new InvalidOperationException($"unknown negotiation status '{value}'"),
    };

    public static async Task InsertAsync(NpgsqlConnection conn, NpgsqlTransaction tx, SwapNegotiation n, CancellationToken ct)
    {
        await conn.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO negotiation (id, requester_user_id, requester_ad_id, target_user_id, target_ad_id, status,
              active_proposal_number, previous_proposal_number, last_proposal_number, cancel_reason, version, created_at, updated_at)
            VALUES (@Id, @RequesterUserId, @RequesterAdId, @TargetUserId, @TargetAdId, @Status,
              @Active, @Previous, @Last, @CancelReason, @Version, @CreatedAt, @UpdatedAt)
            """,
            ToParameters(n), tx, cancellationToken: ct));
        await WriteChildrenAsync(conn, tx, n, ct);
    }

    public static async Task SaveAsync(NpgsqlConnection conn, NpgsqlTransaction tx, SwapNegotiation n, CancellationToken ct)
    {
        await WriteChildrenAsync(conn, tx, n, ct);
        await conn.ExecuteAsync(new CommandDefinition(
            """
            UPDATE negotiation SET status = @Status, active_proposal_number = @Active, previous_proposal_number = @Previous,
              last_proposal_number = @Last, cancel_reason = @CancelReason, version = @Version, updated_at = @UpdatedAt
            WHERE id = @Id
            """,
            ToParameters(n), tx, cancellationToken: ct));
    }

    /// <summary>Loads one negotiation; with <paramref name="forUpdate"/> the row is locked until the transaction ends.</summary>
    public static async Task<SwapNegotiation?> LoadAsync(
        NpgsqlConnection conn, NpgsqlTransaction? tx, Guid id, bool forUpdate, CancellationToken ct)
    {
        var list = await LoadManyAsync(conn, tx, [id], forUpdate, ct);
        return list.Count == 0 ? null : list[0];
    }

    public static async Task<List<SwapNegotiation>> LoadManyAsync(
        NpgsqlConnection conn, NpgsqlTransaction? tx, IReadOnlyCollection<Guid> ids, bool forUpdate, CancellationToken ct)
    {
        if (ids.Count == 0)
        {
            return [];
        }

        var rows = (await conn.QueryAsync<NegotiationRow>(new CommandDefinition(
            $"SELECT {_negotiationColumns} FROM negotiation WHERE id = ANY(@ids) ORDER BY {(forUpdate ? "id" : "created_at DESC, id")}"
            + (forUpdate ? " FOR UPDATE" : string.Empty),
            new { ids = ids.ToArray() }, tx, cancellationToken: ct))).ToList();

        var rowIds = rows.Select(r => r.Id).ToArray();
        var proposals = (await conn.QueryAsync<ProposalRow>(new CommandDefinition(
            """
            SELECT negotiation_id AS NegotiationId, number AS Number, id AS Id, leg_a AS LegA, leg_b AS LegB,
                   price_difference AS PriceDifference, payer_user_id AS PayerUserId, author_user_id AS AuthorUserId
            FROM proposal WHERE negotiation_id = ANY(@ids)
            """,
            new { ids = rowIds }, tx, cancellationToken: ct))).ToLookup(p => p.NegotiationId);
        var approvals = (await conn.QueryAsync<ApprovalRow>(new CommandDefinition(
            "SELECT negotiation_id AS NegotiationId, user_id AS UserId, kind AS Kind, target AS Target FROM approval WHERE negotiation_id = ANY(@ids)",
            new { ids = rowIds }, tx, cancellationToken: ct))).ToLookup(a => a.NegotiationId);

        var result = new List<SwapNegotiation>(rows.Count);
        foreach (var r in rows)
        {
            var byNumber = proposals[r.Id].ToDictionary(p => p.Number, ToProposal);
            result.Add(SwapNegotiation.Restore(
                r.Id, r.RequesterUserId, r.RequesterAdId, r.TargetUserId, r.TargetAdId,
                StatusFromDb(r.Status),
                byNumber[r.ActiveProposalNumber],
                r.PreviousProposalNumber is { } prev ? byNumber[prev] : null,
                r.LastProposalNumber, r.CancelReason, r.Version,
                AsUtc(r.CreatedAt), AsUtc(r.UpdatedAt),
                approvals[r.Id].Select(a => new Approval(a.UserId, KindFromDb(a.Kind), a.Target))));
        }

        return result;
    }

    /// <summary>Newest first; only negotiations where the user is a party, optionally on one ad.</summary>
    public static async Task<List<Guid>> ListIdsAsync(NpgsqlConnection conn, string userId, string? adId, CancellationToken ct)
    {
        var sql = "SELECT id FROM negotiation WHERE (requester_user_id = @userId OR target_user_id = @userId)"
            + (string.IsNullOrEmpty(adId) ? string.Empty : " AND (requester_ad_id = @adId OR target_ad_id = @adId)")
            + " ORDER BY created_at DESC, id";
        return (await conn.QueryAsync<Guid>(new CommandDefinition(sql, new { userId, adId }, cancellationToken: ct))).ToList();
    }

    public static async Task UpsertAdRefAsync(NpgsqlConnection conn, NpgsqlTransaction tx, AdSnapshot ad, CancellationToken ct)
    {
        await conn.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO ad_ref (ad_id, owner_id, current_version, status) VALUES (@AdId, @OwnerId, @Version, @Status)
            ON CONFLICT (ad_id) DO UPDATE
              SET owner_id = excluded.owner_id, current_version = excluded.current_version, status = excluded.status
              WHERE ad_ref.current_version <= excluded.current_version
            """,
            new { ad.AdId, ad.OwnerId, ad.Version, Status = ad.Status.ToString().ToUpperInvariant() },
            tx, cancellationToken: ct));
    }

    /// <summary>Locks the ad_ref rows in a fixed order so concurrent openers serialize without deadlocking.</summary>
    public static async Task LockAdRefsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyCollection<string> adIds, CancellationToken ct)
    {
        await conn.ExecuteAsync(new CommandDefinition(
            "SELECT ad_id FROM ad_ref WHERE ad_id = ANY(@adIds) ORDER BY ad_id FOR UPDATE",
            new { adIds = adIds.ToArray() }, tx, cancellationToken: ct));
    }

    public static async Task<int> CountLiveAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string adId, CancellationToken ct) =>
        await conn.ExecuteScalarAsync<int>(new CommandDefinition(
            """
            SELECT count(*)::int FROM negotiation
            WHERE status IN ('OPEN', 'AGREEMENT_PENDING') AND (requester_ad_id = @adId OR target_ad_id = @adId)
            """,
            new { adId }, tx, cancellationToken: ct));

    public static async Task<bool> LivePairExistsAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, string requesterAdId, string targetAdId, CancellationToken ct) =>
        await conn.ExecuteScalarAsync<bool>(new CommandDefinition(
            """
            SELECT EXISTS (SELECT 1 FROM negotiation
              WHERE requester_ad_id = @requesterAdId AND target_ad_id = @targetAdId AND status IN ('OPEN', 'AGREEMENT_PENDING'))
            """,
            new { requesterAdId, targetAdId }, tx, cancellationToken: ct));

    /// <summary>Current versions known locally for the ads of the given negotiations.</summary>
    public static async Task<Dictionary<string, int>> AdVersionsAsync(
        NpgsqlConnection conn, NpgsqlTransaction? tx, IReadOnlyCollection<string> adIds, CancellationToken ct)
    {
        if (adIds.Count == 0)
        {
            return [];
        }

        var rows = await conn.QueryAsync<(string AdId, int Version)>(new CommandDefinition(
            "SELECT ad_id AS AdId, current_version AS Version FROM ad_ref WHERE ad_id = ANY(@adIds)",
            new { adIds = adIds.ToArray() }, tx, cancellationToken: ct));
        return rows.ToDictionary(r => r.AdId, r => r.Version, StringComparer.Ordinal);
    }

    /// <summary>Ids of live negotiations that involve any of the given ads, locked in id order.</summary>
    public static async Task<List<Guid>> LockLiveInvolvingAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyCollection<string> adIds, Guid exceptId, CancellationToken ct) =>
        (await conn.QueryAsync<Guid>(new CommandDefinition(
            """
            SELECT id FROM negotiation
            WHERE id <> @exceptId AND status IN ('OPEN', 'AGREEMENT_PENDING')
              AND (requester_ad_id = ANY(@adIds) OR target_ad_id = ANY(@adIds))
            ORDER BY id FOR UPDATE
            """,
            new { exceptId, adIds = adIds.ToArray() }, tx, cancellationToken: ct))).ToList();

    private static async Task WriteChildrenAsync(NpgsqlConnection conn, NpgsqlTransaction tx, SwapNegotiation n, CancellationToken ct)
    {
        foreach (var p in new[] { n.ActiveProposal, n.PreviousProposal })
        {
            if (p is null)
            {
                continue;
            }

            await conn.ExecuteAsync(new CommandDefinition(
                """
                INSERT INTO proposal (negotiation_id, number, id, leg_a, leg_b, price_difference, payer_user_id, author_user_id, created_at)
                VALUES (@NegotiationId, @Number, @Id, @LegA, @LegB, @Price, @Payer, @Author, @CreatedAt)
                ON CONFLICT (negotiation_id, number) DO NOTHING
                """,
                new
                {
                    NegotiationId = n.Id,
                    p.Number,
                    p.Id,
                    LegA = DeliveryToDb(p.Terms.LegA),
                    LegB = DeliveryToDb(p.Terms.LegB),
                    Price = p.Terms.PriceDifference,
                    Payer = p.Terms.PayerUserId,
                    Author = p.AuthorUserId,
                    CreatedAt = n.UpdatedAt,
                },
                tx, cancellationToken: ct));
        }

        foreach (var a in n.Approvals)
        {
            await conn.ExecuteAsync(new CommandDefinition(
                """
                INSERT INTO approval (negotiation_id, user_id, kind, target, updated_at)
                VALUES (@NegotiationId, @UserId, @Kind, @Target, @UpdatedAt)
                ON CONFLICT (negotiation_id, user_id, kind) DO UPDATE SET target = excluded.target, updated_at = excluded.updated_at
                WHERE approval.target <> excluded.target
                """,
                new { NegotiationId = n.Id, a.UserId, Kind = KindToDb(a.Kind), a.Target, UpdatedAt = n.UpdatedAt },
                tx, cancellationToken: ct));
        }
    }

    private static object ToParameters(SwapNegotiation n) => new
    {
        n.Id,
        n.RequesterUserId,
        n.RequesterAdId,
        n.TargetUserId,
        n.TargetAdId,
        Status = StatusToDb(n.Status),
        Active = n.ActiveProposal.Number,
        Previous = n.PreviousProposal?.Number,
        Last = n.LastProposalNumber,
        n.CancelReason,
        n.Version,
        n.CreatedAt,
        n.UpdatedAt,
    };

    private static Proposal ToProposal(ProposalRow p) => new(
        p.Id,
        p.Number,
        new Terms(DeliveryFromDb(p.LegA), DeliveryFromDb(p.LegB), p.PriceDifference, p.PayerUserId),
        p.AuthorUserId);

    private static string DeliveryToDb(DeliveryMethod m) => m == DeliveryMethod.Locker ? "LOCKER" : "IN_PERSON";

    private static DeliveryMethod DeliveryFromDb(string v) => v == "LOCKER" ? DeliveryMethod.Locker : DeliveryMethod.InPerson;

    private static string KindToDb(ApprovalKind k) => k == ApprovalKind.Ad ? "AD" : "TERMS";

    private static ApprovalKind KindFromDb(string v) => v == "AD" ? ApprovalKind.Ad : ApprovalKind.Terms;

    private static DateTimeOffset AsUtc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));

    private sealed class NegotiationRow
    {
        public Guid Id { get; set; }

        public string RequesterUserId { get; set; } = string.Empty;

        public string RequesterAdId { get; set; } = string.Empty;

        public string TargetUserId { get; set; } = string.Empty;

        public string TargetAdId { get; set; } = string.Empty;

        public string Status { get; set; } = string.Empty;

        public int ActiveProposalNumber { get; set; }

        public int? PreviousProposalNumber { get; set; }

        public int LastProposalNumber { get; set; }

        public string CancelReason { get; set; } = string.Empty;

        public int Version { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime UpdatedAt { get; set; }
    }

    private sealed class ProposalRow
    {
        public Guid NegotiationId { get; set; }

        public int Number { get; set; }

        public Guid Id { get; set; }

        public string LegA { get; set; } = string.Empty;

        public string LegB { get; set; } = string.Empty;

        public long PriceDifference { get; set; }

        public string PayerUserId { get; set; } = string.Empty;

        public string? AuthorUserId { get; set; }
    }

    private sealed class ApprovalRow
    {
        public Guid NegotiationId { get; set; }

        public string UserId { get; set; } = string.Empty;

        public string Kind { get; set; } = string.Empty;

        public int Target { get; set; }
    }
}
