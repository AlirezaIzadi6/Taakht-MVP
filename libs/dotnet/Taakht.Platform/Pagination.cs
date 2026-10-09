using System.Buffers.Text;
using System.Globalization;
using System.Text;

namespace Taakht.Platform;

/// <summary>
/// Keyset pagination helpers shared by the list endpoints: page-size clamping and the opaque page token
/// (base64url of "created_at|id" of the last item of a page, in the order created_at DESC, id DESC).
/// </summary>
public static class Pagination
{
    public const int DefaultPageSize = 50;

    public const int MaxPageSize = 200;

    // UTC with microseconds, the precision of a Postgres timestamptz, so a cursor compares exactly.
    private const string _timeFormat = "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'";

    /// <summary>Applies the default (requested &lt;= 0) and the cap to the requested page size.</summary>
    public static int PageSize(int requested) =>
        requested <= 0 ? DefaultPageSize : Math.Min(requested, MaxPageSize);

    public static string Encode(DateTimeOffset createdAt, Guid id)
    {
        var raw = createdAt.UtcDateTime.ToString(_timeFormat, CultureInfo.InvariantCulture) + "|" + id.ToString("D");
        return Base64Url.EncodeToString(Encoding.UTF8.GetBytes(raw));
    }

    /// <summary>Parses a token; false for anything malformed. The caller treats an empty token as the first page before calling.</summary>
    public static bool TryDecode(string token, out DateTime createdAtUtc, out Guid id)
    {
        createdAtUtc = default;
        id = default;
        try
        {
            var raw = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(token.TrimEnd('=')));
            var cut = raw.IndexOf('|', StringComparison.Ordinal);
            return cut > 0
                && DateTime.TryParseExact(raw[..cut], _timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out createdAtUtc)
                && Guid.TryParseExact(raw[(cut + 1)..], "D", out id);
        }
        catch (FormatException)
        {
            return false;
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }
}
