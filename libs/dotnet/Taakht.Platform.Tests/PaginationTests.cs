using System.Buffers.Text;
using System.Text;

namespace Taakht.Platform.Tests;

public class PaginationTests
{
    [Theory]
    [InlineData(-5, 50)]
    [InlineData(0, 50)]
    [InlineData(1, 1)]
    [InlineData(200, 200)]
    [InlineData(201, 200)]
    [InlineData(int.MaxValue, 200)]
    public void PageSize_applies_default_and_cap(int requested, int expected) =>
        Assert.Equal(expected, Pagination.PageSize(requested));

    [Fact]
    public void Token_round_trips_at_microsecond_precision()
    {
        var at = new DateTimeOffset(2026, 10, 9, 12, 30, 45, TimeSpan.Zero).AddTicks(1234560);
        var id = Guid.NewGuid();

        Assert.True(Pagination.TryDecode(Pagination.Encode(at, id), out var decoded, out var decodedId));
        Assert.Equal(at.UtcDateTime, decoded);
        Assert.Equal(id, decodedId);
    }

    [Theory]
    [InlineData("!!!")]
    [InlineData("abc")]
    [InlineData("")]
    public void Garbage_tokens_are_rejected(string token) =>
        Assert.False(Pagination.TryDecode(token, out _, out _));

    [Theory]
    [InlineData("nopipe")]
    [InlineData("|")]
    [InlineData("2026-10-09|00000000-0000-0000-0000-000000000001")]
    [InlineData("2026-10-09T12:30:45.123456Z|not-a-guid")]
    public void Well_encoded_but_malformed_tokens_are_rejected(string raw) =>
        Assert.False(Pagination.TryDecode(Base64Url.EncodeToString(Encoding.UTF8.GetBytes(raw)), out _, out _));
}
