using Taakht.Swap.Infrastructure;

namespace Taakht.Swap.Tests;

public class DurationParserTests
{
    [Theory]
    [InlineData("30s", 30)]
    [InlineData("2m", 120)]
    [InlineData("1h", 3600)]
    [InlineData("1h30m", 5400)]
    [InlineData("1.5s", 1.5)]
    public void Parses_go_style_durations(string text, double seconds) =>
        Assert.Equal(TimeSpan.FromSeconds(seconds), DurationParser.Parse(text));

    [Theory]
    [InlineData("0s")]
    [InlineData("0ms")]
    [InlineData("721h")]
    [InlineData("31d")]
    [InlineData("99999999999999999999h")]
    [InlineData("1e9h")]
    public void ParseBounded_rejects_out_of_range_and_overflowing_values(string text)
    {
        var ex = Assert.ThrowsAny<Exception>(() => DurationParser.ParseBounded(
            "PAYMENT_DEADLINE", text, TimeSpan.FromMilliseconds(1), TimeSpan.FromDays(30)));
        Assert.Contains("PAYMENT_DEADLINE", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("5s", 5)]
    [InlineData("1h", 3600)]
    [InlineData("720h", 720 * 3600)]
    public void ParseBounded_accepts_values_in_range(string text, int seconds) =>
        Assert.Equal(
            TimeSpan.FromSeconds(seconds),
            DurationParser.ParseBounded("PAYMENT_DEADLINE", text, TimeSpan.FromMilliseconds(1), TimeSpan.FromDays(30)));

    [Theory]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("10")]
    [InlineData("5x")]
    public void Rejects_invalid_durations(string text) =>
        Assert.ThrowsAny<Exception>(() => DurationParser.Parse(text));
}
