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
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("10")]
    [InlineData("5x")]
    public void Rejects_invalid_durations(string text) =>
        Assert.ThrowsAny<Exception>(() => DurationParser.Parse(text));
}
