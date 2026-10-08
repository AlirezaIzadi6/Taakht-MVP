using System.Globalization;
using System.Text.RegularExpressions;

namespace Taakht.Swap.Infrastructure;

/// <summary>Parses Go-style durations such as "30s", "2m", "1h" or "1h30m".</summary>
public static partial class DurationParser
{
    public static TimeSpan Parse(string text)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(text);
        var trimmed = text.Trim();
        if (!FullPattern().IsMatch(trimmed))
        {
            throw new FormatException($"Invalid duration '{text}'; use values like 30s, 2m, 1h.");
        }

        var total = TimeSpan.Zero;
        foreach (Match m in PartPattern().Matches(trimmed))
        {
            var value = double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
            total += m.Groups[2].Value switch
            {
                "ms" => TimeSpan.FromMilliseconds(value),
                "s" => TimeSpan.FromSeconds(value),
                "m" => TimeSpan.FromMinutes(value),
                _ => TimeSpan.FromHours(value),
            };
        }

        return total;
    }

    [GeneratedRegex(@"^(\d+(\.\d+)?(ms|s|m|h))+$")]
    private static partial Regex FullPattern();

    [GeneratedRegex(@"(\d+(?:\.\d+)?)(ms|s|m|h)")]
    private static partial Regex PartPattern();
}
