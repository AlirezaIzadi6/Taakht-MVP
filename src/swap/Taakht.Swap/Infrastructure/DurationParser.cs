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

        var totalMs = 0.0;
        foreach (Match m in PartPattern().Matches(trimmed))
        {
            var value = double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
            totalMs += m.Groups[2].Value switch
            {
                "ms" => value,
                "s" => value * 1000,
                "m" => value * 60_000,
                _ => value * 3_600_000,
            };
        }

        // Anything beyond a century is nonsense here and would overflow TimeSpan; bounds are checked by the callers.
        if (double.IsNaN(totalMs) || totalMs > TimeSpan.FromDays(36500).TotalMilliseconds)
        {
            throw new FormatException($"Duration '{text}' is too large.");
        }

        return TimeSpan.FromMilliseconds(totalMs);
    }

    /// <summary>
    /// Parses a setting and requires <paramref name="min"/> &lt;= value &lt;= <paramref name="max"/>; the message names the setting.
    /// </summary>
    public static TimeSpan ParseBounded(string name, string text, TimeSpan min, TimeSpan max)
    {
        TimeSpan value;
        try
        {
            value = Parse(text);
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException)
        {
            throw new FormatException($"{name}: {ex.Message}", ex);
        }

        if (value < min || value > max)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name}={text} must be between {min} and {max}.");
        }

        return value;
    }

    [GeneratedRegex(@"^(\d+(\.\d+)?(ms|s|m|h))+$")]
    private static partial Regex FullPattern();

    [GeneratedRegex(@"(\d+(?:\.\d+)?)(ms|s|m|h)")]
    private static partial Regex PartPattern();
}
