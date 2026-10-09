using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;

namespace Taakht.Platform;

/// <summary>
/// One JSON object per log event with the same fields as the Go services: ts (UTC), level, service, msg, request_id and
/// user_id (when known), category, the named values of the message template in snake_case (method, code,
/// duration_ms, ...) and exception. Selected with LOG_FORMAT=json.
/// </summary>
public sealed class JsonConsoleFormatter(string service) : ConsoleFormatter(FormatterName)
{
    public const string FormatterName = "taakht-json";

    public override void Write<TState>(in LogEntry<TState> logEntry, IExternalScopeProvider? scopeProvider, TextWriter textWriter)
    {
        ArgumentNullException.ThrowIfNull(textWriter);
        var message = logEntry.Formatter?.Invoke(logEntry.State, logEntry.Exception);
        if (message is null && logEntry.Exception is null)
        {
            return;
        }

        var fields = new Dictionary<string, object?>();
        scopeProvider?.ForEachScope(
            static (scope, fields) =>
            {
                if (scope is IEnumerable<KeyValuePair<string, object?>> kvs)
                {
                    foreach (var kv in kvs)
                    {
                        fields[kv.Key] = kv.Value;
                    }
                }
            },
            fields);
        if (logEntry.State is IEnumerable<KeyValuePair<string, object?>> state)
        {
            foreach (var kv in state)
            {
                if (kv.Key != "{OriginalFormat}")
                {
                    fields[SnakeCase(kv.Key)] = kv.Value;
                }
            }
        }

        if (!fields.ContainsKey("request_id") && RequestContext.Current is { } rid)
        {
            fields["request_id"] = rid;
        }

        if (!fields.ContainsKey("user_id") && UserContext.Current is { } uid)
        {
            fields["user_id"] = uid;
        }

        var buffer = new MemoryStream();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();
            w.WriteString("ts", DateTime.UtcNow.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
            w.WriteString("level", LevelName(logEntry.LogLevel));
            w.WriteString("service", service);
            w.WriteString("msg", message ?? string.Empty);
            w.WriteString("category", logEntry.Category);
            foreach (var (key, value) in fields)
            {
                w.WritePropertyName(key);
                WriteValue(w, value);
            }

            if (logEntry.Exception is { } ex)
            {
                w.WriteString("exception", ex.ToString());
            }

            w.WriteEndObject();
        }

        textWriter.WriteLine(Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length));
    }

    private static void WriteValue(Utf8JsonWriter w, object? value)
    {
        switch (value)
        {
            case null:
                w.WriteNullValue();
                break;
            case bool b:
                w.WriteBooleanValue(b);
                break;
            case int i:
                w.WriteNumberValue(i);
                break;
            case long l:
                w.WriteNumberValue(l);
                break;
            case double d when double.IsFinite(d):
                w.WriteNumberValue(d);
                break;
            case float f when float.IsFinite(f):
                w.WriteNumberValue(f);
                break;
            default:
                w.WriteStringValue(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
        }
    }

    private static string LevelName(LogLevel level) => level switch
    {
        LogLevel.Trace => "trace",
        LogLevel.Debug => "debug",
        LogLevel.Information => "info",
        LogLevel.Warning => "warn",
        LogLevel.Error => "error",
        LogLevel.Critical => "fatal",
        _ => "info",
    };

    internal static string SnakeCase(string name)
    {
        var sb = new StringBuilder(name.Length + 4);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0 && !char.IsUpper(name[i - 1]) && name[i - 1] != '_')
                {
                    sb.Append('_');
                }

                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }
}
