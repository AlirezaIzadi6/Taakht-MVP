using System.Globalization;
using System.Net;
using System.Text;
using Grpc.AspNetCore.Server;
using Grpc.Net.ClientFactory;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Npgsql;
using Prometheus;

namespace Taakht.Platform;

/// <summary>Resolved settings of the operations listener.</summary>
internal sealed record ObservabilityOptions(string Service, IPAddress Address, int Port);

public static class ObservabilityExtensions
{
    public const string HealthAddrKey = "HEALTH_ADDR";
    public const string LogFormatKey = "LOG_FORMAT";
    public const string LogLevelKey = "LOG_LEVEL";
    public const string ServiceLiveness = "liveness";
    public const string ServiceReadiness = "readiness";

    private const string _liveTag = "live";
    private const string _readyTag = "ready";

    /// <summary>
    /// Operability for a service: logging (LOG_FORMAT=json|text, LOG_LEVEL), request id and per-call logging and metrics,
    /// the standard gRPC health service (grpc.health.v1: "" and "liveness" mean the process is up, "readiness" needs the
    /// database, the consumer and the outbox) and a plain HTTP listener on HEALTH_ADDR (default 127.0.0.1:&lt;port&gt;)
    /// serving /healthz, /readyz and /metrics. Call it before AddTaakhtPlatform and AddGrpc so that its interceptors are
    /// outermost, and call <see cref="MapTaakhtObservability"/> after Build.
    /// </summary>
    public static WebApplicationBuilder AddTaakhtObservability(this WebApplicationBuilder builder, string service, int defaultHealthPort)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(service);
        var config = builder.Configuration;
        var options = ParseHealthAddress(service, config[HealthAddrKey] ?? $"127.0.0.1:{defaultHealthPort}");

        ConfigureLogging(builder, service);

        // The listener is plain HTTP/1.1: the gRPC port speaks cleartext HTTP/2 only, so curl and Prometheus cannot use it.
        builder.WebHost.ConfigureKestrel(k => k.Listen(options.Address, options.Port, o => o.Protocols = HttpProtocols.Http1));

        var services = builder.Services;
        services.AddSingleton(options);
        services.AddSingleton<GrpcObservabilityInterceptor>();
        services.AddSingleton<ClientRequestIdInterceptor>();
        services.Configure<GrpcServiceOptions>(o => o.Interceptors.Add<GrpcObservabilityInterceptor>());
        services.ConfigureAll<GrpcClientFactoryOptions>(o =>
            o.InterceptorRegistrations.Add(new Grpc.Net.ClientFactory.InterceptorRegistration(InterceptorScope.Client, sp => sp.GetRequiredService<ClientRequestIdInterceptor>())));

        var outboxAge = ReadinessSettings.OutboxAge(config);
        var consumerStale = ReadinessSettings.ConsumerStale(config);
        services.AddHealthChecks()
            .AddCheck("process", () => HealthCheckResult.Healthy(), [_liveTag])
            .Add(new HealthCheckRegistration("db", sp => new DatabaseHealthCheck(sp.GetRequiredService<NpgsqlDataSource>()), null, [_readyTag]))
            .Add(new HealthCheckRegistration("outbox", sp => new OutboxHealthCheck(sp.GetRequiredService<NpgsqlDataSource>(), outboxAge), null, [_readyTag]))
            .Add(new HealthCheckRegistration("consumer", sp => new ConsumerHealthCheck(sp, consumerStale), null, [_readyTag]));
        services.AddGrpcHealthChecks(o =>
        {
            o.Services.Map(string.Empty, r => r.Tags.Contains(_liveTag));
            o.Services.Map(ServiceLiveness, r => r.Tags.Contains(_liveTag));
            o.Services.Map(ServiceReadiness, r => r.Tags.Contains(_readyTag));
        });
        services.Configure<HealthCheckPublisherOptions>(o =>
        {
            o.Delay = TimeSpan.FromSeconds(1);
            o.Period = TimeSpan.FromSeconds(2);
        });
        return builder;
    }

    /// <summary>Maps grpc.health.v1, /healthz, /readyz and /metrics (the last three only on the HEALTH_ADDR port).</summary>
    public static WebApplication MapTaakhtObservability(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        var options = app.Services.GetRequiredService<ObservabilityOptions>();
        PlatformMetrics.RegisterDatabase(app.Services.GetRequiredService<NpgsqlDataSource>(), app.Services.GetRequiredService<DatabasePoolSettings>());

        var host = $"*:{options.Port.ToString(CultureInfo.InvariantCulture)}";
        app.MapGrpcHealthChecksService();
        app.MapHealthChecks("/healthz", new HealthCheckOptions
        {
            Predicate = r => r.Tags.Contains(_liveTag),
            ResponseWriter = (ctx, report) => WriteReport(ctx, report, "ok", "not ok"),
        }).RequireHost(host);
        app.MapHealthChecks("/readyz", new HealthCheckOptions
        {
            Predicate = r => r.Tags.Contains(_readyTag),
            ResponseWriter = (ctx, report) => WriteReport(ctx, report, "ready", "not ready"),
        }).RequireHost(host);
        app.MapMetrics("/metrics").RequireHost(host);
        return app;
    }

    internal static ObservabilityOptions ParseHealthAddress(string service, string address)
    {
        var i = address.LastIndexOf(':');
        if (i < 0 || !int.TryParse(address[(i + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out var port) || port is < 1 or > 65535)
        {
            throw new InvalidOperationException($"{HealthAddrKey} must be host:port, got '{address}'");
        }

        var host = address[..i];
        IPAddress ip;
        if (host.Length == 0)
        {
            ip = IPAddress.Any;
        }
        else if (host == "localhost")
        {
            ip = IPAddress.Loopback;
        }
        else if (!IPAddress.TryParse(host, out ip!))
        {
            throw new InvalidOperationException($"{HealthAddrKey} host must be an IP address or localhost, got '{host}'");
        }

        return new ObservabilityOptions(service, ip, port);
    }

    private static void ConfigureLogging(WebApplicationBuilder builder, string service)
    {
        var json = string.Equals(builder.Configuration[LogFormatKey], "json", StringComparison.OrdinalIgnoreCase);
        if (json)
        {
            builder.Logging.Services.AddSingleton<ConsoleFormatter>(new JsonConsoleFormatter(service));
            builder.Logging.AddConsole(o => o.FormatterName = JsonConsoleFormatter.FormatterName);
        }
        else
        {
            builder.Logging.AddSimpleConsole(o =>
            {
                o.IncludeScopes = true; // shows request_id / user_id of the call
                o.SingleLine = true;
                o.TimestampFormat = "HH:mm:ss.fff ";
            });
        }

        if (Enum.TryParse<LogLevel>(builder.Configuration[LogLevelKey], ignoreCase: true, out var level))
        {
            builder.Logging.SetMinimumLevel(level);
        }

        // One "grpc call" line per call replaces the per-request lines of the framework.
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
        builder.Logging.AddFilter("Grpc", LogLevel.Warning);
    }

    private static Task WriteReport(HttpContext context, HealthReport report, string ok, string notOk)
    {
        context.Response.ContentType = "text/plain";
        var sb = new StringBuilder();
        sb.AppendLine(report.Status == HealthStatus.Healthy ? ok : notOk);
        foreach (var (name, entry) in report.Entries.OrderBy(e => e.Key, StringComparer.Ordinal))
        {
            if (name == "process")
            {
                continue;
            }

            sb.Append(name).Append(": ").AppendLine(entry.Status == HealthStatus.Healthy ? "ok" : "FAIL " + Truncate(entry.Description ?? entry.Exception?.Message ?? string.Empty, 200));
        }

        return context.Response.WriteAsync(sb.ToString());
    }

    private static string Truncate(string s, int n) => s.Length > n ? s[..n] + "..." : s;
}
