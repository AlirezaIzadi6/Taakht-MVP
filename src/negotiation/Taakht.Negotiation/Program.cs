using System.Globalization;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Npgsql;
using Taakht.Ad.V1;
using Taakht.Negotiation.Api;
using Taakht.Negotiation.Application;
using Taakht.Negotiation.Infrastructure;
using Taakht.Platform;

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

// Operability: logging, request id, health (grpc.health.v1, /healthz, /readyz) and /metrics on HEALTH_ADDR (default 127.0.0.1:9103).
builder.AddTaakhtObservability("negotiation", 9103);


var grpcAddr = config["GRPC_ADDR"] ?? ":9003";
var adAddr = config["AD_ADDR"] ?? "127.0.0.1:9001";
var cap = int.Parse(config["NEGOTIATION_CAP"] ?? "10", CultureInfo.InvariantCulture);
var agreementPendingTimeout = HousekeepingOptions.ParseBounded(
    "AGREEMENT_PENDING_TIMEOUT",
    config["AGREEMENT_PENDING_TIMEOUT"] ?? "10m",
    NegotiationOptions.MinAgreementPendingTimeout,
    NegotiationOptions.MaxAgreementPendingTimeout);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    var (host, port) = ParseAddress(grpcAddr);
    void Http2(ListenOptions o) => o.Protocols = HttpProtocols.Http2;
    if (host is "localhost" or "127.0.0.1")
    {
        kestrel.ListenLocalhost(port, Http2);
    }
    else
    {
        kestrel.ListenAnyIP(port, Http2);
    }
});

builder.Services.AddTaakhtPlatform(config);
builder.Services.AddTaakhtOutboxRelay();
builder.Services.AddTaakhtHousekeeping();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton(new NegotiationOptions(cap) { AgreementPendingTimeout = agreementPendingTimeout });
builder.Services.AddSingleton<ILockerEligibility, MockLockerEligibility>();
builder.Services.AddSingleton<NegotiationService>();
builder.Services.AddSingleton<EventHandlers>();
builder.Services.AddHostedService<AgreementPendingSweeper>();
builder.Services.AddSingleton<IAdClient, GrpcAdClient>();
builder.Services.AddGrpcClient<AdService.AdServiceClient>(o => o.Address = new Uri(adAddr.Contains("://", StringComparison.Ordinal) ? adAddr : $"http://{adAddr}"))
    .AddInterceptor<ClientIdentityInterceptor>();
builder.Services.AddGrpc(o =>
{
    o.Interceptors.Add<OverloadInterceptor>();
    o.Interceptors.Add<DomainExceptionInterceptor>();
    o.Interceptors.Add<ServerIdentityInterceptor>();
});
if (builder.Environment.IsDevelopment())
{
    builder.Services.AddGrpcReflection();
}

builder.Services.AddHostedService(sp => new EventConsumer(
    sp.GetRequiredService<NpgsqlDataSource>(),
    sp.GetRequiredService<KafkaOptions>(),
    "negotiation",
    [EventHandlers.AdTopic, EventHandlers.SwapTopic],
    sp.GetRequiredService<EventHandlers>().Build(),
    sp.GetRequiredService<ILogger<EventConsumer>>()));

var app = builder.Build();

await app.Services.MigrateAsync(typeof(Program).Assembly);

app.MapGrpcService<NegotiationGrpcService>();
app.MapTaakhtObservability();
if (app.Environment.IsDevelopment())
{
    app.MapGrpcReflectionService();
}

await app.RunAsync();

static (string Host, int Port) ParseAddress(string addr)
{
    var i = addr.LastIndexOf(':');
    var host = i > 0 ? addr[..i] : string.Empty;
    return (host, int.Parse(addr[(i + 1)..], CultureInfo.InvariantCulture));
}

public partial class Program;
