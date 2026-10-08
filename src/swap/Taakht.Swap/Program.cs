using Grpc.Net.ClientFactory;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Npgsql;
using Taakht.Ad.V1;
using Taakht.Negotiation.V1;
using Taakht.Platform;
using Taakht.Swap.Application;
using Taakht.Swap.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

const string DefaultDatabaseUrl = "postgres://taakht:taakht@localhost:5432/swap?sslmode=disable";
if (config["DATABASE_URL"] is null && config.GetConnectionString("Default") is null)
{
    config["DATABASE_URL"] = DefaultDatabaseUrl;
}

var grpcAddr = config["GRPC_ADDR"] ?? ":9004";
var grpcPort = int.Parse(grpcAddr[(grpcAddr.LastIndexOf(':') + 1)..], System.Globalization.CultureInfo.InvariantCulture);
builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(grpcPort, l => l.Protocols = HttpProtocols.Http2));

var adAddr = config["AD_ADDR"] ?? "localhost:9001";
var paymentDeadline = DurationParser.Parse(config["PAYMENT_DEADLINE"] ?? "1h");

builder.Services.AddTaakhtPlatform(config);
builder.Services.AddTaakhtOutboxRelay();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton(new SwapOptions(paymentDeadline));
builder.Services.AddSingleton<SwapStore>();
builder.Services.AddSingleton<IDeliveryProvider, MockDeliveryProvider>();
builder.Services.AddSingleton<SwapWorkflow>();
builder.Services.AddSingleton<IAdClient, GrpcAdClient>();
builder.Services
    .AddGrpcClient<AdService.AdServiceClient>(o => o.Address = new Uri(adAddr.Contains("://", StringComparison.Ordinal) ? adAddr : $"http://{adAddr}"))
    .AddInterceptor<ClientIdentityInterceptor>(InterceptorScope.Client);
builder.Services.AddHostedService<PaymentDeadlineSweeper>();
builder.Services.AddHostedService(sp => new EventConsumer(
    sp.GetRequiredService<NpgsqlDataSource>(),
    sp.GetRequiredService<KafkaOptions>(),
    "swap",
    ["negotiation.events"],
    new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Taakht.Common.V1.Envelope, Task>>
    {
        [AgreementReached.Descriptor.FullName] = (_, _, env) =>
            sp.GetRequiredService<SwapWorkflow>().HandleAgreementReachedAsync(AgreementReached.Parser.ParseFrom(env.Payload), CancellationToken.None),
    },
    sp.GetRequiredService<ILogger<EventConsumer>>()));

builder.Services.AddGrpc(o => o.Interceptors.Add<ServerIdentityInterceptor>());
if (builder.Environment.IsDevelopment())
{
    builder.Services.AddGrpcReflection();
}

var app = builder.Build();
await app.Services.MigrateAsync(typeof(Program).Assembly);

app.MapGrpcService<SwapGrpcService>();
if (app.Environment.IsDevelopment())
{
    app.MapGrpcReflectionService();
}

await app.RunAsync();

public partial class Program;
