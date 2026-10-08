# Taakht.Platform (.NET)

Shared helpers for the .NET services (namespace `Taakht.Platform`). Solution: `Taakht.Platform.slnx`.

## Wiring a service

```csharp
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddTaakhtPlatform(builder.Configuration);   // NpgsqlDataSource (DATABASE_URL), KafkaOptions (KAFKA_BROKERS), interceptors
builder.Services.AddGrpc(o => o.Interceptors.Add<ServerIdentityInterceptor>());
builder.Services.AddGrpcClient<Ad.AdClient>(o => o.Address = new Uri(builder.Configuration["AD_ADDR"] ?? "http://localhost:9001"))
    .AddInterceptor<ClientIdentityInterceptor>();             // forwards x-user-id
builder.Services.AddTaakhtOutboxRelay();
builder.Services.AddTaakhtEventConsumer("swap", ["negotiation.events"],
    new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>
    {
        ["taakht.negotiation.v1.AgreementReached"] = async (conn, tx, env) => { /* use conn + tx */ },
    });

var app = builder.Build();
await app.Services.MigrateAsync(typeof(Program).Assembly);   // embedded migrations/*.sql
```

- Identity: `CurrentUser.Id(context)` in handlers (UNAUTHENTICATED when `x-user-id` is missing). The server interceptor also
  sets the ambient `UserContext.Current`, which `ClientIdentityInterceptor` forwards. Background jobs: `using (UserContext.Use("system")) { ... }`.
- Events: `await Outbox.AddAsync(conn, tx, "swap.events", swapId, new SwapCompleted {...}, ct)` in the same transaction as the business change.
  `Outbox.Wrap` builds the envelope only.
- Tables: ship the `outbox` / `processed_events` DDL from the conventions doc in your own `migrations/001_init.sql`
  (`PlatformSchema.OutboxDdl` / `ProcessedEventsDdl` hold the same text; `PlatformSchema.EnsureTablesAsync` is for tests).
- Migrations: put `migrations/NNN_name.sql` in the service project, embed with `<EmbeddedResource Include="migrations/*.sql" />`.
  Applied in file-name order; version = file name without `.sql`; guarded by an advisory lock, each file in a transaction.
- `DatabaseUrl.ToConnectionString(url)` / `CreateDataSource(url)` convert `postgres://` URLs; `ConnectionStrings:Default` is the fallback.

## Compiling protos in a service

Taakht.Platform already compiles `taakht/common/v1/envelope.proto` (type `Taakht.Common.V1.Envelope`). A service compiles
its own contracts and the ones it calls, referenced by path relative to `api/proto`:

```xml
<ItemGroup>
  <PackageReference Include="Grpc.AspNetCore" />          <!-- add the version to Directory.Packages.props -->
  <PackageReference Include="Grpc.Net.ClientFactory" />
  <PackageReference Include="Grpc.Tools" PrivateAssets="All" />
  <ProjectReference Include="../../../libs/dotnet/Taakht.Platform/Taakht.Platform.csproj" />
  <!-- own service: server stubs; events: messages only; called service: client stubs -->
  <Protobuf Include="../../../api/proto/taakht/swap/v1/swap.proto" ProtoRoot="../../../api/proto" GrpcServices="Server" />
  <Protobuf Include="../../../api/proto/taakht/swap/v1/events.proto" ProtoRoot="../../../api/proto" GrpcServices="None" />
  <Protobuf Include="../../../api/proto/taakht/ad/v1/ad.proto" ProtoRoot="../../../api/proto" GrpcServices="Client" />
</ItemGroup>
```

Adjust the relative depth to the csproj location (`src/<svc>/<Project>/` is three levels below the repo root).
Protos that import `taakht/common/v1/envelope.proto` resolve it through `ProtoRoot`; the generated types may exist in both
assemblies, so avoid compiling `envelope.proto` in the service itself and use the one from Taakht.Platform.
