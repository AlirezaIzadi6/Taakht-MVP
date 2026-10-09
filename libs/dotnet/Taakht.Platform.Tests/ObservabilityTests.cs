using System.Text.Json;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;
using Npgsql;
using Taakht.Common.V1;

namespace Taakht.Platform.Tests;

public class ObservabilityTests
{
    private static readonly string? _dbUrl = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");

    [Fact]
    public async Task Request_id_survives_a_server_to_client_hop()
    {
        var server = new GrpcObservabilityInterceptor(NullLogger<GrpcObservabilityInterceptor>.Instance);
        var headers = new Metadata { { RequestContext.Header, "req-123" }, { IdentityConstants.UserIdHeader, "user-1" } };
        Metadata? sentOnward = null;

        await server.UnaryServerHandler<string, string>("in", new FakeContext(headers), (_, _) =>
        {
            // Inside the handler the id is ambient, and an outgoing call forwards it.
            Assert.Equal("req-123", RequestContext.Current);
            new ClientRequestIdInterceptor().AsyncUnaryCall(
                "x",
                new ClientInterceptorContext<string, string>(new Method<string, string>(MethodType.Unary, "s", "m", Marshallers.Create(_ => [], _ => ""), Marshallers.Create(_ => [], _ => "")), null, new CallOptions()),
                (_, ctx) =>
                {
                    sentOnward = ctx.Options.Headers;
                    return new AsyncUnaryCall<string>(Task.FromResult("ok"), Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => [], () => { });
                });
            return Task.FromResult("done");
        });

        Assert.Equal("req-123", sentOnward?.GetValue(RequestContext.Header));
        Assert.Null(RequestContext.Current); // restored after the call
    }

    [Theory]
    [InlineData(null)]
    [InlineData("has space")]
    public async Task Request_id_is_generated_when_missing_or_unusable(string? incoming)
    {
        var headers = new Metadata();
        if (incoming is not null)
        {
            headers.Add(RequestContext.Header, incoming);
        }

        string? seen = null;
        await new GrpcObservabilityInterceptor(NullLogger<GrpcObservabilityInterceptor>.Instance)
            .UnaryServerHandler<string, string>("in", new FakeContext(headers), (_, _) =>
            {
                seen = RequestContext.Current;
                return Task.FromResult("done");
            });

        Assert.True(RequestContext.IsValid(seen));
        Assert.NotEqual(incoming, seen);
    }

    [Fact]
    public void Outbox_envelope_carries_the_ambient_request_id()
    {
        using (RequestContext.Use("req-abc"))
        {
            Assert.Equal("req-abc", Outbox.Wrap("agg", new StringValue { Value = "x" }).RequestId);
        }

        Assert.Equal(string.Empty, Outbox.Wrap("agg", new StringValue { Value = "x" }).RequestId);
    }

    [Fact]
    public async Task Consumer_restores_the_request_id_of_the_envelope_for_the_handler()
    {
        if (string.IsNullOrEmpty(_dbUrl))
        {
            return;
        }

        var dbName = "platform_rid_" + Guid.NewGuid().ToString("N")[..10];
        var adminCs = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString(_dbUrl));
        await using var admin = NpgsqlDataSource.Create(adminCs.ConnectionString);
        await using (var create = admin.CreateCommand($"CREATE DATABASE {dbName}"))
        {
            await create.ExecuteNonQueryAsync();
        }

        adminCs.Database = dbName;
        adminCs.Pooling = false;
        try
        {
            await using var db = NpgsqlDataSource.Create(adminCs.ConnectionString);
            await PlatformSchema.EnsureTablesAsync(db);
            string? inHandler = null;
            var handlers = new Dictionary<string, Func<NpgsqlConnection, NpgsqlTransaction, Envelope, Task>>
            {
                ["google.protobuf.StringValue"] = (_, _, _) =>
                {
                    inHandler = RequestContext.Current;
                    return Task.CompletedTask;
                },
            };
            var consumer = new EventConsumer(db, new KafkaOptions("localhost:1"), "grp", ["t"], handlers, NullLogger<EventConsumer>.Instance);

            Envelope env;
            using (RequestContext.Use("req-from-the-api"))
            {
                env = Outbox.Wrap("agg", new StringValue { Value = "x" });
            }

            Assert.Null(RequestContext.Current);
            await consumer.ProcessAsync(Google.Protobuf.MessageExtensions.ToByteArray(env));

            Assert.Equal("req-from-the-api", inHandler);
            Assert.Null(RequestContext.Current);
        }
        finally
        {
            await using var drop = admin.CreateCommand($"DROP DATABASE IF EXISTS {dbName} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public void Consumer_check_needs_partitions_and_a_recent_poll()
    {
        var now = DateTime.UtcNow;
        var maxStale = TimeSpan.FromSeconds(60);
        var state = new ConsumerHealthState();

        Assert.Equal(HealthStatus.Unhealthy, ConsumerHealthCheck.Evaluate("g", state, now, maxStale).Status); // never joined

        state.Polled(3, now);
        Assert.Equal(HealthStatus.Healthy, ConsumerHealthCheck.Evaluate("g", state, now.AddSeconds(5), maxStale).Status);

        Assert.Equal(HealthStatus.Unhealthy, ConsumerHealthCheck.Evaluate("g", state, now.AddSeconds(120), maxStale).Status); // stuck

        state.Polled(0, now.AddSeconds(121));
        Assert.Equal(HealthStatus.Unhealthy, ConsumerHealthCheck.Evaluate("g", state, now.AddSeconds(121), maxStale).Status); // lost the group
    }

    [Fact]
    public void Outbox_check_fails_when_the_relay_is_stuck()
    {
        var limit = TimeSpan.FromSeconds(60);
        Assert.Equal(HealthStatus.Healthy, OutboxHealthCheck.Evaluate(TimeSpan.Zero, limit).Status);
        Assert.Equal(HealthStatus.Healthy, OutboxHealthCheck.Evaluate(TimeSpan.FromSeconds(5), limit).Status);
        var stuck = OutboxHealthCheck.Evaluate(TimeSpan.FromMinutes(3), limit);
        Assert.Equal(HealthStatus.Unhealthy, stuck.Status);
        Assert.Contains("oldest unpublished", stuck.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Database_check_is_unhealthy_when_postgres_is_down()
    {
        await using var db = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=x;Password=y;Timeout=1;Pooling=false");
        var result = await new DatabaseHealthCheck(db).CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task Outbox_check_against_postgres_sees_an_old_unpublished_row()
    {
        if (string.IsNullOrEmpty(_dbUrl))
        {
            return;
        }

        var dbName = "platform_ob_" + Guid.NewGuid().ToString("N")[..10];
        var adminCs = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString(_dbUrl));
        await using var admin = NpgsqlDataSource.Create(adminCs.ConnectionString);
        await using (var create = admin.CreateCommand($"CREATE DATABASE {dbName}"))
        {
            await create.ExecuteNonQueryAsync();
        }

        adminCs.Database = dbName;
        adminCs.Pooling = false;
        try
        {
            await using var db = NpgsqlDataSource.Create(adminCs.ConnectionString);
            await PlatformSchema.EnsureTablesAsync(db);
            var check = new OutboxHealthCheck(db, TimeSpan.FromSeconds(60));
            Assert.Equal(HealthStatus.Healthy, (await check.CheckHealthAsync(new HealthCheckContext())).Status);

            await using (var insert = db.CreateCommand(
                "INSERT INTO outbox (id, topic, key, envelope, created_at) VALUES (gen_random_uuid(), 't', 'k', '\\x'::bytea, now() - interval '5 minutes')"))
            {
                await insert.ExecuteNonQueryAsync();
            }

            Assert.Equal(HealthStatus.Unhealthy, (await check.CheckHealthAsync(new HealthCheckContext())).Status);
            var (count, oldest) = await OutboxBacklog.ReadAsync(db);
            Assert.Equal(1, count);
            Assert.True(oldest > TimeSpan.FromMinutes(4));
        }
        finally
        {
            await using var drop = admin.CreateCommand($"DROP DATABASE IF EXISTS {dbName} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public void Json_formatter_writes_one_object_with_the_shared_fields()
    {
        var formatter = new JsonConsoleFormatter("swap");
        var scopes = new LoggerExternalScopeProvider();
        using var scope = scopes.Push(new RequestLogScope("rid-1", "user-1"));
        var writer = new StringWriter();
        var state = new List<KeyValuePair<string, object?>>
        {
            new("Method", "/t.Svc/M"),
            new("DurationMs", 3.5),
            new("{OriginalFormat}", "grpc call {Method} {DurationMs}"),
        };

        formatter.Write(new LogEntry<List<KeyValuePair<string, object?>>>(LogLevel.Warning, "cat", default, state, null, (_, _) => "grpc call /t.Svc/M 3.5"), scopes, writer);

        var line = writer.ToString().TrimEnd();
        Assert.DoesNotContain('\n', line);
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;
        Assert.Equal("warn", root.GetProperty("level").GetString());
        Assert.Equal("swap", root.GetProperty("service").GetString());
        Assert.Equal("grpc call /t.Svc/M 3.5", root.GetProperty("msg").GetString());
        Assert.Equal("rid-1", root.GetProperty("request_id").GetString());
        Assert.Equal("user-1", root.GetProperty("user_id").GetString());
        Assert.Equal("/t.Svc/M", root.GetProperty("method").GetString());
        Assert.Equal(3.5, root.GetProperty("duration_ms").GetDouble());
        Assert.True(DateTime.TryParse(root.GetProperty("ts").GetString(), out _));
        Assert.False(root.TryGetProperty("{OriginalFormat}", out _));
    }

    [Theory]
    [InlineData("127.0.0.1:9103", "127.0.0.1", 9103)]
    [InlineData("localhost:9104", "127.0.0.1", 9104)]
    [InlineData(":9105", "0.0.0.0", 9105)]
    public void Health_address_parses(string input, string ip, int port)
    {
        var o = ObservabilityExtensions.ParseHealthAddress("s", input);
        Assert.Equal((ip, port), (o.Address.ToString(), o.Port));
    }

    [Theory]
    [InlineData("9103")]
    [InlineData("host.example:9103")]
    [InlineData("127.0.0.1:0")]
    public void Health_address_rejects_garbage(string input) =>
        Assert.Throws<InvalidOperationException>(() => ObservabilityExtensions.ParseHealthAddress("s", input));

    private sealed class FakeContext(Metadata headers) : ServerCallContext
    {
        protected override string MethodCore => "/t.Svc/Method";
        protected override string HostCore => "h";
        protected override string PeerCore => "p";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => headers;
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore => [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(null, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
