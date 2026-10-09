using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Taakht.Platform.Tests;

public class OverloadInterceptorTests
{
    private static PostgresException Pg(string sqlState) => new("boom", "ERROR", "ERROR", sqlState);

    [Theory]
    [InlineData("53300", true)]
    [InlineData("57P03", true)]
    [InlineData("57P01", true)]
    [InlineData("08006", true)]
    [InlineData("08001", true)]
    [InlineData("23505", false)]
    [InlineData("40001", false)]
    [InlineData("42P01", false)]
    public void Classifies_postgres_sql_states(string sqlState, bool overload) =>
        Assert.Equal(overload, OverloadInterceptor.IsOverload(Pg(sqlState)));

    [Fact]
    public void Classifies_other_exceptions()
    {
        Assert.True(OverloadInterceptor.IsOverload(new TimeoutException("pool exhausted")));
        Assert.True(OverloadInterceptor.IsOverload(new NpgsqlException("connection broken", new IOException("reset"))));
        Assert.True(OverloadInterceptor.IsOverload(new InvalidOperationException("wrapper", Pg("53300"))));
        Assert.True(OverloadInterceptor.IsOverload(new AggregateException(new TimeoutException(), Pg("57P03"))));
        Assert.False(OverloadInterceptor.IsOverload(new InvalidOperationException("bug")));
        Assert.False(OverloadInterceptor.IsOverload(new OperationCanceledException()));
        Assert.False(OverloadInterceptor.IsOverload(new RpcException(new Status(StatusCode.NotFound, "x"))));
        Assert.False(OverloadInterceptor.IsOverload(new InvalidOperationException("wrapper", new RpcException(new Status(StatusCode.Internal, "x")))));
    }

    [Theory]
    [MemberData(nameof(OverloadExceptions))]
    public async Task Maps_overload_to_unavailable(Exception thrown)
    {
        var ex = await Assert.ThrowsAsync<RpcException>(() => Invoke(thrown));
        Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        Assert.Equal(OverloadInterceptor.Message, ex.Status.Detail);
    }

    public static TheoryData<Exception> OverloadExceptions() =>
    [
        Pg("53300"),
        Pg("57P03"),
        Pg("08006"),
        new TimeoutException("The connection pool has been exhausted"),
        new NpgsqlException("Exception while connecting", new TimeoutException()),
    ];

    [Fact]
    public async Task Leaves_other_errors_alone()
    {
        var mapped = new RpcException(new Status(StatusCode.NotFound, "no such thing"));
        Assert.Same(mapped, await Assert.ThrowsAsync<RpcException>(() => Invoke(mapped)));

        var unique = Pg("23505");
        Assert.Same(unique, await Assert.ThrowsAsync<PostgresException>(() => Invoke(unique)));

        var bug = new InvalidOperationException("bug");
        Assert.Same(bug, await Assert.ThrowsAsync<InvalidOperationException>(() => Invoke(bug)));

        await Assert.ThrowsAsync<OperationCanceledException>(() => Invoke(new OperationCanceledException()));
    }

    [Fact]
    public async Task Passes_successful_responses_through()
    {
        var interceptor = new OverloadInterceptor(NullLogger<OverloadInterceptor>.Instance);
        var result = await interceptor.UnaryServerHandler<string, string>("in", new FakeContext(), (r, _) => Task.FromResult(r + "!"));
        Assert.Equal("in!", result);
    }

    private static Task<string> Invoke(Exception thrown) =>
        new OverloadInterceptor(NullLogger<OverloadInterceptor>.Instance)
            .UnaryServerHandler<string, string>("in", new FakeContext(), (_, _) => throw thrown);

    private static DatabasePoolSettings Lookup(Dictionary<string, string?> values) =>
        DatabasePoolSettings.FromLookup(k => values.GetValueOrDefault(k));

    [Fact]
    public void Pool_settings_default_and_override()
    {
        var def = DatabasePoolSettings.FromLookup(_ => null);
        Assert.Equal((20, 5, TimeSpan.FromSeconds(10)), (def.MaxConnections, def.MinConnections, def.AcquireTimeout));

        var custom = Lookup(new Dictionary<string, string?>
        {
            ["DB_MAX_CONNS"] = "7",
            ["DB_MIN_CONNS"] = "1",
            ["DB_ACQUIRE_TIMEOUT"] = "3s",
        });
        Assert.Equal((7, 1, TimeSpan.FromSeconds(3)), (custom.MaxConnections, custom.MinConnections, custom.AcquireTimeout));

        // A tiny max without an explicit min lowers the default min instead of failing.
        var tiny = Lookup(new Dictionary<string, string?>
        {
            ["DB_MAX_CONNS"] = "1",
        });
        Assert.Equal((1, 1), (tiny.MaxConnections, tiny.MinConnections));
    }

    [Theory]
    [InlineData("DB_MAX_CONNS", "0")]
    [InlineData("DB_MAX_CONNS", "lots")]
    [InlineData("DB_MIN_CONNS", "-1")]
    [InlineData("DB_ACQUIRE_TIMEOUT", "0s")]
    [InlineData("DB_ACQUIRE_TIMEOUT", "soon")]
    public void Pool_settings_reject_invalid_values(string key, string value)
    {
        Assert.ThrowsAny<Exception>(() => Lookup(new Dictionary<string, string?> { [key] = value }));
    }

    [Fact]
    public void Pool_settings_reject_min_above_max()
    {
        Assert.Throws<FormatException>(() => Lookup(new Dictionary<string, string?>
        {
            ["DB_MAX_CONNS"] = "2",
            ["DB_MIN_CONNS"] = "5",
        }));
    }

    [Fact]
    public void Connection_string_gets_pool_bounds_unless_it_sets_them()
    {
        var pool = new DatabasePoolSettings { MaxConnections = 9, MinConnections = 3, AcquireTimeout = TimeSpan.FromSeconds(4) };

        var fromUrl = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString("postgres://u:p@h/d", pool));
        Assert.Equal((9, 3, 4), (fromUrl.MaxPoolSize, fromUrl.MinPoolSize, fromUrl.Timeout));

        var explicitKv = new NpgsqlConnectionStringBuilder(
            DatabaseUrl.ToConnectionString("Host=h;Database=d;Maximum Pool Size=50;Timeout=30", pool));
        Assert.Equal((50, 3, 30), (explicitKv.MaxPoolSize, explicitKv.MinPoolSize, explicitKv.Timeout));

        var defaults = new NpgsqlConnectionStringBuilder(DatabaseUrl.ToConnectionString("postgres://u:p@h/d", DatabasePoolSettings.Default));
        Assert.Equal((20, 5, 10), (defaults.MaxPoolSize, defaults.MinPoolSize, defaults.Timeout));
    }

    /// <summary>
    /// Runs against the compose Postgres (TEST_DATABASE_URL, returns without it): a pool of 2 connections and callers that
    /// hold a connection for 2 s with a 1 s wait limit. The callers that cannot get a connection must fail with
    /// UNAVAILABLE (never UNKNOWN) and the pool must serve requests normally afterwards.
    /// </summary>
    [Fact]
    public async Task Exhausted_pool_answers_unavailable_and_recovers()
    {
        var url = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");
        if (string.IsNullOrWhiteSpace(url))
        {
            return;
        }

        await using var dataSource = DatabaseUrl.CreateDataSource(url, new DatabasePoolSettings
        {
            MaxConnections = 2,
            MinConnections = 0,
            AcquireTimeout = TimeSpan.FromSeconds(1),
        });
        var interceptor = new OverloadInterceptor(NullLogger<OverloadInterceptor>.Instance);

        Task<string> Call(string sleepSeconds) => interceptor.UnaryServerHandler<string, string>("q", new FakeContext(), async (_, _) =>
        {
            await using var cmd = dataSource.CreateCommand($"SELECT pg_sleep({sleepSeconds})");
            await cmd.ExecuteNonQueryAsync();
            return "ok";
        });

        var outcomes = await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
        {
            try
            {
                return (await Call("2"), StatusCode.OK);
            }
            catch (RpcException ex)
            {
                return (string.Empty, ex.StatusCode);
            }
        }));

        Assert.Equal(2, outcomes.Count(o => o.Item2 == StatusCode.OK));
        Assert.Equal(6, outcomes.Count(o => o.Item2 == StatusCode.Unavailable));
        Assert.DoesNotContain(outcomes, o => o.Item2 is StatusCode.Unknown or StatusCode.Internal);

        Assert.Equal("ok", await Call("0"));
    }

    private sealed class FakeContext : ServerCallContext
    {
        protected override string MethodCore => "/t.Svc/Method";
        protected override string HostCore => "h";
        protected override string PeerCore => "p";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => [];
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
