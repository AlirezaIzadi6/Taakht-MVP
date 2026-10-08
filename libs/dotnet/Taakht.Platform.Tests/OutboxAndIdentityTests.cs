using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Taakht.Common.V1;

namespace Taakht.Platform.Tests;

public class OutboxAndIdentityTests
{
    [Fact]
    public void Wrap_sets_envelope_fields_from_message()
    {
        var id = Guid.NewGuid();
        var env = Outbox.Wrap("agg-1", new StringValue { Value = "x" }, id);

        Assert.Equal(id.ToString(), env.EventId);
        Assert.Equal("google.protobuf.StringValue", env.Type);
        Assert.Equal("agg-1", env.AggregateId);
        Assert.Equal("x", StringValue.Parser.ParseFrom(env.Payload).Value);
        Assert.NotNull(env.OccurredAt);
        Assert.Equal(env, Envelope.Parser.ParseFrom(env.ToByteArray()));
    }

    [Fact]
    public void UserContext_scope_restores_previous_value()
    {
        Assert.Null(UserContext.Current);
        using (UserContext.Use("user-1"))
        {
            Assert.Equal("user-1", UserContext.Current);
            using (UserContext.Use("user-2"))
            {
                Assert.Equal("user-2", UserContext.Current);
            }

            Assert.Equal("user-1", UserContext.Current);
        }

        Assert.Null(UserContext.Current);
    }

    [Fact]
    public void Migrator_orders_by_version_and_strips_extension()
    {
        Assert.Equal("001_init", Migrator.VersionOf("Svc.migrations.001_init.sql"));
    }

    [Fact]
    public void Status_code_for_missing_header_is_unauthenticated()
    {
        var ex = Assert.Throws<RpcException>(() => CurrentUser.Id(new FakeContext(new Metadata())));
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
        Assert.Equal("user-3", CurrentUser.Id(new FakeContext(new Metadata { { "x-user-id", "user-3" } })));
    }

    private sealed class FakeContext(Metadata headers) : ServerCallContext
    {
        protected override string MethodCore => "m";
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
