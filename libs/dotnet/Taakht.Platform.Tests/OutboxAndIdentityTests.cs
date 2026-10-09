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

    [Fact]
    public void System_identities_are_an_exact_allowlist()
    {
        Assert.True(SystemIdentities.IsSystem(SystemIdentities.Negotiation));
        Assert.True(SystemIdentities.IsSystem(SystemIdentities.Swap));
        Assert.False(SystemIdentities.IsSystem("system:other"));
        Assert.False(SystemIdentities.IsSystem("SYSTEM:swap"));
        Assert.False(SystemIdentities.IsSystem("user-1"));
        Assert.False(SystemIdentities.IsSystem(null));
        Assert.True(SystemIdentities.HasReservedPrefix("System:x"));
    }

    [Theory]
    [InlineData("", null, false)]
    [InlineData(" ", null, false)]
    [InlineData("user 1", null, false)]
    [InlineData(" user-1", null, false)]
    [InlineData("user-1 ", null, false)]
    [InlineData("user\t1", null, false)]
    [InlineData("user\0", null, false)]
    [InlineData("user-1", null, true)]
    [InlineData("system:swap", "valid", true)]
    [InlineData("system:swap", null, false)]
    [InlineData("system:swap", "wrong", false)]
    [InlineData("system:other", "valid", false)]
    [InlineData("SYSTEM:swap", "valid", false)]
    [InlineData("System:x", null, false)]
    public void Id_validation_table(string id, string? token, bool accepted)
    {
        var ctx = Ctx(id, token == "valid" ? InternalAuth.Token : token);
        if (accepted)
        {
            Assert.Equal(id, CurrentUser.Id(ctx));
        }
        else
        {
            Assert.Equal(StatusCode.Unauthenticated, Assert.Throws<RpcException>(() => CurrentUser.Id(ctx)).StatusCode);
        }
    }

    [Fact]
    public void IsValid_rejects_non_ascii_and_whitespace()
    {
        Assert.False(CurrentUser.IsValid("\u06a9\u0627\u0631\u0628\u0631"));
        Assert.False(CurrentUser.IsValid("caf\u00e9"));
        Assert.False(CurrentUser.IsValid("a b"));
    }

    [Fact]
    public void Id_length_is_bytes_and_capped_at_64()
    {
        Assert.True(CurrentUser.IsValid(new string('a', 64)));
        Assert.False(CurrentUser.IsValid(new string('a', 65)));
    }

    [Fact]
    public void Duplicate_identity_headers_are_rejected()
    {
        var twoIds = new FakeContext(new Metadata { { "x-user-id", "user-1" }, { "x-user-id", "user-2" } });
        Assert.Equal(StatusCode.Unauthenticated, Assert.Throws<RpcException>(() => CurrentUser.Id(twoIds)).StatusCode);

        var twoTokens = new FakeContext(new Metadata
        {
            { "x-user-id", "system:swap" }, { "x-internal-token", InternalAuth.Token }, { "x-internal-token", InternalAuth.Token },
        });
        Assert.Equal(StatusCode.Unauthenticated, Assert.Throws<RpcException>(() => CurrentUser.Id(twoTokens)).StatusCode);
    }

    private static FakeContext Ctx(string user, string? token = null)
    {
        var md = new Metadata { { IdentityConstants.UserIdHeader, user } };
        if (token is not null)
        {
            md.Add(InternalAuth.Header, token);
        }

        return new FakeContext(md);
    }

    [Fact]
    public void System_id_with_the_valid_token_is_a_system_caller()
    {
        var ctx = Ctx(SystemIdentities.Swap, InternalAuth.Token);
        Assert.Equal(SystemIdentities.Swap, CurrentUser.Id(ctx));
        Assert.True(CurrentUser.IsSystem(ctx));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("wrong-token")]
    public void System_id_without_a_valid_token_is_unauthenticated(string? token)
    {
        var ex = Assert.Throws<RpcException>(() => CurrentUser.Id(Ctx(SystemIdentities.Swap, token)));
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
        Assert.False(CurrentUser.IsSystem(Ctx(SystemIdentities.Swap, token)));
    }

    [Fact]
    public void Normal_user_is_not_system_even_when_it_sends_the_token()
    {
        Assert.Equal("user-1", CurrentUser.Id(Ctx("user-1")));
        Assert.False(CurrentUser.IsSystem(Ctx("user-1")));
        Assert.False(CurrentUser.IsSystem(Ctx("user-1", InternalAuth.Token)));
    }

    [Fact]
    public void Token_comparison_matches_only_the_exact_secret()
    {
        Assert.True(InternalAuth.Matches("s3cret", "s3cret"));
        Assert.False(InternalAuth.Matches("s3cre", "s3cret"));
        Assert.False(InternalAuth.Matches("s3cret!", "s3cret"));
        Assert.False(InternalAuth.Matches(null, "s3cret"));
        Assert.False(InternalAuth.Matches("", ""));
    }

    [Fact]
    public async Task Client_interceptor_attaches_the_token_for_system_ids_only()
    {
        var sys = await CapturedHeaders(SystemIdentities.Swap);
        Assert.Equal(SystemIdentities.Swap, sys.GetValue(IdentityConstants.UserIdHeader));
        Assert.Equal(InternalAuth.Token, sys.GetValue(InternalAuth.Header));

        var user = await CapturedHeaders("user-1");
        Assert.Equal("user-1", user.GetValue(IdentityConstants.UserIdHeader));
        Assert.Null(user.GetValue(InternalAuth.Header));
    }

    [Fact]
    public async Task Client_interceptor_attaches_the_token_for_an_explicit_system_header_without_ambient_user()
    {
        var headers = await CapturedHeaders(ambientUser: null, explicitUser: SystemIdentities.Negotiation);
        Assert.Equal(InternalAuth.Token, headers.GetValue(InternalAuth.Header));
    }

    private static Task<Metadata> CapturedHeaders(string? ambientUser, string? explicitUser = null)
    {
        Metadata? seen = null;
        var method = new Method<string, string>(MethodType.Unary, "s", "m", Marshallers.Create(x => [], b => ""), Marshallers.Create(x => [], b => ""));
        var options = new CallOptions(headers: explicitUser is null ? null : new Metadata { { IdentityConstants.UserIdHeader, explicitUser } });
        using var scope = ambientUser is null ? null : UserContext.Use(ambientUser);
        new ClientIdentityInterceptor().AsyncUnaryCall("x", new Grpc.Core.Interceptors.ClientInterceptorContext<string, string>(method, null, options),
            (_, ctx) =>
            {
                seen = ctx.Options.Headers;
                return new AsyncUnaryCall<string>(Task.FromResult(""), Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => [], () => { });
            });
        return Task.FromResult(seen!);
    }
}
