using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Taakht.Common.V1;
using Taakht.Negotiation.V1;
using Taakht.Swap.Application;
using Taakht.Swap.Domain;
using Taakht.Swap.Infrastructure;
using SwapPb = Taakht.Swap.V1;

namespace Taakht.Swap.Tests;

public class SwapGrpcServiceTests(TestDatabase db) : IClassFixture<TestDatabase>
{
    private static AgreementReached Agreement(string negotiationId) => new()
    {
        NegotiationId = negotiationId,
        AdA = new AgreedAd { AdId = $"ad-a-{negotiationId}", OwnerId = "user-1", Version = 1 },
        AdB = new AgreedAd { AdId = $"ad-b-{negotiationId}", OwnerId = "user-2", Version = 1 },
        Terms = new Terms { LegA = Taakht.Negotiation.V1.DeliveryMethod.Locker, LegB = Taakht.Negotiation.V1.DeliveryMethod.Locker },
    };

    private async Task<(SwapGrpcService Service, Guid SwapId)> BuildAsync(bool devEndpoints, string negotiationId)
    {
        var store = new SwapStore(db.DataSource);
        var workflow = new SwapWorkflow(
            store, new FakeAdClient(), new MockDeliveryProvider(store), new SwapOptions(TimeSpan.FromMinutes(2)),
            TimeProvider.System, NullLogger<SwapWorkflow>.Instance);
        await workflow.HandleAgreementReachedAsync(Agreement(negotiationId), CancellationToken.None);
        await using var conn = await db.DataSource.OpenConnectionAsync();
        await using var cmd = new Npgsql.NpgsqlCommand("SELECT id FROM swap WHERE negotiation_id = @n", conn);
        cmd.Parameters.AddWithValue("n", negotiationId);
        var id = (Guid)(await cmd.ExecuteScalarAsync())!;
        return (new SwapGrpcService(store, workflow, new SwapApiOptions(devEndpoints)), id);
    }

    private static SwapPb.SimulateLockerFeePaidRequest Request(Guid swapId, string userId) => new() { SwapId = swapId.ToString(), UserId = userId };

    [DbFact]
    public async Task Simulate_fee_paid_is_unimplemented_unless_dev_endpoints_are_enabled()
    {
        var (service, swapId) = await BuildAsync(false, "neg-grpc-off");

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.SimulateLockerFeePaid(Request(swapId, "user-1"), new FakeContext("user-1")));
        Assert.Equal(StatusCode.Unimplemented, ex.StatusCode);
    }

    [DbFact]
    public async Task Simulate_fee_paid_requires_caller_to_pay_their_own_leg()
    {
        var (service, swapId) = await BuildAsync(true, "neg-grpc-on");

        var other = await Assert.ThrowsAsync<RpcException>(() => service.SimulateLockerFeePaid(Request(swapId, "user-2"), new FakeContext("user-1")));
        Assert.Equal(StatusCode.PermissionDenied, other.StatusCode);

        var stranger = await Assert.ThrowsAsync<RpcException>(() => service.SimulateLockerFeePaid(Request(swapId, "user-9"), new FakeContext("user-9")));
        Assert.Equal(StatusCode.PermissionDenied, stranger.StatusCode);

        var paid = await service.SimulateLockerFeePaid(Request(swapId, "user-1"), new FakeContext("user-1"));
        Assert.True(paid.LegA.FeePaid);
        Assert.False(paid.LegB.FeePaid);
    }

    [Theory]
    [InlineData(StatusCode.FailedPrecondition, true)]
    [InlineData(StatusCode.NotFound, true)]
    [InlineData(StatusCode.InvalidArgument, true)]
    [InlineData(StatusCode.PermissionDenied, false)]
    [InlineData(StatusCode.Unauthenticated, false)]
    [InlineData(StatusCode.Unavailable, false)]
    [InlineData(StatusCode.DeadlineExceeded, false)]
    [InlineData(StatusCode.Internal, false)]
    public void Lock_failures_are_permanent_only_when_retrying_cannot_help(StatusCode code, bool permanent) =>
        Assert.Equal(permanent, GrpcAdClient.IsPermanentRejection(code));

    private sealed class FakeContext(string user) : ServerCallContext
    {
        private readonly Metadata _headers = new() { { "x-user-id", user } };

        protected override string MethodCore => "m";
        protected override string HostCore => "h";
        protected override string PeerCore => "p";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => _headers;
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
