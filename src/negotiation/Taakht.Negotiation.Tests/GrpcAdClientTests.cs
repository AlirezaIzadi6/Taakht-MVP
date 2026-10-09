using Grpc.Core;
using Taakht.Ad.V1;
using Taakht.Negotiation.Infrastructure;
using Taakht.Platform;

namespace Taakht.Negotiation.Tests;

public class GrpcAdClientTests
{
    [Fact]
    public async Task GetAd_is_sent_as_the_negotiation_system_identity_even_inside_a_user_scope()
    {
        var invoker = new CapturingInvoker();
        var client = new GrpcAdClient(new AdService.AdServiceClient(invoker));

        using (UserContext.Use("user-1"))
        {
            var ad = await client.GetAdAsync("ad-1", CancellationToken.None);
            Assert.Equal("ad-1", ad?.AdId);
        }

        Assert.Equal(SystemIdentities.Negotiation, invoker.UserHeader);
    }

    [Fact]
    public async Task GetAdDetails_returns_the_full_ad_as_the_negotiation_system_identity()
    {
        var invoker = new CapturingInvoker();
        var client = new GrpcAdClient(new AdService.AdServiceClient(invoker));

        var ad = await client.GetAdDetailsAsync("ad-1", CancellationToken.None);

        Assert.Equal("ad-1", ad?.Id);
        Assert.Equal(SystemIdentities.Negotiation, invoker.UserHeader);
    }

    private sealed class CapturingInvoker : CallInvoker
    {
        public string? UserHeader { get; private set; }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            UserHeader = options.Headers?.GetValue(IdentityConstants.UserIdHeader);
            var reply = (TResponse)(object)new Taakht.Ad.V1.Ad { Id = "ad-1", OwnerId = "user-2", Version = 1, Status = AdStatus.Published };
            return new AsyncUnaryCall<TResponse>(
                Task.FromResult(reply), Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => [], () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();
    }
}
