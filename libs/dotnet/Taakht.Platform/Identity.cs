using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Taakht.Platform;

public static class IdentityConstants
{
    public const string UserIdHeader = "x-user-id";

    /// <summary>Longest accepted caller id.</summary>
    public const int MaxUserIdLength = 64;
}

/// <summary>Service identities used for service-to-service calls (never real users).</summary>
public static class SystemIdentities
{
    public const string Prefix = "system:";
    public const string Swap = "system:swap";
    public const string Negotiation = "system:negotiation";

    public static bool IsSystem(string? userId) => userId is not null && userId.StartsWith(Prefix, StringComparison.Ordinal);
}

/// <summary>Ambient user id for the current async flow (set by the server interceptor or by background jobs).</summary>
public static class UserContext
{
    private static readonly AsyncLocal<string?> _ambient = new();

    public static string? Current => _ambient.Value;

    /// <summary>Sets the ambient user id until the returned scope is disposed. Use it in jobs that call other services.</summary>
    public static IDisposable Use(string userId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(userId);
        var previous = _ambient.Value;
        _ambient.Value = userId;
        return new Scope(previous);
    }

    private sealed class Scope(string? previous) : IDisposable
    {
        public void Dispose() => _ambient.Value = previous;
    }
}

public static class CurrentUser
{
    /// <summary>The caller's id; throws UNAUTHENTICATED when no x-user-id header was sent.</summary>
    public static string Id(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var id = context.RequestHeaders.GetValue(IdentityConstants.UserIdHeader);
        if (string.IsNullOrWhiteSpace(id))
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "missing x-user-id"));
        }

        return IsValid(id) ? id : throw new RpcException(new Status(StatusCode.Unauthenticated, "invalid x-user-id"));
    }

    /// <summary>A usable caller id has at most 64 characters and no control characters.</summary>
    public static bool IsValid(string id) =>
        id.Length is > 0 and <= IdentityConstants.MaxUserIdLength && !id.Any(char.IsControl);
}

/// <summary>Requires x-user-id on every call and exposes it via <see cref="UserContext"/> for the call's duration.</summary>
public sealed class ServerIdentityInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        using var scope = UserContext.Use(CurrentUser.Id(context));
        return await continuation(request, context);
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        using var scope = UserContext.Use(CurrentUser.Id(context));
        await continuation(request, responseStream, context);
    }
}

/// <summary>Forwards the ambient user id as x-user-id on outgoing unary calls.</summary>
public sealed class ClientIdentityInterceptor : Interceptor
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithUser(context));

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, WithUser(context));

    private static ClientInterceptorContext<TRequest, TResponse> WithUser<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var id = UserContext.Current;
        if (string.IsNullOrEmpty(id))
        {
            return context;
        }

        var headers = context.Options.Headers ?? [];
        if (headers.GetValue(IdentityConstants.UserIdHeader) is null)
        {
            headers.Add(IdentityConstants.UserIdHeader, id);
        }

        return new ClientInterceptorContext<TRequest, TResponse>(
            context.Method, context.Host, context.Options.WithHeaders(headers));
    }
}
