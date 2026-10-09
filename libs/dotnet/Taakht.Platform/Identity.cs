using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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

    /// <summary>True for exactly the allowlisted service ids (case-sensitive). This is not proof: see <see cref="CurrentUser.IsSystem"/>.</summary>
    public static bool IsSystem(string? userId) => userId is Swap or Negotiation;

    /// <summary>True when the id claims the reserved prefix (any case), whether or not it is an allowlisted service.</summary>
    public static bool HasReservedPrefix(string? userId) => userId is not null && userId.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// The shared internal secret (env INTERNAL_AUTH_TOKEN, default <see cref="DevDefault"/>) that proves a
/// <c>system:*</c> caller on service-to-service calls, carried as metadata <see cref="Header"/>.
/// </summary>
public static class InternalAuth
{
    public const string Header = "x-internal-token";

    /// <summary>DEV ONLY: public in the repo.</summary>
    public const string DevDefault = "dev-internal-token";

    private static readonly Lazy<string> _token = new(() =>
        Environment.GetEnvironmentVariable("INTERNAL_AUTH_TOKEN") is { Length: > 0 } t ? t : DevDefault);

    public static string Token => _token.Value;

    public static bool IsDefault => Token == DevDefault;

    /// <summary>Constant-time comparison (over fixed-length digests) of a presented token with the secret.</summary>
    public static bool Matches(string? presented) => Matches(presented, Token);

    internal static bool Matches(string? presented, string expected)
    {
        if (string.IsNullOrEmpty(presented))
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(presented)),
            SHA256.HashData(Encoding.UTF8.GetBytes(expected)));
    }
}

/// <summary>Logs a startup warning when the dev-only default internal token is in use.</summary>
internal sealed class InternalAuthWarning(ILogger<InternalAuthWarning> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (InternalAuth.IsDefault)
        {
            logger.LogWarning("INTERNAL_AUTH_TOKEN is not set: using the public dev-only default; set it in any shared environment");
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
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
    /// <summary>
    /// The caller's id; throws UNAUTHENTICATED when x-user-id is missing, repeated or invalid, or when a system: id
    /// is not an allowlisted service identity with a valid x-internal-token (never downgraded to a normal user).
    /// </summary>
    public static string Id(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (Count(context, IdentityConstants.UserIdHeader) > 1 || Count(context, InternalAuth.Header) > 1)
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "duplicate identity header"));
        }

        var id = context.RequestHeaders.GetValue(IdentityConstants.UserIdHeader);
        if (string.IsNullOrEmpty(id))
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "missing x-user-id"));
        }

        if (!IsValid(id))
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "invalid x-user-id"));
        }

        if (SystemIdentities.HasReservedPrefix(id)
            && !(SystemIdentities.IsSystem(id) && InternalAuth.Matches(context.RequestHeaders.GetValue(InternalAuth.Header))))
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, "system identity requires a valid x-internal-token"));
        }

        return id;
    }

    /// <summary>True when the caller is a service: an allowlisted system: id AND a matching x-internal-token.</summary>
    public static bool IsSystem(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return SystemIdentities.IsSystem(context.RequestHeaders.GetValue(IdentityConstants.UserIdHeader))
            && InternalAuth.Matches(context.RequestHeaders.GetValue(InternalAuth.Header));
    }

    /// <summary>A usable caller id is an opaque token of 1..64 printable ASCII bytes (0x21-0x7E); the Go lib applies the same rule.</summary>
    public static bool IsValid(string id) =>
        id.Length is > 0 and <= IdentityConstants.MaxUserIdLength && id.All(c => c is >= '!' and <= '~');

    private static int Count(ServerCallContext context, string key) =>
        context.RequestHeaders.Count(e => string.Equals(e.Key, key, StringComparison.OrdinalIgnoreCase));
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
        var headers = context.Options.Headers ?? [];
        if (!string.IsNullOrEmpty(id) && headers.GetValue(IdentityConstants.UserIdHeader) is null)
        {
            headers.Add(IdentityConstants.UserIdHeader, id);
        }

        // Calls made as a system: identity (ambient or explicit header) prove it with the internal token.
        if (SystemIdentities.IsSystem(headers.GetValue(IdentityConstants.UserIdHeader))
            && headers.GetValue(InternalAuth.Header) is null)
        {
            headers.Add(InternalAuth.Header, InternalAuth.Token);
        }

        return new ClientInterceptorContext<TRequest, TResponse>(
            context.Method, context.Host, context.Options.WithHeaders(headers));
    }
}
