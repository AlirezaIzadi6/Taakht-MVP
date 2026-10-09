using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Taakht.Platform;

/// <summary>
/// Turns database overload into UNAVAILABLE (a retryable 503 at the gateway) instead of UNKNOWN: Postgres 53300
/// (too many clients), 57P03 (cannot connect now), 57P01 (admin shutdown), class 08 (connection exceptions), other
/// Npgsql failures that are not a server-side SQL error (connection broken, connect timeout) and the TimeoutException
/// raised when waiting for a pooled connection. Register it first so it is the outermost interceptor. An
/// <see cref="RpcException"/> that is already mapped, a cancellation and every other exception pass through.
/// </summary>
public sealed class OverloadInterceptor(ILogger<OverloadInterceptor> logger) : Interceptor
{
    public const string Message = "service temporarily unavailable, retry later";

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (Exception ex) when (IsOverload(ex))
        {
            throw Map(ex, context.Method);
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            await continuation(request, responseStream, context);
        }
        catch (Exception ex) when (IsOverload(ex))
        {
            throw Map(ex, context.Method);
        }
    }

    /// <summary>True when <paramref name="ex"/> (or an inner exception) means the database cannot take the request now.</summary>
    public static bool IsOverload(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            switch (e)
            {
                case RpcException:
                case OperationCanceledException:
                    return false; // already a status, or the caller went away
                case PostgresException pg:
                    return IsOverloadSqlState(pg.SqlState);
                case NpgsqlException:
                case TimeoutException:
                    return true;
                case AggregateException agg:
                    return agg.InnerExceptions.Count > 0 && agg.InnerExceptions.All(IsOverload);
                default:
                    break;
            }
        }

        return false;
    }

    public static bool IsOverloadSqlState(string? sqlState) =>
        sqlState is "53300" or "57P03" or "57P01" || (sqlState is not null && sqlState.StartsWith("08", StringComparison.Ordinal));

    private RpcException Map(Exception ex, string method)
    {
        logger.LogWarning(ex, "{Method} failed: database overloaded or unreachable; answering UNAVAILABLE", method);
        return new RpcException(new Status(StatusCode.Unavailable, Message));
    }
}
