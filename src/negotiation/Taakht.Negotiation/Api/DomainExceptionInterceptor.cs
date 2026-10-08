using Grpc.Core;
using Grpc.Core.Interceptors;
using Taakht.Negotiation.Domain;

namespace Taakht.Negotiation.Api;

/// <summary>Translates domain errors into the gRPC status codes of the service conventions.</summary>
public sealed class DomainExceptionInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (DomainException ex)
        {
            throw new RpcException(new Status(ToCode(ex.Error), ex.Message));
        }
    }

    internal static StatusCode ToCode(DomainError error) => error switch
    {
        DomainError.NotFound => StatusCode.NotFound,
        DomainError.PermissionDenied => StatusCode.PermissionDenied,
        DomainError.InvalidArgument => StatusCode.InvalidArgument,
        DomainError.FailedPrecondition => StatusCode.FailedPrecondition,
        DomainError.Aborted => StatusCode.Aborted,
        DomainError.AlreadyExists => StatusCode.AlreadyExists,
        DomainError.ResourceExhausted => StatusCode.ResourceExhausted,
        _ => StatusCode.Unknown,
    };
}
