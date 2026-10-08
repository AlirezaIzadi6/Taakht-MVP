namespace Taakht.Negotiation.Domain;

public enum DomainError
{
    NotFound,
    PermissionDenied,
    InvalidArgument,
    FailedPrecondition,
    Aborted,
    AlreadyExists,
    ResourceExhausted,
}

public sealed class DomainException : Exception
{
    public DomainException(DomainError error, string message)
        : base(message)
    {
        Error = error;
    }

    public DomainError Error { get; }
}
