package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultRequestTimeout bounds a unary handler whose caller sent no deadline, so that a request waiting for a
// pooled database connection fails fast instead of hanging. Override with REQUEST_TIMEOUT (Go duration).
const DefaultRequestTimeout = 15 * time.Second

// overloadMessage is the short message clients see for every overload error.
const overloadMessage = "service temporarily unavailable, retry later"

// RequestTimeout reads REQUEST_TIMEOUT (default 15s); an invalid or non-positive value falls back to the default.
func RequestTimeout() time.Duration {
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
		slog.Warn("invalid REQUEST_TIMEOUT, using the default", "value", v, "default", DefaultRequestTimeout)
	}
	return DefaultRequestTimeout
}

// IsOverload reports whether err means the database (or its pool) cannot take the request right now: Postgres
// 53300 too_many_connections, 57P03 cannot_connect_now, 57P01 admin_shutdown, any 08xxx connection exception, a
// failed connect or a network timeout. A context deadline or cancellation is not decided here: see Overload.
func IsOverload(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "53300", "57P03", "57P01":
			return true
		}
		return strings.HasPrefix(pgErr.Code, "08")
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false // context errors also satisfy net.Error; they are decided by MapError
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// MapError returns the gRPC status error for err. Errors that already carry a status are returned unchanged.
// Overload errors become UNAVAILABLE. A context deadline set by the server itself (ownDeadline: the caller sent
// none, see OverloadInterceptor) means the request waited too long for a resource and becomes UNAVAILABLE as well;
// a deadline the caller chose becomes DEADLINE_EXCEEDED. Any other plain error becomes INTERNAL.
func MapError(err error, ownDeadline bool) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case IsOverload(err):
		return status.Error(codes.Unavailable, overloadMessage)
	case errors.Is(err, context.DeadlineExceeded):
		if ownDeadline {
			return status.Error(codes.Unavailable, overloadMessage)
		}
		return status.FromContextError(err).Err()
	case errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()
	}
	// Not an overload and no status of its own: report INTERNAL (not UNKNOWN) and keep the details out of the response.
	return status.Error(codes.Internal, "internal error")
}

// OverloadInterceptor translates database overload into UNAVAILABLE for unary calls, centrally, so clients and the
// gateway see a retryable 503 instead of UNKNOWN. It applies timeout to calls that arrive without a deadline (0
// disables that). It sits outermost: handler errors that already are status errors pass through untouched.
func OverloadInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		own := false
		if _, has := ctx.Deadline(); !has && timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
			own = true
		}
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}
		mapped := MapError(err, own)
		if _, had := status.FromError(err); !had {
			if status.Code(mapped) == codes.Unavailable {
				slog.Warn("request failed: service overloaded", "method", info.FullMethod, "err", err)
			} else {
				slog.Error("request failed", "method", info.FullMethod, "err", err)
			}
		}
		return resp, mapped
	}
}
