package observe

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/taakht/taakht/libs/goplatform/identity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// healthPrefix is the method prefix of the standard health service; it is neither logged, counted nor authenticated.
const healthPrefix = "/grpc.health.v1.Health/"

// IsHealthMethod reports whether fullMethod belongs to the standard gRPC health service.
func IsHealthMethod(fullMethod string) bool { return strings.HasPrefix(fullMethod, healthPrefix) }

// ServerInterceptor counts and times every unary call (taakht_grpc_server_*) and writes one log line per call
// with the method, status code, duration, request id and, when the caller sent a valid one, the user id.
// Register it after RequestIDServerInterceptor and outside the overload interceptor so it sees the final code.
func ServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if IsHealthMethod(info.FullMethod) {
			return h(ctx, req)
		}
		start := time.Now()
		resp, err := h(ctx, req)
		d := time.Since(start)
		code := status.Code(err)
		grpcRequests.WithLabelValues(info.FullMethod, code.String()).Inc()
		grpcDuration.WithLabelValues(info.FullMethod).Observe(d.Seconds())

		attrs := []any{"method", info.FullMethod, "code", code.String(), "duration_ms", float64(d.Microseconds()) / 1000}
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(identity.Header); len(v) == 1 && identity.Valid(v[0]) {
				attrs = append(attrs, "user_id", v[0])
			}
		}
		slog.Log(ctx, levelFor(code), "grpc call", attrs...)
		return resp, err
	}
}

func levelFor(c codes.Code) slog.Level {
	if c == codes.OK {
		return slog.LevelInfo
	}
	if slices.Contains([]codes.Code{codes.Unknown, codes.Internal, codes.DataLoss, codes.Unavailable, codes.DeadlineExceeded}, c) {
		return slog.LevelError
	}
	return slog.LevelWarn // the caller's mistake or an expected refusal
}

// SkipHealth wraps next so calls to the standard health service bypass it (health probes carry no identity).
func SkipHealth(next grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if IsHealthMethod(info.FullMethod) {
			return h(ctx, req)
		}
		return next(ctx, req, info, h)
	}
}
