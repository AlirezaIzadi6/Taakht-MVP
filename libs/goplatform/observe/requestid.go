// Package observe holds the operability pieces every Go service shares: request-id propagation, structured
// logging, Prometheus metrics and health (gRPC health service plus a small HTTP listener).
package observe

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RequestIDHeader is the metadata key carrying the request id. Envoy generates it for every REST call.
const RequestIDHeader = "x-request-id"

// maxRequestIDLen bounds an accepted id; longer or non-printable values are replaced by a fresh id.
const maxRequestIDLen = 128

type requestIDKey struct{}

// NewRequestID returns a fresh random request id.
func NewRequestID() string { return uuid.NewString() }

// ValidRequestID reports whether id is 1..128 printable ASCII bytes (0x21-0x7E).
func ValidRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

// WithRequestID returns ctx carrying id (ignored when id is empty).
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the ambient request id, or an empty string.
func RequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// RequestIDServerInterceptor takes x-request-id from the incoming metadata (a fresh id when it is absent or
// unusable), puts it in the context and echoes it as a response header. Register it outermost.
func RequestIDServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(RequestIDHeader); len(v) > 0 {
				id = v[0]
			}
		}
		if !ValidRequestID(id) {
			id = NewRequestID()
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, id))
		return h(WithRequestID(ctx, id), req)
	}
}

// RequestIDClientInterceptor forwards the ambient request id on outgoing calls.
func RequestIDClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id := RequestID(ctx); id != "" {
			if md, ok := metadata.FromOutgoingContext(ctx); !ok || len(md.Get(RequestIDHeader)) == 0 {
				ctx = metadata.AppendToOutgoingContext(ctx, RequestIDHeader, id)
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
