// Package identity carries the caller's user id (gRPC metadata key x-user-id).
package identity

import (
	"context"
	"strings"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Header is the metadata key holding the caller id.
const Header = "x-user-id"

// SystemPrefix marks service identities (never real users); SystemSwap and SystemNegotiation
// are the ones used for service-to-service calls.
const (
	SystemPrefix      = "system:"
	SystemSwap        = "system:swap"
	SystemNegotiation = "system:negotiation"
)

// maxIDLen bounds the caller id; longer values are rejected.
const maxIDLen = 64

// IsSystem reports whether id is a service identity.
func IsSystem(id string) bool { return strings.HasPrefix(id, SystemPrefix) }

// Valid reports whether id is a usable caller id: 1..64 bytes without control characters.
func Valid(id string) bool {
	if id == "" || len(id) > maxIDLen {
		return false
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

type ctxKey struct{}

// ServerInterceptor requires x-user-id and stores it in the context.
func ServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		id := fromMetadata(ctx)
		if id == "" {
			return nil, status.Error(codes.Unauthenticated, "missing x-user-id")
		}
		if !Valid(id) {
			return nil, status.Error(codes.Unauthenticated, "invalid x-user-id")
		}
		return h(context.WithValue(ctx, ctxKey{}, id), req)
	}
}

// ClientInterceptor forwards the caller id of the context to the outgoing call.
func ClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id := UserID(ctx); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, Header, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// UserID returns the caller id, or an empty string when there is none.
func UserID(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKey{}).(string); ok {
		return id
	}
	return fromMetadata(ctx)
}

// WithUserID sets the caller id for outgoing calls made without an incoming request.
func WithUserID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func fromMetadata(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(Header); len(v) > 0 {
			return v[0]
		}
	}
	return ""
}
