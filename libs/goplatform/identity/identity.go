// Package identity carries the caller's user id (gRPC metadata key x-user-id).
package identity

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"os"
	"strings"
	"sync"

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

// TokenHeader is the metadata key carrying the shared internal secret on service-to-service calls.
const TokenHeader = "x-internal-token"

// DefaultInternalToken is the local-dev value of INTERNAL_AUTH_TOKEN. DEV ONLY: it is public in the repo.
const DefaultInternalToken = "dev-internal-token"

var (
	tokenOnce sync.Once
	token     string
)

// InternalToken returns the shared internal secret (env INTERNAL_AUTH_TOKEN, else the dev default).
func InternalToken() string {
	tokenOnce.Do(func() {
		if token = os.Getenv("INTERNAL_AUTH_TOKEN"); token == "" {
			token = DefaultInternalToken
		}
	})
	return token
}

// WarnIfDefaultToken logs a warning when the dev-only default internal token is in use.
func WarnIfDefaultToken() {
	if InternalToken() == DefaultInternalToken {
		slog.Warn("INTERNAL_AUTH_TOKEN is not set: using the public dev-only default; set it in any shared environment")
	}
}

// hasPrefix reports whether id claims to be a service identity (reserved prefix, any case), ignoring proof.
func hasPrefix(id string) bool {
	return len(id) >= len(SystemPrefix) && strings.EqualFold(id[:len(SystemPrefix)], SystemPrefix)
}

// known reports whether id is one of the allowlisted service identities (exact, case-sensitive).
func known(id string) bool { return id == SystemSwap || id == SystemNegotiation }

// validToken compares got with the internal secret in constant time (over fixed-length digests).
func validToken(got string) bool {
	if got == "" {
		return false
	}
	a, b := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(InternalToken()))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

// IsSystem reports whether the caller is a service: its id is exactly system:swap or system:negotiation AND
// the incoming x-internal-token matches the shared secret. The id alone proves nothing.
func IsSystem(ctx context.Context) bool {
	return known(UserID(ctx)) && validToken(tokenFromMetadata(ctx))
}

// IsSystemID reports whether id merely has the reserved prefix (no proof; use IsSystem for decisions).
func IsSystemID(id string) bool { return hasPrefix(id) }

// maxIDLen bounds the caller id in bytes; longer values are rejected.
const maxIDLen = 64

// Valid reports whether id is a usable caller id: an opaque token of 1..64 printable ASCII bytes
// (0x21-0x7E: no spaces, control characters or non-ASCII). The .NET lib applies the identical rule.
func Valid(id string) bool {
	if id == "" || len(id) > maxIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

type ctxKey struct{}

// ServerInterceptor requires exactly one valid x-user-id and stores it in the context. A system: id
// (any case) must be an allowlisted service identity accompanied by a valid x-internal-token, else the
// call is UNAUTHENTICATED (never downgraded to a normal user). Repeated x-user-id or x-internal-token
// headers are rejected.
func ServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if len(md.Get(Header)) > 1 || len(md.Get(TokenHeader)) > 1 {
			return nil, status.Error(codes.Unauthenticated, "duplicate identity header")
		}
		id := fromMetadata(ctx)
		if id == "" {
			return nil, status.Error(codes.Unauthenticated, "missing x-user-id")
		}
		if !Valid(id) {
			return nil, status.Error(codes.Unauthenticated, "invalid x-user-id")
		}
		if hasPrefix(id) && (!known(id) || !validToken(tokenFromMetadata(ctx))) {
			return nil, status.Error(codes.Unauthenticated, "system identity requires a valid x-internal-token")
		}
		return h(context.WithValue(ctx, ctxKey{}, id), req)
	}
}

// ClientInterceptor forwards the caller id of the context to the outgoing call.
func ClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id := UserID(ctx); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, Header, id)
			if hasPrefix(id) {
				ctx = metadata.AppendToOutgoingContext(ctx, TokenHeader, InternalToken())
			}
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

func tokenFromMetadata(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(TokenHeader); len(v) > 0 {
			return v[0]
		}
	}
	return ""
}
