package identity

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestServerInterceptor(t *testing.T) {
	h := func(ctx context.Context, _ any) (any, error) { return UserID(ctx), nil }
	icpt := ServerInterceptor()

	_, err := icpt(context.Background(), nil, &grpc.UnaryServerInfo{}, h)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("want Unauthenticated, got %v", err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(Header, "user-1"))
	got, err := icpt(ctx, nil, &grpc.UnaryServerInfo{}, h)
	if err != nil || got != "user-1" {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestClientInterceptorForwards(t *testing.T) {
	ctx := WithUserID(context.Background(), "user-2")
	inv := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		if v := md.Get(Header); len(v) != 1 || v[0] != "user-2" {
			t.Fatalf("outgoing md = %v", md)
		}
		return nil
	}
	if err := ClientInterceptor()(ctx, "/x", nil, nil, nil, inv); err != nil {
		t.Fatal(err)
	}
}

func incoming(user, tok string) context.Context {
	md := metadata.Pairs(Header, user)
	if tok != "" {
		md.Set(TokenHeader, tok)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestSystemIdentityNeedsToken(t *testing.T) {
	h := func(ctx context.Context, _ any) (any, error) { return IsSystem(ctx), nil }
	icpt := ServerInterceptor()
	run := func(user, tok string) (any, error) {
		return icpt(incoming(user, tok), nil, &grpc.UnaryServerInfo{}, h)
	}

	if got, err := run(SystemSwap, InternalToken()); err != nil || got != true {
		t.Fatalf("valid token: got %v, %v", got, err)
	}
	for name, tok := range map[string]string{"missing": "", "wrong": "nope", "prefix of real": InternalToken()[:5]} {
		if _, err := run(SystemSwap, tok); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("%s token: want Unauthenticated, got %v", name, err)
		}
	}
	if got, err := run("user-1", ""); err != nil || got != false {
		t.Fatalf("normal user: got %v, %v", got, err)
	}
	// A real user sending a token is still not a system caller.
	if got, err := run("user-1", InternalToken()); err != nil || got != false {
		t.Fatalf("user with token: got %v, %v", got, err)
	}
	// Direct check without the interceptor: no proof, no system.
	if IsSystem(incoming(SystemSwap, "")) || !IsSystem(incoming(SystemSwap, InternalToken())) {
		t.Fatal("IsSystem must require the token")
	}
	if !IsSystemID(SystemSwap) || IsSystemID("user-1") {
		t.Fatal("IsSystemID misclassifies")
	}
}

func TestClientInterceptorAddsTokenForSystemOnly(t *testing.T) {
	check := func(user string, wantToken bool) {
		inv := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			if got := len(md.Get(TokenHeader)) == 1 && md.Get(TokenHeader)[0] == InternalToken(); got != wantToken {
				t.Fatalf("%s: token present = %v, want %v", user, got, wantToken)
			}
			return nil
		}
		if err := ClientInterceptor()(WithUserID(context.Background(), user), "/x", nil, nil, nil, inv); err != nil {
			t.Fatal(err)
		}
	}
	check(SystemSwap, true)
	check("user-1", false)
}

func TestIDValidationTable(t *testing.T) {
	h := func(ctx context.Context, _ any) (any, error) { return UserID(ctx), nil }
	cases := []struct {
		name, id, tok string
		ok            bool
	}{
		{"empty", "", "", false},
		{"space", " ", "", false},
		{"inner space", "user 1", "", false},
		{"leading space", " user-1", "", false},
		{"trailing space", "user-1 ", "", false},
		{"64 bytes", strings.Repeat("a", 64), "", true},
		{"65 bytes", strings.Repeat("a", 65), "", false},
		{"non-ascii", "\u06a9\u0627\u0631\u0628\u0631", "", false},
		{"control", "user\t1", "", false},
		{"nul", "user\x00", "", false},
		{"valid", "user-1", "", true},
		{"system swap with token", SystemSwap, InternalToken(), true},
		{"system swap without token", SystemSwap, "", false},
		{"system swap wrong token", SystemSwap, "x", false},
		{"unknown system id with token", "system:other", InternalToken(), false},
		{"upper-case system prefix", "SYSTEM:swap", InternalToken(), false},
		{"mixed-case prefix without token", "System:x", "", false},
	}
	for _, c := range cases {
		md := metadata.MD{}
		if c.id != "" {
			md.Set(Header, c.id)
		}
		if c.tok != "" {
			md.Set(TokenHeader, c.tok)
		}
		_, err := ServerInterceptor()(metadata.NewIncomingContext(context.Background(), md), nil, &grpc.UnaryServerInfo{}, h)
		if (err == nil) != c.ok || (err != nil && status.Code(err) != codes.Unauthenticated) {
			t.Errorf("%s: ok=%v err=%v", c.name, c.ok, err)
		}
	}
}

func TestDuplicateHeadersRejected(t *testing.T) {
	h := func(ctx context.Context, _ any) (any, error) { return UserID(ctx), nil }
	for name, md := range map[string]metadata.MD{
		"two user ids": metadata.Pairs(Header, "user-1", Header, "user-2"),
		"two tokens":   metadata.Pairs(Header, SystemSwap, TokenHeader, InternalToken(), TokenHeader, InternalToken()),
	} {
		_, err := ServerInterceptor()(metadata.NewIncomingContext(context.Background(), md), nil, &grpc.UnaryServerInfo{}, h)
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: want Unauthenticated, got %v", name, err)
		}
	}
}

func TestIsSystemAllowlist(t *testing.T) {
	if !IsSystem(incoming(SystemNegotiation, InternalToken())) || IsSystem(incoming("system:other", InternalToken())) {
		t.Fatal("IsSystem must allowlist exactly the two service ids")
	}
}

// useTokens swaps the token configuration for the duration of a test.
func useTokens(t *testing.T, env map[string]string) {
	t.Helper()
	current() // make sure the Once has fired so it cannot overwrite the override
	saved := tokens
	tokens = loadTokens(func(k string) string { return env[k] })
	t.Cleanup(func() { tokens = saved })
}

func TestRotationAcceptsCurrentAndPrevious(t *testing.T) {
	useTokens(t, map[string]string{"INTERNAL_AUTH_TOKEN": "new-token", "INTERNAL_AUTH_TOKEN_PREVIOUS": "old-token"})
	for tok, want := range map[string]bool{"new-token": true, "old-token": true, "older": false, "": false, "new-toke": false} {
		if got := IsSystem(incoming(SystemSwap, tok)); got != want {
			t.Errorf("token %q: IsSystem=%v want %v", tok, got, want)
		}
	}
	// Clients always send the current token, never the previous one.
	if TokenFor(SystemSwap) != "new-token" || InternalToken() != "new-token" {
		t.Fatalf("client must send the current token, got %q", TokenFor(SystemSwap))
	}
	// After the window closes the old token stops working.
	useTokens(t, map[string]string{"INTERNAL_AUTH_TOKEN": "new-token"})
	if IsSystem(incoming(SystemSwap, "old-token")) {
		t.Fatal("previous token accepted without INTERNAL_AUTH_TOKEN_PREVIOUS")
	}
}

func TestPerIdentityTokenScoping(t *testing.T) {
	useTokens(t, map[string]string{
		"INTERNAL_AUTH_TOKEN": "shared", "INTERNAL_AUTH_TOKEN_PREVIOUS": "shared-old",
		"INTERNAL_AUTH_TOKEN_SWAP": "swap-tok", "INTERNAL_AUTH_TOKEN_NEGOTIATION": "neg-tok",
	})
	cases := []struct {
		id, tok string
		ok      bool
	}{
		{SystemSwap, "swap-tok", true},
		{SystemSwap, "neg-tok", false}, // a compromised negotiation token cannot claim system:swap
		{SystemSwap, "shared", false},  // the shared token no longer proves a scoped identity
		{SystemSwap, "shared-old", false},
		{SystemNegotiation, "neg-tok", true},
		{SystemNegotiation, "swap-tok", false},
	}
	for _, c := range cases {
		if got := IsSystem(incoming(c.id, c.tok)); got != c.ok {
			t.Errorf("%s with %q: got %v want %v", c.id, c.tok, got, c.ok)
		}
		_, err := ServerInterceptor()(incoming(c.id, c.tok), nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return nil, nil })
		if (err == nil) != c.ok {
			t.Errorf("interceptor %s with %q: err=%v", c.id, c.tok, err)
		}
	}
	if TokenFor(SystemSwap) != "swap-tok" || TokenFor(SystemNegotiation) != "neg-tok" {
		t.Fatal("client must send the identity's own token")
	}
}

func TestPerIdentityFallsBackToSharedToken(t *testing.T) {
	useTokens(t, map[string]string{"INTERNAL_AUTH_TOKEN": "shared", "INTERNAL_AUTH_TOKEN_SWAP": "swap-tok"})
	// Only swap is scoped: negotiation still uses the shared token.
	if !IsSystem(incoming(SystemNegotiation, "shared")) || IsSystem(incoming(SystemNegotiation, "swap-tok")) {
		t.Fatal("unscoped identity must keep using the shared token")
	}
	if TokenFor(SystemNegotiation) != "shared" {
		t.Fatalf("got %q", TokenFor(SystemNegotiation))
	}
}
