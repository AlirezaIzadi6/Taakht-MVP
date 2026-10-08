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

func TestServerInterceptorRejectsBadIDs(t *testing.T) {
	h := func(ctx context.Context, _ any) (any, error) { return UserID(ctx), nil }
	for _, id := range []string{strings.Repeat("a", 65), "user\n1", "user\x00"} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(Header, id))
		_, err := ServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, h)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("id %q: want Unauthenticated, got %v", id, err)
		}
	}
	if !IsSystem(SystemSwap) || IsSystem("user-1") {
		t.Fatal("IsSystem misclassifies")
	}
}
