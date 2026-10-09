package server_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/taakht/taakht/libs/goplatform/db"
	"github.com/taakht/taakht/libs/goplatform/server"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

func TestMapError(t *testing.T) {
	pg := func(code string) error {
		return fmt.Errorf("load ad: %w", &pgconn.PgError{Code: code, Message: "boom"})
	}
	tests := []struct {
		name string
		err  error
		own  bool
		want codes.Code
	}{
		{"too many clients", pg("53300"), false, codes.Unavailable},
		{"cannot connect now", pg("57P03"), false, codes.Unavailable},
		{"admin shutdown", pg("57P01"), false, codes.Unavailable},
		{"connection exception", pg("08006"), false, codes.Unavailable},
		{"unique violation is not overload", pg("23505"), false, codes.Internal},
		{"connect error", fmt.Errorf("x: %w", &pgconn.ConnectError{}), false, codes.Unavailable},
		{"network timeout", fmt.Errorf("x: %w", timeoutErr{}), false, codes.Unavailable},
		{"own deadline while waiting for the pool", fmt.Errorf("acquire: %w", context.DeadlineExceeded), true, codes.Unavailable},
		{"caller deadline", context.DeadlineExceeded, false, codes.DeadlineExceeded},
		{"cancelled", context.Canceled, false, codes.Canceled},
		{"plain error", errors.New("bug"), false, codes.Internal},
		{"already mapped NotFound", status.Error(codes.NotFound, "no"), false, codes.NotFound},
		{"already mapped Unknown is kept", status.Error(codes.Unknown, "x"), false, codes.Unknown},
		{"already mapped Internal is kept", status.Error(codes.Internal, "x"), true, codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := server.MapError(tc.err, tc.own)
			if status.Code(got) != tc.want {
				t.Fatalf("code = %v (%v), want %v", status.Code(got), got, tc.want)
			}
			if _, ok := status.FromError(tc.err); ok && !errors.Is(got, tc.err) {
				t.Fatalf("an error that already has a status must be returned unchanged")
			}
		})
	}
	if server.MapError(nil, true) != nil {
		t.Fatal("nil must stay nil")
	}
}

func TestOverloadInterceptorDefaultDeadline(t *testing.T) {
	icept := server.OverloadInterceptor(50 * time.Millisecond)
	_, err := icept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/t/Slow"}, func(ctx context.Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("own deadline: got %v, want Unavailable", err)
	}

	// A caller deadline is the caller's choice and stays DEADLINE_EXCEEDED.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = icept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/t/Slow"}, func(ctx context.Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("caller deadline: got %v, want DeadlineExceeded", err)
	}
}

// TestPoolExhaustion runs against the compose Postgres (TEST_DATABASE_URL): a pool of 2 connections serves
// callers that hold a connection for a while. The ones that cannot get a connection in time must fail with
// UNAVAILABLE (never UNKNOWN or INTERNAL), and the pool must serve requests normally afterwards.
func TestPoolExhaustion(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	t.Setenv("DB_MAX_CONNS", "2")
	t.Setenv("DB_MIN_CONNS", "0")
	ctx := context.Background()
	pool, err := db.Open(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if got := pool.Config().MaxConns; got != 2 {
		t.Fatalf("MaxConns = %d, want 2", got)
	}

	icept := server.OverloadInterceptor(300 * time.Millisecond)
	call := func(sleep string) error {
		_, err := icept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/t/Query"}, func(ctx context.Context, _ any) (any, error) {
			_, err := pool.Exec(ctx, "SELECT pg_sleep("+sleep+")")
			if err != nil {
				return nil, fmt.Errorf("query: %w", err)
			}
			return struct{}{}, nil
		})
		return err
	}

	const callers = 10
	var wg sync.WaitGroup
	codesSeen := make([]codes.Code, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codesSeen[i] = status.Code(call("0.2"))
		}()
	}
	wg.Wait()

	count := map[codes.Code]int{}
	for _, c := range codesSeen {
		count[c]++
	}
	ok, unavailable := count[codes.OK], count[codes.Unavailable]
	if ok+unavailable != callers {
		t.Fatalf("every call must end OK or Unavailable, got %v", count)
	}
	if ok == 0 || unavailable == 0 {
		t.Fatalf("expected some OK and some Unavailable, got ok=%d unavailable=%d", ok, unavailable)
	}

	if err := call("0"); err != nil {
		t.Fatalf("pool did not recover: %v", err)
	}
}
