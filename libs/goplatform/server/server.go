// Package server runs a gRPC server with the identity interceptor and graceful shutdown.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/taakht/taakht/libs/goplatform/identity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Run serves on addr until ctx is cancelled, then stops gracefully. register adds services.
// Reflection is off unless TAAKHT_GRPC_REFLECTION=on.
func Run(ctx context.Context, addr string, register func(*grpc.Server)) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", addr, err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(identity.ServerInterceptor()))
	register(srv)
	if os.Getenv("TAAKHT_GRPC_REFLECTION") == "on" {
		reflection.Register(srv)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	slog.Info("grpc listening", "addr", lis.Addr().String())

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		srv.GracefulStop()
		return nil
	}
}
