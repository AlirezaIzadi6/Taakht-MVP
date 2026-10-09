// Command ad runs the Ad service: gRPC API, outbox relay and swap event consumer.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/libs/goplatform/db"
	"github.com/taakht/taakht/libs/goplatform/housekeeping"
	"github.com/taakht/taakht/libs/goplatform/outbox"
	"github.com/taakht/taakht/libs/goplatform/server"
	"github.com/taakht/taakht/src/ad/internal/ad"
	"github.com/taakht/taakht/src/ad/internal/eligibility"
	"github.com/taakht/taakht/src/ad/migrations"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ad service stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := env("DATABASE_URL", "postgres://taakht:taakht@127.0.0.1:5432/ad?sslmode=disable")
	brokers := strings.Split(env("KAFKA_BROKERS", "127.0.0.1:9094"), ",")
	addr := env("GRPC_ADDR", ":9001")

	elig, err := eligibility.Load(env("ELIGIBILITY_FILE", "../../config/eligibility.json"))
	if err != nil {
		return err
	}
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool, migrations.FS, "."); err != nil {
		return err
	}

	hk, err := housekeeping.OptionsFromEnv()
	if err != nil {
		return err
	}

	svc := ad.NewService(pool, elig)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return server.Run(ctx, addr, func(s *grpc.Server) { adv1.RegisterAdServiceServer(s, svc) })
	})
	g.Go(func() error { return outbox.RunRelay(ctx, pool, brokers) })
	g.Go(func() error { return housekeeping.Run(ctx, pool, hk) })
	g.Go(func() error {
		return consume.Run(ctx, pool, brokers, ad.Group, []string{ad.SwapTopic}, ad.Handlers())
	})
	return g.Wait()
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
