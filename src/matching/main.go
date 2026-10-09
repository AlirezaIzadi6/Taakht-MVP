// Command matching runs the Matching service: it indexes published ads from ad.events,
// serves Search and FindMatches, and emits MatchFound.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/libs/goplatform/db"
	"github.com/taakht/taakht/libs/goplatform/housekeeping"
	"github.com/taakht/taakht/libs/goplatform/observe"
	"github.com/taakht/taakht/libs/goplatform/outbox"
	"github.com/taakht/taakht/libs/goplatform/reload"
	"github.com/taakht/taakht/libs/goplatform/server"
	"github.com/taakht/taakht/src/matching/internal/geo"
	"github.com/taakht/taakht/src/matching/internal/handlers"
	"github.com/taakht/taakht/src/matching/internal/index"
	"github.com/taakht/taakht/src/matching/internal/service"
	"github.com/taakht/taakht/src/matching/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("matching stopped", "err", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	observe.SetupLogging("matching") // LOG_FORMAT=json|text, LOG_LEVEL

	dbURL := env("DATABASE_URL", "postgres://taakht:taakht@127.0.0.1:5432/matching?sslmode=disable")
	brokers := strings.Split(env("KAFKA_BROKERS", "127.0.0.1:9094"), ",")
	addr := env("GRPC_ADDR", ":9002")
	eligibility := env("ELIGIBILITY_FILE", filepath.FromSlash("../../config/eligibility.json"))

	geoSrc, err := geo.LoadReloadable(eligibility, slog.Default())
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
	tombstones, err := housekeeping.EnvDuration(index.EnvTombstoneRetention, index.DefaultTombstoneRetention)
	if err != nil {
		return err
	}
	hk.Extra = append(hk.Extra, index.TombstonePrune(tombstones))

	h := &handlers.Handlers{GeoSource: geoSrc, Emit: outbox.Add, Log: slog.Default()}
	svc := &service.Service{DB: pool, GeoSource: geoSrc}

	// Operability: grpc.health.v1 plus /healthz /readyz /metrics on HEALTH_ADDR (default 127.0.0.1:9102).
	outboxAge, err := observe.OutboxMaxAge()
	if err != nil {
		return err
	}
	observe.RegisterDB(pool)
	obs := observe.New(observe.Options{
		Service: "matching", DefaultHealthAddr: "127.0.0.1:9102",
		Checks: []observe.Check{observe.DBCheck(pool), observe.OutboxCheck(pool, outboxAge), observe.ConsumerCheck()},
	})

	gctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := []func() error{
		func() error {
			return server.Run(gctx, addr, func(s *grpc.Server) { matchingv1.RegisterMatchingServiceServer(s, svc) })
		},
		func() error { return obs.Serve(gctx) },
		func() error { return outbox.RunRelay(gctx, pool, brokers) },
		func() error { return housekeeping.Run(gctx, pool, hk) },
		func() error { geoSrc.Run(gctx, reload.DefaultInterval); return nil },
		func() error { return consume.Run(gctx, pool, brokers, "matching", []string{"ad.events"}, h.Map()) },
	}
	errs := make(chan error, len(jobs))
	for _, job := range jobs {
		go func() { errs <- job() }()
	}
	var first error
	for range jobs {
		if err := <-errs; err != nil && !errors.Is(err, context.Canceled) && first == nil {
			first = err
		}
		cancel()
	}
	return first
}
