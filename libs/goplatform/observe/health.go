package observe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// gRPC health service names. "" and "liveness" mean the process is up (SERVING until shutdown); "readiness" is
// SERVING only while every readiness check passes (database, consumer, outbox).
const (
	ServiceLiveness  = "liveness"
	ServiceReadiness = "readiness"
)

// Environment variables.
const (
	EnvHealthAddr     = "HEALTH_ADDR"      // HTTP listener for /healthz /readyz /metrics
	EnvReadyOutboxAge = "READY_OUTBOX_AGE" // oldest unpublished outbox row tolerated by readiness, default 60s
)

const (
	// DefaultOutboxAge is the readiness limit for the oldest unpublished outbox row.
	DefaultOutboxAge   = 60 * time.Second
	checkTimeout       = 2 * time.Second
	statusLoopInterval = 2 * time.Second
)

// Check is one readiness probe.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Result is the outcome of one Check.
type Result struct {
	Name string
	Err  error
}

// Options configures New.
type Options struct {
	Service           string  // short name used in logs, e.g. "ad"
	DefaultHealthAddr string  // used when HEALTH_ADDR is unset, e.g. "127.0.0.1:9101"
	Checks            []Check // readiness checks, evaluated in order
}

// Obs serves the health endpoints and metrics of one process.
type Obs struct {
	opts     Options
	addr     string
	grpc     *health.Server
	shutdown atomic.Bool
}

var current atomic.Pointer[Obs]

// Current returns the Obs created by New, or nil (tests, tools).
func Current() *Obs { return current.Load() }

// New creates the Obs for this process and makes it the one server.Run registers the health service from.
func New(opts Options) *Obs {
	o := &Obs{opts: opts, grpc: health.NewServer()}
	o.addr = os.Getenv(EnvHealthAddr)
	if o.addr == "" {
		o.addr = opts.DefaultHealthAddr
	}
	o.grpc.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	o.grpc.SetServingStatus(ServiceLiveness, healthpb.HealthCheckResponse_SERVING)
	o.grpc.SetServingStatus(ServiceReadiness, healthpb.HealthCheckResponse_NOT_SERVING)
	current.Store(o)
	return o
}

// RegisterGRPC adds grpc.health.v1.Health to srv.
func (o *Obs) RegisterGRPC(srv *grpc.Server) { healthpb.RegisterHealthServer(srv, o.grpc) }

// Ready runs all readiness checks (each bounded by 2 s) and reports whether all passed.
func (o *Obs) Ready(ctx context.Context) (bool, []Result) {
	res := make([]Result, 0, len(o.opts.Checks))
	ok := true
	for _, c := range o.opts.Checks {
		cctx, cancel := context.WithTimeout(ctx, checkTimeout)
		err := c.Fn(cctx)
		cancel()
		if err != nil {
			ok = false
		}
		res = append(res, Result{Name: c.Name, Err: err})
	}
	return ok && !o.shutdown.Load(), res
}

// RefreshGRPC mirrors the current readiness into the gRPC health service.
func (o *Obs) RefreshGRPC(ctx context.Context) {
	ok, _ := o.Ready(ctx)
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if ok {
		st = healthpb.HealthCheckResponse_SERVING
	}
	o.grpc.SetServingStatus(ServiceReadiness, st)
}

// Handler returns the HTTP mux: /healthz (liveness), /readyz (readiness, 503 when a check fails) and /metrics.
func (o *Obs) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ok, res := o.Ready(r.Context())
		sort.SliceStable(res, func(i, j int) bool { return res[i].Name < res[j].Name })
		w.Header().Set("Content-Type", "text/plain")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintln(w, "not ready")
		} else {
			_, _ = fmt.Fprintln(w, "ready")
		}
		for _, c := range res {
			if c.Err != nil {
				_, _ = fmt.Fprintf(w, "%s: FAIL %s\n", c.Name, truncate(c.Err.Error(), 200))
			} else {
				_, _ = fmt.Fprintf(w, "%s: ok\n", c.Name)
			}
		}
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(Registry, promhttp.HandlerOpts{}))
	return mux
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// Serve runs the HTTP listener and the loop that mirrors readiness into the gRPC health service until ctx ends.
// A listen failure is returned (it stops the service); the listener is for operators and never carries user traffic.
func (o *Obs) Serve(ctx context.Context) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", o.addr)
	if err != nil {
		return fmt.Errorf("observe: listen %s: %w", o.addr, err)
	}
	srv := &http.Server{Handler: o.Handler(), ReadHeaderTimeout: 5 * time.Second}
	slog.Info("health listening", "addr", lis.Addr().String(), "paths", "/healthz /readyz /metrics")

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()
	go func() {
		t := time.NewTicker(statusLoopInterval)
		defer t.Stop()
		for {
			o.RefreshGRPC(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		o.shutdown.Store(true)
		o.grpc.Shutdown()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		return nil
	}
}

// DBCheck verifies the database answers SELECT 1 (the caller's context bounds it to 2 s).
func DBCheck(pool *pgxpool.Pool) Check {
	return Check{Name: "db", Fn: func(ctx context.Context) error {
		var one int
		if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			return fmt.Errorf("database: %w", err)
		}
		return nil
	}}
}

// OutboxCheck fails when the oldest unpublished outbox row is older than maxAge, i.e. the relay is stuck or
// cannot reach Kafka.
func OutboxCheck(pool *pgxpool.Pool, maxAge time.Duration) Check {
	return outboxCheck(func(ctx context.Context) (time.Duration, error) {
		_, age, err := OutboxBacklog(ctx, pool)
		return age, err
	}, maxAge)
}

func outboxCheck(oldest func(context.Context) (time.Duration, error), maxAge time.Duration) Check {
	return Check{Name: "outbox", Fn: func(ctx context.Context) error {
		age, err := oldest(ctx)
		if err != nil {
			return fmt.Errorf("outbox: %w", err)
		}
		if age > maxAge {
			return fmt.Errorf("outbox: oldest unpublished event is %s old (limit %s)", age.Round(time.Second), maxAge)
		}
		return nil
	}}
}

// ConsumerCheck fails until the Kafka consumer has joined its group (and again if it is dropped from it).
func ConsumerCheck() Check {
	return Check{Name: "consumer", Fn: func(context.Context) error {
		if !ConsumerJoined() {
			return errors.New("consumer: not a member of its group")
		}
		return nil
	}}
}

// OutboxMaxAge reads READY_OUTBOX_AGE (a Go duration, default 60s).
func OutboxMaxAge() (time.Duration, error) {
	v := os.Getenv(EnvReadyOutboxAge)
	if v == "" {
		return DefaultOutboxAge, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("observe: %s must be a positive duration, got %q", EnvReadyOutboxAge, v)
	}
	return d, nil
}

// NewOutboxCheckForTest builds the outbox check over a fake age source.
func NewOutboxCheckForTest(oldest func(context.Context) (time.Duration, error), maxAge time.Duration) Check {
	return outboxCheck(oldest, maxAge)
}
