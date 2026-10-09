package observe_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/taakht/taakht/libs/goplatform/observe"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// deadPool points at a port nobody listens on: every query fails fast with a connect error.
func deadPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://x:y@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func get(path string) *http.Request {
	return httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
}

func serve(t *testing.T, srv *grpc.Server) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestRequestIDSurvivesAHop(t *testing.T) {
	// Hop 2 records the id it sees; hop 1 calls hop 2 using only the ambient context.
	var seenAtC string
	c := grpc.NewServer(grpc.ChainUnaryInterceptor(observe.RequestIDServerInterceptor(),
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			seenAtC = observe.RequestID(ctx)
			return h(ctx, req)
		}))
	healthpb.RegisterHealthServer(c, health.NewServer())
	addrC := serve(t, c)

	connC, err := grpc.NewClient(addrC, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(observe.RequestIDClientInterceptor()))
	if err != nil {
		t.Fatal(err)
	}
	defer connC.Close()

	b := grpc.NewServer(grpc.ChainUnaryInterceptor(observe.RequestIDServerInterceptor()))
	healthpb.RegisterHealthServer(b, &callingHealth{call: func(ctx context.Context) error {
		_, err := healthpb.NewHealthClient(connC).Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	}})
	addrB := serve(t, b)

	connB, err := grpc.NewClient(addrB, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Close()
	ctx := metadata.AppendToOutgoingContext(context.Background(), observe.RequestIDHeader, "req-123")
	var hdr metadata.MD
	if _, err := healthpb.NewHealthClient(connB).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Header(&hdr)); err != nil {
		t.Fatal(err)
	}
	if seenAtC != "req-123" {
		t.Fatalf("request id at the second hop = %q, want req-123", seenAtC)
	}
	if got := hdr.Get(observe.RequestIDHeader); len(got) != 1 || got[0] != "req-123" {
		t.Fatalf("response header = %v", got)
	}
}

type callingHealth struct {
	healthpb.UnimplementedHealthServer
	call func(context.Context) error
}

func (h *callingHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if err := h.call(ctx); err != nil {
		return nil, err
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func TestRequestIDGeneratedWhenMissingOrInvalid(t *testing.T) {
	for _, in := range []string{"", "has space", strings.Repeat("x", 200)} {
		var got string
		ic := observe.RequestIDServerInterceptor()
		ctx := context.Background()
		if in != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(observe.RequestIDHeader, in))
		}
		_, _ = ic(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			got = observe.RequestID(ctx)
			return nil, nil
		})
		if !observe.ValidRequestID(got) || got == in {
			t.Fatalf("input %q: got %q", in, got)
		}
	}
}

func TestReadinessNotServingWhenDatabaseDown(t *testing.T) {
	pool := deadPool(t)
	o := observe.New(observe.Options{
		Service: "t", DefaultHealthAddr: "127.0.0.1:0",
		Checks: []observe.Check{observe.DBCheck(pool)},
	})
	hs := grpc.NewServer()
	o.RegisterGRPC(hs)
	addr := serve(t, hs)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	hc := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	o.RefreshGRPC(ctx)
	r, err := hc.Check(ctx, &healthpb.HealthCheckRequest{Service: observe.ServiceReadiness})
	if err != nil || r.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness = %v, %v; want NOT_SERVING", r, err)
	}
	// Liveness stays SERVING: the process is up, only a dependency is down.
	for _, svc := range []string{"", observe.ServiceLiveness} {
		r, err = hc.Check(ctx, &healthpb.HealthCheckRequest{Service: svc})
		if err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("service %q = %v, %v; want SERVING", svc, r, err)
		}
	}

	rec := httptest.NewRecorder()
	o.Handler().ServeHTTP(rec, get("/readyz"))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "db: FAIL") {
		t.Fatalf("/readyz = %d %q", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	o.Handler().ServeHTTP(rec, get("/healthz"))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d", rec.Code)
	}
}

func TestReadyWhenAllChecksPass(t *testing.T) {
	o := observe.New(observe.Options{
		Service: "t", DefaultHealthAddr: "127.0.0.1:0",
		Checks: []observe.Check{{Name: "a", Fn: func(context.Context) error { return nil }}},
	})
	rec := httptest.NewRecorder()
	o.Handler().ServeHTTP(rec, get("/readyz"))
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Body.String(), "ready") {
		t.Fatalf("/readyz = %d %q", rec.Code, rec.Body.String())
	}
}

func TestOutboxCheckStuckRelay(t *testing.T) {
	age := 5 * time.Second
	chk := observe.NewOutboxCheckForTest(func(context.Context) (time.Duration, error) { return age, nil }, 60*time.Second)
	if err := chk.Fn(context.Background()); err != nil {
		t.Fatalf("young backlog must be ready: %v", err)
	}
	age = 3 * time.Minute // the relay stopped: the oldest event keeps ageing
	if err := chk.Fn(context.Background()); err == nil || !strings.Contains(err.Error(), "oldest unpublished") {
		t.Fatalf("stuck outbox must fail readiness, got %v", err)
	}
}

func TestConsumerCheckFollowsGroupMembership(t *testing.T) {
	chk := observe.ConsumerCheck()
	if err := chk.Fn(context.Background()); err == nil {
		t.Fatal("no consumer tracked: want failure")
	}
	m := &fakeMember{id: "m1", gen: 3}
	observe.TrackConsumer(m)
	t.Cleanup(observe.UntrackConsumer)
	if err := chk.Fn(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.id, m.gen = "", -1 // dropped from the group
	if err := chk.Fn(context.Background()); err == nil {
		t.Fatal("dropped from the group: want failure")
	}
}

type fakeMember struct {
	id  string
	gen int32
}

func (f *fakeMember) GroupMetadata() (string, int32) { return f.id, f.gen }

func TestMetricsEndpointExposesNamedMetrics(t *testing.T) {
	o := observe.New(observe.Options{Service: "t", DefaultHealthAddr: "127.0.0.1:0"})
	ic := observe.ServerInterceptor()
	_, _ = ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x.Y/Z"}, func(context.Context, any) (any, error) { return nil, nil })
	rec := httptest.NewRecorder()
	o.Handler().ServeHTTP(rec, get("/metrics"))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`taakht_grpc_server_requests_total{code="OK",method="/x.Y/Z"} 1`,
		"taakht_grpc_server_request_duration_seconds_bucket",
		"taakht_consumer_events_total", "taakht_housekeeping_deleted_total", "taakht_consumer_joined",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

func TestJSONLogLine(t *testing.T) {
	var buf bytes.Buffer
	l := observe.NewLoggerForTest("ad", "json", "info", &buf)
	l.InfoContext(observe.WithRequestID(context.Background(), "rid-1"), "hello", "k", 1)
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("not one JSON object per line: %v: %q", err, buf.String())
	}
	for k, want := range map[string]any{"level": "info", "service": "ad", "msg": "hello", "request_id": "rid-1"} {
		if m[k] != want {
			t.Errorf("%s = %v, want %v", k, m[k], want)
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, m["ts"].(string)); err != nil {
		t.Errorf("ts: %v", err)
	}
}
