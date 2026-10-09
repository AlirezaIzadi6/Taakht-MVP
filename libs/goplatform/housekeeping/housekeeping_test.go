package housekeeping_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/taakht/taakht/libs/goplatform/housekeeping"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ddl = `
CREATE TABLE outbox (
  id uuid PRIMARY KEY, topic text NOT NULL, key text NOT NULL, envelope bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
CREATE TABLE processed_events (
  consumer text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id));
CREATE TABLE dead_letter (
  consumer text NOT NULL, event_id uuid NOT NULL, topic text NOT NULL, payload bytea NOT NULL, error text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (consumer, event_id));`

func TestParseDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"24h":   24 * time.Hour,
		"7d":    7 * 24 * time.Hour,
		"1d12h": 36 * time.Hour,
		"10m":   10 * time.Minute,
		"90s":   90 * time.Second,
	}
	for in, want := range cases {
		got, err := housekeeping.ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "d", "abc", "7x", "1dfoo", "99999999d", "99999999999999999999d"} {
		if _, err := housekeeping.ParseDuration(in); err == nil {
			t.Errorf("ParseDuration(%q) succeeded, want error", in)
		}
	}
}

func TestOptionsFromEnv(t *testing.T) {
	t.Setenv(housekeeping.EnvOutboxRetention, "")
	t.Setenv(housekeeping.EnvProcessedRetention, "")
	t.Setenv(housekeeping.EnvDeadLetterRetention, "")
	t.Setenv(housekeeping.EnvInterval, "")
	o, err := housekeeping.OptionsFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if o.OutboxRetention != 24*time.Hour || o.ProcessedRetention != 7*24*time.Hour || o.Interval != 10*time.Minute ||
		o.DeadLetterRetention != 30*24*time.Hour {
		t.Fatalf("defaults wrong: %+v", o)
	}

	t.Setenv(housekeeping.EnvOutboxRetention, "2d")
	t.Setenv(housekeeping.EnvProcessedRetention, "30d")
	t.Setenv(housekeeping.EnvInterval, "5m")
	t.Setenv(housekeeping.EnvDeadLetterRetention, "3d")
	o, err = housekeeping.OptionsFromEnv()
	if err != nil || o.DeadLetterRetention != 72*time.Hour || o.OutboxRetention != 48*time.Hour || o.ProcessedRetention != 30*24*time.Hour || o.Interval != 5*time.Minute {
		t.Fatalf("env not applied: %+v %v", o, err)
	}

	t.Setenv(housekeeping.EnvInterval, "5m")
	t.Setenv(housekeeping.EnvDeadLetterRetention, "30s")
	if _, err := housekeeping.OptionsFromEnv(); err == nil {
		t.Error("DEAD_LETTER_RETENTION=30s accepted")
	}
	t.Setenv(housekeeping.EnvDeadLetterRetention, "")
	for _, bad := range []string{"30s", "nope", "-1h"} {
		t.Setenv(housekeeping.EnvInterval, bad)
		if _, err := housekeeping.OptionsFromEnv(); err == nil {
			t.Errorf("PRUNE_INTERVAL=%q accepted", bad)
		}
	}
}

func TestPruneOnce(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := newThrowawayDB(ctx, t, base)
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}

	// 25 old published, 5 recent published, 5 old unpublished, 5 recent unpublished outbox rows.
	if _, err := pool.Exec(ctx, `
INSERT INTO outbox (id, topic, key, envelope, created_at, published_at)
SELECT gen_random_uuid(), 't', 'k', '\x00', now() - interval '3 days', now() - interval '2 days' FROM generate_series(1, 25);
INSERT INTO outbox (id, topic, key, envelope, created_at, published_at)
SELECT gen_random_uuid(), 't', 'k', '\x00', now(), now() FROM generate_series(1, 5);
INSERT INTO outbox (id, topic, key, envelope, created_at)
SELECT gen_random_uuid(), 't', 'k', '\x00', now() - interval '30 days' FROM generate_series(1, 5);
INSERT INTO outbox (id, topic, key, envelope, created_at)
SELECT gen_random_uuid(), 't', 'k', '\x00', now() FROM generate_series(1, 5);
INSERT INTO processed_events (consumer, event_id, processed_at)
SELECT 'c', gen_random_uuid(), now() - interval '10 days' FROM generate_series(1, 12);
INSERT INTO processed_events (consumer, event_id, processed_at)
SELECT 'c', gen_random_uuid(), now() - interval '1 day' FROM generate_series(1, 4);
INSERT INTO dead_letter (consumer, event_id, topic, payload, error, created_at)
SELECT 'c', gen_random_uuid(), 't', '\x00', 'boom', now() - interval '40 days' FROM generate_series(1, 3);
INSERT INTO dead_letter (consumer, event_id, topic, payload, error)
SELECT 'c', gen_random_uuid(), 't', '\x00', 'boom' FROM generate_series(1, 2);`); err != nil {
		t.Fatal(err)
	}

	// Batch size 10 forces several statements (25 -> 10+10+5, 12 -> 10+2).
	opts := housekeeping.Options{BatchSize: 10}
	o, p, dl, err := housekeeping.PruneOnce(ctx, pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	if o != 25 || p != 12 || dl != 3 {
		t.Fatalf("deleted outbox=%d processed=%d dead_letter=%d, want 25, 12 and 3", o, p, dl)
	}
	assertCount(ctx, t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`, 5)
	assertCount(ctx, t, pool, `SELECT count(*) FROM outbox WHERE published_at IS NULL`, 10)
	assertCount(ctx, t, pool, `SELECT count(*) FROM processed_events`, 4)
	assertCount(ctx, t, pool, `SELECT count(*) FROM dead_letter`, 2)

	// A second sweep has nothing to do.
	o, p, dl, err = housekeeping.PruneOnce(ctx, pool, opts)
	if err != nil || o != 0 || p != 0 || dl != 0 {
		t.Fatalf("second sweep = %d, %d, %d, %v", o, p, dl, err)
	}
}

func TestRunPrunesAfterInitialDelayAndStopsOnCancel(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := newThrowawayDB(ctx, t, base)
	if _, err := pool.Exec(ctx, ddl+`
INSERT INTO outbox (id, topic, key, envelope, published_at) VALUES (gen_random_uuid(), 't', 'k', '\x00', now() - interval '2 days');`); err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- housekeeping.Run(runCtx, pool, housekeeping.Options{InitialDelay: 50 * time.Millisecond})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old published row was not pruned")
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func assertCount(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, q).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", q, got, want)
	}
}

func newThrowawayDB(ctx context.Context, t *testing.T, base string) *pgxpool.Pool {
	t.Helper()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	name := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		ctx := context.WithoutCancel(ctx)
		c, err := pgx.Connect(ctx, base)
		if err != nil {
			return
		}
		defer c.Close(ctx)
		_, _ = c.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
	})
	return pool
}

func TestEnvDuration(t *testing.T) {
	const name = "HK_TEST_RETENTION"
	t.Setenv(name, "")
	if d, err := housekeeping.EnvDuration(name, 7*24*time.Hour); err != nil || d != 7*24*time.Hour {
		t.Fatalf("default = %v, %v", d, err)
	}
	t.Setenv(name, "2d")
	if d, err := housekeeping.EnvDuration(name, time.Hour); err != nil || d != 48*time.Hour {
		t.Fatalf("2d = %v, %v", d, err)
	}
	for _, bad := range []string{"30s", "nope", "-1h"} {
		t.Setenv(name, bad)
		if _, err := housekeeping.EnvDuration(name, time.Hour); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestExtraPrune(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := newThrowawayDB(ctx, t, base)
	if _, err := pool.Exec(ctx, ddl+`
CREATE TABLE extra (id serial PRIMARY KEY, at timestamptz NOT NULL);
INSERT INTO extra (at) SELECT now() - interval '3 days' FROM generate_series(1, 25);
INSERT INTO extra (at) SELECT now() FROM generate_series(1, 4);`); err != nil {
		t.Fatal(err)
	}
	opts := housekeeping.Options{BatchSize: 10, Extra: []housekeeping.Prune{{
		Name: "EXTRA", Retention: 24 * time.Hour,
		SQL: `DELETE FROM extra WHERE id IN (SELECT id FROM extra WHERE at < now() - ($1 * interval '1 second') LIMIT $2)`,
	}}}
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := housekeeping.PruneOnce(ctx, pool, opts); err != nil {
		t.Fatal(err)
	}
	assertCount(ctx, t, pool, `SELECT count(*) FROM extra`, 4)
	opts.Extra[0].Retention = time.Second
	if err := opts.Validate(); err == nil {
		t.Error("retention below minimum accepted")
	}
}
