package goplatform_test

import (
	"context"
	"os"
	"testing"
	"testing/fstest"
	"time"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/libs/goplatform/db"
	"github.com/taakht/taakht/libs/goplatform/observe"
	"github.com/taakht/taakht/libs/goplatform/outbox"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

// TestRequestIDCrossesOutboxAndConsumer: the request id of the writing call is stored in the envelope and restored
// into the context of the consumer's handler (no Kafka needed: Process is what the consumer loop runs per event).
func TestRequestIDCrossesOutboxAndConsumer(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := NewThrowawayDB(ctx, t, base)
	if err := db.Migrate(ctx, pool, fstest.MapFS{"m/001_init.sql": {Data: []byte(ddl)}}, "m"); err != nil {
		t.Fatal(err)
	}

	writeCtx := observe.WithRequestID(ctx, "req-abc")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Add(writeCtx, tx, "t", "k", &adv1.AdHidden{AdId: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT envelope FROM outbox`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	env := &commonv1.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		t.Fatal(err)
	}
	if env.GetRequestId() != "req-abc" {
		t.Fatalf("envelope request_id = %q, want req-abc", env.GetRequestId())
	}

	var inHandler string
	err = consume.Process(ctx, pool, "g", env, func(ctx context.Context, _ pgx.Tx, _ *commonv1.Envelope) error {
		inHandler = observe.RequestID(ctx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if inHandler != "req-abc" {
		t.Fatalf("request id in the consumer handler = %q, want req-abc", inHandler)
	}
}

// TestOutboxCheckAgainstPostgres: a fresh unpublished row keeps the service ready, an old one (a stuck relay) does not,
// and publishing it makes the service ready again.
func TestOutboxCheckAgainstPostgres(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := NewThrowawayDB(ctx, t, base)
	if err := db.Migrate(ctx, pool, fstest.MapFS{"m/001_init.sql": {Data: []byte(ddl)}}, "m"); err != nil {
		t.Fatal(err)
	}
	chk := observe.OutboxCheck(pool, 60*time.Second)
	if err := chk.Fn(ctx); err != nil {
		t.Fatalf("empty outbox: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO outbox (id, topic, key, envelope, created_at)
		VALUES (gen_random_uuid(), 't', 'k', '\x'::bytea, now())`); err != nil {
		t.Fatal(err)
	}
	if err := chk.Fn(ctx); err != nil {
		t.Fatalf("fresh row must be tolerated: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox SET created_at = now() - interval '5 minutes'`); err != nil {
		t.Fatal(err)
	}
	if err := chk.Fn(ctx); err == nil {
		t.Fatal("5-minute-old unpublished row must fail readiness")
	}
	count, age, err := observe.OutboxBacklog(ctx, pool)
	if err != nil || count != 1 || age < 4*time.Minute {
		t.Fatalf("backlog = %d, %s, %v", count, age, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now()`); err != nil {
		t.Fatal(err)
	}
	if err := chk.Fn(ctx); err != nil {
		t.Fatalf("published row must not count: %v", err)
	}
}
