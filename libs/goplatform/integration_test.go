package goplatform_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/libs/goplatform/db"
	"github.com/taakht/taakht/libs/goplatform/outbox"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

const ddl = `
CREATE TABLE outbox (
  id uuid PRIMARY KEY, topic text NOT NULL, key text NOT NULL, envelope bytea NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
CREATE TABLE processed_events (
  consumer text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id));
CREATE TABLE dead_letter (
  consumer text NOT NULL, event_id uuid NOT NULL, topic text NOT NULL, payload bytea NOT NULL, error text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (consumer, event_id));
CREATE TABLE seen (event_id uuid, ad_id text);
`

// TestOutboxKafkaConsume runs only against real infra (make up).
func TestOutboxKafkaConsume(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	brokers := os.Getenv("KAFKA_BROKERS")
	if base == "" || brokers == "" {
		t.Skip("TEST_DATABASE_URL and KAFKA_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := NewThrowawayDB(ctx, t, base)
	if err := db.Migrate(ctx, pool, fstest.MapFS{"m/001_init.sql": {Data: []byte(ddl)}}, "m"); err != nil {
		t.Fatal(err)
	}
	// Second run must be a no-op.
	if err := db.Migrate(ctx, pool, fstest.MapFS{"m/001_init.sql": {Data: []byte(ddl)}}, "m"); err != nil {
		t.Fatal(err)
	}

	topic := "test.events." + uuid.NewString()
	adID := uuid.NewString()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Add(ctx, tx, topic, adID, &adv1.AdHidden{AdId: adID}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	bs := strings.Split(brokers, ",")
	go func() { _ = outbox.RunRelay(ctx, pool, bs) }()
	handlers := map[string]consume.Handler{
		"taakht.ad.v1.AdHidden": func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
			m := &adv1.AdHidden{}
			if err := proto.Unmarshal(env.Payload, m); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO seen VALUES ($1, $2)`, env.EventId, m.AdId)
			return err
		},
	}
	go func() {
		_ = consume.Run(ctx, pool, bs, "itest-"+uuid.NewString(), []string{topic}, handlers)
	}()

	deadline := time.Now().Add(45 * time.Second)
	for {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM seen WHERE ad_id = $1`, adID).Scan(&n)
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("event was not consumed in time")
		}
		time.Sleep(200 * time.Millisecond)
	}
	var published bool
	if err := pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox`).Scan(&published); err != nil || !published {
		t.Fatalf("outbox row not marked published: %v %v", published, err)
	}

	// Redelivery of the same event id must not run the handler twice.
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT envelope FROM outbox`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	env := &commonv1.Envelope{}
	if err := proto.Unmarshal(raw, env); err != nil {
		t.Fatal(err)
	}
	if err := consume.Process(ctx, pool, "dup", env, handlers["taakht.ad.v1.AdHidden"]); err != nil {
		t.Fatal(err)
	}
	if err := consume.Process(ctx, pool, "dup", env, handlers["taakht.ad.v1.AdHidden"]); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM seen`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 { // one from Kafka consumer, one from the first "dup" Process
		t.Fatalf("seen rows = %d, want 2", n)
	}
}

// TestPermanentFailureDoesNotBlockPartition: a poison event is skipped (and recorded) so the next one is handled.
func TestPermanentFailureDoesNotBlockPartition(t *testing.T) {
	base := os.Getenv("TEST_DATABASE_URL")
	brokers := os.Getenv("KAFKA_BROKERS")
	if base == "" || brokers == "" {
		t.Skip("TEST_DATABASE_URL and KAFKA_BROKERS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := NewThrowawayDB(ctx, t, base)
	if err := db.Migrate(ctx, pool, fstest.MapFS{"m/001_init.sql": {Data: []byte(ddl)}}, "m"); err != nil {
		t.Fatal(err)
	}
	topic := "test.events." + uuid.NewString()
	key := uuid.NewString()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Add(ctx, tx, topic, key, &adv1.AdPublished{Ad: &adv1.Ad{Id: key}}); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Add(ctx, tx, topic, key, &adv1.AdHidden{AdId: key}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	bs := strings.Split(brokers, ",")
	go func() { _ = outbox.RunRelay(ctx, pool, bs) }()
	handlers := map[string]consume.Handler{
		"taakht.ad.v1.AdPublished": func(context.Context, pgx.Tx, *commonv1.Envelope) error {
			return consume.Permanent(fmt.Errorf("poison"))
		},
		"taakht.ad.v1.AdHidden": func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
			_, err := tx.Exec(ctx, `INSERT INTO seen VALUES ($1, $2)`, env.EventId, key)
			return err
		},
	}
	group := "itest-" + uuid.NewString()
	go func() { _ = consume.Run(ctx, pool, bs, group, []string{topic}, handlers) }()
	deadline := time.Now().Add(45 * time.Second)
	for {
		var seen, processed int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM seen WHERE ad_id = $1`, key).Scan(&seen)
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE consumer = $1`, group).Scan(&processed)
		if seen == 1 && processed == 2 {
			var topicGot, errText string
			var payload []byte
			if err := pool.QueryRow(ctx, `SELECT topic, payload, error FROM dead_letter WHERE consumer = $1`, group).
				Scan(&topicGot, &payload, &errText); err != nil {
				t.Fatalf("dead_letter row missing: %v", err)
			}
			env := &commonv1.Envelope{}
			if err := proto.Unmarshal(payload, env); err != nil || env.GetType() != "taakht.ad.v1.AdPublished" {
				t.Fatalf("dead_letter payload = %v, %v", env, err)
			}
			if topicGot != topic || !strings.Contains(errText, "poison") {
				t.Fatalf("dead_letter topic=%q error=%q", topicGot, errText)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("seen=%d processed=%d, want 1 and 2", seen, processed)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// NewThrowawayDB creates an empty database next to base and drops it at test end.
func NewThrowawayDB(ctx context.Context, t *testing.T, base string) *pgxpool.Pool {
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
