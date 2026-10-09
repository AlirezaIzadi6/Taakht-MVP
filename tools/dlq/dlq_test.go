package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
)

func envelope(t *testing.T, typ, aggregate string, payload []byte) (id string, raw []byte) {
	t.Helper()
	id = uuid.NewString()
	raw, err := proto.Marshal(&commonv1.Envelope{
		EventId: id, Type: typ, AggregateId: aggregate, OccurredAt: timestamppb.Now(), Payload: payload, RequestId: "req-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return id, raw
}

func TestDescribeKnownType(t *testing.T) {
	payload, _ := proto.Marshal(&adv1.AdHidden{AdId: "ad-123", Seq: 7})
	id, raw := envelope(t, "taakht.ad.v1.AdHidden", "ad-123", payload)
	out := Describe(Entry{Consumer: "matching", EventID: id, Topic: "ad.events", Error: "boom", CreatedAt: time.Now(), Payload: raw})
	for _, want := range []string{"matching", id, "ad.events", "boom", "taakht.ad.v1.AdHidden", "req-1", `"adId": "ad-123"`, `"seq": "7"`} {
		if !strings.Contains(out, want) {
			t.Errorf("Describe output lacks %q:\n%s", want, out)
		}
	}
	if TypeOf(raw) != "taakht.ad.v1.AdHidden" {
		t.Errorf("TypeOf = %q", TypeOf(raw))
	}
}

func TestDescribeUnknownAndUndecodable(t *testing.T) {
	// Unknown type: the payload prints as hex.
	_, raw := envelope(t, "taakht.nope.v1.Mystery", "x", []byte{0xde, 0xad, 0xbe, 0xef})
	out := Describe(Entry{Consumer: "c", EventID: uuid.NewString(), Topic: "t", Payload: raw})
	if !strings.Contains(out, "deadbeef") || !strings.Contains(out, "unknown type") {
		t.Errorf("unknown type not shown as hex:\n%s", out)
	}
	// Known type whose payload does not parse (the poison-message case): hex too.
	_, raw = envelope(t, "taakht.ad.v1.AdPublished", "x", []byte{0xff, 0xff, 0xff})
	out = Describe(Entry{Consumer: "c", EventID: uuid.NewString(), Topic: "t", Payload: raw})
	if !strings.Contains(out, "ffffff") || !strings.Contains(out, "cannot be decoded") {
		t.Errorf("undecodable payload not shown as hex:\n%s", out)
	}
	// Bytes that are not an envelope at all.
	out = Describe(Entry{Consumer: "c", EventID: uuid.NewString(), Topic: "t", Payload: []byte{0xff, 0x01}})
	if !strings.Contains(out, "undecodable") || !strings.Contains(out, "ff01") {
		t.Errorf("non-envelope not shown as hex:\n%s", out)
	}
	if TypeOf([]byte{0xff, 0x01}) != "?" {
		t.Error("TypeOf of garbage should be ?")
	}
}

func TestConfirm(t *testing.T) {
	var out bytes.Buffer
	if err := confirm("go", true, strings.NewReader(""), &out); err != nil {
		t.Errorf("--yes: %v", err)
	}
	if err := confirm("go", false, strings.NewReader("y\n"), &out); err != nil {
		t.Errorf("interactive y: %v", err)
	}
	for _, in := range []string{"n\n", "\n", ""} {
		if err := confirm("go", false, strings.NewReader(in), &out); err == nil {
			t.Errorf("input %q confirmed", in)
		}
	}
	// A real stdin that is not a terminal (the null device is a char device on Linux, a pipe elsewhere).
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if err := confirm("go", false, r, &out); err == nil {
		t.Error("piped stdin confirmed without --yes")
	}
}

func TestReplayRefusesNonEnvelopeAndMissingKey(t *testing.T) {
	p := &fakeProducer{}
	err := Replay(context.Background(), nil, p, Entry{Payload: []byte{0xff, 0x01}}, "", false)
	if err == nil || len(p.sent) != 0 {
		t.Fatalf("non-envelope replayed: %v", err)
	}
	_, raw := envelope(t, "taakht.ad.v1.AdHidden", "", nil)
	err = Replay(context.Background(), nil, p, Entry{Payload: raw}, "", false)
	if err == nil || len(p.sent) != 0 {
		t.Fatalf("keyless envelope replayed: %v", err)
	}
}

type sent struct {
	topic, key string
	value      []byte
}

type fakeProducer struct {
	sent []sent
	err  error
}

func (f *fakeProducer) Produce(_ context.Context, topic, key string, value []byte) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, sent{topic, key, value})
	return nil
}

func throwawayDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	name := "dlq_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
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
		_, _ = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name))
		admin.Close()
	})
	if _, err := pool.Exec(ctx, `
CREATE TABLE processed_events (consumer text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (consumer, event_id));
CREATE TABLE dead_letter (consumer text NOT NULL, event_id uuid NOT NULL, topic text NOT NULL, payload bytea NOT NULL, error text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (consumer, event_id));`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func park(t *testing.T, pool *pgxpool.Pool, consumer, topic string, raw []byte, id string, age time.Duration) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO dead_letter (consumer, event_id, topic, payload, error, created_at)
VALUES ($1, $2, $3, $4, 'decode failed', now() - ($5 * interval '1 second'))`, consumer, id, topic, raw, age.Seconds()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)`, consumer, id); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, q string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestListFindReplayPurge(t *testing.T) {
	pool := throwawayDB(t)
	ctx := context.Background()
	payload, _ := proto.Marshal(&adv1.AdHidden{AdId: "agg-1", Seq: 1})
	id1, raw1 := envelope(t, "taakht.ad.v1.AdHidden", "agg-1", payload)
	id2, raw2 := envelope(t, "taakht.ad.v1.AdHidden", "agg-2", payload)
	id3, raw3 := envelope(t, "taakht.ad.v1.AdHidden", "agg-3", payload)
	park(t, pool, "matching", "ad.events", raw1, id1, time.Minute)
	park(t, pool, "matching", "ad.events", raw2, id2, time.Hour)
	park(t, pool, "matching", "ad.events", raw3, id3, 40*24*time.Hour)

	entries, err := List(ctx, pool, 2)
	if err != nil || len(entries) != 2 || entries[0].EventID != id1 || entries[1].EventID != id2 {
		t.Fatalf("List newest first with limit: %v %v", entries, err)
	}
	if all, err := List(ctx, pool, 0); err != nil || len(all) != 3 {
		t.Fatalf("List all: %d %v", len(all), err)
	}
	got, err := Find(ctx, pool, strings.ToUpper(id1), "")
	if err != nil || len(got) != 1 || got[0].Topic != "ad.events" || !bytes.Equal(got[0].Payload, raw1) {
		t.Fatalf("Find: %v %v", got, err)
	}
	if none, _ := Find(ctx, pool, id1, "other"); len(none) != 0 {
		t.Fatal("Find ignored the consumer filter")
	}

	// A failing produce keeps everything, including the processed marker.
	p := &fakeProducer{err: errors.New("broker down")}
	if err := Replay(ctx, pool, p, got[0], "", true); err == nil {
		t.Fatal("produce failure not reported")
	}
	if count(t, pool, `SELECT count(*) FROM processed_events WHERE event_id = $1`, id1) != 1 || count(t, pool, `SELECT count(*) FROM dead_letter`) != 3 {
		t.Fatal("failed replay changed state")
	}

	// A successful replay publishes the original bytes with the aggregate key and frees the processed marker.
	p = &fakeProducer{}
	if err := Replay(ctx, pool, p, got[0], "", false); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || p.sent[0].topic != "ad.events" || p.sent[0].key != "agg-1" || !bytes.Equal(p.sent[0].value, raw1) {
		t.Fatalf("sent %+v", p.sent)
	}
	if count(t, pool, `SELECT count(*) FROM processed_events WHERE event_id = $1`, id1) != 0 || count(t, pool, `SELECT count(*) FROM dead_letter WHERE event_id = $1`, id1) != 1 {
		t.Fatal("replay without --delete must clear the marker and keep the row")
	}
	// With deleteRow the row goes too; --key overrides the key.
	e2, _ := Find(ctx, pool, id2, "")
	p = &fakeProducer{}
	if err := Replay(ctx, pool, p, e2[0], "forced", true); err != nil || p.sent[0].key != "forced" {
		t.Fatalf("replay --delete: %v %+v", err, p.sent)
	}
	if count(t, pool, `SELECT count(*) FROM dead_letter WHERE event_id = $1`, id2) != 0 {
		t.Fatal("--delete kept the row")
	}

	// Purge removes only rows older than the cutoff.
	n, err := Purge(ctx, pool, 30*24*time.Hour)
	if err != nil || n != 1 {
		t.Fatalf("Purge = %d, %v", n, err)
	}
	if count(t, pool, `SELECT count(*) FROM dead_letter`) != 1 {
		t.Fatal("purge removed the wrong rows")
	}
}

// TestReplayThroughKafka publishes a replayed envelope to a real broker and reads it back.
func TestReplayThroughKafka(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	pool := throwawayDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := "dlq-test-" + uuid.NewString()[:8]
	payload, _ := proto.Marshal(&adv1.AdHidden{AdId: "agg-k", Seq: 3})
	id, raw := envelope(t, "taakht.ad.v1.AdHidden", "agg-k", payload)
	park(t, pool, "matching", topic, raw, id, time.Minute)

	seeds := strings.Split(brokers, ",")
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	entries, err := Find(ctx, pool, id, "")
	if err != nil || len(entries) != 1 {
		t.Fatalf("Find: %v %v", entries, err)
	}
	if err := Replay(ctx, pool, kafkaProducer{cl}, entries[0], "", true); err != nil {
		t.Fatal(err)
	}

	reader, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	fetches := reader.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		t.Fatal(err)
	}
	recs := fetches.Records()
	if len(recs) != 1 || string(recs[0].Key) != "agg-k" || !bytes.Equal(recs[0].Value, raw) {
		t.Fatalf("records: %+v", recs)
	}
	if count(t, pool, `SELECT count(*) FROM dead_letter`) != 0 {
		t.Fatal("row not deleted")
	}
}

func TestRunValidation(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	for _, args := range [][]string{
		{},
		{"bogus"},
		{"--db", "postgres://x", "bogus"},
		{"list"}, // no database
		{"--bad-flag", "list"},
	} {
		t.Setenv("DATABASE_URL", "")
		if err := run(ctx, args, strings.NewReader(""), &out); err == nil {
			t.Errorf("run(%v) succeeded", args)
		}
	}
}
