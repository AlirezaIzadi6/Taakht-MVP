package goplatform_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const hiddenType = "taakht.ad.v1.AdHidden"

const orderDDL = ddl + `CREATE TABLE handled (n bigserial PRIMARY KEY, key text NOT NULL, seq bigint NOT NULL, event_id uuid NOT NULL);`

func infra(t *testing.T) (string, []string) {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	brokers := os.Getenv("KAFKA_BROKERS")
	if base == "" || brokers == "" {
		t.Skip("TEST_DATABASE_URL and KAFKA_BROKERS not set")
	}
	return base, strings.Split(brokers, ",")
}

func newDB(ctx context.Context, t *testing.T, base string) *pgxpool.Pool {
	t.Helper()
	pool := NewThrowawayDB(ctx, t, base)
	if err := db.Migrate(ctx, pool, fstest.MapFS{"m/001_init.sql": {Data: []byte(orderDDL)}}, "m"); err != nil {
		t.Fatal(err)
	}
	return pool
}

func envelope(t *testing.T, key string, seq int64) *commonv1.Envelope {
	t.Helper()
	payload, err := proto.Marshal(&adv1.AdHidden{AdId: key, Seq: seq})
	if err != nil {
		t.Fatal(err)
	}
	return &commonv1.Envelope{EventId: uuid.NewString(), Type: hiddenType, AggregateId: key, Payload: payload}
}

// produce sends seq 1..perKey for every key (round robin over keys, so partitions interleave) and returns
// the partition each key landed on.
func produce(ctx context.Context, t *testing.T, brokers []string, topic string, keys []string, perKey int) map[string]int32 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation(), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for seq := 1; seq <= perKey; seq++ {
		for _, k := range keys {
			raw, err := proto.Marshal(envelope(t, k, int64(seq)))
			if err != nil {
				t.Fatal(err)
			}
			recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(k), Value: raw})
		}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	parts := map[string]int32{}
	for _, r := range recs {
		parts[string(r.Key)] = r.Partition
	}
	return parts
}

func recordHandled(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
	m := &adv1.AdHidden{}
	if err := consume.Decode(env, m); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO handled (key, seq, event_id) VALUES ($1, $2, $3)`, m.AdId, m.Seq, env.EventId)
	return err
}

func countHandled(ctx context.Context, pool *pgxpool.Pool) int {
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM handled`).Scan(&n)
	return n
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertExactlyOnceInOrder checks every (key, seq) was handled exactly once and that per key seq rose with the handling order.
func assertExactlyOnceInOrder(ctx context.Context, t *testing.T, pool *pgxpool.Pool, keys []string, perKey int) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT key, seq FROM handled ORDER BY n`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	last := map[string]int64{}
	total := 0
	for rows.Next() {
		var k string
		var seq int64
		if err := rows.Scan(&k, &seq); err != nil {
			t.Fatal(err)
		}
		if seq != last[k]+1 {
			t.Fatalf("key %s handled seq %d after %d: lost, duplicated or reordered", k, seq, last[k])
		}
		last[k] = seq
		total++
	}
	if total != len(keys)*perKey {
		t.Fatalf("handled %d events, want %d", total, len(keys)*perKey)
	}
}

func manyKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d-%s", i, uuid.NewString()[:8])
	}
	return keys
}

// TestParallelConsumeKeepsPerKeyOrderAndRunsPartitionsTogether: with CONSUMER_CONCURRENCY style parallelism every
// event is handled exactly once, per key in order, and handlers of different partitions overlap in time.
func TestParallelConsumeKeepsPerKeyOrderAndRunsPartitionsTogether(t *testing.T) {
	base, brokers := infra(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := newDB(ctx, t, base)
	topic := "test.parallel." + uuid.NewString()
	keys := manyKeys(24)
	const perKey = 25
	produce(ctx, t, brokers, topic, keys, perKey)

	var running, maxRunning atomic.Int32
	handlers := map[string]consume.Handler{hiddenType: func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		n := running.Add(1)
		defer running.Add(-1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(3 * time.Millisecond)
		return recordHandled(ctx, tx, env)
	}}
	group := "itest-" + uuid.NewString()
	done := make(chan error, 1)
	go func() {
		done <- consume.RunWith(ctx, pool, brokers, group, []string{topic}, handlers, consume.Options{Concurrency: 4})
	}()
	waitFor(t, 60*time.Second, "all events", func() bool { return countHandled(ctx, pool) >= len(keys)*perKey })
	cancel()
	<-done
	assertExactlyOnceInOrder(context.Background(), t, pool, keys, perKey)
	if maxRunning.Load() < 2 {
		t.Fatalf("max concurrent handlers = %d, want at least 2 with Concurrency 4", maxRunning.Load())
	}
}

// TestParallelConsumeStuckPartitionDoesNotBlockTheOthers: a transient failure that never clears blocks only its own
// partition; once it clears the stuck keys catch up, in order.
func TestParallelConsumeStuckPartitionDoesNotBlockTheOthers(t *testing.T) {
	base, brokers := infra(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := newDB(ctx, t, base)
	topic := "test.stuck." + uuid.NewString()
	keys := manyKeys(20)
	const perKey = 5
	parts := produce(ctx, t, brokers, topic, keys, perKey)
	stuckKey := keys[0]
	stuckPartition := parts[stuckKey]
	var other string
	for _, k := range keys {
		if parts[k] != stuckPartition {
			other = k
			break
		}
	}
	if other == "" {
		t.Skip("all keys landed on one partition")
	}

	var release atomic.Bool
	handlers := map[string]consume.Handler{hiddenType: func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		m := &adv1.AdHidden{}
		if err := consume.Decode(env, m); err != nil {
			return err
		}
		if m.AdId == stuckKey && m.Seq == 2 && !release.Load() {
			return errors.New("transient: dependency down")
		}
		return recordHandled(ctx, tx, env)
	}}
	group := "itest-" + uuid.NewString()
	done := make(chan error, 1)
	go func() {
		done <- consume.RunWith(ctx, pool, brokers, group, []string{topic}, handlers, consume.Options{Concurrency: 3})
	}()

	otherPartitionDone := func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM handled WHERE key = $1`, other).Scan(&n)
		return n == perKey
	}
	waitFor(t, 60*time.Second, "a key on another partition to finish while one partition is stuck", otherPartitionDone)
	var stuckHandled int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM handled WHERE key = $1`, stuckKey).Scan(&stuckHandled); err != nil {
		t.Fatal(err)
	}
	if stuckHandled != 1 {
		t.Fatalf("stuck key handled %d events while blocked, want only seq 1", stuckHandled)
	}

	release.Store(true)
	waitFor(t, 60*time.Second, "everything after the failure cleared", func() bool { return countHandled(ctx, pool) == len(keys)*perKey })
	cancel()
	<-done
	assertExactlyOnceInOrder(context.Background(), t, pool, keys, perKey)
}

// TestParallelConsumeSurvivesARebalance: a second member of the group joins while the first is working; no event is
// lost, none is handled twice and per-key order holds across the hand-over.
func TestParallelConsumeSurvivesARebalance(t *testing.T) {
	base, brokers := infra(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool := newDB(ctx, t, base)
	topic := "test.rebalance." + uuid.NewString()
	keys := manyKeys(30)
	const perKey = 40
	produce(ctx, t, brokers, topic, keys, perKey)

	handlers := map[string]consume.Handler{hiddenType: func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		time.Sleep(4 * time.Millisecond)
		return recordHandled(ctx, tx, env)
	}}
	group := "itest-" + uuid.NewString()
	var wg sync.WaitGroup
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consume.RunWith(ctxA, pool, brokers, group, []string{topic}, handlers, consume.Options{Concurrency: 3})
	}()
	waitFor(t, 60*time.Second, "first member to make progress", func() bool { return countHandled(ctx, pool) >= 50 })
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consume.RunWith(ctx, pool, brokers, group, []string{topic}, handlers, consume.Options{Concurrency: 3})
	}()
	waitFor(t, 100*time.Second, "all events", func() bool { return countHandled(ctx, pool) >= len(keys)*perKey })
	cancelA()
	cancel()
	wg.Wait()
	assertExactlyOnceInOrder(context.Background(), t, pool, keys, perKey)
}

// TestAtLeastOnceHandlerRunsOutsideTheTransaction: the handler runs before the processed_events row exists and with
// no transaction of ours open; a failure leaves no row (redelivery runs it again); a success inserts the row;
// a redelivery then skips the handler; a permanent failure is parked in dead_letter.
func TestAtLeastOnceHandlerRunsOutsideTheTransaction(t *testing.T) {
	base, brokers := infra(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := newDB(ctx, t, base)
	group := "itest-" + uuid.NewString()

	// Direct (no Kafka): semantics of ProcessAtLeastOnce.
	env := envelope(t, "k", 1)
	var calls int
	failFirst := func(ctx context.Context, e *commonv1.Envelope) error {
		calls++
		var inTx int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'idle in transaction'`).Scan(&inTx); err != nil {
			return err
		}
		if inTx != 0 {
			return fmt.Errorf("%d transactions are open while the handler runs", inTx)
		}
		var row int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE event_id = $1`, e.EventId).Scan(&row); err != nil {
			return err
		}
		if row != 0 {
			return errors.New("processed_events row exists before the handler succeeded")
		}
		if calls == 1 {
			return errors.New("transient")
		}
		return nil
	}
	if err := consume.ProcessAtLeastOnce(ctx, pool, group, env, failFirst); err == nil || err.Error() != "transient" {
		t.Fatalf("first attempt error = %v, want transient", err)
	}
	var rows int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE consumer = $1`, group).Scan(&rows)
	if rows != 0 {
		t.Fatalf("failed handler left %d processed_events rows", rows)
	}
	if err := consume.ProcessAtLeastOnce(ctx, pool, group, env, failFirst); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE consumer = $1`, group).Scan(&rows)
	if rows != 1 || calls != 2 {
		t.Fatalf("rows=%d calls=%d, want 1 and 2", rows, calls)
	}
	if err := consume.ProcessAtLeastOnce(ctx, pool, group, env, failFirst); err != nil || calls != 2 {
		t.Fatalf("redelivery after success must skip the handler: err=%v calls=%d", err, calls)
	}

	// Through Kafka: success, permanent failure and a transient failure that clears, in one partition order.
	topic := "test.atleastonce." + uuid.NewString()
	var attempts atomic.Int32
	ext := map[string]consume.ExternalHandler{hiddenType: func(ctx context.Context, e *commonv1.Envelope) error {
		m := &adv1.AdHidden{}
		if err := consume.Decode(e, m); err != nil {
			return err
		}
		switch m.Seq {
		case 2:
			return consume.Permanent(errors.New("poison"))
		case 3:
			if attempts.Add(1) < 3 {
				return errors.New("transient")
			}
		}
		_, err := pool.Exec(ctx, `INSERT INTO handled (key, seq, event_id) VALUES ($1, $2, $3)`, m.AdId, m.Seq, e.EventId)
		return err
	}}
	key := uuid.NewString()
	produce(ctx, t, brokers, topic, []string{key}, 4)
	done := make(chan error, 1)
	go func() {
		done <- consume.RunWith(ctx, pool, brokers, group+"-k", []string{topic}, nil, consume.Options{Concurrency: 1, AtLeastOnce: ext})
	}()
	waitFor(t, 45*time.Second, "events 1, 3 and 4", func() bool { return countHandled(ctx, pool) == 3 })
	cancel()
	<-done
	var dead int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter WHERE consumer = $1`, group+"-k").Scan(&dead)
	var processed int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM processed_events WHERE consumer = $1`, group+"-k").Scan(&processed)
	if dead != 1 || processed != 4 {
		t.Fatalf("dead_letter=%d processed_events=%d, want 1 and 4", dead, processed)
	}
	if attempts.Load() != 3 {
		t.Fatalf("transient event attempted %d times, want 3", attempts.Load())
	}
}

// TestTwoRelaysPublishEachRowOnceAndInOrder: two relay instances on one database; the advisory lock lets only
// one relay at a time, every row reaches Kafka exactly once and per key in creation order, and the standby takes
// over when the active instance stops.
func TestTwoRelaysPublishEachRowOnceAndInOrder(t *testing.T) {
	base, brokers := infra(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool := newDB(ctx, t, base)
	pool2 := openSecond(ctx, t, pool)
	topic := "test.relay." + uuid.NewString()
	keys := manyKeys(10)

	add := func(from, to int) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for seq := from; seq <= to; seq++ {
			for _, k := range keys {
				if err := outbox.Add(ctx, tx, topic, k, &adv1.AdHidden{AdId: k, Seq: int64(seq)}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	add(1, 40) // 400 rows: several batches

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	relay1 := make(chan error, 1)
	relay2 := make(chan error, 1)
	go func() { relay1 <- outbox.RunRelay(ctx1, pool, brokers) }()
	waitFor(t, 20*time.Second, "first relay to hold the lock", func() bool { return advisoryLocks(ctx, pool) == 1 })
	go func() { relay2 <- outbox.RunRelay(ctx, pool2, brokers) }()

	waitFor(t, 30*time.Second, "all rows published", func() bool { return unpublished(ctx, pool) == 0 })
	time.Sleep(1500 * time.Millisecond) // the standby must not hold the lock meanwhile
	if n := advisoryLocks(ctx, pool); n != 1 {
		t.Fatalf("advisory relay locks held = %d, want exactly 1", n)
	}

	// Stop the active relay: the standby takes over within a few seconds and publishes new rows.
	cancel1()
	<-relay1
	add(41, 45)
	waitFor(t, 30*time.Second, "standby to take over", func() bool { return unpublished(ctx, pool) == 0 })

	seen := consumeAll(ctx, t, brokers, topic, len(keys)*45)
	last := map[string]int64{}
	for _, m := range seen {
		if m.seq != last[m.key]+1 {
			t.Fatalf("key %s: got seq %d after %d (duplicate, gap or reorder)", m.key, m.seq, last[m.key])
		}
		last[m.key] = m.seq
	}
	if len(seen) != len(keys)*45 {
		t.Fatalf("published %d messages, want %d", len(seen), len(keys)*45)
	}
	cancel()
	<-relay2
}

type seenMsg struct {
	key string
	seq int64
}

func unpublished(ctx context.Context, pool *pgxpool.Pool) int {
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n
}

func advisoryLocks(ctx context.Context, pool *pgxpool.Pool) int {
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&n)
	return n
}

func openSecond(ctx context.Context, t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// consumeAll reads topic from the start with a fresh group until want messages arrived (or 30 s passed); the
// result is in partition order per key, which is the Kafka order.
func consumeAll(ctx context.Context, t *testing.T, brokers []string, topic string, want int) []seenMsg {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []seenMsg
	deadline := time.Now().Add(30 * time.Second)
	for len(out) < want && time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pctx)
		pcancel()
		fetches.EachRecord(func(r *kgo.Record) {
			env := &commonv1.Envelope{}
			m := &adv1.AdHidden{}
			if proto.Unmarshal(r.Value, env) != nil || proto.Unmarshal(env.Payload, m) != nil {
				t.Fatal("undecodable record")
			}
			out = append(out, seenMsg{key: m.AdId, seq: m.Seq})
		})
	}
	// One more short poll: a duplicate would show up as an extra message.
	pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
	cl.PollFetches(pctx).EachRecord(func(r *kgo.Record) {
		env := &commonv1.Envelope{}
		m := &adv1.AdHidden{}
		_ = proto.Unmarshal(r.Value, env)
		_ = proto.Unmarshal(env.Payload, m)
		out = append(out, seenMsg{key: m.AdId, seq: m.Seq})
	})
	pcancel()
	return out
}
