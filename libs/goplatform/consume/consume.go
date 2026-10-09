// Package consume runs idempotent Kafka consumers over envelopes.
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	"github.com/taakht/taakht/libs/goplatform/observe"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// permanentError marks a failure that retrying cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so Run records the event as processed and moves on instead of retrying forever.
// Use it for events that can never succeed (undecodable payloads, rejected business input).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// Decode unmarshals the envelope payload into msg; a failure is Permanent because redelivery cannot fix it.
func Decode(env *commonv1.Envelope, msg proto.Message) error {
	if err := proto.Unmarshal(env.GetPayload(), msg); err != nil {
		return Permanent(fmt.Errorf("decode %s: %w", env.GetType(), err))
	}
	return nil
}

// Handler applies one event inside tx, which also holds the dedupe row.
type Handler func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error

// ExternalHandler applies one event outside any dedupe transaction (see Options.AtLeastOnce).
type ExternalHandler func(ctx context.Context, env *commonv1.Envelope) error

// Options tunes RunWith.
type Options struct {
	// Concurrency is the number of partitions whose records are handled at the same time. Order is kept per
	// partition (so per Kafka key); partitions run in parallel. 0 reads CONSUMER_CONCURRENCY (default 1).
	// With 1 the consumer is strictly sequential across partitions.
	Concurrency int

	// AtLeastOnce lists handlers, keyed by envelope type, that run FIRST and outside any transaction of ours; the
	// processed_events row is inserted afterwards, in its own short transaction, only after the handler succeeded.
	// Use it for a handler that spends long on a remote call and must not keep a pooled connection and an open
	// transaction idle meanwhile. The price: delivery to the handler is at-least-once, so the handler MUST be
	// idempotent and own its transactions (a crash or a failed insert after a successful handler runs it again,
	// and two instances may run it concurrently for one event during a rebalance). Types here take precedence
	// over the same type in the plain handlers map.
	AtLeastOnce map[string]ExternalHandler
}

// ConcurrencyFromEnv returns CONSUMER_CONCURRENCY as an integer of at least 1 (default 1; garbage falls back to 1).
func ConcurrencyFromEnv() int {
	v := os.Getenv("CONSUMER_CONCURRENCY")
	if v == "" {
		return 1
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		slog.Warn("consume: invalid CONSUMER_CONCURRENCY, using 1", "value", v)
		return 1
	}
	return n
}

// Run consumes topics as group until ctx is cancelled. handlers are keyed by envelope type;
// unknown types are skipped. For each known event the processed_events insert and the handler
// share one transaction; the offset is committed afterwards. A failing handler is retried
// with backoff, which blocks its partition, unless it returns Permanent(err): then the failure is
// logged at error level, the event is recorded as processed and its envelope plus the error are
// stored in dead_letter (its handler writes are rolled back), and consumption continues.
func Run(ctx context.Context, pool *pgxpool.Pool, brokers []string, group string, topics []string, handlers map[string]Handler) error {
	return RunWith(ctx, pool, brokers, group, topics, handlers, Options{})
}

// RunWith is Run with Options: partition-level parallelism and handlers that run outside the dedupe transaction.
func RunWith(ctx context.Context, pool *pgxpool.Pool, brokers []string, group string, topics []string, handlers map[string]Handler, opts Options) error {
	r := &runner{pool: pool, group: group, handlers: handlers, external: opts.AtLeastOnce}
	conc := opts.Concurrency
	if conc == 0 {
		conc = ConcurrencyFromEnv()
	}
	if conc > 1 {
		return r.runParallel(ctx, brokers, topics, conc)
	}
	return r.runSequential(ctx, brokers, topics)
}

type runner struct {
	pool     *pgxpool.Pool
	group    string
	handlers map[string]Handler
	external map[string]ExternalHandler
}

func (r *runner) runSequential(ctx context.Context, brokers []string, topics []string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(r.group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("consume: kafka client: %w", err)
	}
	defer cl.Close()
	observe.TrackConsumer(cl) // readiness: still a member of the group
	defer observe.UntrackConsumer()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Warn("consume: fetch error", "topic", t, "partition", p, "err", err)
		})
		stopped := false
		fetches.EachRecord(func(rec *kgo.Record) {
			if stopped {
				return
			}
			if err := r.handleWithRetry(ctx, rec); err != nil {
				stopped = true // only happens when ctx is cancelled
				return
			}
			if err := cl.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
				slog.Warn("consume: commit failed", "err", err)
			}
		})
		if stopped {
			return nil
		}
	}
}

func (r *runner) handleWithRetry(ctx context.Context, rec *kgo.Record) error {
	env := &commonv1.Envelope{}
	if err := proto.Unmarshal(rec.Value, env); err != nil {
		slog.Error("consume: undecodable envelope, skipping", "topic", rec.Topic, "offset", rec.Offset, "err", err)
		return nil
	}
	ext, isExt := r.external[env.GetType()]
	h, isPlain := r.handlers[env.GetType()]
	if !isExt && !isPlain {
		return nil
	}
	backoff := 200 * time.Millisecond
	for {
		var err error
		if isExt {
			err = ProcessAtLeastOnce(ctx, r.pool, r.group, env, ext)
		} else {
			err = Process(ctx, r.pool, r.group, env, h)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsPermanent(err) {
			slog.Error("consume: permanent handler failure, skipping event", "type", env.GetType(), "event_id", env.GetEventId(), "err", err)
			if serr := recordSkipped(ctx, r.pool, r.group, rec.Topic, env, err); serr != nil {
				slog.Error("consume: cannot record skipped event, retrying", "event_id", env.GetEventId(), "err", serr)
				err = serr
			} else {
				return nil
			}
		}
		slog.Error("consume: handler failed, retrying", "type", env.GetType(), "event_id", env.GetEventId(), "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// recordSkipped records a permanently failed event as processed and keeps its envelope and the
// error in dead_letter, in one statement so both rows exist or neither does.
func recordSkipped(ctx context.Context, pool *pgxpool.Pool, group, topic string, env *commonv1.Envelope, cause error) error {
	eventID, perr := uuid.Parse(env.GetEventId())
	if perr != nil {
		slog.Error("consume: skipped event has no valid id, cannot record it", "type", env.GetType(), "err", cause)
		return nil //nolint:nilerr // an event without a valid id can never be recorded; skipping is all we can do
	}
	payload, merr := proto.Marshal(env)
	if merr != nil {
		payload = env.GetPayload()
	}
	_, err := pool.Exec(ctx, `
WITH p AS (
  INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING
)
INSERT INTO dead_letter (consumer, event_id, topic, payload, error) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING`, group, eventID, topic, payload, cause.Error())
	return err
}

// Process runs h once per (group, event id): the dedupe insert and the handler share a transaction.
// Exported so services can test handlers without Kafka.
func Process(ctx context.Context, pool *pgxpool.Pool, group string, env *commonv1.Envelope, h Handler) (err error) {
	ctx, finish := observe.BeginEvent(ctx, env) // restores the request id, counts and logs the outcome
	defer func() { finish(err, IsPermanent(err)) }()
	eventID, err := uuid.Parse(env.GetEventId())
	if err != nil {
		return Permanent(fmt.Errorf("consume: bad event id %q: %w", env.GetEventId(), err))
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tag, err := tx.Exec(ctx, `INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, group, eventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if err := h(ctx, tx, env); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ProcessAtLeastOnce is the AtLeastOnce counterpart of Process: it skips an event already in processed_events,
// otherwise runs h with no transaction of ours open and, only after h succeeded, inserts the processed_events row in
// its own short transaction. An error from h (or a failed insert) leaves no row, so the event is retried. Exported so
// services can test handlers without Kafka.
func ProcessAtLeastOnce(ctx context.Context, pool *pgxpool.Pool, group string, env *commonv1.Envelope, h ExternalHandler) (err error) {
	ctx, finish := observe.BeginEvent(ctx, env)
	defer func() { finish(err, IsPermanent(err)) }()
	eventID, err := uuid.Parse(env.GetEventId())
	if err != nil {
		return Permanent(fmt.Errorf("consume: bad event id %q: %w", env.GetEventId(), err))
	}
	var done bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM processed_events WHERE consumer = $1 AND event_id = $2)`, group, eventID).Scan(&done); err != nil {
		return err
	}
	if done {
		return nil
	}
	if err := h(ctx, env); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, group, eventID)
	return err
}
