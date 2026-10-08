// Package consume runs idempotent Kafka consumers over envelopes.
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"

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

// Run consumes topics as group until ctx is cancelled. handlers are keyed by envelope type;
// unknown types are skipped. For each known event the processed_events insert and the handler
// share one transaction; the offset is committed afterwards. A failing handler is retried
// with backoff, which blocks its partition (no DLQ in the MVP), unless it returns Permanent(err):
// then the failure is logged, the event is recorded as processed (its handler writes are rolled back)
// and consumption continues.
func Run(ctx context.Context, pool *pgxpool.Pool, brokers []string, group string, topics []string, handlers map[string]Handler) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("consume: kafka client: %w", err)
	}
	defer cl.Close()

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
			if err := handleWithRetry(ctx, pool, group, handlers, rec); err != nil {
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

func handleWithRetry(ctx context.Context, pool *pgxpool.Pool, group string, handlers map[string]Handler, rec *kgo.Record) error {
	env := &commonv1.Envelope{}
	if err := proto.Unmarshal(rec.Value, env); err != nil {
		slog.Error("consume: undecodable envelope, skipping", "topic", rec.Topic, "offset", rec.Offset, "err", err)
		return nil
	}
	h, ok := handlers[env.GetType()]
	if !ok {
		return nil
	}
	backoff := 200 * time.Millisecond
	for {
		err := Process(ctx, pool, group, env, h)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsPermanent(err) {
			slog.Error("consume: permanent handler failure, skipping event", "type", env.GetType(), "event_id", env.GetEventId(), "err", err)
			if serr := markProcessed(ctx, pool, group, env); serr != nil {
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

// markProcessed records the event as handled without running a handler.
func markProcessed(ctx context.Context, pool *pgxpool.Pool, group string, env *commonv1.Envelope) error {
	eventID, perr := uuid.Parse(env.GetEventId())
	if perr != nil {
		return nil //nolint:nilerr // an event without a valid id can never be recorded; skipping is all we can do
	}
	_, err := pool.Exec(ctx, `INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, group, eventID)
	return err
}

// Process runs h once per (group, event id): the dedupe insert and the handler share a transaction.
// Exported so services can test handlers without Kafka.
func Process(ctx context.Context, pool *pgxpool.Pool, group string, env *commonv1.Envelope, h Handler) error {
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
