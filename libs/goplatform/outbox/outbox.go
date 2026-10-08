// Package outbox writes events in the business transaction and relays them to Kafka.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Add wraps msg in an Envelope and inserts it into the outbox table within tx.
// key is the aggregate id and the Kafka key.
func Add(ctx context.Context, tx pgx.Tx, topic, key string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox: marshal payload: %w", err)
	}
	id := uuid.NewString()
	env := &commonv1.Envelope{
		EventId:     id,
		Type:        string(msg.ProtoReflect().Descriptor().FullName()),
		AggregateId: key,
		OccurredAt:  timestamppb.Now(),
		Payload:     payload,
	}
	raw, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal envelope: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (id, topic, key, envelope) VALUES ($1, $2, $3, $4)`,
		id, topic, key, raw); err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

const (
	pollInterval = 200 * time.Millisecond
	batchSize    = 100
)

// RunRelay publishes unpublished outbox rows in creation order until ctx is cancelled.
// Delivery is at-least-once.
func RunRelay(ctx context.Context, pool *pgxpool.Pool, brokers []string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("outbox: kafka client: %w", err)
	}
	defer cl.Close()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		for {
			n, err := relayOnce(ctx, pool, cl)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				slog.Error("outbox relay failed", "err", err)
				break
			}
			if n < batchSize {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func relayOnce(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := tx.Query(ctx, `SELECT id, topic, key, envelope FROM outbox
		WHERE published_at IS NULL ORDER BY created_at, id LIMIT $1 FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return 0, err
	}
	var ids []string
	var recs []*kgo.Record
	for rows.Next() {
		var id, topic, key string
		var raw []byte
		if err := rows.Scan(&id, &topic, &key, &raw); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(key), Value: raw})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(recs) == 0 {
		return 0, nil
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return 0, fmt.Errorf("produce: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])`, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(recs), nil
}
