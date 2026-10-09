// Package main implements the dlq command: inspect, replay and purge the dead_letter table of a service.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	// Importing the generated packages registers their event types, so they can be resolved by name.
	_ "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	_ "github.com/taakht/taakht/gen/taakht/matching/v1"
	_ "github.com/taakht/taakht/gen/taakht/negotiation/v1"
	_ "github.com/taakht/taakht/gen/taakht/swap/v1"
)

// Querier is satisfied by *pgxpool.Pool, pgx.Tx and *pgx.Conn.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Entry is one dead_letter row.
type Entry struct {
	Consumer  string
	EventID   string
	Topic     string
	Error     string
	CreatedAt time.Time
	Payload   []byte // the serialized Envelope
}

// Producer publishes one record to Kafka and returns once it is acknowledged.
type Producer interface {
	Produce(ctx context.Context, topic, key string, value []byte) error
}

const selectEntries = `SELECT consumer, event_id::text, topic, error, created_at, payload FROM dead_letter`

func scanEntries(rows pgx.Rows) ([]Entry, error) {
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.Consumer, &e.EventID, &e.Topic, &e.Error, &e.CreatedAt, &e.Payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// List returns the newest limit entries (all when limit <= 0).
func List(ctx context.Context, q Querier, limit int) ([]Entry, error) {
	rows, err := q.Query(ctx, selectEntries+` ORDER BY created_at DESC, event_id LIMIT CASE WHEN $1::int > 0 THEN $1::int END`, limit)
	if err != nil {
		return nil, err
	}
	return scanEntries(rows)
}

// Find returns the entries for an event id (one per consumer group that parked it), optionally for one consumer.
func Find(ctx context.Context, q Querier, eventID, consumer string) ([]Entry, error) {
	rows, err := q.Query(ctx, selectEntries+` WHERE event_id::text = lower($1) AND ($2 = '' OR consumer = $2) ORDER BY consumer`, eventID, consumer)
	if err != nil {
		return nil, err
	}
	return scanEntries(rows)
}

// Decode parses the stored payload as an Envelope. A payload without a type is not an envelope.
func Decode(payload []byte) (*commonv1.Envelope, error) {
	env := &commonv1.Envelope{}
	if err := proto.Unmarshal(payload, env); err != nil {
		return nil, fmt.Errorf("payload is not an Envelope: %w", err)
	}
	if env.GetType() == "" {
		return nil, errors.New("payload is not an Envelope: no type")
	}
	return env, nil
}

// TypeOf returns the envelope type of a stored payload, or "?" when it does not decode.
func TypeOf(payload []byte) string {
	env, err := Decode(payload)
	if err != nil {
		return "?"
	}
	return env.GetType()
}

// Describe renders an entry for humans: the row, the envelope fields, and the payload as protojson using the
// generated type. A payload that cannot be decoded (unknown type, or bytes that do not parse as that type)
// is printed as hex instead.
func Describe(e Entry) string {
	var b strings.Builder
	fmt.Fprintf(&b, "consumer:   %s\nevent_id:   %s\ntopic:      %s\ncreated_at: %s\nerror:      %s\n",
		e.Consumer, e.EventID, e.Topic, e.CreatedAt.UTC().Format(time.RFC3339), e.Error)
	env, err := Decode(e.Payload)
	if err != nil {
		fmt.Fprintf(&b, "envelope:   undecodable (%v)\nraw (hex):  %s\n", err, hex.EncodeToString(e.Payload))
		return b.String()
	}
	fmt.Fprintf(&b, "type:       %s\naggregate:  %s\noccurred:   %s\nrequest_id: %s\n",
		env.GetType(), env.GetAggregateId(), env.GetOccurredAt().AsTime().UTC().Format(time.RFC3339), env.GetRequestId())
	if js, err := decodePayload(env); err == nil {
		fmt.Fprintf(&b, "payload (%s):\n%s\n", env.GetType(), js)
	} else {
		fmt.Fprintf(&b, "payload (%s) cannot be decoded: %v\nraw (hex):  %s\n", env.GetType(), err, hex.EncodeToString(env.GetPayload()))
	}
	return b.String()
}

func decodePayload(env *commonv1.Envelope) (string, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(env.GetType()))
	if err != nil {
		return "", fmt.Errorf("unknown type: %w", err)
	}
	msg := mt.New().Interface()
	if err := proto.Unmarshal(env.GetPayload(), msg); err != nil {
		return "", err
	}
	raw, err := protojson.Marshal(msg)
	if err != nil {
		return "", err
	}
	var pretty bytes.Buffer // protojson output is deliberately unstable in whitespace; re-indent it
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		return "", err
	}
	return pretty.String(), nil
}

// Replay re-publishes the original envelope bytes to the original topic with the original key (the envelope's
// aggregate id, or keyOverride when set). A consumer records every event it handled or parked in
// processed_events and would skip the replay as a duplicate, so that marker is removed first and restored if
// the produce fails. With deleteRow the dead_letter row is removed after the broker acknowledged the record.
func Replay(ctx context.Context, db Querier, p Producer, e Entry, keyOverride string, deleteRow bool) error {
	env, err := Decode(e.Payload)
	if err != nil {
		return fmt.Errorf("refusing to replay: %w", err)
	}
	key := keyOverride
	if key == "" {
		key = env.GetAggregateId()
	}
	if key == "" {
		return errors.New("refusing to replay: the envelope has no aggregate id to use as key (pass --key)")
	}
	if _, err := db.Exec(ctx, `DELETE FROM processed_events WHERE consumer = $1 AND event_id = $2::uuid`, e.Consumer, e.EventID); err != nil {
		return fmt.Errorf("clear processed_events marker: %w", err)
	}
	if err := p.Produce(ctx, e.Topic, key, e.Payload); err != nil {
		if _, rerr := db.Exec(context.WithoutCancel(ctx),
			`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2::uuid) ON CONFLICT DO NOTHING`, e.Consumer, e.EventID); rerr != nil {
			return fmt.Errorf("produce: %w (restoring the processed_events marker also failed: %w)", err, rerr)
		}
		return fmt.Errorf("produce: %w", err)
	}
	if deleteRow {
		if _, err := db.Exec(ctx, `DELETE FROM dead_letter WHERE consumer = $1 AND event_id = $2::uuid`, e.Consumer, e.EventID); err != nil {
			return fmt.Errorf("produced, but deleting the dead_letter row failed: %w", err)
		}
	}
	return nil
}

// Purge deletes dead_letter rows older than olderThan and returns how many.
func Purge(ctx context.Context, db Querier, olderThan time.Duration) (int64, error) {
	tag, err := db.Exec(ctx, `DELETE FROM dead_letter WHERE created_at < now() - ($1 * interval '1 second')`, olderThan.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
