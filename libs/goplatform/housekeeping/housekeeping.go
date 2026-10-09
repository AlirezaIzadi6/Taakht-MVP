// Package housekeeping prunes the outbox and processed_events tables so they do not grow forever.
//
// Outbox rows are deleted only after they were published (published_at set) and are older than the
// outbox retention; unpublished rows are never touched. processed_events rows are the dedupe
// ledger of idempotent consumers: deleting one re-opens the door for a redelivery of that event to
// be handled a second time. The retention must therefore be much larger than any realistic
// redelivery window (Kafka topic retention, consumer-group offset reset, replays); the default of
// 7 days is deliberately conservative. Shorter retention saves space but weakens idempotency.
package housekeeping

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Defaults and limits.
const (
	DefaultOutboxRetention    = 24 * time.Hour
	DefaultProcessedRetention = 7 * 24 * time.Hour
	DefaultInterval           = 10 * time.Minute
	DefaultInitialDelay       = 30 * time.Second
	DefaultBatchSize          = 1000
	MinDuration               = time.Minute
)

// Environment variable names.
const (
	EnvOutboxRetention    = "OUTBOX_RETENTION"
	EnvProcessedRetention = "PROCESSED_EVENTS_RETENTION"
	EnvInterval           = "PRUNE_INTERVAL"
)

// Options configures Run. Zero values select the defaults.
type Options struct {
	OutboxRetention    time.Duration // published outbox rows older than this are deleted
	ProcessedRetention time.Duration // processed_events rows older than this are deleted
	Interval           time.Duration // time between sweeps
	InitialDelay       time.Duration // delay before the first sweep; 0 selects the default
	BatchSize          int           // rows per DELETE statement
	Logger             *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.OutboxRetention == 0 {
		o.OutboxRetention = DefaultOutboxRetention
	}
	if o.ProcessedRetention == 0 {
		o.ProcessedRetention = DefaultProcessedRetention
	}
	if o.Interval == 0 {
		o.Interval = DefaultInterval
	}
	if o.InitialDelay == 0 {
		o.InitialDelay = DefaultInitialDelay
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Validate rejects retentions and intervals below MinDuration.
func (o Options) Validate() error {
	o = o.withDefaults()
	for name, d := range map[string]time.Duration{
		EnvOutboxRetention:    o.OutboxRetention,
		EnvProcessedRetention: o.ProcessedRetention,
		EnvInterval:           o.Interval,
	} {
		if d < MinDuration {
			return fmt.Errorf("housekeeping: %s=%s is below the minimum of %s", name, d, MinDuration)
		}
	}
	return nil
}

var dayPrefix = regexp.MustCompile(`^(\d+)d(.*)$`)

// ParseDuration parses Go durations (1h30m, 10m) and additionally accepts a leading whole-day
// component with a 'd' suffix: "7d", "1d12h".
func ParseDuration(s string) (time.Duration, error) {
	if m := dayPrefix.FindStringSubmatch(s); m != nil {
		days, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d := time.Duration(days) * 24 * time.Hour
		if m[2] != "" {
			rest, err := time.ParseDuration(m[2])
			if err != nil {
				return 0, fmt.Errorf("invalid duration %q: %w", s, err)
			}
			d += rest
		}
		return d, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return d, nil
}

// OptionsFromEnv reads OUTBOX_RETENTION, PROCESSED_EVENTS_RETENTION and PRUNE_INTERVAL,
// falling back to the defaults, and validates the result.
func OptionsFromEnv() (Options, error) {
	var o Options
	for _, f := range []struct {
		env string
		dst *time.Duration
	}{
		{EnvOutboxRetention, &o.OutboxRetention},
		{EnvProcessedRetention, &o.ProcessedRetention},
		{EnvInterval, &o.Interval},
	} {
		v := os.Getenv(f.env)
		if v == "" {
			continue
		}
		d, err := ParseDuration(v)
		if err != nil {
			return Options{}, fmt.Errorf("housekeeping: %s: %w", f.env, err)
		}
		*f.dst = d
	}
	o = o.withDefaults()
	if err := o.Validate(); err != nil {
		return Options{}, err
	}
	return o, nil
}

// Run prunes once after InitialDelay and then every Interval until ctx is cancelled.
// Failures are logged and retried at the next sweep. It returns nil when ctx is done.
func Run(ctx context.Context, pool *pgxpool.Pool, opts Options) error {
	opts = opts.withDefaults()
	if err := opts.Validate(); err != nil {
		return err
	}
	timer := time.NewTimer(opts.InitialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		if _, _, err := PruneOnce(ctx, pool, opts); err != nil && ctx.Err() == nil {
			opts.Logger.Error("housekeeping: prune failed", "err", err)
		}
		timer.Reset(opts.Interval)
	}
}

// PruneOnce runs one sweep and returns how many outbox and processed_events rows were deleted.
// It logs at info only when something was deleted.
func PruneOnce(ctx context.Context, pool *pgxpool.Pool, opts Options) (outboxDeleted, processedDeleted int64, err error) {
	opts = opts.withDefaults()
	outboxDeleted, err = deleteBatches(ctx, pool, opts.BatchSize, opts.OutboxRetention,
		`DELETE FROM outbox WHERE id IN (
			SELECT id FROM outbox
			WHERE published_at IS NOT NULL AND published_at < now() - ($1 * interval '1 second')
			LIMIT $2 FOR UPDATE SKIP LOCKED)`)
	if err != nil {
		return outboxDeleted, 0, fmt.Errorf("prune outbox: %w", err)
	}
	processedDeleted, err = deleteBatches(ctx, pool, opts.BatchSize, opts.ProcessedRetention,
		`DELETE FROM processed_events WHERE (consumer, event_id) IN (
			SELECT consumer, event_id FROM processed_events
			WHERE processed_at < now() - ($1 * interval '1 second')
			LIMIT $2 FOR UPDATE SKIP LOCKED)`)
	if err != nil {
		return outboxDeleted, processedDeleted, fmt.Errorf("prune processed_events: %w", err)
	}
	if outboxDeleted > 0 || processedDeleted > 0 {
		opts.Logger.Info("housekeeping: pruned",
			"outbox_deleted", outboxDeleted, "processed_events_deleted", processedDeleted)
	}
	return outboxDeleted, processedDeleted, nil
}

// deleteBatches repeats the statement until a batch deletes fewer rows than the batch size.
func deleteBatches(ctx context.Context, pool *pgxpool.Pool, batch int, retention time.Duration, sql string) (int64, error) {
	var total int64
	for {
		tag, err := pool.Exec(ctx, sql, retention.Seconds(), batch)
		if err != nil {
			return total, err
		}
		n := tag.RowsAffected()
		total += n
		if n < int64(batch) {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}
