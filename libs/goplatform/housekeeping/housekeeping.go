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

	"github.com/taakht/taakht/libs/goplatform/observe"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Defaults and limits.
const (
	DefaultOutboxRetention     = 24 * time.Hour
	DefaultProcessedRetention  = 7 * 24 * time.Hour
	DefaultDeadLetterRetention = 30 * 24 * time.Hour
	DefaultInterval            = 10 * time.Minute
	DefaultInitialDelay        = 30 * time.Second
	DefaultBatchSize           = 1000
	MinDuration                = time.Minute
)

// Environment variable names.
const (
	EnvOutboxRetention     = "OUTBOX_RETENTION"
	EnvProcessedRetention  = "PROCESSED_EVENTS_RETENTION"
	EnvDeadLetterRetention = "DEAD_LETTER_RETENTION"
	EnvInterval            = "PRUNE_INTERVAL"
)

// Options configures Run. Zero values select the defaults.
type Options struct {
	OutboxRetention     time.Duration // published outbox rows older than this are deleted
	ProcessedRetention  time.Duration // processed_events rows older than this are deleted
	DeadLetterRetention time.Duration // dead_letter rows older than this are deleted
	Extra               []Prune       // service-specific prunes run after the built-in ones
	Interval            time.Duration // time between sweeps
	InitialDelay        time.Duration // delay before the first sweep; 0 selects the default
	BatchSize           int           // rows per DELETE statement
	Logger              *slog.Logger
}

// Prune is a service-specific retention rule. SQL is a batched DELETE that takes the retention in
// seconds as $1 and the batch size as $2 and must delete at most $2 rows (use a LIMIT subquery).
type Prune struct {
	Name      string        // used in logs and validation messages, e.g. the env variable name
	Retention time.Duration // at least MinDuration
	SQL       string
}

// EnvDuration reads a retention-style duration from the environment (same syntax as the other
// housekeeping variables, "d" suffix included). An unset variable yields def; a value below
// MinDuration or unparseable is an error.
func EnvDuration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("housekeeping: %s: %w", name, err)
	}
	if d < MinDuration {
		return 0, fmt.Errorf("housekeeping: %s=%s is below the minimum of %s", name, d, MinDuration)
	}
	return d, nil
}

func (o Options) withDefaults() Options {
	if o.OutboxRetention == 0 {
		o.OutboxRetention = DefaultOutboxRetention
	}
	if o.ProcessedRetention == 0 {
		o.ProcessedRetention = DefaultProcessedRetention
	}
	if o.DeadLetterRetention == 0 {
		o.DeadLetterRetention = DefaultDeadLetterRetention
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
		EnvOutboxRetention:     o.OutboxRetention,
		EnvProcessedRetention:  o.ProcessedRetention,
		EnvDeadLetterRetention: o.DeadLetterRetention,
		EnvInterval:            o.Interval,
	} {
		if d < MinDuration {
			return fmt.Errorf("housekeeping: %s=%s is below the minimum of %s", name, d, MinDuration)
		}
	}
	for _, p := range o.Extra {
		if p.Retention < MinDuration {
			return fmt.Errorf("housekeeping: %s=%s is below the minimum of %s", p.Name, p.Retention, MinDuration)
		}
	}
	return nil
}

// maxDays keeps day-based durations far from time.Duration overflow (about 292 years).
const maxDays = 36500

var dayPrefix = regexp.MustCompile(`^(\d+)d(.*)$`)

// ParseDuration parses Go durations (1h30m, 10m) and additionally accepts a leading whole-day
// component with a 'd' suffix: "7d", "1d12h".
func ParseDuration(s string) (time.Duration, error) {
	if m := dayPrefix.FindStringSubmatch(s); m != nil {
		days, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		if days > maxDays {
			return 0, fmt.Errorf("invalid duration %q: more than %d days", s, maxDays)
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

// OptionsFromEnv reads OUTBOX_RETENTION, PROCESSED_EVENTS_RETENTION, DEAD_LETTER_RETENTION and PRUNE_INTERVAL,
// falling back to the defaults, and validates the result.
func OptionsFromEnv() (Options, error) {
	var o Options
	for _, f := range []struct {
		env string
		dst *time.Duration
	}{
		{EnvOutboxRetention, &o.OutboxRetention},
		{EnvProcessedRetention, &o.ProcessedRetention},
		{EnvDeadLetterRetention, &o.DeadLetterRetention},
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
		if _, _, _, err := PruneOnce(ctx, pool, opts); err != nil && ctx.Err() == nil {
			opts.Logger.Error("housekeeping: prune failed", "err", err)
		}
		timer.Reset(opts.Interval)
	}
}

// PruneOnce runs one sweep and returns how many outbox, processed_events and dead_letter rows were deleted.
// It logs at info only when something was deleted.
func PruneOnce(ctx context.Context, pool *pgxpool.Pool, opts Options) (outboxDeleted, processedDeleted, deadLetterDeleted int64, err error) {
	opts = opts.withDefaults()
	defer func() { // taakht_housekeeping_deleted_total, also for a sweep that stopped on an error
		observe.HousekeepingDeleted.WithLabelValues("outbox").Add(float64(outboxDeleted))
		observe.HousekeepingDeleted.WithLabelValues("processed_events").Add(float64(processedDeleted))
		observe.HousekeepingDeleted.WithLabelValues("dead_letter").Add(float64(deadLetterDeleted))
	}()
	outboxDeleted, err = deleteBatches(ctx, pool, opts.BatchSize, opts.OutboxRetention,
		`DELETE FROM outbox WHERE id IN (
			SELECT id FROM outbox
			WHERE published_at IS NOT NULL AND published_at < now() - ($1 * interval '1 second')
			LIMIT $2 FOR UPDATE SKIP LOCKED)`)
	if err != nil {
		return outboxDeleted, 0, 0, fmt.Errorf("prune outbox: %w", err)
	}
	processedDeleted, err = deleteBatches(ctx, pool, opts.BatchSize, opts.ProcessedRetention,
		`DELETE FROM processed_events WHERE (consumer, event_id) IN (
			SELECT consumer, event_id FROM processed_events
			WHERE processed_at < now() - ($1 * interval '1 second')
			LIMIT $2 FOR UPDATE SKIP LOCKED)`)
	if err != nil {
		return outboxDeleted, processedDeleted, 0, fmt.Errorf("prune processed_events: %w", err)
	}
	deadLetterDeleted, err = deleteBatches(ctx, pool, opts.BatchSize, opts.DeadLetterRetention,
		`DELETE FROM dead_letter WHERE (consumer, event_id) IN (
			SELECT consumer, event_id FROM dead_letter
			WHERE created_at < now() - ($1 * interval '1 second')
			LIMIT $2 FOR UPDATE SKIP LOCKED)`)
	if err != nil {
		return outboxDeleted, processedDeleted, deadLetterDeleted, fmt.Errorf("prune dead_letter: %w", err)
	}
	for _, p := range opts.Extra {
		n, err := deleteBatches(ctx, pool, opts.BatchSize, p.Retention, p.SQL)
		observe.HousekeepingDeleted.WithLabelValues(p.Name).Add(float64(n))
		if n > 0 {
			opts.Logger.Info("housekeeping: pruned", "prune", p.Name, "deleted", n)
		}
		if err != nil {
			return outboxDeleted, processedDeleted, deadLetterDeleted, fmt.Errorf("prune %s: %w", p.Name, err)
		}
	}
	if outboxDeleted > 0 || processedDeleted > 0 || deadLetterDeleted > 0 {
		opts.Logger.Info("housekeeping: pruned",
			"outbox_deleted", outboxDeleted, "processed_events_deleted", processedDeleted,
			"dead_letter_deleted", deadLetterDeleted)
	}
	return outboxDeleted, processedDeleted, deadLetterDeleted, nil
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
