package observe

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the process-wide Prometheus registry served on /metrics. The metric names are the contract with
// the .NET services (same names, same labels) and with deploy/observability/grafana. Labels stay low-cardinality:
// gRPC method and status code, never ids.
var Registry = prometheus.NewRegistry()

var (
	grpcRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "taakht_grpc_server_requests_total", Help: "Unary gRPC calls handled, by full method and status code.",
	}, []string{"method", "code"})
	grpcDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "taakht_grpc_server_request_duration_seconds", Help: "Unary gRPC handling time by full method.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"method"})

	// ConsumerEvents counts consumer outcomes: handled, failed (one attempt; it is retried) and permanent (dead-lettered).
	ConsumerEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "taakht_consumer_events_total", Help: "Events processed by the consumer, by result (handled, failed, permanent).",
	}, []string{"result"})
	// OutboxPublished counts outbox rows acknowledged by Kafka.
	OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "taakht_outbox_published_total", Help: "Outbox rows published to Kafka.",
	})
	// HousekeepingDeleted counts rows pruned by housekeeping, by table.
	HousekeepingDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "taakht_housekeeping_deleted_total", Help: "Rows deleted by housekeeping, by table.",
	}, []string{"table"})
)

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		grpcRequests, grpcDuration, ConsumerEvents, OutboxPublished, HousekeepingDeleted,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "taakht_consumer_joined", Help: "1 while the Kafka consumer is a member of its group.",
		}, func() float64 {
			if ConsumerJoined() {
				return 1
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "taakht_consumer_last_event_timestamp_seconds", Help: "Unix time of the last event the consumer processed (0 if none yet).",
		}, func() float64 { return float64(consumerLastEvt.Load()) / 1e9 }),
	)
	for _, r := range []string{"handled", "failed", "permanent"} {
		ConsumerEvents.WithLabelValues(r)
	}
	for _, t := range []string{"outbox", "processed_events", "dead_letter"} {
		HousekeepingDeleted.WithLabelValues(t)
	}
}

// RegisterDB exposes pool statistics and the outbox backlog (unpublished rows and the age of the oldest one,
// computed at scrape time) for pool. Call it once per process.
func RegisterDB(pool *pgxpool.Pool) {
	Registry.MustRegister(&dbCollector{pool: pool})
}

type dbCollector struct{ pool *pgxpool.Pool }

var (
	descConns = prometheus.NewDesc("taakht_db_pool_connections", "Database pool connections by state (acquired, idle, constructing, total, max).", []string{"state"}, nil)
	descWait  = prometheus.NewDesc("taakht_db_pool_acquire_wait_seconds_total", "Total time callers waited to acquire a pooled connection.", nil, nil)
	descEmpty = prometheus.NewDesc("taakht_db_pool_empty_acquires_total", "Acquires that had to wait because the pool was empty.", nil, nil)
	descUnpub = prometheus.NewDesc("taakht_outbox_unpublished", "Outbox rows not yet published to Kafka.", nil, nil)
	descAge   = prometheus.NewDesc("taakht_outbox_oldest_unpublished_age_seconds", "Age of the oldest unpublished outbox row (0 when none).", nil, nil)
)

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descConns, descWait, descEmpty, descUnpub, descAge} {
		ch <- d
	}
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	for state, v := range map[string]int32{
		"acquired": s.AcquiredConns(), "idle": s.IdleConns(), "constructing": s.ConstructingConns(),
		"total": s.TotalConns(), "max": s.MaxConns(),
	} {
		ch <- prometheus.MustNewConstMetric(descConns, prometheus.GaugeValue, float64(v), state)
	}
	ch <- prometheus.MustNewConstMetric(descWait, prometheus.CounterValue, s.AcquireDuration().Seconds())
	ch <- prometheus.MustNewConstMetric(descEmpty, prometheus.CounterValue, float64(s.EmptyAcquireCount()))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	count, age, err := OutboxBacklog(ctx, c.pool)
	if err != nil {
		return // no sample is better than a wrong one: the gauge goes stale and alerts on absence
	}
	ch <- prometheus.MustNewConstMetric(descUnpub, prometheus.GaugeValue, float64(count))
	ch <- prometheus.MustNewConstMetric(descAge, prometheus.GaugeValue, age.Seconds())
}

// OutboxBacklog returns the number of unpublished outbox rows and the age of the oldest one (0 when none).
// It uses the partial index outbox_unpublished.
func OutboxBacklog(ctx context.Context, pool *pgxpool.Pool) (count int64, oldest time.Duration, err error) {
	var secs float64
	err = pool.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(created_at)), 0)::float8
		FROM outbox WHERE published_at IS NULL`).Scan(&count, &secs)
	return count, time.Duration(secs * float64(time.Second)), err
}
