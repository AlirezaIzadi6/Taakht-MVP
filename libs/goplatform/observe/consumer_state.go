package observe

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
)

// Consumer liveness, written by libs/goplatform/consume and read by the readiness check. State is per process
// because every Go service runs exactly one consumer client.
var (
	consumerProbe   atomic.Pointer[func() bool]
	consumerLastEvt atomic.Int64 // unix nanoseconds of the last processed event
)

// GroupMember is the part of a franz-go client TrackConsumer needs.
type GroupMember interface {
	GroupMetadata() (memberID string, generation int32)
}

// TrackConsumer registers the Kafka client of the consumer so readiness can ask whether it is still a member of
// its group (franz-go reports an empty member id and generation -1 when it is not).
func TrackConsumer(m GroupMember) {
	f := func() bool {
		id, gen := m.GroupMetadata()
		return id != "" && gen >= 0
	}
	consumerProbe.Store(&f)
}

// UntrackConsumer forgets the client (it is closing).
func UntrackConsumer() { consumerProbe.Store(nil) }

// ConsumerJoined reports whether the consumer is currently a member of its group.
func ConsumerJoined() bool {
	if f := consumerProbe.Load(); f != nil {
		return (*f)()
	}
	return false
}

// ConsumerLastEvent returns when the consumer last processed an event (zero if never).
func ConsumerLastEvent() time.Time {
	if n := consumerLastEvt.Load(); n > 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

// BeginEvent restores the request id of env into ctx and returns a function to call when handling ends. It
// counts the outcome (taakht_consumer_events_total) and logs one line carrying the request id: "consume: event
// processed" on success, "consume: event failed" otherwise. A duplicate delivery also counts as processed.
func BeginEvent(ctx context.Context, env *commonv1.Envelope) (context.Context, func(err error, permanent bool)) {
	ctx = WithRequestID(ctx, env.GetRequestId())
	start := time.Now()
	return ctx, func(err error, permanent bool) {
		d := float64(time.Since(start).Microseconds()) / 1000
		consumerLastEvt.Store(time.Now().UnixNano())
		attrs := []any{"type", env.GetType(), "event_id", env.GetEventId(), "duration_ms", d}
		switch {
		case err == nil:
			ConsumerEvents.WithLabelValues("handled").Inc()
			slog.InfoContext(ctx, "consume: event processed", attrs...)
		case permanent:
			ConsumerEvents.WithLabelValues("permanent").Inc()
			slog.WarnContext(ctx, "consume: event failed permanently", append(attrs, "err", err)...)
		default:
			ConsumerEvents.WithLabelValues("failed").Inc()
			slog.WarnContext(ctx, "consume: event failed", append(attrs, "err", err)...)
		}
	}
}
