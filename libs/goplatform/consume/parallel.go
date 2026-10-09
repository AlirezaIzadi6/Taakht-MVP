package consume

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/taakht/taakht/libs/goplatform/observe"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// A partition whose worker falls this far behind stops being fetched until the worker catches up.
	queueHighWater = 1000
	queueLowWater  = 250
)

type topicPartition struct {
	topic     string
	partition int32
}

// partWorker owns one assigned partition: its records are handled strictly in order by one goroutine.
type partWorker struct {
	tp     topicPartition
	cancel context.CancelFunc
	done   chan struct{}
	wake   chan struct{}

	mu     sync.Mutex
	queue  []*kgo.Record
	paused bool
}

func (w *partWorker) push(recs []*kgo.Record) (pause bool) {
	w.mu.Lock()
	w.queue = append(w.queue, recs...)
	if !w.paused && len(w.queue) >= queueHighWater {
		w.paused = true
		pause = true
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return pause
}

// pop returns the next record (nil if none) and whether the partition should be fetched again.
func (w *partWorker) pop() (rec *kgo.Record, resume bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return nil, false
	}
	rec = w.queue[0]
	w.queue[0] = nil
	w.queue = w.queue[1:]
	if w.paused && len(w.queue) <= queueLowWater {
		w.paused = false
		resume = true
	}
	return rec, resume
}

// runParallel handles up to conc partitions at the same time. Records of one partition stay in order (one
// goroutine per assigned partition); a semaphore bounds how many handlers run at once. Offsets are marked
// after a record was handled and committed by the client in the background (every second, on rebalance and on close),
// so a crash can redeliver a few handled records; the processed_events table absorbs them.
func (r *runner) runParallel(ctx context.Context, brokers, topics []string, conc int) error {
	var (
		mu      sync.Mutex
		workers = map[topicPartition]*partWorker{}
		sem     = make(chan struct{}, conc)
	)

	wctx, stopAll := context.WithCancel(ctx)
	defer stopAll()

	startWorkers := func(cl *kgo.Client, assigned map[string][]int32) {
		mu.Lock()
		defer mu.Unlock()
		for topic, parts := range assigned {
			for _, p := range parts {
				tp := topicPartition{topic, p}
				if _, ok := workers[tp]; ok {
					continue
				}
				c, cancel := context.WithCancel(wctx)
				w := &partWorker{tp: tp, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1)}
				workers[tp] = w
				go r.work(c, cl, w, sem)
			}
		}
	}
	stopWorkers := func(revoked map[string][]int32) {
		var stopping []*partWorker
		mu.Lock()
		for topic, parts := range revoked {
			for _, p := range parts {
				tp := topicPartition{topic, p}
				if w, ok := workers[tp]; ok {
					delete(workers, tp)
					stopping = append(stopping, w)
				}
			}
		}
		mu.Unlock()
		for _, w := range stopping {
			w.cancel()
		}
		for _, w := range stopping {
			<-w.done
		}
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(r.group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(time.Second),
		kgo.AllowAutoTopicCreation(),
		kgo.OnPartitionsAssigned(func(_ context.Context, c *kgo.Client, assigned map[string][]int32) { startWorkers(c, assigned) }),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) { stopWorkers(revoked) }),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) { stopWorkers(lost) }),
	)
	if err != nil {
		return fmt.Errorf("consume: kafka client: %w", err)
	}
	observe.TrackConsumer(cl) // readiness: still a member of the group
	defer observe.UntrackConsumer()
	// Workers must be gone before the client closes: a worker still marking offsets would race the final commit.
	defer func() {
		stopAll()
		mu.Lock()
		all := make([]*partWorker, 0, len(workers))
		for _, w := range workers {
			all = append(all, w)
		}
		mu.Unlock()
		for _, w := range all {
			<-w.done
		}
		cl.CommitMarkedOffsets(context.WithoutCancel(ctx)) //nolint:errcheck // best effort; a redelivery is harmless
		cl.Close()
	}()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Warn("consume: fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			mu.Lock()
			w := workers[topicPartition{p.Topic, p.Partition}]
			mu.Unlock()
			if w == nil {
				return // revoked since the fetch; the new owner starts from the committed offset
			}
			if w.push(p.Records) {
				cl.PauseFetchPartitions(map[string][]int32{p.Topic: {p.Partition}})
			}
		})
	}
}

func (r *runner) work(ctx context.Context, cl *kgo.Client, w *partWorker, sem chan struct{}) {
	defer close(w.done)
	for {
		rec, resume := w.pop()
		if resume {
			cl.ResumeFetchPartitions(map[string][]int32{w.tp.topic: {w.tp.partition}})
		}
		if rec == nil {
			select {
			case <-ctx.Done():
				return
			case <-w.wake:
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		}
		err := r.handleWithRetry(ctx, rec)
		<-sem
		if err != nil {
			return // only when ctx is cancelled
		}
		cl.MarkCommitRecords(rec)
	}
}
