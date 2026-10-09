package chaos

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Brokers is the external listener of the local Kafka container.
const Brokers = "localhost:9094"

// NewUUID returns a random version 4 UUID in the textual form the services expect for event ids.
func NewUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// FindEnvelope reads topic from the start with a throwaway consumer (no consumer group, so no offsets are
// committed and no real group is disturbed) until match accepts an envelope or the timeout passes.
func FindEnvelope(t TB, topic string, timeout time.Duration, match func(*commonv1.Envelope) bool) *commonv1.Envelope {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(Brokers),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			var env commonv1.Envelope
			if proto.Unmarshal(r.Value, &env) != nil {
				continue
			}
			if match(&env) {
				if out, ok := proto.Clone(&env).(*commonv1.Envelope); ok {
					return out
				}
			}
		}
	}
	return nil
}

// Publish writes one envelope to topic with the given Kafka key through a throwaway producer.
func Publish(t TB, topic, key string, env *commonv1.Envelope) {
	t.Helper()
	value, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(Brokers), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
	t.Logf("chaos: published %s event_id=%s key=%s to %s", shortType(env.GetType()), env.GetEventId(), key, topic)
}

func shortType(t string) string {
	if i := strings.LastIndex(t, "."); i >= 0 {
		return t[i+1:]
	}
	return t
}
