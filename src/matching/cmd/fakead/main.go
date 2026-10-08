// Command fakead publishes two matching fake AdPublished envelopes to ad.events and then
// queries the Matching service. It is a smoke-test tool for local runs.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	brokers := "localhost:9094"
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		brokers = v
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		return err
	}
	defer cl.Close()

	a := publish(ctx, cl, "user-1", "books", "tools")
	b := publish(ctx, cl, "user-2", "tools", "books")
	fmt.Println("published ads", a, b)
	time.Sleep(3 * time.Second)

	conn, err := grpc.NewClient("localhost:9002", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := matchingv1.NewMatchingServiceClient(conn)
	uctx := metadata.AppendToOutgoingContext(ctx, "x-user-id", "user-1")

	sr, err := c.Search(uctx, &matchingv1.SearchRequest{Criteria: &matchingv1.Criteria{WantCategories: []string{"tools"}}})
	fmt.Println("Search:", sr, err)
	fm, err := c.FindMatches(uctx, &matchingv1.FindMatchesRequest{AdId: a})
	fmt.Println("FindMatches:", fm, err)
	return nil
}

func publish(ctx context.Context, cl *kgo.Client, owner, have, want string) string {
	id := uuid.NewString()
	ad := &adv1.Ad{
		Id: id, OwnerId: owner, Version: 1, Status: adv1.AdStatus_AD_STATUS_PUBLISHED,
		Spec:      &adv1.AdSpec{Title: have + " for " + want, HaveCategory: have, WantCategories: []string{want}, NeighborhoodIds: []string{"n-valiasr"}},
		CreatedAt: timestamppb.Now(), UpdatedAt: timestamppb.Now(),
	}
	payload, _ := proto.Marshal(&adv1.AdPublished{Ad: ad})
	val, _ := proto.Marshal(&commonv1.Envelope{
		EventId: uuid.NewString(), Type: "taakht.ad.v1.AdPublished", AggregateId: id,
		OccurredAt: timestamppb.Now(), Payload: payload,
	})
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "ad.events", Key: []byte(id), Value: val}).FirstErr(); err != nil {
		log.Fatal(err)
	}
	return id
}
