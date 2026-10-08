package handlers_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/src/matching/internal/geo"
	"github.com/taakht/taakht/src/matching/internal/handlers"
	"github.com/taakht/taakht/src/matching/internal/index"
	"github.com/taakht/taakht/src/matching/internal/testdb"
)

type env struct {
	pool *pgxpool.Pool
	h    *handlers.Handlers
	m    map[string]handlers.Handler
	sent []*matchingv1.MatchFound
}

func setup(t *testing.T) *env {
	e := &env{pool: testdb.New(t)}
	e.h = &handlers.Handlers{
		Geo: geo.Map{"n1": {ID: "n1", Lat: 35.7, Lon: 51.4}, "n2": {ID: "n2", Lat: 35.8, Lon: 51.4}},
		Emit: func(_ context.Context, _ pgx.Tx, topic, key string, msg proto.Message) error {
			if topic != handlers.MatchTopic {
				t.Errorf("unexpected topic %s", topic)
			}
			e.sent = append(e.sent, msg.(*matchingv1.MatchFound))
			return nil
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	e.m = e.h.Map()
	return e
}

func (e *env) deliver(t *testing.T, typ string, msg proto.Message) {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	h, ok := e.m[typ]
	if !ok {
		t.Fatalf("no handler for %s", typ)
	}
	if err := h(ctx, tx, &commonv1.Envelope{EventId: uuid.NewString(), Type: typ, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func ad(id, owner string, version int32, status adv1.AdStatus, have string, want ...string) *adv1.Ad {
	return &adv1.Ad{
		Id: id, OwnerId: owner, Version: version, Status: status,
		Spec: &adv1.AdSpec{Title: "t", HaveCategory: have, WantCategories: want, NeighborhoodIds: []string{"n1"}},
	}
}

const (
	published = adv1.AdStatus_AD_STATUS_PUBLISHED
	hidden    = adv1.AdStatus_AD_STATUS_HIDDEN
	typPub    = "taakht.ad.v1.AdPublished"
	typEdit   = "taakht.ad.v1.AdEdited"
)

func (e *env) indexed(t *testing.T, id string) *adv1.Ad {
	t.Helper()
	got, err := index.Get(context.Background(), e.pool, id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestPublishIndexesAndMatchesOnce(t *testing.T) {
	e := setup(t)
	a, b := uuid.NewString(), uuid.NewString()
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(a, "user-1", 1, published, "books", "tools")})
	if len(e.sent) != 0 {
		t.Fatal("no candidates yet")
	}
	pubB := &adv1.AdPublished{Ad: ad(b, "user-2", 1, published, "tools", "books")}
	e.deliver(t, typPub, pubB)
	if len(e.sent) != 1 || e.sent[0].AdId != b || e.sent[0].MatchedAdId != a || e.sent[0].Score <= 1 {
		t.Fatalf("expected one MatchFound b->a, got %v", e.sent)
	}
	// Redelivery (at-least-once) must not notify again.
	e.deliver(t, typPub, pubB)
	if len(e.sent) != 1 {
		t.Fatalf("duplicate delivery re-notified: %d", len(e.sent))
	}
	var rows int
	_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM ad_index`).Scan(&rows)
	if rows != 2 {
		t.Fatalf("expected 2 index rows, got %d", rows)
	}
}

func TestSameOwnerAndNonMatchingNotNotified(t *testing.T) {
	e := setup(t)
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(uuid.NewString(), "user-1", 1, published, "books", "tools")})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(uuid.NewString(), "user-1", 1, published, "tools", "books")})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(uuid.NewString(), "user-2", 1, published, "sports", "books")})
	if len(e.sent) != 0 {
		t.Fatalf("unexpected matches: %v", e.sent)
	}
}

func TestOutOfOrderEdit(t *testing.T) {
	e := setup(t)
	id := uuid.NewString()
	e.deliver(t, typEdit, &adv1.AdEdited{Ad: ad(id, "user-1", 3, published, "books", "tools")})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(id, "user-1", 1, published, "kitchen", "tools")})
	if got := e.indexed(t, id); got.GetVersion() != 3 || got.GetSpec().GetHaveCategory() != "books" {
		t.Fatalf("older event overwrote newer version: %v", got)
	}
	e.deliver(t, typEdit, &adv1.AdEdited{Ad: ad(id, "user-1", 4, published, "sports", "tools")})
	if got := e.indexed(t, id); got.GetVersion() != 4 {
		t.Fatalf("newer edit not applied: %v", got)
	}
}

func TestRemovalEvents(t *testing.T) {
	e := setup(t)
	id := uuid.NewString()
	pub := func(v int32) {
		e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(id, "user-1", v, published, "books", "tools")})
	}
	cases := []struct {
		name string
		typ  string
		msg  proto.Message
	}{
		{"hidden", "taakht.ad.v1.AdHidden", &adv1.AdHidden{AdId: id}},
		{"locked", "taakht.ad.v1.AdLocked", &adv1.AdLocked{AdId: id, SwapId: "s"}},
		{"closed", "taakht.ad.v1.AdClosed", &adv1.AdClosed{AdId: id, SwapId: "s"}},
		{"edit to hidden snapshot", typEdit, &adv1.AdEdited{Ad: ad(id, "user-1", 100, hidden, "books", "tools")}},
		{"released as hidden", "taakht.ad.v1.AdReleased", &adv1.AdReleased{AdId: id, Ad: ad(id, "user-1", 100, hidden, "books", "tools")}},
	}
	for i, c := range cases {
		pub(int32(i + 1))
		if e.indexed(t, id) == nil {
			t.Fatalf("%s: setup failed", c.name)
		}
		e.deliver(t, c.typ, c.msg)
		if e.indexed(t, id) != nil {
			t.Fatalf("%s: ad still indexed", c.name)
		}
	}
	// A release as published puts the ad back.
	pub(10)
	e.deliver(t, "taakht.ad.v1.AdLocked", &adv1.AdLocked{AdId: id})
	e.deliver(t, "taakht.ad.v1.AdReleased", &adv1.AdReleased{AdId: id, Ad: ad(id, "user-1", 10, published, "books", "tools")})
	if e.indexed(t, id) == nil {
		t.Fatal("released published ad should be back in the index")
	}
}

func TestMatchLimitAndTwoSidedFilter(t *testing.T) {
	e := setup(t)
	for i := 0; i < 8; i++ {
		e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(uuid.NewString(), "other", 1, published, "tools", "books")})
	}
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(uuid.NewString(), "other", 1, published, "tools", "kitchen")})
	e.sent = nil
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(uuid.NewString(), "user-1", 1, published, "books", "tools")})
	if len(e.sent) != handlers.MaxMatchesPerAd {
		t.Fatalf("expected %d matches, got %d", handlers.MaxMatchesPerAd, len(e.sent))
	}
}

func TestProcessedEventIsIdempotent(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	other := uuid.NewString()
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(other, "user-1", 1, published, "books", "tools")})

	payload, _ := proto.Marshal(&adv1.AdPublished{Ad: ad(uuid.NewString(), "user-2", 1, published, "tools", "books")})
	envelope := &commonv1.Envelope{EventId: uuid.NewString(), Type: typPub, Payload: payload}
	for range 3 {
		if err := consume.Process(ctx, e.pool, "matching", envelope, e.m[typPub]); err != nil {
			t.Fatal(err)
		}
	}
	if len(e.sent) != 1 {
		t.Fatalf("expected exactly one MatchFound across redeliveries, got %d", len(e.sent))
	}
}
