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

func (e *env) tombstoned(t *testing.T, id string) bool {
	t.Helper()
	var removed bool
	if err := e.pool.QueryRow(context.Background(), `SELECT removed FROM ad_index WHERE ad_id = $1`, id).Scan(&removed); err != nil {
		t.Fatalf("no index row for %s: %v", id, err)
	}
	return removed
}

func TestRemovalKeepsTombstoneAndReplayedOlderEventsAreIgnored(t *testing.T) {
	e := setup(t)
	id, other := uuid.NewString(), uuid.NewString()
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(other, "user-2", 1, published, "tools", "books"), Seq: 1})
	pub := &adv1.AdPublished{Ad: ad(id, "user-1", 1, published, "books", "tools"), Seq: 1}
	e.deliver(t, typPub, pub)
	if len(e.sent) != 1 {
		t.Fatalf("expected the initial match, got %v", e.sent)
	}
	e.sent = nil

	e.deliver(t, "taakht.ad.v1.AdHidden", &adv1.AdHidden{AdId: id, Seq: 2})
	if e.indexed(t, id) != nil || !e.tombstoned(t, id) {
		t.Fatal("hide must leave a tombstone and no search result")
	}
	// Replay of the old publish: must not resurrect the ad or notify again.
	e.deliver(t, typPub, pub)
	if e.indexed(t, id) != nil || len(e.sent) != 0 {
		t.Fatalf("replayed AdPublished resurrected a hidden ad (sent %v)", e.sent)
	}

	// Release re-publishes the SAME version after a lock: its higher seq re-adds the ad.
	e.deliver(t, "taakht.ad.v1.AdLocked", &adv1.AdLocked{AdId: id, SwapId: "s", Seq: 3})
	e.deliver(t, "taakht.ad.v1.AdReleased", &adv1.AdReleased{AdId: id, SwapId: "s", Ad: ad(id, "user-1", 1, published, "books", "tools"), Seq: 4})
	if e.indexed(t, id) == nil {
		t.Fatal("AdReleased with a higher seq must re-add the ad at the same version")
	}
	// A replayed lock (seq 3) is older than the release and must not remove it again.
	e.deliver(t, "taakht.ad.v1.AdLocked", &adv1.AdLocked{AdId: id, SwapId: "s", Seq: 3})
	if e.indexed(t, id) == nil {
		t.Fatal("replayed AdLocked removed a re-released ad")
	}

	e.deliver(t, "taakht.ad.v1.AdClosed", &adv1.AdClosed{AdId: id, SwapId: "s2", Seq: 6})
	e.deliver(t, "taakht.ad.v1.AdReleased", &adv1.AdReleased{AdId: id, SwapId: "s", Ad: ad(id, "user-1", 1, published, "books", "tools"), Seq: 4})
	if e.indexed(t, id) != nil {
		t.Fatal("replayed AdReleased resurrected a closed ad")
	}
	// An equal seq is a duplicate, a strictly higher one wins.
	e.deliver(t, typEdit, &adv1.AdEdited{Ad: ad(id, "user-1", 2, published, "books", "tools"), Seq: 6})
	if e.indexed(t, id) != nil {
		t.Fatal("an event with seq equal to the tombstone's must be ignored")
	}
	e.deliver(t, typEdit, &adv1.AdEdited{Ad: ad(id, "user-1", 2, published, "books", "tools"), Seq: 7})
	if got := e.indexed(t, id); got == nil || got.GetVersion() != 2 {
		t.Fatalf("higher seq must re-add: %v", got)
	}
}

func TestRemovalBeforeAnyIndexRowStillProtects(t *testing.T) {
	e := setup(t)
	id := uuid.NewString()
	// The hide overtakes the publish (replay from an offset reset, or a reordered topic): no row exists yet.
	e.deliver(t, "taakht.ad.v1.AdHidden", &adv1.AdHidden{AdId: id, Seq: 5})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(id, "user-1", 1, published, "books", "tools"), Seq: 4})
	if e.indexed(t, id) != nil {
		t.Fatal("older publish after a newer hide must be ignored")
	}
	// A snapshot that is not published removes with its seq too.
	e.deliver(t, typEdit, &adv1.AdEdited{Ad: ad(id, "user-1", 3, hidden, "books", "tools"), Seq: 6})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(id, "user-1", 2, published, "books", "tools"), Seq: 5})
	if e.indexed(t, id) != nil {
		t.Fatal("older publish after an unpublished snapshot must be ignored")
	}
}

func TestLegacyEventsWithoutSeqKeepWorking(t *testing.T) {
	e := setup(t)
	id := uuid.NewString()
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(id, "user-1", 1, published, "books", "tools")})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(id, "user-1", 1, published, "books", "tools"), Seq: 3})
	// A legacy hide (no seq) is older than sequenced state: ignored.
	e.deliver(t, "taakht.ad.v1.AdHidden", &adv1.AdHidden{AdId: id})
	if e.indexed(t, id) == nil {
		t.Fatal("legacy removal must not override sequenced state")
	}
}
