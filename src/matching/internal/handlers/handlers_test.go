package handlers_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

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

func (e *env) matchCount(ad, matched string) int {
	n := 0
	for _, m := range e.sent {
		if m.AdId == ad && m.MatchedAdId == matched {
			n++
		}
	}
	return n
}

func TestEditAndReReleaseRenotifyOncePerSeq(t *testing.T) {
	e := setup(t)
	a, b := uuid.NewString(), uuid.NewString()
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(a, "user-1", 1, published, "books", "tools"), Seq: 1})
	pubB := &adv1.AdPublished{Ad: ad(b, "user-2", 1, published, "tools", "books"), Seq: 1}
	e.deliver(t, typPub, pubB)
	if e.matchCount(b, a) != 1 {
		t.Fatalf("initial match missing: %v", e.sent)
	}

	// Redelivery of the same event (same seq) never re-notifies.
	e.deliver(t, typPub, pubB)
	if e.matchCount(b, a) != 1 {
		t.Fatalf("redelivery re-notified: %v", e.sent)
	}

	// An edit (higher version and seq) notifies again, once, however often it is redelivered.
	edit := &adv1.AdEdited{Ad: ad(b, "user-2", 2, published, "tools", "books"), Seq: 2}
	e.deliver(t, typEdit, edit)
	e.deliver(t, typEdit, edit)
	if e.matchCount(b, a) != 2 {
		t.Fatalf("edit should re-notify exactly once: %v", e.sent)
	}

	// Out of order: the older publish arriving after the edit changes nothing.
	e.deliver(t, typPub, pubB)
	if e.matchCount(b, a) != 2 {
		t.Fatalf("stale event re-notified: %v", e.sent)
	}

	// Lock and release at the same version: the re-add (higher seq) notifies again; its redelivery does not.
	e.deliver(t, "taakht.ad.v1.AdLocked", &adv1.AdLocked{AdId: b, SwapId: "s", Seq: 3})
	rel := &adv1.AdReleased{AdId: b, SwapId: "s", Ad: ad(b, "user-2", 2, published, "tools", "books"), Seq: 4}
	e.deliver(t, "taakht.ad.v1.AdReleased", rel)
	e.deliver(t, "taakht.ad.v1.AdReleased", rel)
	if e.matchCount(b, a) != 3 {
		t.Fatalf("re-release should re-notify exactly once: %v", e.sent)
	}
	// A late lock (seq 3) and a replayed release must neither remove the ad nor notify.
	e.deliver(t, "taakht.ad.v1.AdLocked", &adv1.AdLocked{AdId: b, SwapId: "s", Seq: 3})
	e.deliver(t, "taakht.ad.v1.AdReleased", rel)
	if e.matchCount(b, a) != 3 || e.indexed(t, b) == nil {
		t.Fatalf("late event had an effect: %v", e.sent)
	}

	// Legacy events without a seq keep once-per-pair.
	l1, l2 := uuid.NewString(), uuid.NewString()
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(l1, "user-3", 1, published, "games", "music")})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(l2, "user-4", 1, published, "music", "games")})
	e.deliver(t, typEdit, &adv1.AdEdited{Ad: ad(l2, "user-4", 2, published, "music", "games")})
	if e.matchCount(l2, l1) != 1 {
		t.Fatalf("legacy edit re-notified: %v", e.sent)
	}
}

func TestEditOnlyClearsThatAdsPairs(t *testing.T) {
	e := setup(t)
	a, b, c := uuid.NewString(), uuid.NewString(), uuid.NewString()
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(a, "user-1", 1, published, "books", "tools"), Seq: 1})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(b, "user-2", 1, published, "tools", "books"), Seq: 1})
	e.deliver(t, typPub, &adv1.AdPublished{Ad: ad(c, "user-3", 1, published, "tools", "books"), Seq: 1})
	before := len(e.sent)
	e.deliver(t, typEdit, &adv1.AdEdited{Ad: ad(a, "user-1", 2, published, "books", "tools"), Seq: 2})
	// a is notified about b and c again; b and c are not re-notified about a.
	if e.matchCount(a, b) != 1 || e.matchCount(a, c) != 1 || len(e.sent) != before+2 || e.matchCount(b, a) != 1 || e.matchCount(c, a) != 1 {
		t.Fatalf("unexpected notifications: %v", e.sent)
	}
}

// Two compatible ads on different partitions can be applied at the same time by parallel consumers (or by two
// instances). Each transaction must see the other's committed row, otherwise neither notifies.
func TestConcurrentApplyOfCompatibleAdsStillNotifies(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	a, b := uuid.NewString(), uuid.NewString()

	tx1, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.h.ApplySnapshot(ctx, tx1, ad(a, "user-1", 1, published, "books", "tools"), 1); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		tx2, err := e.pool.Begin(ctx)
		if err != nil {
			done <- err
			return
		}
		if err := e.h.ApplySnapshot(ctx, tx2, ad(b, "user-2", 1, published, "tools", "books"), 1); err != nil {
			_ = tx2.Rollback(ctx)
			done <- err
			return
		}
		done <- tx2.Commit(ctx)
	}()

	// Let the second transaction run up to the point where it would query candidates, then commit the first.
	time.Sleep(300 * time.Millisecond)
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got := e.matchCount(a, b) + e.matchCount(b, a); got != 1 {
		t.Fatalf("expected exactly one MatchFound for the pair, got %d (%v)", got, e.sent)
	}
}
