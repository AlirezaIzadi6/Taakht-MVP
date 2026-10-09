package ad

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/libs/goplatform/db"
	"github.com/taakht/taakht/libs/goplatform/identity"
	"github.com/taakht/taakht/src/ad/migrations"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func newTestService(t *testing.T) (*Service, *pgxpool.Pool) {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	name := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		c, err := pgx.Connect(context.Background(), base)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
	})
	if err := db.Migrate(ctx, pool, migrations.FS, "."); err != nil {
		t.Fatal(err)
	}
	return NewService(pool, testElig), pool
}

// as builds the context the identity interceptor would produce; system ids carry the internal token.
func as(user string) context.Context {
	md := metadata.Pairs(identity.Header, user)
	if identity.IsSystemID(user) {
		md.Set(identity.TokenHeader, identity.InternalToken())
	}
	return identity.WithUserID(metadata.NewIncomingContext(context.Background(), md), user)
}

// asWithoutToken is a bare system id with no proof (what the interceptor refuses before any handler runs).
func asWithoutToken(user string) context.Context {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identity.Header, user))
	return identity.WithUserID(ctx, user)
}

func TestSystemIdentityWithoutTokenIsNotSystem(t *testing.T) {
	s, _ := newTestService(t)
	a := mustCreate(t, s, "user-1", "a")
	b := mustCreate(t, s, "user-2", "b")
	_, err := s.LockAds(asWithoutToken(identity.SystemSwap), lockReq("s-notoken", a, b, 1, 1))
	wantCode(t, err, codes.PermissionDenied)
	_, err = s.GetAd(asWithoutToken(identity.SystemNegotiation), &adv1.GetAdRequest{AdId: a.Id})
	wantCode(t, err, codes.NotFound)
}

func spec(title string) *adv1.AdSpec {
	return &adv1.AdSpec{Title: title, HaveCategory: "books", WantCategories: []string{"tools"}}
}

func mustCreate(t *testing.T, s *Service, owner, title string) *adv1.Ad {
	t.Helper()
	a, err := s.CreateAd(as(owner), &adv1.CreateAdRequest{Spec: spec(title)})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("code = %v (%v), want %v", status.Code(err), err, want)
	}
}

func TestAdLifecycle(t *testing.T) {
	s, pool := newTestService(t)
	a := mustCreate(t, s, "user-1", "book")
	if a.Status != adv1.AdStatus_AD_STATUS_HIDDEN || a.Version != 1 {
		t.Fatalf("new ad = %v", a)
	}

	_, err := s.PublishAd(as("user-2"), &adv1.AdIdRequest{AdId: a.Id})
	wantCode(t, err, codes.PermissionDenied)
	_, err = s.HideAd(as("user-1"), &adv1.AdIdRequest{AdId: a.Id})
	wantCode(t, err, codes.FailedPrecondition)

	// Publish needs a filter.
	noFilter, err := s.CreateAd(as("user-1"), &adv1.CreateAdRequest{Spec: &adv1.AdSpec{Title: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.PublishAd(as("user-1"), &adv1.AdIdRequest{AdId: noFilter.Id})
	wantCode(t, err, codes.InvalidArgument)

	pub, err := s.PublishAd(as("user-1"), &adv1.AdIdRequest{AdId: a.Id})
	if err != nil || pub.Status != adv1.AdStatus_AD_STATUS_PUBLISHED {
		t.Fatalf("publish: %v %v", pub, err)
	}
	_, err = s.PublishAd(as("user-1"), &adv1.AdIdRequest{AdId: a.Id})
	wantCode(t, err, codes.FailedPrecondition)

	// Edit: stale version aborts, good one bumps.
	_, err = s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 5, Spec: spec("v2")})
	wantCode(t, err, codes.Aborted)
	ed, err := s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 1, Spec: spec("v2")})
	if err != nil || ed.Version != 2 || ed.Spec.Title != "v2" {
		t.Fatalf("edit: %v %v", ed, err)
	}
	_, err = s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 2, Spec: &adv1.AdSpec{Title: "nofilter"}})
	wantCode(t, err, codes.InvalidArgument)

	old, err := s.GetAd(as("user-1"), &adv1.GetAdRequest{AdId: a.Id, Version: 1})
	if err != nil || old.Spec.Title != "book" || old.Version != 1 {
		t.Fatalf("get v1: %v %v", old, err)
	}
	cur, err := s.GetAd(as(identity.SystemSwap), &adv1.GetAdRequest{AdId: a.Id})
	if err != nil || cur.Version != 2 {
		t.Fatalf("get current: %v %v", cur, err)
	}
	_, err = s.GetAd(as("user-3"), &adv1.GetAdRequest{AdId: uuid.NewString()})
	wantCode(t, err, codes.NotFound)

	if _, err := s.HideAd(as("user-1"), &adv1.AdIdRequest{AdId: a.Id}); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListMyAds(as("user-1"), &adv1.ListMyAdsRequest{})
	if err != nil || len(list.Ads) != 2 {
		t.Fatalf("list: %v %v", list, err)
	}

	// create, publish, edit, hide -> AdPublished, AdEdited, AdHidden among others.
	if n := countEvents(t, pool, "taakht.ad.v1.AdEdited"); n != 1 {
		t.Fatalf("AdEdited events = %d", n)
	}
	if n := countEvents(t, pool, "taakht.ad.v1.AdHidden"); n != 1 {
		t.Fatalf("AdHidden events = %d", n)
	}
}

func countEvents(t *testing.T, pool *pgxpool.Pool, typ string) int {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT envelope FROM outbox`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		env := &commonv1.Envelope{}
		if err := proto.Unmarshal(raw, env); err != nil {
			t.Fatal(err)
		}
		if env.Type == typ {
			n++
		}
	}
	return n
}

func publish(t *testing.T, s *Service, a *adv1.Ad) {
	t.Helper()
	if _, err := s.PublishAd(as(a.OwnerId), &adv1.AdIdRequest{AdId: a.Id}); err != nil {
		t.Fatal(err)
	}
}

func lockReq(swap string, a, b *adv1.Ad, va, vb int32) *adv1.LockAdsRequest {
	return &adv1.LockAdsRequest{SwapId: swap, Ads: []*adv1.AdRef{{AdId: a.Id, Version: va}, {AdId: b.Id, Version: vb}}}
}

func TestLockAdsRulesAndIdempotency(t *testing.T) {
	s, pool := newTestService(t)
	a := mustCreate(t, s, "user-1", "a")
	b := mustCreate(t, s, "user-2", "b")
	publish(t, s, a) // b stays hidden: hidden ads are lockable too

	_, err := s.LockAds(as(identity.SystemSwap), lockReq("s0", a, b, 9, 1))
	wantCode(t, err, codes.FailedPrecondition) // wrong version
	st := func(ad *adv1.Ad) string {
		var v string
		_ = pool.QueryRow(context.Background(), `SELECT status FROM ad WHERE id = $1`, ad.Id).Scan(&v)
		return v
	}
	if st(a) != StatusPublished || st(b) != StatusHidden {
		t.Fatal("failed lock must change nothing")
	}

	if _, err := s.LockAds(as(identity.SystemSwap), lockReq("s1", a, b, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if st(a) != StatusLocked || st(b) != StatusLocked {
		t.Fatal("both ads must be locked")
	}
	// Retry with the same swap id and ads (either order) is OK and emits nothing new.
	before := countEvents(t, pool, "taakht.ad.v1.AdLocked")
	if _, err := s.LockAds(as(identity.SystemSwap), lockReq("s1", b, a, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if countEvents(t, pool, "taakht.ad.v1.AdLocked") != before || before != 2 {
		t.Fatalf("AdLocked events = %d", before)
	}
	// Another swap cannot take them; locked ads reject edits.
	_, err = s.LockAds(as(identity.SystemSwap), lockReq("s2", a, b, 1, 1))
	wantCode(t, err, codes.FailedPrecondition)
	_, err = s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 1, Spec: spec("z")})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = s.LockAds(as(identity.SystemSwap), &adv1.LockAdsRequest{SwapId: "s3", Ads: []*adv1.AdRef{{AdId: a.Id, Version: 1}}})
	wantCode(t, err, codes.InvalidArgument)
}

func TestLockAdsConcurrentOverlap(t *testing.T) {
	s, pool := newTestService(t)
	a := mustCreate(t, s, "user-1", "a")
	candidates := make([]*adv1.Ad, 6)
	publish(t, s, a)
	for i := range candidates {
		candidates[i] = mustCreate(t, s, fmt.Sprintf("user-%d", i+2), "c")
		publish(t, s, candidates[i])
	}

	var wins, rejected atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, c := range candidates {
		// Two retries of each swap id run concurrently; every swap competes for ad a.
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := s.LockAds(as(identity.SystemSwap), lockReq(fmt.Sprintf("swap-%d", i), a, c, 1, 1))
				if code := status.Code(err); code == codes.OK {
					wins.Add(1)
				} else if code == codes.FailedPrecondition {
					rejected.Add(1)
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
	}
	close(start)
	wg.Wait()

	// Exactly one swap id won, and both of its concurrent calls returned OK.
	if wins.Load() != 2 || rejected.Load() != 10 {
		t.Fatalf("wins = %d, rejected = %d", wins.Load(), rejected.Load())
	}
	var swaps, locked int
	_ = pool.QueryRow(context.Background(), `SELECT count(DISTINCT swap_id) FROM ad_lock`).Scan(&swaps)
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM ad WHERE status = 'locked'`).Scan(&locked)
	if swaps != 1 || locked != 2 {
		t.Fatalf("lock swaps = %d, locked ads = %d", swaps, locked)
	}
}

func envelopeOf(t *testing.T, msg proto.Message) *commonv1.Envelope {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return &commonv1.Envelope{EventId: uuid.NewString(), Type: string(msg.ProtoReflect().Descriptor().FullName()), Payload: raw}
}

func TestSwapEventConsumers(t *testing.T) {
	s, pool := newTestService(t)
	ctx := context.Background()
	a := mustCreate(t, s, "user-1", "a")
	b := mustCreate(t, s, "user-2", "b")
	publish(t, s, a) // a published, b hidden before the lock
	if _, err := s.LockAds(as(identity.SystemSwap), lockReq("sw", a, b, 1, 1)); err != nil {
		t.Fatal(err)
	}
	h := Handlers()

	cancelled := envelopeOf(t, &swapv1.SwapCancelled{SwapId: "sw", AdAId: a.Id, AdBId: b.Id})
	for range 2 { // second delivery is deduplicated
		if err := consume.Process(ctx, pool, Group, cancelled, h["taakht.swap.v1.SwapCancelled"]); err != nil {
			t.Fatal(err)
		}
	}
	ga, _ := s.GetAd(as(identity.SystemSwap), &adv1.GetAdRequest{AdId: a.Id})
	gb, _ := s.GetAd(as(identity.SystemSwap), &adv1.GetAdRequest{AdId: b.Id})
	if ga.Status != adv1.AdStatus_AD_STATUS_PUBLISHED || gb.Status != adv1.AdStatus_AD_STATUS_HIDDEN {
		t.Fatalf("release restored %v / %v", ga.Status, gb.Status)
	}
	if n := countEvents(t, pool, "taakht.ad.v1.AdReleased"); n != 2 {
		t.Fatalf("AdReleased = %d", n)
	}

	// A late SwapCompleted for the released swap must not close anything.
	late := envelopeOf(t, &swapv1.SwapCompleted{SwapId: "sw", AdAId: a.Id, AdBId: b.Id})
	if err := consume.Process(ctx, pool, Group, late, h["taakht.swap.v1.SwapCompleted"]); err != nil {
		t.Fatal(err)
	}
	if ga, _ = s.GetAd(as(identity.SystemSwap), &adv1.GetAdRequest{AdId: a.Id}); ga.Status != adv1.AdStatus_AD_STATUS_PUBLISHED {
		t.Fatalf("status = %v", ga.Status)
	}

	// New swap on the same ads completes.
	if _, err := s.LockAds(as(identity.SystemSwap), lockReq("sw2", a, b, 1, 1)); err != nil {
		t.Fatal(err)
	}
	done := envelopeOf(t, &swapv1.SwapCompleted{SwapId: "sw2", AdAId: a.Id, AdBId: b.Id})
	if err := consume.Process(ctx, pool, Group, done, h["taakht.swap.v1.SwapCompleted"]); err != nil {
		t.Fatal(err)
	}
	for _, ad := range []*adv1.Ad{a, b} {
		g, _ := s.GetAd(as(identity.SystemSwap), &adv1.GetAdRequest{AdId: ad.Id})
		if g.Status != adv1.AdStatus_AD_STATUS_CLOSED {
			t.Fatalf("status = %v", g.Status)
		}
	}
	if n := countEvents(t, pool, "taakht.ad.v1.AdClosed"); n != 2 {
		t.Fatalf("AdClosed = %d", n)
	}
	// Closed ads can be neither edited nor locked again.
	_, err := s.LockAds(as(identity.SystemSwap), lockReq("sw3", a, b, 1, 1))
	wantCode(t, err, codes.FailedPrecondition)
}

func TestLockAdsRequiresSwapIdentity(t *testing.T) {
	s, _ := newTestService(t)
	a := mustCreate(t, s, "user-1", "a")
	b := mustCreate(t, s, "user-2", "b")
	for _, who := range []string{"user-1", "user-2", identity.SystemNegotiation} {
		_, err := s.LockAds(as(who), lockReq("s-auth", a, b, 1, 1))
		wantCode(t, err, codes.PermissionDenied)
	}
	if _, err := s.LockAds(as(identity.SystemSwap), lockReq("s-auth", a, b, 1, 1)); err != nil {
		t.Fatal(err)
	}
}

func TestGetAdVisibility(t *testing.T) {
	s, _ := newTestService(t)
	a := mustCreate(t, s, "user-1", "a")

	// Hidden: only the owner and system identities see it; others get NOT_FOUND.
	_, err := s.GetAd(as("user-2"), &adv1.GetAdRequest{AdId: a.Id})
	wantCode(t, err, codes.NotFound)
	for _, who := range []string{"user-1", identity.SystemNegotiation, identity.SystemSwap} {
		if _, err := s.GetAd(as(who), &adv1.GetAdRequest{AdId: a.Id}); err != nil {
			t.Fatalf("%s: %v", who, err)
		}
	}

	publish(t, s, a)
	if _, err := s.GetAd(as("user-2"), &adv1.GetAdRequest{AdId: a.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 1, Spec: spec("v2")}); err != nil {
		t.Fatal(err)
	}
	// Old versions are for the owner and systems only.
	_, err = s.GetAd(as("user-2"), &adv1.GetAdRequest{AdId: a.Id, Version: 1})
	wantCode(t, err, codes.NotFound)
	if _, err := s.GetAd(as("user-2"), &adv1.GetAdRequest{AdId: a.Id, Version: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAd(as("user-1"), &adv1.GetAdRequest{AdId: a.Id, Version: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAd(as(identity.SystemNegotiation), &adv1.GetAdRequest{AdId: a.Id, Version: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestUndecodableSwapEventIsPermanent(t *testing.T) {
	s, pool := newTestService(t)
	_ = s
	env := &commonv1.Envelope{EventId: uuid.NewString(), Type: "taakht.swap.v1.SwapCompleted", Payload: []byte{0xff, 0xff, 0xff}}
	for typ, h := range Handlers() {
		err := consume.Process(context.Background(), pool, Group, env, h)
		if !consume.IsPermanent(err) {
			t.Fatalf("%s: err = %v, want permanent", typ, err)
		}
		env.EventId = uuid.NewString()
	}
}

// eventSeqs returns the seq of every ad event written for adID, in outbox order.
func eventSeqs(t *testing.T, pool *pgxpool.Pool, adID string) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT envelope FROM outbox WHERE key = $1 ORDER BY created_at, id`, adID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		env := &commonv1.Envelope{}
		if err := proto.Unmarshal(raw, env); err != nil {
			t.Fatal(err)
		}
		mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(env.Type))
		if err != nil {
			t.Fatal(err)
		}
		m := mt.New().Interface()
		if err := proto.Unmarshal(env.Payload, m); err != nil {
			t.Fatal(err)
		}
		f := m.ProtoReflect().Descriptor().Fields().ByName("seq")
		if f == nil {
			t.Fatalf("%s has no seq field", env.Type)
		}
		out = append(out, m.ProtoReflect().Get(f).Int())
	}
	return out
}

func TestEventSeqIncreasesPerAd(t *testing.T) {
	s, pool := newTestService(t)
	ctx := context.Background()
	a := mustCreate(t, s, "user-1", "a")
	b := mustCreate(t, s, "user-2", "b")
	publish(t, s, a)                                                                                                         // 1
	if _, err := s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 1, Spec: spec("v2")}); err != nil { // 2
		t.Fatal(err)
	}
	if _, err := s.LockAds(as(identity.SystemSwap), lockReq("sw-seq", a, b, 2, 1)); err != nil { // 3 (a), 1 (b)
		t.Fatal(err)
	}
	cancelled := envelopeOf(t, &swapv1.SwapCancelled{SwapId: "sw-seq", AdAId: a.Id, AdBId: b.Id})
	if err := consume.Process(ctx, pool, Group, cancelled, Handlers()["taakht.swap.v1.SwapCancelled"]); err != nil { // 4 (a), 2 (b)
		t.Fatal(err)
	}
	if got := eventSeqs(t, pool, a.Id); !slices.Equal(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("ad a seqs = %v", got)
	}
	if got := eventSeqs(t, pool, b.Id); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("ad b seqs = %v", got)
	}
}

func TestEditAdWithUnchangedSpecIsNoOp(t *testing.T) {
	s, pool := newTestService(t)
	a := mustCreate(t, s, "user-1", "a")
	publish(t, s, a)
	before := countEvents(t, pool, "taakht.ad.v1.AdEdited")

	// Same content, differently spelled (padding in the title, duplicate want entries): still no change.
	same := spec("a")
	same.Title = "  a "
	same.WantCategories = []string{"tools", "tools"}
	got, err := s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 1, Spec: same})
	if err != nil || got.Version != 1 {
		t.Fatalf("no-op edit = %v, %v", got, err)
	}
	if n := countEvents(t, pool, "taakht.ad.v1.AdEdited"); n != before {
		t.Fatalf("AdEdited events %d -> %d", before, n)
	}
	cur, _ := s.GetAd(as("user-1"), &adv1.GetAdRequest{AdId: a.Id})
	if cur.Version != 1 {
		t.Fatalf("version = %d", cur.Version)
	}

	// A stale expected_version still aborts even when the spec is unchanged.
	_, err = s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 7, Spec: spec("a")})
	wantCode(t, err, codes.Aborted)

	// A real change still bumps the version and emits.
	ed, err := s.EditAd(as("user-1"), &adv1.EditAdRequest{AdId: a.Id, ExpectedVersion: 1, Spec: spec("changed")})
	if err != nil || ed.Version != 2 {
		t.Fatalf("edit = %v, %v", ed, err)
	}
	if n := countEvents(t, pool, "taakht.ad.v1.AdEdited"); n != before+1 {
		t.Fatalf("AdEdited events = %d", n)
	}
}

func TestGetAdMissingAndInvisibleAreIndistinguishable(t *testing.T) {
	s, _ := newTestService(t)
	hidden := mustCreate(t, s, "user-1", "hidden")
	_, errMissing := s.GetAd(as("user-2"), &adv1.GetAdRequest{AdId: uuid.NewString()})
	_, errHidden := s.GetAd(as("user-2"), &adv1.GetAdRequest{AdId: hidden.Id})
	wantCode(t, errMissing, codes.NotFound)
	wantCode(t, errHidden, codes.NotFound)
	if errMissing.Error() != errHidden.Error() {
		t.Fatalf("messages differ: %q vs %q", errMissing, errHidden)
	}
}
