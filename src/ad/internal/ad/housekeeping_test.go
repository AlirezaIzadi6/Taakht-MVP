package ad

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/libs/goplatform/housekeeping"
	"github.com/taakht/taakht/libs/goplatform/identity"
	"github.com/taakht/taakht/src/ad/internal/eligibility"
)

func TestLockPruneDeletesOnlyOldSettledRows(t *testing.T) {
	s, pool := newTestService(t)
	ctx := context.Background()
	h := Handlers()
	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// swap "done": completed (closed); "cancelled": released; "fresh": released just now; "open": still locked.
	type pair struct{ a, b string }
	mk := func(swap string) pair {
		a := mustCreate(t, s, "user-1", swap+"-a")
		b := mustCreate(t, s, "user-2", swap+"-b")
		if _, err := s.LockAds(as(identity.SystemSwap), lockReq(swap, a, b, 1, 1)); err != nil {
			t.Fatal(err)
		}
		return pair{a.Id, b.Id}
	}
	done, cancelled, fresh := mk("done"), mk("cancelled"), mk("fresh")
	mk("open")
	if err := consume.Process(ctx, pool, Group, envelopeOf(t, &swapv1.SwapCompleted{SwapId: "done", AdAId: done.a, AdBId: done.b}), h["taakht.swap.v1.SwapCompleted"]); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		swap string
		p    pair
	}{{"cancelled", cancelled}, {"fresh", fresh}} {
		if err := consume.Process(ctx, pool, Group, envelopeOf(t, &swapv1.SwapCancelled{SwapId: c.swap, AdAId: c.p.a, AdBId: c.p.b}), h["taakht.swap.v1.SwapCancelled"]); err != nil {
			t.Fatal(err)
		}
	}
	// The trigger stamped settled_at on every settled row and left the locked ones alone.
	if n := count(`SELECT count(*) FROM ad_lock WHERE settled_at IS NOT NULL`); n != 6 {
		t.Fatalf("settled rows = %d, want 6", n)
	}
	if n := count(`SELECT count(*) FROM ad_lock WHERE state = 'locked' AND settled_at IS NULL`); n != 2 {
		t.Fatalf("open rows = %d, want 2", n)
	}
	// Age the rows: settled ones for done/cancelled 40 days, every created_at 90 days (including the locked swap).
	if _, err := pool.Exec(ctx, `UPDATE ad_lock SET created_at = now() - interval '90 days'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ad_lock SET settled_at = now() - interval '40 days' WHERE swap_id IN ('done', 'cancelled')`); err != nil {
		t.Fatal(err)
	}
	versions := count(`SELECT count(*) FROM ad_version`)

	opts := housekeeping.Options{BatchSize: 3, Extra: []housekeeping.Prune{LockPrune(DefaultLockRetention)}}
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := housekeeping.PruneOnce(ctx, pool, opts); err != nil {
		t.Fatal(err)
	}
	if n := count(`SELECT count(*) FROM ad_lock WHERE swap_id IN ('done', 'cancelled')`); n != 0 {
		t.Fatalf("old settled rows left: %d", n)
	}
	if n := count(`SELECT count(*) FROM ad_lock WHERE swap_id IN ('fresh', 'open')`); n != 4 {
		t.Fatalf("rows of fresh or open swaps = %d, want 4", n)
	}
	if n := count(`SELECT count(*) FROM ad_version`); n != versions {
		t.Fatalf("ad_version changed: %d -> %d", versions, n)
	}
}

func createReq(have string) *adv1.CreateAdRequest {
	return &adv1.CreateAdRequest{Spec: &adv1.AdSpec{Title: "t", HaveCategory: have, WantCategories: []string{"tools"}}}
}

func TestEligibilityReloadAffectsValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "e.json")
	t0 := time.Now().Add(-time.Hour)
	write := func(body string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"categories":["tools"],"neighborhoods":[{"id":"n1"}]}`, t0)
	src, err := eligibility.LoadReloadable(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := newTestService(t)
	s = NewReloadableService(s.pool, src)

	_, err = s.CreateAd(as("user-1"), createReq("books"))
	wantCode(t, err, codes.InvalidArgument)

	write(`{"categories":["tools","books"],"neighborhoods":[{"id":"n1"}]}`, t0.Add(time.Minute))
	if changed, err := src.Check(); !changed || err != nil {
		t.Fatalf("reload: %v %v", changed, err)
	}
	if _, err := s.CreateAd(as("user-1"), createReq("books")); err != nil {
		t.Fatalf("new category not accepted after reload: %v", err)
	}

	// An invalid file (empty category list) is rejected and the previous config stays.
	write(`{"categories":[],"neighborhoods":[{"id":"n1"}]}`, t0.Add(2*time.Minute))
	if _, err := src.Check(); err == nil {
		t.Fatal("invalid config accepted")
	}
	if _, err := s.CreateAd(as("user-1"), createReq("books")); err != nil {
		t.Fatalf("old config not kept: %v", err)
	}
}
