package index_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	"github.com/taakht/taakht/libs/goplatform/housekeeping"
	"github.com/taakht/taakht/src/matching/internal/index"
	"github.com/taakht/taakht/src/matching/internal/testdb"
)

func published(id string) *adv1.Ad {
	return &adv1.Ad{Id: id, OwnerId: "u", Version: 1, Status: adv1.AdStatus_AD_STATUS_PUBLISHED, Spec: &adv1.AdSpec{HaveCategory: "books"}}
}

func TestTombstonePruneDeletesOnlyOldTombstones(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	live := published(uuid.NewString())
	if ok, err := index.Upsert(ctx, pool, live, 1); err != nil || !ok {
		t.Fatal(ok, err)
	}
	// A live row that has not been touched for months must survive.
	oldLive := published(uuid.NewString())
	if _, err := index.Upsert(ctx, pool, oldLive, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ad_index SET updated_at = now() - interval '90 days' WHERE ad_id = $1`, oldLive.Id); err != nil {
		t.Fatal(err)
	}
	var oldTombs, newTombs []string
	for range 25 {
		oldTombs = append(oldTombs, uuid.NewString())
	}
	for range 3 {
		newTombs = append(newTombs, uuid.NewString())
	}
	for _, id := range append(append([]string{}, oldTombs...), newTombs...) {
		if ok, err := index.Remove(ctx, pool, id, 2); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE ad_index SET removed_at = now() - interval '8 days' WHERE ad_id = ANY($1::uuid[])`, oldTombs); err != nil {
		t.Fatal(err)
	}

	// Batch size 10 forces several statements (25 -> 10+10+5).
	opts := housekeeping.Options{BatchSize: 10, Extra: []housekeeping.Prune{index.TombstonePrune(index.DefaultTombstoneRetention)}}
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := housekeeping.PruneOnce(ctx, pool, opts); err != nil {
		t.Fatal(err)
	}
	var rows, tombs int
	if err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE removed) FROM ad_index`).Scan(&rows, &tombs); err != nil {
		t.Fatal(err)
	}
	if rows != 5 || tombs != 3 { // 2 live rows + 3 fresh tombstones
		t.Fatalf("rows=%d tombstones=%d, want 5 and 3", rows, tombs)
	}
	if got, err := index.Get(ctx, pool, oldLive.Id); err != nil || got == nil {
		t.Fatalf("an old live row was pruned: %v %v", got, err)
	}

	// A fresh tombstone still blocks an older replayed event; a pruned one no longer does (the documented tradeoff).
	if ok, err := index.Upsert(ctx, pool, published(newTombs[0]), 1); err != nil || ok {
		t.Fatalf("fresh tombstone must block a replay: %v %v", ok, err)
	}
	if ok, err := index.Upsert(ctx, pool, published(oldTombs[0]), 1); err != nil || !ok {
		t.Fatalf("replay after pruning re-indexes: %v %v", ok, err)
	}

	opts.Extra = []housekeeping.Prune{index.TombstonePrune(time.Second)}
	if opts.Validate() == nil {
		t.Fatal("retention below one minute accepted")
	}
}
