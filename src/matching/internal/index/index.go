// Package index is the Postgres-backed read model of published ads.
package index

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	"github.com/taakht/taakht/libs/goplatform/housekeeping"
)

// Querier is satisfied by both pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Upsert stores the snapshot of a published ad carried by an event with the given seq. It reports whether the
// row was written. With seq > 0 an event applies only when it is newer than the last one seen for the ad
// (tombstones included), so a replayed older event cannot overwrite or resurrect anything; a re-added ad
// clears its tombstone. Events without a seq (seq 0, written before the counter existed) fall back to
// the version check and never touch an ad that already has sequenced state.
func Upsert(ctx context.Context, q Querier, ad *adv1.Ad, seq int64) (bool, error) {
	snap, err := proto.Marshal(ad)
	if err != nil {
		return false, fmt.Errorf("marshal snapshot: %w", err)
	}
	spec := ad.GetSpec()
	tag, err := q.Exec(ctx, `
INSERT INTO ad_index (ad_id, owner_id, status, version, have_category, want_categories,
                      neighborhood_ids, title, value_estimate, snapshot, last_seq, removed, removed_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, false, NULL, now())
ON CONFLICT (ad_id) DO UPDATE SET
  owner_id = EXCLUDED.owner_id, status = EXCLUDED.status, version = EXCLUDED.version,
  have_category = EXCLUDED.have_category, want_categories = EXCLUDED.want_categories,
  neighborhood_ids = EXCLUDED.neighborhood_ids, title = EXCLUDED.title,
  value_estimate = EXCLUDED.value_estimate, snapshot = EXCLUDED.snapshot,
  last_seq = GREATEST(ad_index.last_seq, EXCLUDED.last_seq), removed = false, removed_at = NULL, updated_at = now()
WHERE (EXCLUDED.last_seq > 0 AND ad_index.last_seq < EXCLUDED.last_seq)
   OR (EXCLUDED.last_seq = 0 AND ad_index.last_seq = 0 AND NOT ad_index.removed AND ad_index.version <= EXCLUDED.version)`,
		ad.GetId(), ad.GetOwnerId(), ad.GetStatus().String(), ad.GetVersion(),
		spec.GetHaveCategory(), nonNil(spec.GetWantCategories()), nonNil(spec.GetNeighborhoodIds()),
		spec.GetTitle(), spec.GetValueEstimate(), snap, seq)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Remove takes an ad out of the search results while keeping a tombstone row (removed, last_seq) so
// that older events cannot bring it back. It reports whether the event was applied. Without a seq
// (legacy event) the row is deleted, unless it already holds sequenced state, in which case the legacy
// event is older than that state and ignored.
func Remove(ctx context.Context, q Querier, adID string, seq int64) (bool, error) {
	if seq <= 0 {
		tag, err := q.Exec(ctx, `DELETE FROM ad_index WHERE ad_id = $1 AND last_seq = 0`, adID)
		if err != nil {
			return false, err
		}
		return tag.RowsAffected() == 1, nil
	}
	tag, err := q.Exec(ctx, `
INSERT INTO ad_index (ad_id, owner_id, status, version, have_category, snapshot, last_seq, removed, removed_at, updated_at)
VALUES ($1, '', 'AD_STATUS_REMOVED', 0, '', ''::bytea, $2, true, now(), now())
ON CONFLICT (ad_id) DO UPDATE SET
  status = 'AD_STATUS_REMOVED', last_seq = EXCLUDED.last_seq, removed = true, removed_at = now(), updated_at = now()
WHERE ad_index.last_seq < EXCLUDED.last_seq`, adID, seq)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// TombstonePrune builds the housekeeping rule that deletes tombstones (removed rows) older than retention.
//
// Tradeoff: a tombstone is what stops a replayed older event from resurrecting a removed ad (the replayed
// event has a seq at or below the tombstone's last_seq). Once the tombstone is gone, a replay of an
// AdPublished/AdEdited that is still in Kafka would index the ad again. The retention must therefore exceed
// the retention of the ad.events topic (and any planned offset reset) by a wide margin.
func TombstonePrune(retention time.Duration) housekeeping.Prune {
	return housekeeping.Prune{
		Name:      EnvTombstoneRetention,
		Retention: retention,
		SQL: `DELETE FROM ad_index WHERE ad_id IN (
			SELECT ad_id FROM ad_index
			WHERE removed AND removed_at IS NOT NULL AND removed_at < now() - ($1 * interval '1 second')
			LIMIT $2 FOR UPDATE SKIP LOCKED)`,
	}
}

// Tombstone retention settings.
const (
	EnvTombstoneRetention     = "TOMBSTONE_RETENTION"
	DefaultTombstoneRetention = 7 * 24 * time.Hour
)

// Get returns the indexed ad, or nil when it is not indexed (a tombstone counts as not indexed).
func Get(ctx context.Context, q Querier, adID string) (*adv1.Ad, error) {
	var snap []byte
	err := q.QueryRow(ctx, `SELECT snapshot FROM ad_index WHERE ad_id = $1 AND NOT removed`, adID).Scan(&snap)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return unmarshal(snap)
}

// Filter narrows a candidate scan in SQL; scoring and ranking happen in Go.
// Zero-value fields mean "no constraint".
type Filter struct {
	ExcludeOwner    string
	ExcludeAdID     string
	HaveCategories  []string // candidate.have_category must be one of these
	WantsCategory   string   // candidate must want this category, or be open
	NeighborhoodIDs []string // candidate neighborhoods must overlap
}

// MaxCandidates bounds how many rows one scan reads and unmarshals before scoring.
const MaxCandidates = 1000

// Candidates returns up to MaxCandidates published indexed ads that pass the
// filter, most recently updated first (ties by ad_id), so the bound drops the
// oldest ads deterministically.
func Candidates(ctx context.Context, q Querier, f Filter) ([]*adv1.Ad, error) {
	rows, err := q.Query(ctx, `
SELECT snapshot FROM ad_index
WHERE NOT removed AND status = 'AD_STATUS_PUBLISHED'
  AND ($1 = '' OR owner_id <> $1)
  AND ($2 = '' OR ad_id <> $2::uuid)
  AND (cardinality($3::text[]) = 0 OR have_category = ANY($3))
  AND ($4 = '' OR cardinality(want_categories) = 0 OR $4 = ANY(want_categories))
  AND (cardinality($5::text[]) = 0 OR neighborhood_ids && $5)
ORDER BY updated_at DESC, ad_id
LIMIT $6`,
		f.ExcludeOwner, f.ExcludeAdID, nonNil(f.HaveCategories), f.WantsCategory, nonNil(f.NeighborhoodIDs), MaxCandidates)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*adv1.Ad
	for rows.Next() {
		var snap []byte
		if err := rows.Scan(&snap); err != nil {
			return nil, err
		}
		ad, err := unmarshal(snap)
		if err != nil {
			return nil, err
		}
		out = append(out, ad)
	}
	return out, rows.Err()
}

func unmarshal(snap []byte) (*adv1.Ad, error) {
	ad := &adv1.Ad{}
	if err := proto.Unmarshal(snap, ad); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return ad, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
