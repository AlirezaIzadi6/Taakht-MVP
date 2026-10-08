// Package index is the Postgres-backed read model of published ads.
package index

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
)

// Querier is satisfied by both pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Upsert stores the snapshot unless the index already holds a newer version.
// It reports whether the row was written.
func Upsert(ctx context.Context, q Querier, ad *adv1.Ad) (bool, error) {
	snap, err := proto.Marshal(ad)
	if err != nil {
		return false, fmt.Errorf("marshal snapshot: %w", err)
	}
	spec := ad.GetSpec()
	tag, err := q.Exec(ctx, `
INSERT INTO ad_index (ad_id, owner_id, status, version, have_category, want_categories,
                      neighborhood_ids, title, value_estimate, snapshot, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
ON CONFLICT (ad_id) DO UPDATE SET
  owner_id = EXCLUDED.owner_id, status = EXCLUDED.status, version = EXCLUDED.version,
  have_category = EXCLUDED.have_category, want_categories = EXCLUDED.want_categories,
  neighborhood_ids = EXCLUDED.neighborhood_ids, title = EXCLUDED.title,
  value_estimate = EXCLUDED.value_estimate, snapshot = EXCLUDED.snapshot, updated_at = now()
WHERE ad_index.version <= EXCLUDED.version`,
		ad.GetId(), ad.GetOwnerId(), ad.GetStatus().String(), ad.GetVersion(),
		spec.GetHaveCategory(), nonNil(spec.GetWantCategories()), nonNil(spec.GetNeighborhoodIds()),
		spec.GetTitle(), spec.GetValueEstimate(), snap)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func Delete(ctx context.Context, q Querier, adID string) error {
	_, err := q.Exec(ctx, `DELETE FROM ad_index WHERE ad_id = $1`, adID)
	return err
}

// Get returns the indexed ad, or nil when it is not indexed.
func Get(ctx context.Context, q Querier, adID string) (*adv1.Ad, error) {
	var snap []byte
	err := q.QueryRow(ctx, `SELECT snapshot FROM ad_index WHERE ad_id = $1`, adID).Scan(&snap)
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

// Candidates returns published indexed ads that pass the filter.
func Candidates(ctx context.Context, q Querier, f Filter) ([]*adv1.Ad, error) {
	rows, err := q.Query(ctx, `
SELECT snapshot FROM ad_index
WHERE status = 'AD_STATUS_PUBLISHED'
  AND ($1 = '' OR owner_id <> $1)
  AND ($2 = '' OR ad_id <> $2::uuid)
  AND (cardinality($3::text[]) = 0 OR have_category = ANY($3))
  AND ($4 = '' OR cardinality(want_categories) = 0 OR $4 = ANY(want_categories))
  AND (cardinality($5::text[]) = 0 OR neighborhood_ids && $5)
ORDER BY ad_id`,
		f.ExcludeOwner, f.ExcludeAdID, nonNil(f.HaveCategories), f.WantsCategory, nonNil(f.NeighborhoodIDs))
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
