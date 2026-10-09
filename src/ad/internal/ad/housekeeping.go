package ad

import (
	"time"

	"github.com/taakht/taakht/libs/goplatform/housekeeping"
)

// Lock retention settings.
const (
	EnvLockRetention     = "AD_LOCK_RETENTION"
	DefaultLockRetention = 30 * 24 * time.Hour
)

// LockPrune builds the housekeeping rule that deletes finished ad_lock rows.
//
// Only rows whose swap finished (state released or closed) and that settled longer ago than retention are
// deleted; a row still 'locked' is never touched, however old. The (swap_id, ad_id) key is what makes a
// repeated LockAds for the same swap idempotent (and lets the ad service reject a different ad pair for a
// reused swap id), so deleting a settled row only matters if a LockAds retry for that long-finished swap
// arrives after the retention; 30 days is far beyond any swap retry window.
//
// ad_version is deliberately never pruned: approvals in the negotiation service and GetAd(version) refer to
// exact versions, and the rows are the audit history of an ad. They are small and bounded by the number of
// edits.
func LockPrune(retention time.Duration) housekeeping.Prune {
	return housekeeping.Prune{
		Name:      EnvLockRetention,
		Retention: retention,
		SQL: `DELETE FROM ad_lock WHERE (swap_id, ad_id) IN (
			SELECT swap_id, ad_id FROM ad_lock
			WHERE state IN ('released', 'closed') AND settled_at IS NOT NULL
			  AND settled_at < now() - ($1 * interval '1 second')
			LIMIT $2 FOR UPDATE SKIP LOCKED)`,
	}
}
