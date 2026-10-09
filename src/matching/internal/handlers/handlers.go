// Package handlers applies ad.events to the index and emits MatchFound.
package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"
	"github.com/taakht/taakht/libs/goplatform/reload"
	"github.com/taakht/taakht/src/matching/internal/geo"
	"github.com/taakht/taakht/src/matching/internal/index"
	"github.com/taakht/taakht/src/matching/internal/match"
)

const (
	MatchTopic = "matching.events"
	// MaxMatchesPerAd bounds how many MatchFound events one indexed ad produces.
	MaxMatchesPerAd = 5
)

// Emit writes an event to the outbox inside tx (outbox.Add in production).
type Emit func(ctx context.Context, tx pgx.Tx, topic, key string, msg proto.Message) error

type Handler = consume.Handler

type Handlers struct {
	Geo geo.Map
	// GeoSource, when set, supplies the (reloadable) neighborhood map and takes precedence over Geo.
	GeoSource *reload.Reloadable[geo.Map]
	Emit      Emit
	Log       *slog.Logger
}

func (h *Handlers) geoMap() geo.Map {
	if h.GeoSource != nil {
		return *h.GeoSource.Get()
	}
	return h.Geo
}

// Map returns the handlers keyed by envelope type.
func (h *Handlers) Map() map[string]Handler {
	return map[string]Handler{
		"taakht.ad.v1.AdPublished": onSnapshot[adv1.AdPublished](h),
		"taakht.ad.v1.AdEdited":    onSnapshot[adv1.AdEdited](h),
		"taakht.ad.v1.AdReleased":  onSnapshot[adv1.AdReleased](h),
		"taakht.ad.v1.AdHidden":    onRemove[adv1.AdHidden](h),
		"taakht.ad.v1.AdLocked":    onRemove[adv1.AdLocked](h),
		"taakht.ad.v1.AdClosed":    onRemove[adv1.AdClosed](h),
	}
}

// onSnapshot decodes an event carrying a full ad snapshot and applies it.
func onSnapshot[T any, P interface {
	*T
	proto.Message
	GetAd() *adv1.Ad
	GetSeq() int64
}](h *Handlers) Handler {
	return func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		msg := P(new(T))
		if err := consume.Decode(env, msg); err != nil {
			return err
		}
		return h.ApplySnapshot(ctx, tx, msg.GetAd(), msg.GetSeq())
	}
}

// onRemove decodes an event that only carries an ad id and removes that ad from the index.
func onRemove[T any, P interface {
	*T
	proto.Message
	GetAdId() string
	GetSeq() int64
}](h *Handlers) Handler {
	return func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		msg := P(new(T))
		if err := consume.Decode(env, msg); err != nil {
			return err
		}
		applied, err := index.Remove(ctx, tx, msg.GetAdId(), msg.GetSeq())
		if err == nil && !applied {
			h.Log.Info("ignored stale removal event", "ad_id", msg.GetAdId(), "seq", msg.GetSeq())
		}
		return err
	}
}

// matchLockKey serializes the index-and-match step of ApplySnapshot (transaction-scoped advisory lock).
const matchLockKey int64 = 7_424_002

// ApplySnapshot indexes a published snapshot (and matches it) or removes a non-published one. seq is the
// event's per-ad sequence number (0 for events written before it existed).
func (h *Handlers) ApplySnapshot(ctx context.Context, tx pgx.Tx, ad *adv1.Ad, seq int64) error {
	if ad == nil || ad.GetId() == "" {
		return consume.Permanent(fmt.Errorf("event without ad snapshot"))
	}
	if ad.GetStatus() != adv1.AdStatus_AD_STATUS_PUBLISHED {
		_, err := index.Remove(ctx, tx, ad.GetId(), seq)
		return err
	}
	// Compatible ads that arrive together (parallel partitions, several instances) must see each other: without
	// this lock both transactions upsert, neither sees the other's uncommitted row, and no MatchFound is made.
	// The lock is held until commit, so the second transaction queries candidates after the first one is visible.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, matchLockKey); err != nil {
		return err
	}
	applied, err := index.Upsert(ctx, tx, ad, seq)
	if err != nil {
		return err
	}
	if !applied {
		h.Log.Info("ignored stale ad event", "ad_id", ad.GetId(), "version", ad.GetVersion(), "seq", seq)
		return nil
	}
	if seq > 0 {
		// MatchFound is deduplicated per (ad, matched ad, seq of the ad's event): this event is newer than
		// everything applied for the ad (an edit, or a re-add after a release or hide), so the pairs
		// notified for the previous state no longer count. A redelivery of the same event is not applied
		// (seq <= last_seq) and never reaches this point; legacy events without a seq keep the old
		// once-per-pair behaviour.
		if _, err := tx.Exec(ctx, `DELETE FROM notified_pair WHERE ad_id = $1`, ad.GetId()); err != nil {
			return err
		}
	}
	return h.notifyMatches(ctx, tx, ad)
}

func (h *Handlers) notifyMatches(ctx context.Context, tx pgx.Tx, ad *adv1.Ad) error {
	spec := ad.GetSpec()
	cands, err := index.Candidates(ctx, tx, index.Filter{
		ExcludeOwner:   ad.GetOwnerId(),
		ExcludeAdID:    ad.GetId(),
		HaveCategories: spec.GetWantCategories(),
		WantsCategory:  spec.GetHaveCategory(),
	})
	if err != nil {
		return err
	}
	for _, m := range match.RankTwoSided(h.geoMap(), ad, cands, MaxMatchesPerAd) {
		tag, err := tx.Exec(ctx,
			`INSERT INTO notified_pair (ad_id, matched_ad_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			ad.GetId(), m.Ad.GetId())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		ev := &matchingv1.MatchFound{AdId: ad.GetId(), MatchedAdId: m.Ad.GetId(), Score: m.Score}
		if err := h.Emit(ctx, tx, MatchTopic, ad.GetId(), ev); err != nil {
			return err
		}
		h.Log.Info("MATCH", "ad_id", ad.GetId(), "matched_ad_id", m.Ad.GetId(), "score", m.Score)
	}
	return nil
}
