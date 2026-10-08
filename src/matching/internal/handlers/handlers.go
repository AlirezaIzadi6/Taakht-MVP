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
	Geo  geo.Map
	Emit Emit
	Log  *slog.Logger
}

// Map returns the handlers keyed by envelope type.
func (h *Handlers) Map() map[string]Handler {
	return map[string]Handler{
		"taakht.ad.v1.AdPublished": onSnapshot[adv1.AdPublished](h),
		"taakht.ad.v1.AdEdited":    onSnapshot[adv1.AdEdited](h),
		"taakht.ad.v1.AdReleased":  onSnapshot[adv1.AdReleased](h),
		"taakht.ad.v1.AdHidden":    onRemove[adv1.AdHidden](),
		"taakht.ad.v1.AdLocked":    onRemove[adv1.AdLocked](),
		"taakht.ad.v1.AdClosed":    onRemove[adv1.AdClosed](),
	}
}

// onSnapshot decodes an event carrying a full ad snapshot and applies it.
func onSnapshot[T any, P interface {
	*T
	proto.Message
	GetAd() *adv1.Ad
}](h *Handlers) Handler {
	return func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		msg := P(new(T))
		if err := consume.Decode(env, msg); err != nil {
			return err
		}
		return h.ApplySnapshot(ctx, tx, msg.GetAd())
	}
}

// onRemove decodes an event that only carries an ad id and removes that ad from the index.
func onRemove[T any, P interface {
	*T
	proto.Message
	GetAdId() string
}]() Handler {
	return func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		msg := P(new(T))
		if err := consume.Decode(env, msg); err != nil {
			return err
		}
		return index.Delete(ctx, tx, msg.GetAdId())
	}
}

// ApplySnapshot indexes a published snapshot (and matches it) or removes a non-published one.
func (h *Handlers) ApplySnapshot(ctx context.Context, tx pgx.Tx, ad *adv1.Ad) error {
	if ad == nil || ad.GetId() == "" {
		return consume.Permanent(fmt.Errorf("event without ad snapshot"))
	}
	if ad.GetStatus() != adv1.AdStatus_AD_STATUS_PUBLISHED {
		return index.Delete(ctx, tx, ad.GetId())
	}
	applied, err := index.Upsert(ctx, tx, ad)
	if err != nil {
		return err
	}
	if !applied {
		h.Log.Info("ignored stale ad event", "ad_id", ad.GetId(), "version", ad.GetVersion())
		return nil
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
	for _, m := range match.RankTwoSided(h.Geo, ad, cands, MaxMatchesPerAd) {
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
