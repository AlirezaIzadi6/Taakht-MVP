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
		"taakht.ad.v1.AdPublished": h.onSnapshot(func(m proto.Message) *adv1.Ad { return m.(*adv1.AdPublished).GetAd() }, func() proto.Message { return &adv1.AdPublished{} }),
		"taakht.ad.v1.AdEdited":    h.onSnapshot(func(m proto.Message) *adv1.Ad { return m.(*adv1.AdEdited).GetAd() }, func() proto.Message { return &adv1.AdEdited{} }),
		"taakht.ad.v1.AdReleased":  h.onSnapshot(func(m proto.Message) *adv1.Ad { return m.(*adv1.AdReleased).GetAd() }, func() proto.Message { return &adv1.AdReleased{} }),
		"taakht.ad.v1.AdHidden": h.onRemove(func() proto.Message { return &adv1.AdHidden{} },
			func(m proto.Message) string { return m.(*adv1.AdHidden).GetAdId() }),
		"taakht.ad.v1.AdLocked": h.onRemove(func() proto.Message { return &adv1.AdLocked{} },
			func(m proto.Message) string { return m.(*adv1.AdLocked).GetAdId() }),
		"taakht.ad.v1.AdClosed": h.onRemove(func() proto.Message { return &adv1.AdClosed{} },
			func(m proto.Message) string { return m.(*adv1.AdClosed).GetAdId() }),
	}
}

func (h *Handlers) onSnapshot(ad func(proto.Message) *adv1.Ad, newMsg func() proto.Message) Handler {
	return func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		msg := newMsg()
		if err := proto.Unmarshal(env.GetPayload(), msg); err != nil {
			return fmt.Errorf("decode %s: %w", env.GetType(), err)
		}
		return h.ApplySnapshot(ctx, tx, ad(msg))
	}
}

func (h *Handlers) onRemove(newMsg func() proto.Message, id func(proto.Message) string) Handler {
	return func(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
		msg := newMsg()
		if err := proto.Unmarshal(env.GetPayload(), msg); err != nil {
			return fmt.Errorf("decode %s: %w", env.GetType(), err)
		}
		return index.Delete(ctx, tx, id(msg))
	}
}

// ApplySnapshot indexes a published snapshot (and matches it) or removes a non-published one.
func (h *Handlers) ApplySnapshot(ctx context.Context, tx pgx.Tx, ad *adv1.Ad) error {
	if ad == nil || ad.GetId() == "" {
		return fmt.Errorf("event without ad snapshot")
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
