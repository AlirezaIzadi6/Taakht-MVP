package ad

import (
	"context"
	"log/slog"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"github.com/taakht/taakht/libs/goplatform/consume"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Group is the consumer group (and processed_events consumer) of this service.
const Group = "ad"

// SwapTopic is the topic the swap service publishes to.
const SwapTopic = "swap.events"

// Handlers returns the swap event handlers keyed by envelope type.
func Handlers() map[string]consume.Handler {
	return map[string]consume.Handler{
		"taakht.swap.v1.SwapCompleted": onSwapCompleted,
		"taakht.swap.v1.SwapCancelled": onSwapCancelled,
	}
}

func onSwapCompleted(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
	ev := &swapv1.SwapCompleted{}
	if err := consume.Decode(env, ev); err != nil {
		return err
	}
	return settle(ctx, tx, ev.SwapId, []string{ev.AdAId, ev.AdBId}, func(row *adRow) error {
		if _, err := tx.Exec(ctx, `UPDATE ad SET status = 'closed', status_before_lock = NULL, updated_at = $2 WHERE id = $1`,
			row.ID, nowUTC()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE ad_lock SET state = 'closed' WHERE swap_id = $1 AND ad_id = $2`, ev.SwapId, row.ID); err != nil {
			return err
		}
		return emit(ctx, tx, row.ID, &adv1.AdClosed{AdId: row.ID, SwapId: ev.SwapId})
	})
}

func onSwapCancelled(ctx context.Context, tx pgx.Tx, env *commonv1.Envelope) error {
	ev := &swapv1.SwapCancelled{}
	if err := consume.Decode(env, ev); err != nil {
		return err
	}
	return settle(ctx, tx, ev.SwapId, []string{ev.AdAId, ev.AdBId}, func(row *adRow) error {
		restored := row.StatusBeforeLock
		if restored != StatusPublished && restored != StatusHidden {
			restored = StatusHidden
		}
		row.Status = restored
		row.StatusBeforeLock = ""
		row.UpdatedAt = nowUTC()
		if _, err := tx.Exec(ctx, `UPDATE ad SET status = $2, status_before_lock = NULL, updated_at = $3 WHERE id = $1`,
			row.ID, restored, row.UpdatedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE ad_lock SET state = 'released' WHERE swap_id = $1 AND ad_id = $2`, ev.SwapId, row.ID); err != nil {
			return err
		}
		snap, err := currentSnapshot(ctx, tx, row)
		if err != nil {
			return err
		}
		return emit(ctx, tx, row.ID, &adv1.AdReleased{AdId: row.ID, SwapId: ev.SwapId, Ad: snap})
	})
}

// settle applies change to each ad that this swap still holds locked; anything else is skipped,
// which makes redelivery and reordering harmless.
func settle(ctx context.Context, tx pgx.Tx, swapID string, adIDs []string, change func(*adRow) error) error {
	for _, raw := range adIDs {
		id, err := uuid.Parse(raw)
		if err != nil {
			slog.Warn("swap event with invalid ad id", "ad_id", raw, "swap_id", swapID)
			continue
		}
		var held bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ad_lock WHERE swap_id = $1 AND ad_id = $2 AND state = 'locked')`,
			swapID, id).Scan(&held); err != nil {
			return err
		}
		if !held {
			slog.Info("swap event for an ad this swap does not hold, skipping", "ad_id", raw, "swap_id", swapID)
			continue
		}
		row, err := loadAdForUpdate(ctx, tx, id)
		if err != nil {
			return err
		}
		if row.Status != StatusLocked {
			slog.Info("ad is not locked, skipping", "ad_id", raw, "status", row.Status)
			continue
		}
		if err := change(row); err != nil {
			return err
		}
	}
	return nil
}
