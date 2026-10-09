package ad

import (
	"context"
	"fmt"
	"sort"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	"github.com/taakht/taakht/libs/goplatform/identity"
	"github.com/taakht/taakht/libs/goplatform/outbox"
	"github.com/taakht/taakht/libs/goplatform/pagination"
	"github.com/taakht/taakht/src/ad/internal/eligibility"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements adv1.AdServiceServer.
type Service struct {
	adv1.UnimplementedAdServiceServer
	pool *pgxpool.Pool
	elig *eligibility.Config
}

// NewService wires the service to its database and eligibility config.
func NewService(pool *pgxpool.Pool, elig *eligibility.Config) *Service {
	return &Service{pool: pool, elig: elig}
}

func (s *Service) CreateAd(ctx context.Context, req *adv1.CreateAdRequest) (*adv1.Ad, error) {
	if err := ValidateSpec(req.GetSpec(), s.elig); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	id := uuid.New()
	now := nowUTC()
	row := &adRow{ID: id.String(), OwnerID: identity.UserID(ctx), Status: StatusHidden, Version: 1, CreatedAt: now, UpdatedAt: now}
	err := inTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO ad (id, owner_id, status, current_version, created_at, updated_at)
			VALUES ($1, $2, $3, 1, $4, $4)`, id, row.OwnerID, row.Status, now); err != nil {
			return internal("insert ad", err)
		}
		return insertSpec(ctx, tx, id, 1, req.Spec, now)
	})
	if err != nil {
		return nil, err
	}
	return row.snapshot(req.Spec), nil
}

func (s *Service) EditAd(ctx context.Context, req *adv1.EditAdRequest) (*adv1.Ad, error) {
	id, err := parseID(req.GetAdId())
	if err != nil {
		return nil, err
	}
	if err := ValidateSpec(req.GetSpec(), s.elig); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var result *adv1.Ad
	err = inTx(ctx, s.pool, func(tx pgx.Tx) error {
		row, err := loadAdForUpdate(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := requireOwner(ctx, row); err != nil {
			return err
		}
		if !Editable(row.Status) {
			return status.Errorf(codes.FailedPrecondition, "ad is %s and cannot be edited", row.Status)
		}
		if row.Version != req.ExpectedVersion {
			return status.Errorf(codes.Aborted, "ad is at version %d, not %d; re-read it", row.Version, req.ExpectedVersion)
		}
		current, err := loadSpec(ctx, tx, id, row.Version)
		if err != nil {
			return err
		}
		if proto.Equal(current.spec, req.Spec) {
			// Nothing changed: no new version and no event, so approvals and the index are not disturbed.
			result = row.snapshot(current.spec)
			return nil
		}
		if row.Status == StatusPublished && !HasFilter(req.Spec) {
			return status.Error(codes.InvalidArgument, "a published ad needs at least one want category or neighborhood")
		}
		now := nowUTC()
		row.Version++
		row.UpdatedAt = now
		if err := insertSpec(ctx, tx, id, row.Version, req.Spec, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE ad SET current_version = $2, updated_at = $3 WHERE id = $1`, id, row.Version, now); err != nil {
			return internal("update ad", err)
		}
		result = row.snapshot(req.Spec)
		return emit(ctx, tx, row.ID, &adv1.AdEdited{Ad: result})
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) PublishAd(ctx context.Context, req *adv1.AdIdRequest) (*adv1.Ad, error) {
	return s.transition(ctx, req.GetAdId(), func(row *adRow, spec *adv1.AdSpec) (proto.Message, error) {
		if !CanPublish(row.Status) {
			return nil, status.Errorf(codes.FailedPrecondition, "ad is %s; only a hidden ad can be published", row.Status)
		}
		if !HasFilter(spec) {
			return nil, status.Error(codes.InvalidArgument, "publishing needs at least one want category or neighborhood")
		}
		row.Status = StatusPublished
		return &adv1.AdPublished{Ad: row.snapshot(spec)}, nil
	})
}

func (s *Service) HideAd(ctx context.Context, req *adv1.AdIdRequest) (*adv1.Ad, error) {
	return s.transition(ctx, req.GetAdId(), func(row *adRow, _ *adv1.AdSpec) (proto.Message, error) {
		if !CanHide(row.Status) {
			return nil, status.Errorf(codes.FailedPrecondition, "ad is %s; only a published ad can be hidden", row.Status)
		}
		row.Status = StatusHidden
		return &adv1.AdHidden{AdId: row.ID}, nil
	})
}

// transition runs an owner-only status change under the row lock and emits its event.
func (s *Service) transition(ctx context.Context, adID string, apply func(*adRow, *adv1.AdSpec) (proto.Message, error)) (*adv1.Ad, error) {
	id, err := parseID(adID)
	if err != nil {
		return nil, err
	}
	var result *adv1.Ad
	err = inTx(ctx, s.pool, func(tx pgx.Tx) error {
		row, err := loadAdForUpdate(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := requireOwner(ctx, row); err != nil {
			return err
		}
		sv, err := loadSpec(ctx, tx, id, row.Version)
		if err != nil {
			return err
		}
		event, err := apply(row, sv.spec)
		if err != nil {
			return err
		}
		row.UpdatedAt = nowUTC()
		if _, err := tx.Exec(ctx, `UPDATE ad SET status = $2, updated_at = $3 WHERE id = $1`, id, row.Status, row.UpdatedAt); err != nil {
			return internal("update ad", err)
		}
		result = row.snapshot(sv.spec)
		return emit(ctx, tx, row.ID, event)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetAd: owners read all their ads and versions, system identities (negotiation, swap) read anything,
// everyone else sees only published ads at their current version. Anything else is NOT_FOUND with
// one fixed message, so a missing ad and a hidden one cannot be told apart.
func (s *Service) GetAd(ctx context.Context, req *adv1.GetAdRequest) (*adv1.Ad, error) {
	id, err := parseID(req.GetAdId())
	if err != nil {
		return nil, err
	}
	if req.Version < 0 {
		return nil, status.Error(codes.InvalidArgument, "version must not be negative")
	}
	row, err := loadAd(ctx, s.pool, id)
	if status.Code(err) == codes.NotFound {
		return nil, errAdNotFound // the same answer as for an ad the caller may not see
	}
	if err != nil {
		return nil, err
	}
	version := req.Version
	if version == 0 {
		version = row.Version
	}
	caller := identity.UserID(ctx)
	if caller != row.OwnerID && !identity.IsSystem(ctx) &&
		(row.Status != StatusPublished || version != row.Version) {
		return nil, errAdNotFound
	}
	sv, err := loadSpec(ctx, s.pool, id, version)
	if err != nil {
		return nil, err
	}
	ad := row.snapshot(sv.spec)
	ad.Version = version
	if version != row.Version {
		ad.UpdatedAt = tsOf(sv.createdAt)
	}
	return ad, nil
}

// ListMyAds returns the caller's ads, newest first, one keyset page at a time (created_at DESC, id DESC).
func (s *Service) ListMyAds(ctx context.Context, req *adv1.ListMyAdsRequest) (*adv1.ListMyAdsResponse, error) {
	cur, hasCur, err := pagination.Decode(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	size := pagination.PageSize(req.GetPageSize())
	const base = `SELECT ad.id::text, ad.owner_id, ad.status, ad.current_version,
		coalesce(ad.status_before_lock, ''), ad.created_at, ad.updated_at, v.spec FROM ad
		JOIN ad_version v ON v.ad_id = ad.id AND v.version = ad.current_version
		WHERE ad.owner_id = $1`
	const order = ` ORDER BY ad.created_at DESC, ad.id DESC LIMIT `
	var rows pgx.Rows
	if hasCur {
		rows, err = s.pool.Query(ctx, base+` AND (ad.created_at, ad.id) < ($2, $3::uuid)`+order+`$4`,
			identity.UserID(ctx), cur.CreatedAt, cur.ID, size+1)
	} else {
		rows, err = s.pool.Query(ctx, base+order+`$2`, identity.UserID(ctx), size+1)
	}
	if err != nil {
		return nil, internal("list ads", err)
	}
	defer rows.Close()
	resp := &adv1.ListMyAdsResponse{}
	var last *adRow
	for rows.Next() {
		r := &adRow{}
		var raw []byte
		if err := rows.Scan(&r.ID, &r.OwnerID, &r.Status, &r.Version, &r.StatusBeforeLock, &r.CreatedAt, &r.UpdatedAt, &raw); err != nil {
			return nil, internal("scan ad", err)
		}
		if len(resp.Ads) == size {
			// The extra row only proves there is a next page; the token points at the last returned item.
			resp.NextPageToken = pagination.Encode(last.CreatedAt, last.ID)
			break
		}
		spec, err := decodeSpec(raw)
		if err != nil {
			return nil, err
		}
		resp.Ads = append(resp.Ads, r.snapshot(spec))
		last = r
	}
	if err := rows.Err(); err != nil {
		return nil, internal("list ads", err)
	}
	return resp, nil
}

// LockAds atomically claims both ads for a swap, or none. Idempotent by swap_id.
// Only the swap service (identity system:swap) may call it.
func (s *Service) LockAds(ctx context.Context, req *adv1.LockAdsRequest) (*emptypb.Empty, error) {
	if identity.UserID(ctx) != identity.SystemSwap || !identity.IsSystem(ctx) {
		return nil, status.Error(codes.PermissionDenied, "only the swap service can lock ads")
	}
	if req.GetSwapId() == "" {
		return nil, status.Error(codes.InvalidArgument, "swap_id is required")
	}
	if len(req.GetAds()) != 2 {
		return nil, status.Error(codes.InvalidArgument, "exactly two ads are required")
	}
	type target struct {
		id      uuid.UUID
		version int32
	}
	targets := make([]target, 0, 2)
	for _, ref := range req.Ads {
		id, err := parseID(ref.GetAdId())
		if err != nil {
			return nil, err
		}
		if ref.Version <= 0 {
			return nil, status.Error(codes.InvalidArgument, "ad version must be positive")
		}
		targets = append(targets, target{id, ref.Version})
	}
	if targets[0].id == targets[1].id {
		return nil, status.Error(codes.InvalidArgument, "the two ads must differ")
	}
	// A fixed lock order prevents deadlocks between swaps sharing an ad.
	sort.Slice(targets, func(i, j int) bool { return targets[i].id.String() < targets[j].id.String() })

	err := inTx(ctx, s.pool, func(tx pgx.Tx) error {
		rows := make([]*adRow, len(targets))
		for i, t := range targets {
			r, err := loadAdForUpdate(ctx, tx, t.id)
			if err != nil {
				return err
			}
			rows[i] = r
		}

		// Checked after the row locks so a concurrent retry of the same swap sees the first one's lock.
		existing, err := lockedVersions(ctx, tx, req.SwapId)
		if err != nil {
			return err
		}
		if len(existing) > 0 {
			for _, t := range targets {
				if v, ok := existing[t.id.String()]; !ok || v != t.version {
					return status.Error(codes.FailedPrecondition, "swap_id was already used for different ads")
				}
			}
			if st, err := lockStates(ctx, tx, req.SwapId); err != nil {
				return err
			} else if st != "locked" {
				return status.Errorf(codes.FailedPrecondition, "swap %s is already %s", req.SwapId, st)
			}
			return nil
		}

		for i, t := range targets {
			r := rows[i]
			if !Lockable(r.Status) {
				return status.Errorf(codes.FailedPrecondition, "ad %s is %s and cannot be locked", r.ID, r.Status)
			}
			if r.Version != t.version {
				return status.Errorf(codes.FailedPrecondition, "ad %s is at version %d, the agreement is for version %d", r.ID, r.Version, t.version)
			}
		}
		now := nowUTC()
		for i, t := range targets {
			r := rows[i]
			if _, err := tx.Exec(ctx, `UPDATE ad SET status = 'locked', status_before_lock = $2, updated_at = $3 WHERE id = $1`,
				t.id, r.Status, now); err != nil {
				return internal("lock ad", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO ad_lock (swap_id, ad_id, version) VALUES ($1, $2, $3)`,
				req.SwapId, t.id, t.version); err != nil {
				return internal("record lock", err)
			}
			if err := emit(ctx, tx, r.ID, &adv1.AdLocked{AdId: r.ID, SwapId: req.SwapId}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func lockedVersions(ctx context.Context, tx pgx.Tx, swapID string) (map[string]int32, error) {
	rows, err := tx.Query(ctx, `SELECT ad_id::text, version FROM ad_lock WHERE swap_id = $1`, swapID)
	if err != nil {
		return nil, internal("read locks", err)
	}
	defer rows.Close()
	out := map[string]int32{}
	for rows.Next() {
		var id string
		var v int32
		if err := rows.Scan(&id, &v); err != nil {
			return nil, internal("scan lock", err)
		}
		out[id] = v
	}
	return out, rows.Err()
}

// lockStates returns the shared state of a swap's lock rows ("locked", "released" or "closed").
func lockStates(ctx context.Context, tx pgx.Tx, swapID string) (string, error) {
	var st string
	err := tx.QueryRow(ctx, `SELECT coalesce(min(state) FILTER (WHERE state <> 'locked'), 'locked') FROM ad_lock WHERE swap_id = $1`, swapID).Scan(&st)
	if err != nil {
		return "", internal("read lock state", err)
	}
	return st, nil
}

func requireOwner(ctx context.Context, row *adRow) error {
	if identity.UserID(ctx) != row.OwnerID {
		return status.Error(codes.PermissionDenied, "only the owner can change this ad")
	}
	return nil
}

var errAdNotFound = status.Error(codes.NotFound, "ad not found")

// emit writes an ad event to the outbox. The caller holds the ad's row lock (key is the ad id), so the
// per-ad counter moves in the same order as the commits; the event carries the new value as seq.
func emit(ctx context.Context, tx pgx.Tx, key string, msg proto.Message) error {
	var seq int64
	if err := tx.QueryRow(ctx, `UPDATE ad SET event_seq = event_seq + 1 WHERE id = $1 RETURNING event_seq`, key).Scan(&seq); err != nil {
		return internal("event seq", err)
	}
	switch m := msg.(type) {
	case *adv1.AdPublished:
		m.Seq = seq
	case *adv1.AdEdited:
		m.Seq = seq
	case *adv1.AdHidden:
		m.Seq = seq
	case *adv1.AdLocked:
		m.Seq = seq
	case *adv1.AdReleased:
		m.Seq = seq
	case *adv1.AdClosed:
		m.Seq = seq
	default:
		return internal("outbox", fmt.Errorf("unsupported ad event %T", msg))
	}
	if err := outbox.Add(ctx, tx, Topic, key, msg); err != nil {
		return internal("outbox", err)
	}
	return nil
}
