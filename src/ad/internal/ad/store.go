package ad

import (
	"context"
	"errors"
	"fmt"
	"time"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Topic is the Kafka topic the ad service publishes to.
const Topic = "ad.events"

// adRow is the mutable head of an ad.
type adRow struct {
	ID               string
	OwnerID          string
	Status           string
	Version          int32
	StatusBeforeLock string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (r *adRow) snapshot(spec *adv1.AdSpec) *adv1.Ad {
	return &adv1.Ad{
		Id:        r.ID,
		OwnerId:   r.OwnerID,
		Status:    statusToProto(r.Status),
		Version:   r.Version,
		Spec:      spec,
		CreatedAt: timestamppb.New(r.CreatedAt),
		UpdatedAt: timestamppb.New(r.UpdatedAt),
	}
}

func inTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "database: %v", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return status.Errorf(codes.Internal, "commit: %v", err)
	}
	return nil
}

func parseID(id string) (uuid.UUID, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "invalid ad id %q", id)
	}
	return u, nil
}

const adColumns = `id::text, owner_id, status, current_version, coalesce(status_before_lock, ''), created_at, updated_at`

func scanAd(row pgx.Row) (*adRow, error) {
	r := &adRow{}
	if err := row.Scan(&r.ID, &r.OwnerID, &r.Status, &r.Version, &r.StatusBeforeLock, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
	return r, nil
}

// loadAdForUpdate row-locks the ad until the transaction ends.
func loadAdForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*adRow, error) {
	r, err := scanAd(tx.QueryRow(ctx, `SELECT `+adColumns+` FROM ad WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "ad %s not found", id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load ad: %v", err)
	}
	return r, nil
}

func loadAd(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, id uuid.UUID,
) (*adRow, error) {
	r, err := scanAd(q.QueryRow(ctx, `SELECT `+adColumns+` FROM ad WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "ad %s not found", id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load ad: %v", err)
	}
	return r, nil
}

type specVersion struct {
	spec      *adv1.AdSpec
	createdAt time.Time
}

func loadSpec(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, id uuid.UUID, version int32,
) (*specVersion, error) {
	var raw []byte
	var at time.Time
	err := q.QueryRow(ctx, `SELECT spec, created_at FROM ad_version WHERE ad_id = $1 AND version = $2`, id, version).Scan(&raw, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "ad %s has no version %d", id, version)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load spec: %v", err)
	}
	spec := &adv1.AdSpec{}
	if err := protojson.Unmarshal(raw, spec); err != nil {
		return nil, status.Errorf(codes.Internal, "decode spec: %v", err)
	}
	return &specVersion{spec: spec, createdAt: at}, nil
}

func insertSpec(ctx context.Context, tx pgx.Tx, id uuid.UUID, version int32, spec *adv1.AdSpec, at time.Time) error {
	raw, err := protojson.Marshal(spec)
	if err != nil {
		return status.Errorf(codes.Internal, "encode spec: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ad_version (ad_id, version, spec, created_at) VALUES ($1, $2, $3, $4)`,
		id, version, raw, at); err != nil {
		return status.Errorf(codes.Internal, "insert version: %v", err)
	}
	return nil
}

// currentSnapshot builds the full Ad for the head row at its current version.
func currentSnapshot(ctx context.Context, tx pgx.Tx, r *adRow) (*adv1.Ad, error) {
	id, err := uuid.Parse(r.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "bad stored id: %v", err)
	}
	sv, err := loadSpec(ctx, tx, id, r.Version)
	if err != nil {
		return nil, err
	}
	return r.snapshot(sv.spec), nil
}

func nowUTC() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func tsOf(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }

func decodeSpec(raw []byte) (*adv1.AdSpec, error) {
	spec := &adv1.AdSpec{}
	if err := protojson.Unmarshal(raw, spec); err != nil {
		return nil, status.Errorf(codes.Internal, "decode spec: %v", err)
	}
	return spec, nil
}

func internal(op string, err error) error {
	return status.Error(codes.Internal, fmt.Sprintf("%s: %v", op, err))
}
