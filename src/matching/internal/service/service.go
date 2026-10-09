// Package service implements the MatchingService gRPC API.
package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
	"github.com/taakht/taakht/libs/goplatform/identity"
	"github.com/taakht/taakht/libs/goplatform/reload"
	"github.com/taakht/taakht/src/matching/internal/geo"
	"github.com/taakht/taakht/src/matching/internal/index"
	"github.com/taakht/taakht/src/matching/internal/match"
	"github.com/taakht/taakht/src/matching/internal/scoring"
)

const (
	defaultLimit = 20
	maxLimit     = 100
)

type Service struct {
	matchingv1.UnimplementedMatchingServiceServer
	DB  index.Querier
	Geo geo.Map
	// GeoSource, when set, supplies the (reloadable) neighborhood map and takes precedence over Geo.
	GeoSource *reload.Reloadable[geo.Map]
}

func clampLimit(n int32) int {
	switch {
	case n <= 0:
		return defaultLimit
	case n > maxLimit:
		return maxLimit
	}
	return int(n)
}

func (s *Service) Search(ctx context.Context, req *matchingv1.SearchRequest) (*matchingv1.SearchResponse, error) {
	user := identity.UserID(ctx)
	crit := scoring.Criteria{
		WantCategories:  req.GetCriteria().GetWantCategories(),
		NeighborhoodIDs: req.GetCriteria().GetNeighborhoodIds(),
	}
	cands, err := index.Candidates(ctx, s.DB, index.Filter{
		ExcludeOwner:    user,
		HaveCategories:  crit.WantCategories,
		NeighborhoodIDs: crit.NeighborhoodIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	return toResponse(match.RankBrowse(s.geoMap(), crit, cands, clampLimit(req.GetLimit()))), nil
}

func (s *Service) FindMatches(ctx context.Context, req *matchingv1.FindMatchesRequest) (*matchingv1.SearchResponse, error) {
	user := identity.UserID(ctx)
	if req.GetAdId() == "" {
		return nil, status.Error(codes.InvalidArgument, "ad_id is required")
	}
	if _, err := uuid.Parse(req.GetAdId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "ad_id must be a UUID")
	}
	me, err := index.Get(ctx, s.DB, req.GetAdId())
	if err != nil {
		return nil, fmt.Errorf("find matches: %w", err)
	}
	if me == nil {
		return nil, status.Error(codes.NotFound, "ad is not in the match index; publish it first")
	}
	if me.GetOwnerId() != user {
		return nil, status.Error(codes.PermissionDenied, "not the owner of this ad")
	}
	spec := me.GetSpec()
	cands, err := index.Candidates(ctx, s.DB, index.Filter{
		ExcludeOwner:   user,
		ExcludeAdID:    me.GetId(),
		HaveCategories: spec.GetWantCategories(),
		WantsCategory:  spec.GetHaveCategory(),
	})
	if err != nil {
		return nil, fmt.Errorf("find matches: %w", err)
	}
	return toResponse(match.RankTwoSided(s.geoMap(), me, cands, clampLimit(req.GetLimit()))), nil
}

func toResponse(scored []match.Scored) *matchingv1.SearchResponse {
	resp := &matchingv1.SearchResponse{}
	for _, m := range scored {
		resp.Candidates = append(resp.Candidates, &matchingv1.Candidate{Ad: m.Ad, Score: m.Score})
	}
	return resp
}

func (s *Service) geoMap() geo.Map {
	if s.GeoSource != nil {
		return *s.GeoSource.Get()
	}
	return s.Geo
}
