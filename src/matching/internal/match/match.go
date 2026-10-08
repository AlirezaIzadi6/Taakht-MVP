// Package match ranks index candidates using the scoring rules.
package match

import (
	"cmp"
	"slices"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	"github.com/taakht/taakht/src/matching/internal/geo"
	"github.com/taakht/taakht/src/matching/internal/scoring"
)

type Scored struct {
	Ad    *adv1.Ad
	Score float64
}

func SpecOf(ad *adv1.Ad) scoring.Spec {
	s := ad.GetSpec()
	return scoring.Spec{
		HaveCategory:    s.GetHaveCategory(),
		WantCategories:  s.GetWantCategories(),
		NeighborhoodIDs: s.GetNeighborhoodIds(),
	}
}

// RankTwoSided keeps the candidates that match `me` on both sides, best first.
func RankTwoSided(g geo.Map, me *adv1.Ad, cands []*adv1.Ad, limit int) []Scored {
	var out []Scored
	for _, c := range cands {
		if c.GetOwnerId() == me.GetOwnerId() || c.GetId() == me.GetId() {
			continue
		}
		if s, ok := scoring.TwoSided(g, SpecOf(me), SpecOf(c)); ok {
			out = append(out, Scored{c, s})
		}
	}
	return top(out, limit)
}

// RankBrowse keeps the candidates that satisfy the criteria, best first.
func RankBrowse(g geo.Map, crit scoring.Criteria, cands []*adv1.Ad, limit int) []Scored {
	var out []Scored
	for _, c := range cands {
		if s, ok := scoring.Browse(g, crit, SpecOf(c)); ok {
			out = append(out, Scored{c, s})
		}
	}
	return top(out, limit)
}

func top(s []Scored, limit int) []Scored {
	slices.SortFunc(s, func(a, b Scored) int {
		if c := cmp.Compare(b.Score, a.Score); c != 0 {
			return c
		}
		return cmp.Compare(a.Ad.GetId(), b.Ad.GetId())
	})
	if limit > 0 && len(s) > limit {
		s = s[:limit]
	}
	return s
}
