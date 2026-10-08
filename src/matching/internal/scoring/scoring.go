// Package scoring holds the pure matching and ranking rules.
package scoring

import (
	"slices"

	"github.com/taakht/taakht/src/matching/internal/geo"
)

const (
	baseScore = 1.0
	// exactBonus is added per side on which the other ad is named explicitly (not "open to anything").
	exactBonus = 0.5
	// proximityScale is the distance in km at which proximity drops to 0.5.
	proximityScale = 10.0
	// unknownProximity is used when either side names no neighborhood.
	unknownProximity = 0.5
)

// Spec is the part of an ad that matching looks at.
type Spec struct {
	HaveCategory    string
	WantCategories  []string // empty = open
	NeighborhoodIDs []string
}

// Criteria is a browse request; it is never stored.
type Criteria struct {
	WantCategories  []string
	NeighborhoodIDs []string
}

// Proximity returns 0..1: 1 for a shared neighborhood, decaying with the distance
// between the closest pair, and a neutral value when a side has no neighborhoods.
func Proximity(g geo.Map, a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return unknownProximity
	}
	km, ok := g.MinDistanceKm(a, b)
	if !ok {
		return 0
	}
	return 1 / (1 + km/proximityScale)
}

// TwoSided scores a candidate against my ad. ok is false when the pair is not a match:
// the candidate's item must be wanted by me and my item must be wanted by the candidate.
func TwoSided(g geo.Map, me, cand Spec) (score float64, ok bool) {
	score = baseScore
	if len(me.WantCategories) > 0 {
		if !slices.Contains(me.WantCategories, cand.HaveCategory) {
			return 0, false
		}
		score += exactBonus
	}
	if len(cand.WantCategories) > 0 {
		if !slices.Contains(cand.WantCategories, me.HaveCategory) {
			return 0, false
		}
		score += exactBonus
	}
	return score + Proximity(g, me.NeighborhoodIDs, cand.NeighborhoodIDs), true
}

// Browse scores a candidate against search criteria. Given filters are hard:
// the candidate category must be wanted and neighborhoods must overlap.
func Browse(g geo.Map, c Criteria, cand Spec) (score float64, ok bool) {
	score = baseScore
	if len(c.WantCategories) > 0 {
		if !slices.Contains(c.WantCategories, cand.HaveCategory) {
			return 0, false
		}
		score += exactBonus
	}
	if len(c.NeighborhoodIDs) > 0 {
		if !overlaps(c.NeighborhoodIDs, cand.NeighborhoodIDs) {
			return 0, false
		}
		return score + 1, true
	}
	return score + unknownProximity, true
}

func overlaps(a, b []string) bool {
	for _, x := range a {
		if slices.Contains(b, x) {
			return true
		}
	}
	return false
}
