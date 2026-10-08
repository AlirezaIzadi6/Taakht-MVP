// Package ad holds the Ad service: pure rules, persistence and gRPC handlers.
package ad

import (
	"fmt"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	"github.com/taakht/taakht/src/ad/internal/eligibility"
)

const maxTitleLen = 200

// ValidateSpec checks a spec against the eligibility config. The have side may be partial
// (empty category), but a given category must be allowed.
func ValidateSpec(spec *adv1.AdSpec, elig *eligibility.Config) error {
	if spec == nil {
		return fmt.Errorf("spec is required")
	}
	if spec.Title == "" {
		return fmt.Errorf("title is required")
	}
	if len([]rune(spec.Title)) > maxTitleLen {
		return fmt.Errorf("title is longer than %d characters", maxTitleLen)
	}
	if spec.HaveCategory != "" && !elig.HasCategory(spec.HaveCategory) {
		return fmt.Errorf("unknown have_category %q", spec.HaveCategory)
	}
	for _, c := range spec.WantCategories {
		if !elig.HasCategory(c) {
			return fmt.Errorf("unknown want category %q", c)
		}
	}
	for _, n := range spec.NeighborhoodIds {
		if !elig.HasNeighborhood(n) {
			return fmt.Errorf("unknown neighborhood %q", n)
		}
	}
	if spec.ValueEstimate < 0 {
		return fmt.Errorf("value_estimate must not be negative")
	}
	return nil
}

// HasFilter reports whether the spec narrows the search on the want side.
func HasFilter(spec *adv1.AdSpec) bool {
	return len(spec.GetWantCategories()) > 0 || len(spec.GetNeighborhoodIds()) > 0
}

// Status values as stored in the database.
const (
	StatusPublished = "published"
	StatusHidden    = "hidden"
	StatusLocked    = "locked"
	StatusClosed    = "closed"
)

// Editable: only ads that no swap holds or has finished may change.
func Editable(status string) bool { return status == StatusPublished || status == StatusHidden }

// CanPublish: only from hidden.
func CanPublish(status string) bool { return status == StatusHidden }

// CanHide: only from published.
func CanHide(status string) bool { return status == StatusPublished }

// Lockable: a swap may claim published or hidden ads.
func Lockable(status string) bool { return Editable(status) }

func statusToProto(s string) adv1.AdStatus {
	switch s {
	case StatusPublished:
		return adv1.AdStatus_AD_STATUS_PUBLISHED
	case StatusHidden:
		return adv1.AdStatus_AD_STATUS_HIDDEN
	case StatusLocked:
		return adv1.AdStatus_AD_STATUS_LOCKED
	case StatusClosed:
		return adv1.AdStatus_AD_STATUS_CLOSED
	}
	return adv1.AdStatus_AD_STATUS_UNSPECIFIED
}
