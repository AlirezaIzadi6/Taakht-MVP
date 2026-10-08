package harness

import (
	"strings"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	negotiationv1 "github.com/taakht/taakht/gen/taakht/negotiation/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
)

// AdStatusName is the status without its enum prefix, for narration.
func AdStatusName(s adv1.AdStatus) string {
	return strings.TrimPrefix(s.String(), "AD_STATUS_")
}

// NegStatusName is the status without its enum prefix, for narration.
func NegStatusName(s negotiationv1.NegotiationStatus) string {
	return strings.TrimPrefix(s.String(), "NEGOTIATION_STATUS_")
}

// SwapStatusName is the status without its enum prefix, for narration.
func SwapStatusName(s swapv1.SwapStatus) string {
	return strings.TrimPrefix(s.String(), "SWAP_STATUS_")
}
