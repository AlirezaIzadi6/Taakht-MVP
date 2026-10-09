package harness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	matchingv1 "github.com/taakht/taakht/gen/taakht/matching/v1"
	negotiationv1 "github.com/taakht/taakht/gen/taakht/negotiation/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	rpcTimeout = 10 * time.Second
	// matchLimit is the largest page the matching service serves.
	matchLimit = 100
)

// Delivery methods, shortened.
const (
	InPerson = negotiationv1.DeliveryMethod_DELIVERY_METHOD_IN_PERSON
	Locker   = negotiationv1.DeliveryMethod_DELIVERY_METHOD_LOCKER
)

// Actor is one seeded user calling the system; every call carries its x-user-id.
type Actor struct {
	ID string
	C  *Clients
	T  TB
}

// NewActor returns the actor for a seeded user (user-1 .. user-4).
func NewActor(t TB, c *Clients, userID string) *Actor {
	return &Actor{ID: userID, C: c, T: t}
}

// Ctx returns a call context for this user. The returned cancel must be called.
func (a *Actor) Ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	return AsUser(ctx, a.ID), cancel
}

// RandSuffix returns a short random string to keep titles unique across runs.
func RandSuffix() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Spec builds an ad spec: the owner has haveCategory and wants wantCategory, in Valiasr.
func Spec(title, haveCategory, wantCategory string) *adv1.AdSpec {
	return &adv1.AdSpec{
		Title:           fmt.Sprintf("%s [%s]", title, RandSuffix()),
		Description:     "created by the e2e harness",
		HaveCategory:    haveCategory,
		WantCategories:  []string{wantCategory},
		NeighborhoodIds: []string{"n-valiasr"},
		ValueEstimate:   500000,
	}
}

// Terms builds proposal terms without a price difference.
func Terms(legA, legB negotiationv1.DeliveryMethod) *negotiationv1.Terms {
	return &negotiationv1.Terms{LegA: legA, LegB: legB}
}

// PublishAd creates and publishes an ad. When the test ends, the ad is hidden again if it is still
// published, so repeated runs do not fill the match index with leftovers.
func (a *Actor) PublishAd(spec *adv1.AdSpec) *adv1.Ad {
	a.T.Helper()
	ctx, cancel := a.Ctx()
	defer cancel()
	ad, err := a.C.Ad.CreateAd(ctx, &adv1.CreateAdRequest{Spec: spec})
	NoErr(a.T, err, a.ID+" CreateAd")
	ad, err = a.C.Ad.PublishAd(ctx, &adv1.AdIdRequest{AdId: ad.GetId()})
	NoErr(a.T, err, a.ID+" PublishAd")
	adID := ad.GetId()
	a.T.Cleanup(func() { a.hideIfStillPublished(adID) })
	return ad
}

// hideIfStillPublished is best-effort test cleanup: ads that were LOCKED at test end may settle back
// to PUBLISHED (swap cancelled), so it polls briefly for a final state. It never fails the test.
func (a *Actor) hideIfStillPublished(adID string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := a.Ctx()
		cur, err := a.C.Ad.GetAd(ctx, &adv1.GetAdRequest{AdId: adID})
		if err == nil && cur.GetStatus() == adv1.AdStatus_AD_STATUS_PUBLISHED {
			_, _ = a.C.Ad.HideAd(ctx, &adv1.AdIdRequest{AdId: adID})
			cancel()
			return
		}
		cancel()
		if err == nil && cur.GetStatus() != adv1.AdStatus_AD_STATUS_LOCKED {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// GetAd reads an ad (any user may).
func (a *Actor) GetAd(adID string) *adv1.Ad {
	a.T.Helper()
	ctx, cancel := a.Ctx()
	defer cancel()
	ad, err := a.C.Ad.GetAd(ctx, &adv1.GetAdRequest{AdId: adID})
	NoErr(a.T, err, a.ID+" GetAd "+adID)
	return ad
}

// WaitAdStatus polls until the ad has one of the wanted statuses and returns it.
func (a *Actor) WaitAdStatus(adID string, want ...adv1.AdStatus) *adv1.Ad {
	a.T.Helper()
	var ad *adv1.Ad
	Poll(a.T, fmt.Sprintf("ad %s to become %s", adID, join(want)), DefaultTimeout, func() (bool, string) {
		ctx, cancel := a.Ctx()
		defer cancel()
		var err error
		ad, err = a.C.Ad.GetAd(ctx, &adv1.GetAdRequest{AdId: adID})
		if err != nil {
			return false, err.Error()
		}
		return slices.Contains(want, ad.GetStatus()), ad.GetStatus().String()
	})
	return ad
}

// Matches returns the current FindMatches result for one of the actor's ads.
func (a *Actor) Matches(adID string) ([]*matchingv1.Candidate, error) {
	ctx, cancel := a.Ctx()
	defer cancel()
	resp, err := a.C.Matching.FindMatches(ctx, &matchingv1.FindMatchesRequest{AdId: adID, Limit: matchLimit})
	if err != nil {
		return nil, err
	}
	return resp.GetCandidates(), nil
}

func hasCandidate(cands []*matchingv1.Candidate, adID string) bool {
	return slices.ContainsFunc(cands, func(c *matchingv1.Candidate) bool { return c.GetAd().GetId() == adID })
}

// WaitMatch polls FindMatches for myAdID until otherAdID shows up (index events travel through Kafka).
func (a *Actor) WaitMatch(myAdID, otherAdID string) {
	a.T.Helper()
	a.waitMatchState(myAdID, otherAdID, true)
}

// WaitNoMatch polls until otherAdID is no longer a candidate for myAdID.
func (a *Actor) WaitNoMatch(myAdID, otherAdID string) {
	a.T.Helper()
	a.waitMatchState(myAdID, otherAdID, false)
}

func (a *Actor) waitMatchState(myAdID, otherAdID string, wantPresent bool) {
	a.T.Helper()
	what := fmt.Sprintf("ad %s to appear in matches of %s", otherAdID, myAdID)
	if !wantPresent {
		what = fmt.Sprintf("ad %s to leave the matches of %s", otherAdID, myAdID)
	}
	Poll(a.T, what, DefaultTimeout, func() (bool, string) {
		cands, err := a.Matches(myAdID)
		if err != nil {
			return false, err.Error()
		}
		return hasCandidate(cands, otherAdID) == wantPresent, fmt.Sprintf("%d candidates", len(cands))
	})
}

// Open opens a negotiation from the actor's ad to targetAdID.
func (a *Actor) Open(myAdID, targetAdID string) *negotiationv1.Negotiation {
	a.T.Helper()
	n, err := a.TryOpen(myAdID, targetAdID)
	NoErr(a.T, err, a.ID+" OpenNegotiation")
	return n
}

// TryOpen is Open without failing the test on error.
func (a *Actor) TryOpen(myAdID, targetAdID string) (*negotiationv1.Negotiation, error) {
	ctx, cancel := a.Ctx()
	defer cancel()
	return a.C.Negotiation.OpenNegotiation(ctx, &negotiationv1.OpenNegotiationRequest{
		RequesterAdId: myAdID, TargetAdId: targetAdID,
	})
}

// ApproveAd approves the other side's ad at the given version.
func (a *Actor) ApproveAd(negID string, adVersion int32) *negotiationv1.Negotiation {
	a.T.Helper()
	ctx, cancel := a.Ctx()
	defer cancel()
	n, err := a.C.Negotiation.ApproveAd(ctx, &negotiationv1.ApproveAdRequest{NegotiationId: negID, AdVersion: adVersion})
	NoErr(a.T, err, a.ID+" ApproveAd")
	return n
}

// TryApproveProposal approves the active proposal without failing the test on error.
func (a *Actor) TryApproveProposal(negID string, number int32) (*negotiationv1.Negotiation, error) {
	ctx, cancel := a.Ctx()
	defer cancel()
	return a.C.Negotiation.ApproveProposal(ctx, &negotiationv1.ProposalRefRequest{NegotiationId: negID, ProposalNumber: number})
}

// ApproveProposal approves the active proposal.
func (a *Actor) ApproveProposal(negID string, number int32) *negotiationv1.Negotiation {
	a.T.Helper()
	n, err := a.TryApproveProposal(negID, number)
	NoErr(a.T, err, a.ID+" ApproveProposal")
	return n
}

// TryRevise creates a new proposal without failing the test on error.
func (a *Actor) TryRevise(negID string, seen int32, terms *negotiationv1.Terms) (*negotiationv1.Negotiation, error) {
	ctx, cancel := a.Ctx()
	defer cancel()
	return a.C.Negotiation.ReviseProposal(ctx, &negotiationv1.ReviseProposalRequest{
		NegotiationId: negID, SeenProposalNumber: seen, Terms: terms,
	})
}

// Revise creates a new proposal.
func (a *Actor) Revise(negID string, seen int32, terms *negotiationv1.Terms) *negotiationv1.Negotiation {
	a.T.Helper()
	n, err := a.TryRevise(negID, seen, terms)
	NoErr(a.T, err, a.ID+" ReviseProposal")
	return n
}

// Negotiation reads a negotiation.
func (a *Actor) Negotiation(negID string) *negotiationv1.Negotiation {
	a.T.Helper()
	ctx, cancel := a.Ctx()
	defer cancel()
	n, err := a.C.Negotiation.GetNegotiation(ctx, &negotiationv1.NegotiationIdRequest{NegotiationId: negID})
	NoErr(a.T, err, a.ID+" GetNegotiation")
	return n
}

// WaitNegotiation polls until the negotiation has one of the wanted statuses.
func (a *Actor) WaitNegotiation(negID string, want ...negotiationv1.NegotiationStatus) *negotiationv1.Negotiation {
	a.T.Helper()
	var n *negotiationv1.Negotiation
	Poll(a.T, fmt.Sprintf("negotiation %s to become %s", negID, join(want)), DefaultTimeout, func() (bool, string) {
		ctx, cancel := a.Ctx()
		defer cancel()
		var err error
		n, err = a.C.Negotiation.GetNegotiation(ctx, &negotiationv1.NegotiationIdRequest{NegotiationId: negID})
		if err != nil {
			return false, err.Error()
		}
		return slices.Contains(want, n.GetStatus()), n.GetStatus().String()
	})
	return n
}

// Approval finds the approval of a user for a kind, or nil.
func Approval(n *negotiationv1.Negotiation, userID string, kind negotiationv1.ApprovalKind) *negotiationv1.Approval {
	for _, ap := range n.GetApprovals() {
		if ap.GetUserId() == userID && ap.GetKind() == kind {
			return ap
		}
	}
	return nil
}

// SwapFor returns the actor's swap of a negotiation, or nil when none exists yet.
func (a *Actor) SwapFor(negID string) (*swapv1.Swap, error) {
	ctx, cancel := a.Ctx()
	defer cancel()
	resp, err := a.C.Swap.ListMySwaps(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	for _, s := range resp.GetSwaps() {
		if s.GetNegotiationId() == negID {
			return s, nil
		}
	}
	return nil, nil
}

// WaitSwap polls until the negotiation has a swap in one of the wanted statuses.
func (a *Actor) WaitSwap(negID string, want ...swapv1.SwapStatus) *swapv1.Swap {
	a.T.Helper()
	var s *swapv1.Swap
	Poll(a.T, fmt.Sprintf("swap of negotiation %s to become %s", negID, join(want)), DefaultTimeout, func() (bool, string) {
		var err error
		s, err = a.SwapFor(negID)
		switch {
		case err != nil:
			return false, err.Error()
		case s == nil:
			return false, "no swap yet"
		}
		return slices.Contains(want, s.GetStatus()), s.GetStatus().String()
	})
	return s
}

// PayLockerFee calls the mocked locker-partner webhook as the leg owner (the endpoint only
// accepts a caller paying for their own leg).
func (a *Actor) PayLockerFee(swapID, legOwner string) *swapv1.Swap {
	a.T.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	ctx = AsUser(ctx, legOwner)
	s, err := a.C.Swap.SimulateLockerFeePaid(ctx, &swapv1.SimulateLockerFeePaidRequest{SwapId: swapID, UserId: legOwner})
	NoErr(a.T, err, "SimulateLockerFeePaid for "+legOwner)
	return s
}
