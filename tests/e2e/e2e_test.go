// Package e2e holds end-to-end tests that drive the running services over gRPC.
//
// They need the whole stack up (make up, then the four services) and run only with E2E=1.
package e2e

import (
	"os"
	"sync"
	"testing"
	"time"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	negotiationv1 "github.com/taakht/taakht/gen/taakht/negotiation/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"github.com/taakht/taakht/tests/e2e/internal/harness"
	"google.golang.org/grpc/codes"
)

// setup skips the test unless E2E=1 and fails it when a required service is down.
func setup(t *testing.T, services ...string) *harness.Clients {
	t.Helper()
	if os.Getenv("E2E") != "1" {
		t.Skip("set E2E=1 to run end-to-end tests against the running services")
	}
	c, err := harness.Dial(harness.AddrsFromEnv())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(c.Close)
	if down := c.Unreachable(services...); len(down) > 0 {
		t.Fatalf("services not reachable: %v", down)
	}
	return c
}

// requireLongDeadline skips tests that need the default (long) payment deadline; they would
// race the swap timeout when swap runs with PAYMENT_DEADLINE=5s (E2E_SHORT_DEADLINE=1).
func requireLongDeadline(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_SHORT_DEADLINE") == "1" {
		t.Skip("E2E_SHORT_DEADLINE=1: this test needs the long payment deadline")
	}
}

func allServices() []string { return []string{"ad", "matching", "negotiation", "swap"} }

func TestAdMatchingOnly(t *testing.T) {
	requireLongDeadline(t)
	c := setup(t, "ad", "matching")
	u1, u2, u3 := harness.NewActor(t, c, harness.User1), harness.NewActor(t, c, harness.User2), harness.NewActor(t, c, harness.User3)

	a := u1.PublishAd(harness.Spec("Novel", "books", "tools"))
	b := u2.PublishAd(harness.Spec("Wrench", "tools", "books"))
	// Same side as u2's ad: it wants books too, so it must not match u2's ad.
	same := u3.PublishAd(harness.Spec("Pliers", "tools", "books"))

	harness.Require(t, a.GetStatus() == adv1.AdStatus_AD_STATUS_PUBLISHED, "published ad has status %s", a.GetStatus())
	harness.Require(t, a.GetVersion() == 1, "new ad has version %d, want 1", a.GetVersion())

	u1.WaitMatch(a.GetId(), b.GetId())
	u2.WaitMatch(b.GetId(), a.GetId())
	u1.WaitMatch(a.GetId(), same.GetId())

	cands, err := u2.Matches(b.GetId())
	harness.NoErr(t, err, "FindMatches")
	for _, cand := range cands {
		harness.Require(t, cand.GetAd().GetId() != same.GetId(), "ad %s offers tools and wants books, it must not match another tools-for-books ad", same.GetId())
		harness.Require(t, cand.GetAd().GetOwnerId() != u2.ID, "own ad %s returned as a candidate", cand.GetAd().GetId())
	}

	_, err = u2.Matches(a.GetId())
	harness.RequireCode(t, err, codes.PermissionDenied, "FindMatches on someone else's ad")

	// Hiding removes the ad from the index.
	ctx, cancel := u1.Ctx()
	defer cancel()
	_, err = c.Ad.HideAd(ctx, &adv1.AdIdRequest{AdId: a.GetId()})
	harness.NoErr(t, err, "HideAd")
	u2.WaitNoMatch(b.GetId(), a.GetId())
}

func TestHappyPath(t *testing.T) {
	requireLongDeadline(t)
	c := setup(t, allServices()...)
	harness.HappyPath(t, c, t.Logf)
}

// Locker legs unpaid at the deadline: the swap is cancelled and the ads return to the index.
// Needs the swap service started with a short PAYMENT_DEADLINE (for example PAYMENT_DEADLINE=5s).
func TestPaymentTimeout(t *testing.T) {
	if os.Getenv("E2E_SHORT_DEADLINE") != "1" {
		t.Skip("set E2E_SHORT_DEADLINE=1 (and run swap with PAYMENT_DEADLINE=5s) to run the payment-timeout test")
	}
	c := setup(t, allServices()...)
	u1, u2, u3 := harness.NewActor(t, c, harness.User1), harness.NewActor(t, c, harness.User2), harness.NewActor(t, c, harness.User3)

	adA := u1.PublishAd(harness.Spec("Atlas", "books", "tools"))
	adB := u2.PublishAd(harness.Spec("Chisel", "tools", "books"))
	observer := u3.PublishAd(harness.Spec("Observer saw", "tools", "books"))
	// A second observer that matches adB (have books, want tools).
	observerB := u3.PublishAd(harness.Spec("Observer saw B", "books", "tools"))
	u3.WaitMatch(observer.GetId(), adA.GetId())

	n := u2.Open(adB.GetId(), adA.GetId())
	// Both parties see both ads on the negotiation.
	if n.GetRequesterAd().GetId() != adB.GetId() || n.GetTargetAd().GetId() != adA.GetId() {
		t.Fatalf("negotiation did not carry both ads: requester_ad=%v target_ad=%v", n.GetRequesterAd().GetId(), n.GetTargetAd().GetId())
	}
	if got := u1.Negotiation(n.GetId()); got.GetRequesterAd().GetSpec().GetTitle() != adB.GetSpec().GetTitle() {
		t.Fatalf("counterpart ad missing for the target owner: %v", got.GetRequesterAd())
	}
	u1.ApproveAd(n.GetId(), adB.GetVersion())
	u1.Revise(n.GetId(), 1, harness.Terms(harness.Locker, harness.Locker))
	u2.ApproveProposal(n.GetId(), 2)

	sw := u1.WaitSwap(n.GetId(), swapv1.SwapStatus_SWAP_STATUS_AWAITING_PAYMENT)
	u1.WaitAdStatus(adA.GetId(), adv1.AdStatus_AD_STATUS_LOCKED)
	u2.WaitAdStatus(adB.GetId(), adv1.AdStatus_AD_STATUS_LOCKED)
	u3.WaitNoMatch(observer.GetId(), adA.GetId())

	// Only one side pays; the other misses the deadline.
	u1.PayLockerFee(sw.GetId(), u2.ID)

	cancelled := u1.WaitSwap(n.GetId(), swapv1.SwapStatus_SWAP_STATUS_CANCELLED)
	t.Logf("swap %s cancelled: %q", cancelled.GetId(), cancelled.GetCancelReason())
	u1.WaitAdStatus(adA.GetId(), adv1.AdStatus_AD_STATUS_PUBLISHED)
	u2.WaitAdStatus(adB.GetId(), adv1.AdStatus_AD_STATUS_PUBLISHED)
	u3.WaitMatch(observer.GetId(), adA.GetId())
	u3.WaitMatch(observerB.GetId(), adB.GetId())
	// SwapCancelled names the negotiation, which becomes CANCELLED.
	u1.WaitNegotiation(n.GetId(), negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_CANCELLED)
}

// Two negotiations on the same ad both reach full agreement at the same moment: exactly one lock wins.
func TestLockRace(t *testing.T) {
	requireLongDeadline(t)
	c := setup(t, allServices()...)
	u1, u2, u3 := harness.NewActor(t, c, harness.User1), harness.NewActor(t, c, harness.User2), harness.NewActor(t, c, harness.User3)

	adA := u1.PublishAd(harness.Spec("Contested lamp", "furniture", "sports"))
	adB := u2.PublishAd(harness.Spec("Football", "sports", "furniture"))
	adC := u3.PublishAd(harness.Spec("Tennis racket", "sports", "furniture"))

	nB := u2.Open(adB.GetId(), adA.GetId())
	nC := u3.Open(adC.GetId(), adA.GetId())
	u1.ApproveAd(nB.GetId(), adB.GetVersion())
	u1.ApproveAd(nC.GetId(), adC.GetVersion())
	u2.ApproveProposal(nB.GetId(), 1)
	u3.ApproveProposal(nC.GetId(), 1)

	// Only the last approval (u1 on proposal v1) is missing in both negotiations. Fire both at once.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, negID := range []string{nB.GetId(), nC.GetId()} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := u1.TryApproveProposal(negID, 1)
			if err != nil {
				t.Errorf("final approval in negotiation %s failed: %v", negID, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	settled := func(id string) *negotiationv1.Negotiation {
		return u1.WaitNegotiation(id,
			negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREED,
			negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_CANCELLED)
	}
	resB, resC := settled(nB.GetId()), settled(nC.GetId())
	agreed := 0
	winnerNeg, loserNeg := resB, resC
	winnerAd, loserAd := adB, adC
	winnerUser, loserUser := u2, u3
	if resC.GetStatus() == negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREED {
		winnerNeg, loserNeg = resC, resB
		winnerAd, loserAd = adC, adB
		winnerUser, loserUser = u3, u2
	}
	for _, r := range []*negotiationv1.Negotiation{resB, resC} {
		if r.GetStatus() == negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREED {
			agreed++
		}
	}
	harness.Require(t, agreed == 1, "expected exactly one AGREED negotiation, got statuses %s (B) and %s (C)",
		harness.NegStatusName(resB.GetStatus()), harness.NegStatusName(resC.GetStatus()))
	harness.Require(t, loserNeg.GetStatus() == negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_CANCELLED,
		"losing negotiation %s is %s, want CANCELLED", loserNeg.GetId(), harness.NegStatusName(loserNeg.GetStatus()))
	t.Logf("winner: negotiation %s; loser %s cancelled (%q)", winnerNeg.GetId(), loserNeg.GetId(), loserNeg.GetCancelReason())

	// In-person legs: the winning swap completes at once, so its ads end CLOSED.
	sw := u1.WaitSwap(winnerNeg.GetId(), swapv1.SwapStatus_SWAP_STATUS_AWAITING_PAYMENT, swapv1.SwapStatus_SWAP_STATUS_COMPLETED)
	t.Logf("winning swap %s is %s", sw.GetId(), harness.SwapStatusName(sw.GetStatus()))
	u1.WaitAdStatus(adA.GetId(), adv1.AdStatus_AD_STATUS_LOCKED, adv1.AdStatus_AD_STATUS_CLOSED)
	winnerUser.WaitAdStatus(winnerAd.GetId(), adv1.AdStatus_AD_STATUS_LOCKED, adv1.AdStatus_AD_STATUS_CLOSED)

	// The loser may have a REJECTED swap (its lock was refused) or none, but never a live one.
	if ls, err := u1.SwapFor(loserNeg.GetId()); err != nil {
		t.Fatalf("ListMySwaps: %v", err)
	} else if ls != nil {
		harness.Require(t, ls.GetStatus() == swapv1.SwapStatus_SWAP_STATUS_REJECTED || ls.GetStatus() == swapv1.SwapStatus_SWAP_STATUS_LOCKING || ls.GetStatus() == swapv1.SwapStatus_SWAP_STATUS_CANCELLED,
			"losing negotiation has a live swap %s in status %s", ls.GetId(), harness.SwapStatusName(ls.GetStatus()))
		if ls.GetStatus() == swapv1.SwapStatus_SWAP_STATUS_LOCKING {
			loserSwap := u1.WaitSwap(loserNeg.GetId(), swapv1.SwapStatus_SWAP_STATUS_REJECTED, swapv1.SwapStatus_SWAP_STATUS_CANCELLED)
			t.Logf("losing swap %s settled as %s", loserSwap.GetId(), harness.SwapStatusName(loserSwap.GetStatus()))
		}
	}

	// The losing ad was never touched.
	time.Sleep(time.Second)
	la := loserUser.GetAd(loserAd.GetId())
	harness.Require(t, la.GetStatus() == adv1.AdStatus_AD_STATUS_PUBLISHED && la.GetVersion() == loserAd.GetVersion(),
		"losing ad %s is %s v%d, want untouched PUBLISHED v%d", la.GetId(), harness.AdStatusName(la.GetStatus()), la.GetVersion(), loserAd.GetVersion())
}

// A user the locker partner does not accept (user-4) cannot be put on a locker leg.
func TestLockerEligibility(t *testing.T) {
	requireLongDeadline(t)
	c := setup(t, "ad", "negotiation")
	u1, u4 := harness.NewActor(t, c, harness.User1), harness.NewActor(t, c, harness.User4)

	adA := u1.PublishAd(harness.Spec("Dictionary", "books", "tools"))
	adD := u4.PublishAd(harness.Spec("Screwdriver set", "tools", "books"))
	n := u4.Open(adD.GetId(), adA.GetId())

	// Leg A is the requester's (user-4) item.
	_, err := u4.TryRevise(n.GetId(), 1, harness.Terms(harness.Locker, harness.InPerson))
	harness.RequireCode(t, err, codes.FailedPrecondition, "user-4 proposing a locker leg for their own item")

	// The other party could ask for a locker on their own leg; user-4 may not be put on one either.
	_, err = u1.TryRevise(n.GetId(), 1, harness.Terms(harness.Locker, harness.InPerson))
	harness.RequireCode(t, err, codes.FailedPrecondition, "user-1 proposing a locker leg for user-4's item")

	// Control: a locker leg for the eligible user is accepted, so the refusal above is about eligibility.
	rev, err := u4.TryRevise(n.GetId(), 1, harness.Terms(harness.InPerson, harness.Locker))
	harness.NoErr(t, err, "user-4 proposing a locker leg for user-1's item")
	harness.Require(t, rev.GetActiveProposal().GetNumber() == 2, "revision created proposal v%d, want v2", rev.GetActiveProposal().GetNumber())
}
