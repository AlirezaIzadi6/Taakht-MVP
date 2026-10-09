package harness

import (
	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	negotiationv1 "github.com/taakht/taakht/gen/taakht/negotiation/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Say narrates a step. Tests pass t.Logf, the demo program prints to stdout.
type Say func(format string, args ...any)

// Seeded users.
const (
	User1 = "user-1"
	User2 = "user-2"
	User3 = "user-3"
	User4 = "user-4"
)

// RequireCode fails unless err is a gRPC status with the wanted code.
func RequireCode(t TB, err error, want codes.Code, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected %s, got success", what, want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("%s: expected %s, got %s (%v)", what, want, got, err)
	}
}

// HappyPath runs the full demo flow: publish, match, three negotiations, a revised proposal, four
// approvals, the exclusive lock, locker fees and the final closing of both ads.
func HappyPath(t TB, c *Clients, say Say) {
	t.Helper()
	u1, u2, u3, u4 := NewActor(t, c, User1), NewActor(t, c, User2), NewActor(t, c, User3), NewActor(t, c, User4)

	say("Step 1: %s and %s publish ads. %s offers books and wants tools; the others offer tools and want books.", u1.ID, u2.ID, u1.ID)
	adA := u1.PublishAd(Spec("Cookbook collection", "books", "tools"))
	adB := u2.PublishAd(Spec("Cordless drill", "tools", "books"))
	adC := u3.PublishAd(Spec("Hand saw", "tools", "books"))
	adD := u4.PublishAd(Spec("Toolbox", "tools", "books"))
	say("  %s published ad %s (%s)", u1.ID, adA.GetId(), adA.GetSpec().GetTitle())
	say("  %s published ad %s (%s)", u2.ID, adB.GetId(), adB.GetSpec().GetTitle())
	say("  %s published ad %s, %s published ad %s", u3.ID, adC.GetId(), u4.ID, adD.GetId())

	say("Step 2: the matching service finds the ads for each other (the index is fed through Kafka).")
	u1.WaitMatch(adA.GetId(), adB.GetId())
	u2.WaitMatch(adB.GetId(), adA.GetId())
	u3.WaitMatch(adC.GetId(), adA.GetId())
	say("  matching: %s's ad now lists %s's ad as a candidate, and the other way round", u1.ID, u2.ID)

	say("Step 3: %s, %s and %s each send a swap request to %s's ad.", u2.ID, u3.ID, u4.ID, u1.ID)
	n2 := u2.Open(adB.GetId(), adA.GetId())
	n3 := u3.Open(adC.GetId(), adA.GetId())
	n4 := u4.Open(adD.GetId(), adA.GetId())
	for _, p := range []struct {
		who string
		n   *negotiationv1.Negotiation
	}{{u2.ID, n2}, {u3.ID, n3}, {u4.ID, n4}} {
		Require(t, p.n.GetStatus() == negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_OPEN, "negotiation of %s is %s, want OPEN", p.who, p.n.GetStatus())
		Require(t, p.n.GetActiveProposal().GetNumber() == 1, "negotiation of %s starts at proposal %d, want 1", p.who, p.n.GetActiveProposal().GetNumber())
		say("  negotiation %s from %s: %s, proposal v%d", p.n.GetId(), p.who, NegStatusName(p.n.GetStatus()), p.n.GetActiveProposal().GetNumber())
	}

	say("Step 4: %s accepts all three requests by approving each requester's ad.", u1.ID)
	u1.ApproveAd(n2.GetId(), adB.GetVersion())
	u1.ApproveAd(n3.GetId(), adC.GetVersion())
	u1.ApproveAd(n4.GetId(), adD.GetVersion())

	say("Step 5: %s approves proposal v1 with %s; then %s revises the terms (both parcels via locker, %s pays 50000 toman).", u2.ID, u1.ID, u1.ID, u2.ID)
	u2.ApproveProposal(n2.GetId(), 1)
	terms := &negotiationv1.Terms{LegA: Locker, LegB: Locker, PriceDifference: 50000, PayerUserId: u2.ID}
	revised := u1.Revise(n2.GetId(), 1, terms)
	Require(t, revised.GetActiveProposal().GetNumber() == 2, "after the revision the active proposal is v%d, want v2", revised.GetActiveProposal().GetNumber())
	old := Approval(revised, u2.ID, negotiationv1.ApprovalKind_APPROVAL_KIND_TERMS)
	Require(t, old != nil && old.GetTarget() == 1 && !old.GetValid(), "%s's TERMS approval of v1 should be stale after the revision, got %v", u2.ID, old)
	say("  the proposal is now v2; %s's earlier approval of v1 is marked stale (valid=%v)", u2.ID, old.GetValid())
	_, err := u2.TryApproveProposal(n2.GetId(), 1)
	RequireCode(t, err, codes.Aborted, u2.ID+" approving the superseded proposal v1")
	say("  approving the old v1 again is refused with ABORTED, as expected")

	say("Step 6: %s approves v2. That is the fourth valid approval, so the agreement is reached.", u2.ID)
	u2.ApproveProposal(n2.GetId(), 2)
	n := u2.WaitNegotiation(n2.GetId(), negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREEMENT_PENDING, negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREED)
	say("  negotiation %s is %s", n.GetId(), NegStatusName(n.GetStatus()))

	say("Step 7: the swap service locks both ads through the Ad service (exclusive lock).")
	sw := u1.WaitSwap(n2.GetId(), swapv1.SwapStatus_SWAP_STATUS_AWAITING_PAYMENT)
	say("  swap %s is %s, locker fees due before %s", sw.GetId(), SwapStatusName(sw.GetStatus()), sw.GetPaymentDeadline().AsTime().Format("15:04:05"))
	u1.WaitAdStatus(adA.GetId(), adv1.AdStatus_AD_STATUS_LOCKED)
	u2.WaitAdStatus(adB.GetId(), adv1.AdStatus_AD_STATUS_LOCKED)
	say("  both ads are LOCKED")
	u2.WaitNegotiation(n2.GetId(), negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREED)
	say("  negotiation %s is AGREED", n2.GetId())
	for _, p := range []struct {
		who string
		id  string
	}{{u3.ID, n3.GetId()}, {u4.ID, n4.GetId()}} {
		lost := u1.WaitNegotiation(p.id, negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_CANCELLED)
		say("  competing negotiation of %s is CANCELLED (%s)", p.who, lost.GetCancelReason())
	}
	u3.WaitNoMatch(adC.GetId(), adA.GetId())
	say("  the locked ad A left the match index: %s no longer sees it as a candidate", u3.ID)
	Require(t, u3.GetAd(adC.GetId()).GetStatus() == adv1.AdStatus_AD_STATUS_PUBLISHED, "ad of %s must stay PUBLISHED", u3.ID)

	say("Step 8: the locker partner (mock) reports the fees: first %s, then %s.", u2.ID, u1.ID)
	s := u1.PayLockerFee(sw.GetId(), u2.ID)
	say("  after %s paid, the swap is %s", u2.ID, SwapStatusName(s.GetStatus()))
	Require(t, s.GetStatus() == swapv1.SwapStatus_SWAP_STATUS_AWAITING_PAYMENT, "swap should still await %s's fee, got %s", u1.ID, s.GetStatus())
	u1.PayLockerFee(sw.GetId(), u1.ID)
	done := u1.WaitSwap(n2.GetId(), swapv1.SwapStatus_SWAP_STATUS_COMPLETED)
	say("  both fees paid: swap %s is %s", done.GetId(), SwapStatusName(done.GetStatus()))
	u1.WaitAdStatus(adA.GetId(), adv1.AdStatus_AD_STATUS_CLOSED)
	u2.WaitAdStatus(adB.GetId(), adv1.AdStatus_AD_STATUS_CLOSED)
	say("  both ads are CLOSED. The swap is done.")
}
