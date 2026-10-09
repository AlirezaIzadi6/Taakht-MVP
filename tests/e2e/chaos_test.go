package e2e

// Fault-injection tests: they run the agreement flow on the real stack while killing services or
// containers, then assert that the system converges to a consistent state. Run them with
//
//	CHAOS=1 E2E=1 go test -run TestChaos -v -timeout 60m ./...      (or: make chaos)
//
// They stop and start services (scripts/dev.sh, taskkill) and the Kafka and Postgres containers, so they
// must have the stack to themselves. They are skipped unless CHAOS=1 and E2E=1 are both set.
//
// Timing: a killed Kafka consumer does not leave its group, so the broker only notices after the session
// timeout (about 45 s) and then rebalances. Every wait for convergence below therefore allows
// convergeTimeout (90 s) instead of the 20 s used by the functional tests.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	adv1 "github.com/taakht/taakht/gen/taakht/ad/v1"
	commonv1 "github.com/taakht/taakht/gen/taakht/common/v1"
	negotiationv1 "github.com/taakht/taakht/gen/taakht/negotiation/v1"
	swapv1 "github.com/taakht/taakht/gen/taakht/swap/v1"
	"github.com/taakht/taakht/tests/e2e/internal/chaos"
	"github.com/taakht/taakht/tests/e2e/internal/harness"
	"google.golang.org/protobuf/proto"
)

const (
	convergeTimeout = 90 * time.Second
	defaultDeadline = "PAYMENT_DEADLINE=2m" // the stack default; restored after the payment-timeout test
)

const (
	adLocked    = adv1.AdStatus_AD_STATUS_LOCKED
	adPublished = adv1.AdStatus_AD_STATUS_PUBLISHED
	adClosed    = adv1.AdStatus_AD_STATUS_CLOSED

	negAgreed    = negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREED
	negPending   = negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_AGREEMENT_PENDING
	negCancelled = negotiationv1.NegotiationStatus_NEGOTIATION_STATUS_CANCELLED

	swapAwaiting  = swapv1.SwapStatus_SWAP_STATUS_AWAITING_PAYMENT
	swapCompleted = swapv1.SwapStatus_SWAP_STATUS_COMPLETED
	swapCancelled = swapv1.SwapStatus_SWAP_STATUS_CANCELLED
)

// chaosSetup skips unless CHAOS=1 and E2E=1, checks that the stack is healthy and registers a cleanup that
// brings everything back up (containers, services on the default payment deadline) even when the test fails.
func chaosSetup(t *testing.T) *harness.Clients {
	t.Helper()
	if os.Getenv("CHAOS") != "1" {
		t.Skip("set CHAOS=1 (and E2E=1) to run the fault-injection tests; they stop services and containers")
	}
	c := setup(t, allServices()...)
	t.Cleanup(func() { chaos.Heal(t, defaultDeadline) })
	return c
}

// record appends one result line to .run/chaos-results.txt (used to fill docs/testing/chaos-test-results.md).
func record(t *testing.T, format string, args ...any) {
	t.Helper()
	line := fmt.Sprintf("%s %s: %s", time.Now().Format("15:04:05"), t.Name(), fmt.Sprintf(format, args...))
	t.Log("RESULT " + line)
	root, err := chaos.Root()
	if err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(root, ".run", "chaos-results.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = fmt.Fprintln(f, line)
}

// flow is one agreement under test: user-2 (ad B) opens a negotiation to user-1's ad A, user-3 (ad C)
// opens a competing negotiation to the same ad A. Both legs use lockers so the swap waits for fees.
type flow struct {
	t          *testing.T
	u1, u2, u3 *harness.Actor
	adA, adB   *adv1.Ad
	adC        *adv1.Ad
	n, nC      *negotiationv1.Negotiation
	proposal   int32
	approvedAt time.Time
}

// newFlow publishes the ads, opens both negotiations and brings the main one to three of four approvals.
func newFlow(t *testing.T, c *harness.Clients) *flow {
	t.Helper()
	f := &flow{
		t:  t,
		u1: harness.NewActor(t, c, harness.User1),
		u2: harness.NewActor(t, c, harness.User2),
		u3: harness.NewActor(t, c, harness.User3),
	}
	f.adA = f.u1.PublishAd(harness.Spec("Chaos atlas", "books", "tools"))
	f.adB = f.u2.PublishAd(harness.Spec("Chaos chisel", "tools", "books"))
	f.adC = f.u3.PublishAd(harness.Spec("Chaos competitor", "tools", "books"))
	f.n = f.u2.Open(f.adB.GetId(), f.adA.GetId())
	f.nC = f.u3.Open(f.adC.GetId(), f.adA.GetId())
	f.u1.ApproveAd(f.n.GetId(), f.adB.GetVersion())
	f.u1.Revise(f.n.GetId(), 1, harness.Terms(harness.Locker, harness.Locker)) // u1 approves terms v2
	f.proposal = 2
	t.Logf("flow: ad A %s (user-1), ad B %s (user-2), competitor ad C %s (user-3); negotiation %s, competitor %s",
		f.adA.GetId(), f.adB.GetId(), f.adC.GetId(), f.n.GetId(), f.nC.GetId())
	return f
}

// approve gives the fourth approval (user-2 approves the terms): the negotiation becomes AGREEMENT_PENDING.
func (f *flow) approve() {
	f.t.Helper()
	n, err := f.u2.TryApproveProposal(f.n.GetId(), f.proposal)
	harness.NoErr(f.t, err, "final ApproveProposal")
	harness.Require(f.t, n.GetStatus() == negPending, "after the fourth approval the negotiation is %s, want AGREEMENT_PENDING", n.GetStatus())
	f.approvedAt = time.Now()
}

func (f *flow) negID() string { return f.n.GetId() }

// swapRows lists "id|status" of all swap rows of the negotiation, straight from the swap database.
func (f *flow) swapRows() []string {
	f.t.Helper()
	out := chaos.Psql(f.t, "swap", fmt.Sprintf("SELECT id||'|'||status FROM swap WHERE negotiation_id='%s'", f.negID()))
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

// swapStatusDB returns the DB status of the negotiation's swap, or "" when there is none.
func (f *flow) swapStatusDB() string {
	rows := f.swapRows()
	if len(rows) == 0 {
		return ""
	}
	return strings.SplitN(rows[0], "|", 2)[1]
}

// waitSwap waits until the swap of the negotiation has one of the statuses (as the API reports it).
func (f *flow) waitSwap(timeout time.Duration, want ...swapv1.SwapStatus) *swapv1.Swap {
	f.t.Helper()
	var sw *swapv1.Swap
	harness.Poll(f.t, fmt.Sprintf("swap of %s to reach %v", f.negID(), want), timeout, func() (bool, string) {
		var err error
		sw, err = f.u1.SwapFor(f.negID())
		if err != nil {
			return false, err.Error()
		}
		if sw == nil {
			return false, "no swap yet"
		}
		for _, w := range want {
			if sw.GetStatus() == w {
				return true, ""
			}
		}
		return false, sw.GetStatus().String()
	})
	return sw
}

// waitAd waits for an ad status with the long chaos timeout.
func (f *flow) waitAd(a *harness.Actor, adID string, timeout time.Duration, want adv1.AdStatus) {
	f.t.Helper()
	harness.Poll(f.t, fmt.Sprintf("ad %s to be %s", adID, want), timeout, func() (bool, string) {
		ctx, cancel := a.Ctx()
		defer cancel()
		ad, err := a.C.Ad.GetAd(ctx, &adv1.GetAdRequest{AdId: adID})
		if err != nil {
			return false, err.Error()
		}
		return ad.GetStatus() == want, ad.GetStatus().String()
	})
}

func (f *flow) waitNeg(id string, timeout time.Duration, want negotiationv1.NegotiationStatus) {
	f.t.Helper()
	harness.Poll(f.t, fmt.Sprintf("negotiation %s to be %s", id, want), timeout, func() (bool, string) {
		ctx, cancel := f.u1.Ctx()
		defer cancel()
		n, err := f.u1.C.Negotiation.GetNegotiation(ctx, &negotiationv1.NegotiationIdRequest{NegotiationId: id})
		if err != nil {
			return false, err.Error()
		}
		return n.GetStatus() == want, n.GetStatus().String()
	})
}

// waitLocked waits for the converged "lock held" state: swap AWAITING_PAYMENT, both ads LOCKED, main
// negotiation AGREED, competitor CANCELLED. It returns the swap and how long it took.
func (f *flow) waitLocked(timeout time.Duration) (*swapv1.Swap, time.Duration) {
	f.t.Helper()
	start := time.Now()
	sw := f.waitSwap(timeout, swapAwaiting)
	f.waitAd(f.u1, f.adA.GetId(), timeout, adLocked)
	f.waitAd(f.u2, f.adB.GetId(), timeout, adLocked)
	f.waitNeg(f.negID(), timeout, negAgreed)
	f.waitNeg(f.nC.GetId(), timeout, negCancelled)
	return sw, time.Since(start)
}

// payAll simulates both locker fees and waits for COMPLETED and both ads CLOSED.
func (f *flow) payAll(timeout time.Duration) {
	f.t.Helper()
	sw := f.waitSwap(timeout, swapAwaiting, swapCompleted)
	if sw.GetStatus() == swapAwaiting {
		f.u1.PayLockerFee(sw.GetId(), f.u2.ID)
		f.u1.PayLockerFee(sw.GetId(), f.u1.ID)
	}
	f.waitSwap(timeout, swapCompleted)
	f.waitAd(f.u1, f.adA.GetId(), timeout, adClosed)
	f.waitAd(f.u2, f.adB.GetId(), timeout, adClosed)
}

// assertConsistent checks the end-state invariants straight from the databases and the APIs.
func (f *flow) assertConsistent() {
	t := f.t
	t.Helper()
	rows := f.swapRows()
	harness.Require(t, len(rows) == 1, "negotiation %s has %d swaps, want exactly 1: %v", f.negID(), len(rows), rows)
	swapID, st, _ := strings.Cut(rows[0], "|")

	// No duplicate swaps for the ad pair, and at most one live-or-completed swap per ad.
	a, b := f.adA.GetId(), f.adB.GetId()
	pair := chaos.PsqlInt(t, "swap", fmt.Sprintf("SELECT count(*) FROM swap WHERE (ad_a_id='%s' AND ad_b_id='%s') OR (ad_a_id='%s' AND ad_b_id='%s')", a, b, b, a))
	harness.Require(t, pair == 1, "ad pair has %d swap rows, want 1", pair)
	winners := chaos.PsqlInt(t, "swap", fmt.Sprintf("SELECT count(*) FROM swap WHERE status IN ('LOCKING','AWAITING_PAYMENT','COMPLETED') AND (ad_a_id='%s' OR ad_b_id='%s')", a, a))
	harness.Require(t, winners <= 1, "ad A is part of %d active/completed swaps, want at most 1", winners)

	adStatus := func(id string) string {
		return chaos.Psql(t, "ad", fmt.Sprintf("SELECT status FROM ad WHERE id='%s'", id))
	}
	lockStates := chaos.Psql(t, "ad", fmt.Sprintf("SELECT string_agg(state, ',' ORDER BY ad_id) FROM ad_lock WHERE swap_id='%s'", swapID))
	negA := f.u1.Negotiation(f.negID()).GetStatus()
	negC := f.u1.Negotiation(f.nC.GetId()).GetStatus()
	t.Logf("end state: swap %s %s; ads A=%s B=%s C=%s; ad_lock=%s; negotiation=%s competitor=%s",
		swapID, st, adStatus(a), adStatus(b), adStatus(f.adC.GetId()), lockStates, negA, negC)

	var wantAd, wantLock string
	switch st {
	case "AWAITING_PAYMENT":
		wantAd, wantLock = "locked", "locked,locked"
		harness.Require(t, negA == negAgreed, "swap AWAITING_PAYMENT but negotiation is %s", negA)
	case "COMPLETED":
		wantAd, wantLock = "closed", "closed,closed"
		harness.Require(t, negA == negAgreed, "swap COMPLETED but negotiation is %s", negA)
	case "CANCELLED":
		wantAd, wantLock = "published", "released,released"
		harness.Require(t, negA == negCancelled, "swap CANCELLED but negotiation is %s", negA)
	default:
		t.Fatalf("swap %s is %s: not a settled state", swapID, st)
	}
	harness.Require(t, adStatus(a) == wantAd && adStatus(b) == wantAd, "swap %s: ads are %s/%s, want %s", st, adStatus(a), adStatus(b), wantAd)
	harness.Require(t, lockStates == wantLock, "swap %s: ad_lock states %q, want %q", st, lockStates, wantLock)
	if st != "CANCELLED" {
		harness.Require(t, negC == negCancelled, "the competing negotiation is %s, want CANCELLED (exactly one winner)", negC)
	}
	harness.Require(t, adStatus(f.adC.GetId()) == "published", "the competitor's ad is %s, want published", adStatus(f.adC.GetId()))
	// No ad of this flow may sit LOCKED unless an AWAITING_PAYMENT swap holds it.
	stuck := chaos.PsqlInt(t, "ad", fmt.Sprintf("SELECT count(*) FROM ad WHERE status='locked' AND id IN ('%s','%s','%s')", a, b, f.adC.GetId()))
	harness.Require(t, stuck == 0 || st == "AWAITING_PAYMENT", "%d ads stuck LOCKED while the swap is %s", stuck, st)
}

// sleep logs why the test waits.
func sleep(t *testing.T, d time.Duration, why string) {
	t.Helper()
	t.Logf("waiting %s: %s", d, why)
	time.Sleep(d)
}

// (1) Swap is killed before it handles AgreementReached; the event waits in Kafka.
func TestChaosSwapKilledBeforeAgreement(t *testing.T) {
	c := chaosSetup(t)
	f := newFlow(t, c)

	chaos.StopService(t, "swap")
	f.approve()
	sleep(t, 8*time.Second, "agreement is published while swap is down")
	harness.Require(t, f.swapStatusDB() == "", "a swap exists while the swap service is down: %v", f.swapRows())
	f.waitAd(f.u1, f.adA.GetId(), 5*time.Second, adPublished)
	harness.Require(t, f.u1.Negotiation(f.negID()).GetStatus() == negPending, "negotiation should wait in AGREEMENT_PENDING")

	t0 := time.Now()
	chaos.StartService(t, "swap", defaultDeadline)
	_, took := f.waitLocked(convergeTimeout)
	f.assertConsistent()
	f.payAll(convergeTimeout)
	f.assertConsistent()
	record(t, "PASS swap restarted at +%s of the outage; converged (locked, AGREED, competitor CANCELLED) %s after the swap port opened (%s after start), then fees -> COMPLETED",
		time.Since(f.approvedAt).Round(time.Second), took.Round(time.Second), time.Since(t0).Round(time.Second))
}

// (2) Ad is down while the swap is LOCKING; the swap retries LockAds until Ad is back.
func TestChaosAdKilledWhileLocking(t *testing.T) {
	c := chaosSetup(t)
	f := newFlow(t, c)

	// Hold the event back so Ad can be taken down before the swap tries to lock (deterministic).
	chaos.StopService(t, "swap")
	f.approve()
	chaos.StopService(t, "ad")
	chaos.StartService(t, "swap", defaultDeadline)
	// The restarted consumer joins its group only after the dead member's session expires (up to ~45 s), so
	// wait until the handler has really failed against the missing Ad service before bringing Ad back.
	harness.Poll(t, "the swap handler to fail with Ad down (log line 'Handler failed')", convergeTimeout, func() (bool, string) {
		n := len(chaos.LogLines(t, "swap", "Handler failed at", 5))
		return n > 0, fmt.Sprintf("%d failure lines", n)
	})
	sleep(t, 5*time.Second, "handler keeps retrying with Ad down")
	rows := f.swapRows()
	t.Logf("swap rows while ad is down: %v", rows)
	harness.Require(t, f.swapStatusDB() != "AWAITING_PAYMENT" && f.swapStatusDB() != "COMPLETED", "swap advanced without the Ad service: %v", rows)
	t.Logf("swap log: %v", chaos.LogLines(t, "swap", "Handler failed at", 2))

	t0 := time.Now()
	chaos.StartService(t, "ad")
	_, took := f.waitLocked(convergeTimeout)
	f.assertConsistent()
	f.payAll(convergeTimeout)
	f.assertConsistent()
	record(t, "PASS swap state while ad down: %q (rows=%v); after ad restart converged in %s (port-open to locked, %s)",
		f.swapStatusDBOrNone(rows), rows, time.Since(t0).Round(time.Second), took.Round(time.Second))
}

func (f *flow) swapStatusDBOrNone(rows []string) string {
	if len(rows) == 0 {
		return "no row"
	}
	return strings.SplitN(rows[0], "|", 2)[1]
}

// (3) Kafka is stopped for 15 s around the agreement; the outbox keeps the event, the relay resumes.
func TestChaosKafkaDown(t *testing.T) {
	c := chaosSetup(t)
	f := newFlow(t, c)

	chaos.StopKafka(t)
	f.approve() // business operation works with Kafka down
	sleep(t, 15*time.Second, "Kafka is down; the outbox must hold AgreementReached")
	pending := chaos.PsqlInt(t, "negotiation", "SELECT count(*) FROM outbox WHERE published_at IS NULL")
	t.Logf("negotiation outbox rows not yet published: %d", pending)
	harness.Require(t, pending >= 1, "expected unpublished outbox rows while Kafka is down, found %d", pending)
	harness.Require(t, f.swapStatusDB() == "", "swap created while Kafka is down: %v", f.swapRows())
	harness.Require(t, f.u1.Negotiation(f.negID()).GetStatus() == negPending, "negotiation should stay AGREEMENT_PENDING")
	// Writes keep working with Kafka down.
	adX := f.u3.PublishAd(harness.Spec("Chaos during kafka outage", "tools", "books"))
	t.Logf("CreateAd/PublishAd with Kafka down worked: %s", adX.GetId())

	t0 := time.Now()
	chaos.StartKafka(t)
	_, took := f.waitLocked(convergeTimeout)
	f.assertConsistent()
	left := chaos.PsqlInt(t, "negotiation", "SELECT count(*) FROM outbox WHERE published_at IS NULL")
	harness.Require(t, left == 0, "negotiation outbox still has %d unpublished rows after recovery", left)
	f.payAll(convergeTimeout)
	f.assertConsistent()
	record(t, "PASS outbox held %d row(s) for 15 s; after kafka start (+5 s settle) converged in %s (%s total since start); writes worked during the outage",
		pending, took.Round(time.Second), time.Since(t0).Round(time.Second))
}

// (4) Postgres is restarted right after the agreement. Services must reconnect on their own.
func TestChaosPostgresRestart(t *testing.T) {
	c := chaosSetup(t)
	f := newFlow(t, c)

	f.approve()
	chaos.StopPostgres(t)
	sleep(t, 10*time.Second, "postgres is down")
	ctx, cancel := f.u1.Ctx()
	_, err := f.u1.C.Ad.GetAd(ctx, &adv1.GetAdRequest{AdId: f.adA.GetId()})
	cancel()
	t.Logf("GetAd with postgres down: err=%v", err)
	harness.Require(t, err != nil, "GetAd succeeded while Postgres is down")

	t0 := time.Now()
	chaos.StartPostgres(t)
	_, took := f.waitLocked(convergeTimeout)
	f.assertConsistent()
	f.payAll(convergeTimeout)
	f.assertConsistent()
	// Fresh work after the outage proves every service writes again.
	g := newFlow(t, c)
	g.approve()
	g.waitLocked(convergeTimeout)
	g.assertConsistent()
	record(t, "PASS postgres down 10 s right after the 4th approval; services reconnected without restart; converged in %s (%s total), second flow after the outage also fine",
		took.Round(time.Second), time.Since(t0).Round(time.Second))
}

// (5) Negotiation dies after ApproveProposal committed AgreementReached to its outbox but before the relay
// published it. Kafka is stopped first so the event is certain to still be in the outbox (the relay polls
// every 200 ms, so a plain kill right after the call would be a race).
func TestChaosNegotiationKilledBeforeRelay(t *testing.T) {
	c := chaosSetup(t)
	f := newFlow(t, c)

	chaos.StopKafka(t)
	f.approve()
	pending := chaos.PsqlInt(t, "negotiation", "SELECT count(*) FROM outbox WHERE published_at IS NULL")
	chaos.StopService(t, "negotiation")
	chaos.StartKafka(t)
	sleep(t, 15*time.Second, "Kafka is back but negotiation (and its relay) is down: the event must still wait in the outbox")
	harness.Require(t, f.swapStatusDB() == "", "swap exists although AgreementReached never left the negotiation outbox: %v", f.swapRows())
	f.waitAd(f.u1, f.adA.GetId(), 5*time.Second, adPublished)

	t0 := time.Now()
	chaos.StartService(t, "negotiation")
	_, took := f.waitLocked(convergeTimeout)
	f.assertConsistent()
	f.payAll(convergeTimeout)
	f.assertConsistent()
	record(t, "PASS outbox held %d unpublished row(s) across negotiation kill; after restart converged in %s (%s total since start)",
		pending, took.Round(time.Second), time.Since(t0).Round(time.Second))
}

// (6) Re-delivery of an already processed AgreementReached: same event id (new Kafka key), then a fresh
// event id for the same negotiation, while the swap awaits payment and again after it completed.
func TestChaosDuplicateAgreementReached(t *testing.T) {
	c := chaosSetup(t)
	f := newFlow(t, c)
	f.approve()
	sw, _ := f.waitLocked(convergeTimeout)

	orig := chaos.FindEnvelope(t, "negotiation.events", 30*time.Second, func(e *commonv1.Envelope) bool {
		if !strings.HasSuffix(e.GetType(), ".AgreementReached") {
			return false
		}
		var m negotiationv1.AgreementReached
		return proto.Unmarshal(e.GetPayload(), &m) == nil && m.GetNegotiationId() == f.negID()
	})
	harness.Require(t, orig != nil, "AgreementReached for %s not found on negotiation.events", f.negID())

	swapState := func() string {
		return chaos.Psql(t, "swap", fmt.Sprintf("SELECT id||'|'||status||'|'||(SELECT count(*) FROM outbox)||'|'||(SELECT count(*) FROM processed_events) FROM swap WHERE negotiation_id='%s'", f.negID()))
	}
	sleep(t, 5*time.Second, "let the swap consumer drain the remaining negotiation events")
	before := swapState()
	core := func(s string) string { return strings.Join(strings.Split(s, "|")[:3], "|") } // id|status|outbox rows

	// (a) same event_id, new Kafka key (so a possibly different partition): the consumer must dedupe.
	chaos.Publish(t, "negotiation.events", "dup-"+chaos.NewUUID(), orig)
	sleep(t, 10*time.Second, "duplicate with the same event_id")
	afterSame := swapState()
	t.Logf("swap state before=%s afterSameEventID=%s", before, afterSame)
	harness.Require(t, core(afterSame) == core(before), "same event_id changed the swap state (id|status|outbox|processed): %s -> %s", before, afterSame)

	// (b) fresh event_id, same negotiation: not deduped by event id, the handler itself must be idempotent.
	fresh := proto.Clone(orig).(*commonv1.Envelope)
	fresh.EventId = chaos.NewUUID()
	chaos.Publish(t, "negotiation.events", f.negID(), fresh)
	sleep(t, 10*time.Second, "duplicate with a fresh event_id, swap awaiting payment")
	afterFresh := swapState()
	t.Logf("swap state afterFreshEventID=%s", afterFresh)
	f.assertConsistent()
	harness.Require(t, strings.HasPrefix(afterFresh, sw.GetId()+"|AWAITING_PAYMENT|"), "swap changed after a fresh-id duplicate: %s", afterFresh)
	outboxB := strings.Split(before, "|")[2]
	outboxA := strings.Split(afterFresh, "|")[2]
	harness.Require(t, outboxA == outboxB, "swap outbox grew from %s to %s after a duplicate (extra events emitted)", outboxB, outboxA)

	// (c) after completion a late duplicate must not reject or reopen anything.
	f.payAll(convergeTimeout)
	f.assertConsistent()
	done := swapState()
	fresh2 := proto.Clone(orig).(*commonv1.Envelope)
	fresh2.EventId = chaos.NewUUID()
	chaos.Publish(t, "negotiation.events", "late-"+chaos.NewUUID(), fresh2)
	sleep(t, 10*time.Second, "duplicate with a fresh event_id after COMPLETED")
	late := swapState()
	t.Logf("swap state completed=%s afterLateDuplicate=%s", done, late)
	f.assertConsistent()
	harness.Require(t, strings.Split(late, "|")[1] == "COMPLETED", "late duplicate changed the swap: %s", late)
	harness.Require(t, strings.Split(late, "|")[2] == strings.Split(done, "|")[2], "late duplicate emitted extra swap events: outbox %s -> %s", strings.Split(done, "|")[2], strings.Split(late, "|")[2])
	record(t, "PASS same event_id deduped (swap row, outbox and processed_events unchanged); fresh event_id recorded in processed_events but handled as a no-op while AWAITING_PAYMENT and after COMPLETED; one swap per negotiation, no extra swap events (state %s -> %s)", before, late)
}

// (7) Payment timeout combined with an Ad outage: the deadline passes while Ad is down, SwapCancelled waits in
// Kafka and Ad releases both ads after it restarts.
func TestChaosPaymentTimeoutWithAdRestart(t *testing.T) {
	c := chaosSetup(t)
	// A 25 s deadline. The test body restores the default deadline in the cleanup (Heal only starts what is down).
	t.Cleanup(func() {
		if err := chaosRestartSwap(t, defaultDeadline); err != nil {
			t.Logf("restoring swap with the default deadline: %v", err)
		}
	})
	chaos.StopService(t, "swap")
	chaos.StartService(t, "swap", "PAYMENT_DEADLINE=25s")
	f := newFlow(t, c)
	f.approve()
	// The restarted swap consumer may need up to ~45 s to be assigned the partitions again.
	sw, took := f.waitLocked(convergeTimeout)
	lockedAt := time.Now()
	t.Logf("locked after %s, deadline %s", took.Round(time.Second), sw.GetPaymentDeadline().AsTime().Format("15:04:05"))
	f.u1.PayLockerFee(sw.GetId(), f.u2.ID) // only one side pays
	chaos.StopService(t, "ad")
	harness.Poll(t, "swap to be CANCELLED by the deadline sweeper while Ad is down", convergeTimeout, func() (bool, string) {
		return f.swapStatusDB() == "CANCELLED", f.swapStatusDB()
	})
	cancelledAt := time.Now()
	sleep(t, 5*time.Second, "SwapCancelled waits in Kafka; Ad is still down")

	chaos.StartService(t, "ad")
	t0 := time.Now()
	f.waitAd(f.u1, f.adA.GetId(), convergeTimeout, adPublished)
	f.waitAd(f.u2, f.adB.GetId(), convergeTimeout, adPublished)
	f.waitNeg(f.negID(), convergeTimeout, negCancelled)
	f.assertConsistent()
	record(t, "PASS deadline 25 s, one fee paid; swap CANCELLED %s after the lock while ad was down; after ad restart ads PUBLISHED and negotiation CANCELLED in %s",
		cancelledAt.Sub(lockedAt).Round(time.Second), time.Since(t0).Round(time.Second))
}

func chaosRestartSwap(t *testing.T, env string) error {
	chaos.StopService(t, "swap")
	chaos.StartService(t, "swap", env)
	return nil
}

// negotiationRow returns "status|cancel_reason|republish_count" of the negotiation straight from its database.
func (f *flow) negotiationRow() string {
	f.t.Helper()
	return chaos.Psql(f.t, "negotiation", fmt.Sprintf("SELECT status||'|'||cancel_reason||'|'||republish_count FROM negotiation WHERE id='%s'", f.negID()))
}

// withAgreementTimeout restarts negotiation with a short AGREEMENT_PENDING_TIMEOUT and restores the default at the end.
func withAgreementTimeout(t *testing.T, timeout string) {
	t.Helper()
	chaos.RestartService(t, "negotiation", "AGREEMENT_PENDING_TIMEOUT="+timeout)
	t.Cleanup(func() { chaos.RestartService(t, "negotiation") })
}

// (8) Matching is down while ads are published; after the restart its index catches up from Kafka, every pair is
// notified exactly once and replayed ad events (same and fresh event id) change nothing.
func TestChaosMatchingDownDuringPublish(t *testing.T) {
	c := chaosSetup(t)
	u1, u2 := harness.NewActor(t, c, harness.User1), harness.NewActor(t, c, harness.User2)

	chaos.StopService(t, "matching")
	adP := u1.PublishAd(harness.Spec("Chaos match atlas", "books", "tools"))
	adQ := u2.PublishAd(harness.Spec("Chaos match chisel", "tools", "books"))
	sleep(t, 5*time.Second, "ads are published while matching is down; their events wait in Kafka")

	t0 := time.Now()
	chaos.StartService(t, "matching")
	harness.Poll(t, "matching to index both ads and find the pair after the restart", convergeTimeout, func() (bool, string) {
		cands, err := u1.Matches(adP.GetId())
		if err != nil {
			return false, err.Error()
		}
		for _, cand := range cands {
			if cand.GetAd().GetId() == adQ.GetId() {
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d candidates, ad Q not among them", len(cands))
	})
	caught := time.Since(t0)

	ids := fmt.Sprintf("'%s','%s'", adP.GetId(), adQ.GetId())
	counts := func() (pairs, notified, events, processed int) {
		pairs = chaos.PsqlInt(t, "matching", fmt.Sprintf("SELECT count(*) FROM notified_pair WHERE (ad_id='%s' AND matched_ad_id='%s') OR (ad_id='%s' AND matched_ad_id='%s')",
			adP.GetId(), adQ.GetId(), adQ.GetId(), adP.GetId()))
		notified = chaos.PsqlInt(t, "matching", fmt.Sprintf("SELECT count(*) FROM notified_pair WHERE ad_id IN (%s)", ids))
		events = chaos.PsqlInt(t, "matching", fmt.Sprintf("SELECT count(*) FROM outbox WHERE key IN (%s)", ids))
		processed = chaos.PsqlInt(t, "matching", "SELECT count(*) FROM processed_events")
		return pairs, notified, events, processed
	}
	sleep(t, 3*time.Second, "let the consumer drain")
	pairs, notified, events, processed := counts()
	t.Logf("after catch-up: pair rows=%d notified_pair rows of P/Q=%d MatchFound outbox rows=%d processed_events=%d", pairs, notified, events, processed)
	harness.Require(t, pairs == 1, "the P/Q pair is recorded %d times in notified_pair, want exactly 1", pairs)
	harness.Require(t, events == notified, "MatchFound events (%d) differ from notified_pair rows (%d) for the new ads", events, notified)
	live := chaos.PsqlInt(t, "matching", fmt.Sprintf("SELECT count(*) FROM ad_index WHERE ad_id IN (%s) AND NOT removed", ids))
	harness.Require(t, live == 2, "%d of the 2 published ads are live in the index", live)

	// Replay AdPublished of ad Q: once with the same event id (consumer dedupe), once with a fresh one (seq guard).
	orig := chaos.FindEnvelope(t, "ad.events", 30*time.Second, func(e *commonv1.Envelope) bool {
		var m adv1.AdPublished
		return strings.HasSuffix(e.GetType(), ".AdPublished") && proto.Unmarshal(e.GetPayload(), &m) == nil && m.GetAd().GetId() == adQ.GetId()
	})
	harness.Require(t, orig != nil, "AdPublished of ad Q not found on ad.events")
	chaos.Publish(t, "ad.events", adQ.GetId(), orig)
	fresh := proto.Clone(orig).(*commonv1.Envelope)
	fresh.EventId = chaos.NewUUID()
	chaos.Publish(t, "ad.events", adQ.GetId(), fresh)
	sleep(t, 10*time.Second, "replayed AdPublished events are consumed")
	pairs2, notified2, events2, processed2 := counts()
	t.Logf("after replay: pair rows=%d notified=%d events=%d processed_events=%d (was %d)", pairs2, notified2, events2, processed2, processed)
	harness.Require(t, pairs2 == pairs && notified2 == notified && events2 == events, "replay changed MatchFound state: pairs %d->%d notified %d->%d events %d->%d", pairs, pairs2, notified, notified2, events, events2)
	harness.Require(t, processed2 == processed+1, "processed_events grew by %d after replaying one duplicate and one fresh-id event, want 1", processed2-processed)
	record(t, "PASS matching down while 2 ads were published; index caught up %s after the matching port opened; pair notified once (notified_pair=%d, MatchFound outbox rows=%d); replay with the same event_id and with a fresh one changed nothing", caught.Round(time.Second), notified, events)
}

// (9) swap is killed before it consumes AgreementReached; with AGREEMENT_PENDING_TIMEOUT=20s the sweeper publishes the
// event again; when swap returns it creates exactly one swap and the flow completes.
func TestChaosAgreementPendingRepublish(t *testing.T) {
	c := chaosSetup(t)
	withAgreementTimeout(t, "20s")
	f := newFlow(t, c)

	chaos.StopService(t, "swap")
	f.approve()
	harness.Poll(t, "the sweeper to republish AgreementReached (republish_count >= 1)", 3*time.Minute, func() (bool, string) {
		row := f.negotiationRow()
		parts := strings.Split(row, "|")
		return len(parts) == 3 && parts[2] != "0", row
	})
	republishedAt := time.Since(f.approvedAt)
	harness.Require(t, f.swapStatusDB() == "", "swap rows exist although swap is down: %v", f.swapRows())

	t0 := time.Now()
	chaos.StartService(t, "swap", defaultDeadline)
	_, took := f.waitLocked(convergeTimeout)
	row := f.negotiationRow()
	f.assertConsistent()
	reopened := len(chaos.LogLines(t, "negotiation", "is AGREED again", 5)) > 0
	f.payAll(convergeTimeout)
	f.assertConsistent()
	record(t, "PASS timeout 20s, swap killed before consuming; first republish %s after the 4th approval; swap back, converged %s after its port opened (%s after start); negotiation row at lock time %q; exactly one swap; negotiation had been cancelled and reopened by the late lock: %v",
		republishedAt.Round(time.Second), took.Round(time.Second), time.Since(t0).Round(time.Second), row, reopened)
}

// (10) The sweeper gives up (timeout 20s: three republishes, then CANCELLED 'agreement timed out') while swap is down;
// the late lock then reopens the negotiation as AGREED and the competitor is cancelled.
func TestChaosLateLockAfterAgreementTimeout(t *testing.T) {
	c := chaosSetup(t)
	withAgreementTimeout(t, "20s")
	f := newFlow(t, c)

	chaos.StopService(t, "swap")
	f.approve()
	harness.Poll(t, "the sweeper to give up (CANCELLED, agreement timed out)", 6*time.Minute, func() (bool, string) {
		row := f.negotiationRow()
		return strings.HasPrefix(row, "CANCELLED|agreement timed out|"), row
	})
	cancelledAt := time.Since(f.approvedAt)
	competitor := f.u1.Negotiation(f.nC.GetId()).GetStatus()
	t.Logf("negotiation cancelled by the sweeper after %s; competitor is %s", cancelledAt.Round(time.Second), competitor)
	harness.Require(t, f.swapStatusDB() == "", "swap rows exist although swap is down: %v", f.swapRows())

	t0 := time.Now()
	chaos.StartService(t, "swap", defaultDeadline)
	f.waitSwap(convergeTimeout, swapAwaiting)
	f.waitNeg(f.negID(), convergeTimeout, negAgreed)
	f.waitNeg(f.nC.GetId(), convergeTimeout, negCancelled)
	f.assertConsistent()
	row := f.negotiationRow()
	harness.Require(t, strings.HasPrefix(row, "AGREED||"), "reopened negotiation row is %q, want AGREED with an empty cancel reason", row)
	f.payAll(convergeTimeout)
	f.assertConsistent()
	record(t, "PASS sweeper cancelled the negotiation %s after the approval (swap down); late lock %s after the swap start reopened it as AGREED (row %q), competitor (was %s) CANCELLED, one swap, fees -> COMPLETED",
		cancelledAt.Round(time.Second), time.Since(t0).Round(time.Second), row, competitor)
}

// (11) Kafka is down for 70 s, longer than the consumer session timeout, around the agreement. Producers and
// consumers must reconnect and rejoin their groups on their own.
func TestChaosKafkaDownLongerThanSessionTimeout(t *testing.T) {
	c := chaosSetup(t)
	f := newFlow(t, c)

	chaos.StopKafka(t)
	f.approve()
	sleep(t, 70*time.Second, "Kafka is down longer than the session timeout; the outbox must hold AgreementReached")
	pending := chaos.PsqlInt(t, "negotiation", "SELECT count(*) FROM outbox WHERE published_at IS NULL")
	harness.Require(t, pending >= 1, "expected unpublished outbox rows while Kafka is down, found %d", pending)
	harness.Require(t, f.swapStatusDB() == "", "swap created while Kafka is down: %v", f.swapRows())

	t0 := time.Now()
	chaos.StartKafka(t)
	_, took := f.waitLocked(3 * time.Minute)
	f.assertConsistent()
	left := chaos.PsqlInt(t, "negotiation", "SELECT count(*) FROM outbox WHERE published_at IS NULL")
	harness.Require(t, left == 0, "negotiation outbox still has %d unpublished rows after recovery", left)
	f.payAll(convergeTimeout)
	f.assertConsistent()
	// Work started after the recovery must flow without any restart.
	g := newFlow(t, c)
	g.approve()
	_, tookG := g.waitLocked(convergeTimeout)
	g.assertConsistent()
	record(t, "PASS Kafka down 70 s (> 45 s session timeout) around the 4th approval; outbox held %d row(s); converged %s after Kafka start (%s total); a fresh flow afterwards locked in %s without any restart",
		pending, took.Round(time.Second), time.Since(t0).Round(time.Second), tookG.Round(time.Second))
}
