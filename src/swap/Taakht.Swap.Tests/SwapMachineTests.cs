using Taakht.Swap.Domain;

namespace Taakht.Swap.Tests;

public class SwapMachineTests
{
    private static readonly DateTimeOffset _now = new(2026, 10, 8, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan _window = TimeSpan.FromMinutes(2);

    private static SwapModel NewSwap(DeliveryMethod a, DeliveryMethod b) =>
        SwapModel.Create(
            Guid.NewGuid(), "neg-1", "ad-a", 1, "ad-b", 2,
            new SwapLeg("user-1", a, false), new SwapLeg("user-2", b, false), _now);

    private static SwapModel Awaiting(DeliveryMethod a, DeliveryMethod b) =>
        SwapMachine.LockAcquired(NewSwap(a, b), _now, _window).Swap;

    [Fact]
    public void LockAcquired_WithLockerLeg_AwaitsPaymentWithDeadline()
    {
        var t = SwapMachine.LockAcquired(NewSwap(DeliveryMethod.Locker, DeliveryMethod.InPerson), _now, _window);

        Assert.True(t.Changed);
        Assert.Equal(SwapStatus.AwaitingPayment, t.Swap.Status);
        Assert.Equal(_now + _window, t.Swap.PaymentDeadline);
        Assert.IsType<ExclusiveLockAcquiredEvent>(Assert.Single(t.Events));
    }

    [Fact]
    public void LockAcquired_WithoutLockerLegs_CompletesImmediately()
    {
        var t = SwapMachine.LockAcquired(NewSwap(DeliveryMethod.InPerson, DeliveryMethod.InPerson), _now, _window);

        Assert.Equal(SwapStatus.Completed, t.Swap.Status);
        Assert.Null(t.Swap.PaymentDeadline);
        Assert.Collection(
            t.Events,
            e => Assert.IsType<ExclusiveLockAcquiredEvent>(e),
            e => Assert.IsType<SwapCompletedEvent>(e));
    }

    [Fact]
    public void LockAcquired_Replay_IsNoOp()
    {
        var awaiting = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);

        var t = SwapMachine.LockAcquired(awaiting, _now.AddMinutes(1), _window);

        Assert.False(t.Changed);
        Assert.Equal(awaiting, t.Swap);
        Assert.Empty(t.Events);
    }

    [Fact]
    public void LockRejected_MovesToRejectedOnce()
    {
        var swap = NewSwap(DeliveryMethod.Locker, DeliveryMethod.Locker);

        var t = SwapMachine.LockRejected(swap, "ad changed");

        Assert.Equal(SwapStatus.Rejected, t.Swap.Status);
        Assert.Equal("ad changed", Assert.IsType<SwapRejectedEvent>(Assert.Single(t.Events)).Reason);
        Assert.False(SwapMachine.LockRejected(t.Swap, "again").Changed);
    }

    [Fact]
    public void LockRejected_AfterLockAcquired_IsIgnored()
    {
        var awaiting = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);

        Assert.False(SwapMachine.LockRejected(awaiting, "late").Changed);
    }

    [Fact]
    public void FeePaid_PartialPayment_KeepsAwaiting()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);

        var t = SwapMachine.FeePaid(swap, "user-1");

        Assert.True(t.Changed);
        Assert.Equal(SwapStatus.AwaitingPayment, t.Swap.Status);
        Assert.True(t.Swap.LegA.FeePaid);
        Assert.False(t.Swap.LegB.FeePaid);
        Assert.Empty(t.Events);
    }

    [Fact]
    public void FeePaid_AllLockerLegsPaid_Completes()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);

        var done = SwapMachine.FeePaid(SwapMachine.FeePaid(swap, "user-1").Swap, "user-2");

        Assert.Equal(SwapStatus.Completed, done.Swap.Status);
        Assert.IsType<SwapCompletedEvent>(Assert.Single(done.Events));
    }

    [Fact]
    public void FeePaid_OnlyOneLockerLeg_CompletesOnThatPayment()
    {
        var swap = Awaiting(DeliveryMethod.InPerson, DeliveryMethod.Locker);

        var t = SwapMachine.FeePaid(swap, "user-2");

        Assert.Equal(SwapStatus.Completed, t.Swap.Status);
    }

    [Fact]
    public void FeePaid_Twice_IsIdempotent()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);
        var once = SwapMachine.FeePaid(swap, "user-1").Swap;

        var again = SwapMachine.FeePaid(once, "user-1");

        Assert.False(again.Changed);
        Assert.Equal(once, again.Swap);
    }

    [Fact]
    public void FeePaid_AfterCompletion_IsIdempotent()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.InPerson);
        var done = SwapMachine.FeePaid(swap, "user-1").Swap;

        var again = SwapMachine.FeePaid(done, "user-1");

        Assert.False(again.Changed);
        Assert.Empty(again.Events);
    }

    [Fact]
    public void FeePaid_ForNonLockerLegOrStranger_IsRejected()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.InPerson);

        Assert.Equal(DomainErrorKind.FailedPrecondition, Assert.Throws<SwapDomainException>(() => SwapMachine.FeePaid(swap, "user-2")).Kind);
        Assert.Equal(DomainErrorKind.FailedPrecondition, Assert.Throws<SwapDomainException>(() => SwapMachine.FeePaid(swap, "user-9")).Kind);
    }

    [Theory]
    [InlineData(SwapStatus.Locking)]
    [InlineData(SwapStatus.Cancelled)]
    [InlineData(SwapStatus.Rejected)]
    public void FeePaid_InWrongStatus_IsRejected(SwapStatus status)
    {
        var swap = NewSwap(DeliveryMethod.Locker, DeliveryMethod.Locker) with { Status = status };

        Assert.Throws<SwapDomainException>(() => SwapMachine.FeePaid(swap, "user-1"));
    }

    [Fact]
    public void PaymentTimedOut_BeforeDeadline_IsNoOp()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);

        Assert.False(SwapMachine.PaymentTimedOut(swap, _now + _window - TimeSpan.FromSeconds(1)).Changed);
    }

    [Fact]
    public void PaymentTimedOut_BlamesFirstUnpaidLockerLeg()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);
        var aPaid = SwapMachine.FeePaid(swap, "user-1").Swap;

        var t = SwapMachine.PaymentTimedOut(aPaid, _now + _window);

        Assert.Equal(SwapStatus.Cancelled, t.Swap.Status);
        var e = Assert.IsType<SwapCancelledEvent>(Assert.Single(t.Events));
        Assert.Equal("user-2", e.DefaultingUserId);
        Assert.Equal("locker fee not paid in time", e.Reason);
    }

    [Fact]
    public void PaymentTimedOut_NeitherPaid_BlamesLegA()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.Locker);

        var e = Assert.IsType<SwapCancelledEvent>(Assert.Single(SwapMachine.PaymentTimedOut(swap, _now + _window).Events));

        Assert.Equal("user-1", e.DefaultingUserId);
    }

    [Fact]
    public void Race_PaymentThenTimeout_PaymentWins()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.InPerson);
        var paid = SwapMachine.FeePaid(swap, "user-1").Swap;

        Assert.False(SwapMachine.PaymentTimedOut(paid, _now + _window).Changed);
        Assert.Equal(SwapStatus.Completed, paid.Status);
    }

    [Fact]
    public void Race_TimeoutThenPayment_TimeoutWins()
    {
        var swap = Awaiting(DeliveryMethod.Locker, DeliveryMethod.InPerson);
        var cancelled = SwapMachine.PaymentTimedOut(swap, _now + _window).Swap;

        Assert.Throws<SwapDomainException>(() => SwapMachine.FeePaid(cancelled, "user-1"));
        Assert.Equal(SwapStatus.Cancelled, cancelled.Status);
    }
}
