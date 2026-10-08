namespace Taakht.Swap.Domain;

/// <summary>Pure state machine for a swap. No I/O; every transition is a replay-safe function of the current state.</summary>
public static class SwapMachine
{
    public const string TimeoutReason = "locker fee not paid in time";

    /// <summary>The exclusive lock on both ads was taken. Locking -> AwaitingPayment, or straight to Completed when nobody owes a fee.</summary>
    public static Transition LockAcquired(SwapModel swap, DateTimeOffset now, TimeSpan paymentWindow)
    {
        if (swap.Status != SwapStatus.Locking)
        {
            return Transition.NoChange(swap);
        }

        if (!swap.LegA.NeedsFee && !swap.LegB.NeedsFee)
        {
            return new Transition(
                swap with { Status = SwapStatus.Completed },
                [new ExclusiveLockAcquiredEvent(), new SwapCompletedEvent()],
                true);
        }

        return new Transition(
            swap with { Status = SwapStatus.AwaitingPayment, PaymentDeadline = now + paymentWindow },
            [new ExclusiveLockAcquiredEvent()],
            true);
    }

    /// <summary>The Ad service refused the lock. Locking -> Rejected.</summary>
    public static Transition LockRejected(SwapModel swap, string reason)
    {
        if (swap.Status != SwapStatus.Locking)
        {
            return Transition.NoChange(swap);
        }

        return new Transition(
            swap with { Status = SwapStatus.Rejected, CancelReason = reason },
            [new SwapRejectedEvent(reason)],
            true);
    }

    /// <summary>
    /// A locker fee was paid by <paramref name="userId"/>. Idempotent for an already paid leg.
    /// Completes the swap when no locker leg owes a fee any more.
    /// </summary>
    public static Transition FeePaid(SwapModel swap, string userId)
    {
        var isA = swap.LegA.NeedsFee && swap.LegA.OwnerUserId == userId;
        var isB = swap.LegB.NeedsFee && swap.LegB.OwnerUserId == userId;
        if (!isA && !isB)
        {
            throw new SwapDomainException(DomainErrorKind.FailedPrecondition, "user has no locker leg in this swap");
        }

        if (swap.Status == SwapStatus.Completed)
        {
            return Transition.NoChange(swap);
        }

        if (swap.Status != SwapStatus.AwaitingPayment)
        {
            throw new SwapDomainException(DomainErrorKind.FailedPrecondition, $"swap is {swap.Status}, not awaiting payment");
        }

        var updated = swap with
        {
            LegA = isA ? swap.LegA with { FeePaid = true } : swap.LegA,
            LegB = isB ? swap.LegB with { FeePaid = true } : swap.LegB,
        };
        if (updated == swap)
        {
            return Transition.NoChange(swap);
        }

        if (updated.LegA.OwesFee || updated.LegB.OwesFee)
        {
            return new Transition(updated, [], true);
        }

        return new Transition(updated with { Status = SwapStatus.Completed }, [new SwapCompletedEvent()], true);
    }

    public static bool IsOverdue(SwapModel swap, DateTimeOffset now) =>
        swap.Status == SwapStatus.AwaitingPayment && swap.PaymentDeadline is { } deadline && now >= deadline;

    /// <summary>The payment deadline passed with fees still unpaid. AwaitingPayment -> Cancelled, blaming the first unpaid locker leg.</summary>
    public static Transition PaymentTimedOut(SwapModel swap, DateTimeOffset now)
    {
        if (!IsOverdue(swap, now))
        {
            return Transition.NoChange(swap);
        }

        var defaulting = swap.LegA.OwesFee ? swap.LegA.OwnerUserId
            : swap.LegB.OwesFee ? swap.LegB.OwnerUserId
            : "";
        if (defaulting.Length == 0)
        {
            return Transition.NoChange(swap);
        }

        return new Transition(
            swap with { Status = SwapStatus.Cancelled, CancelReason = TimeoutReason },
            [new SwapCancelledEvent(TimeoutReason, defaulting)],
            true);
    }
}
