using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Taakht.Swap.Application;

/// <summary>Periodically cancels swaps whose locker fees were not paid before the deadline.</summary>
public sealed class PaymentDeadlineSweeper(SwapWorkflow workflow, ILogger<PaymentDeadlineSweeper> logger) : BackgroundService
{
    public TimeSpan Interval { get; init; } = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval);
        do
        {
            try
            {
                await workflow.SweepOverdueAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Payment deadline sweep failed; will retry");
            }
        }
        while (await WaitAsync(timer, stoppingToken));
    }

    private static async Task<bool> WaitAsync(PeriodicTimer timer, CancellationToken ct)
    {
        try
        {
            return await timer.WaitForNextTickAsync(ct);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
