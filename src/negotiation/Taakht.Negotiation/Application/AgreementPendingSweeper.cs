using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Taakht.Negotiation.Application;

/// <summary>Periodically runs the AGREEMENT_PENDING recovery (republish AgreementReached, then give up).</summary>
public sealed class AgreementPendingSweeper(NegotiationService service, NegotiationOptions options, ILogger<AgreementPendingSweeper> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.SweepInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await service.SweepAgreementPendingAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "AGREEMENT_PENDING sweep failed; will retry");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }
}
