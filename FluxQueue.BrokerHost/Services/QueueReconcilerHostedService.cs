using FluxQueue.BrokerHost.Configuration;
using FluxQueue.Core;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Services;
public sealed class QueueReconcilerHostedService : IHostedService
{
    private readonly QueueEngine _engine;
    private readonly IOptions<QueueReconcilerOptions> _opt;
    private readonly ILogger<QueueReconcilerHostedService> _log;

    public QueueReconcilerHostedService(
        QueueEngine engine,
        IOptions<QueueReconcilerOptions> opt,
        ILogger<QueueReconcilerHostedService> log)
    {
        _engine = engine;
        _opt = opt;
        _log = log;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        if (!_opt.Value.Enabled)
        {
            _log.LogInformation("FluxQueue reconciliation is disabled.");
            return;
        }

        _log.LogWarning(
            "FluxQueue reconciliation starting. WipeAndRebuildIndexes={WipeAndRebuildIndexes}, MaxMessages={MaxMessages}, SweepBatchSize={SweepBatchSize}",
            _opt.Value.WipeAndRebuildIndexes,
            _opt.Value.MaxMessages,
            _opt.Value.SweepBatchSize);

        var sw = System.Diagnostics.Stopwatch.StartNew();

        await _engine.ReconcileAsync(
            new ReconcileOptions
            {
                WipeAndRebuildIndexes = _opt.Value.WipeAndRebuildIndexes,
                MaxMessages = _opt.Value.MaxMessages,
                SweepBatchSize = _opt.Value.SweepBatchSize
            },
            ct);

        _log.LogWarning(
            "FluxQueue reconciliation finished in {ElapsedMs}ms",
            sw.ElapsedMilliseconds);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
