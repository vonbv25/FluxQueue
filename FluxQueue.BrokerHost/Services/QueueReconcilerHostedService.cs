using FluxQueue.Core;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Services;
public sealed class QueueReconcilerOptions
{
    public bool Enabled { get; set; } = true;

    // safest “option 2” that you chose: wipe old index CFs and rebuild from CF_MSG
    public bool WipeAndRebuildIndexes { get; set; } = true;

    public int MaxMessagesPerRun { get; set; } = 5_000_000; // safety cap
    public int SweepBatchSize { get; set; } = 20_000;       // when expiring inflight during reconcile
}
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
        if (!_opt.Value.Enabled) return;

        _log.LogWarning("FluxQueue reconciliation starting...");
        var sw = System.Diagnostics.Stopwatch.StartNew();

        await _engine.ReconcileAsync(new ReconcileOptions
        {
            WipeAndRebuildIndexes = _opt.Value.WipeAndRebuildIndexes,
            MaxMessages = _opt.Value.MaxMessagesPerRun,
            SweepBatchSize = _opt.Value.SweepBatchSize
        }, ct);

        _log.LogWarning("FluxQueue reconciliation finished in {ElapsedMs}ms", sw.ElapsedMilliseconds);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
