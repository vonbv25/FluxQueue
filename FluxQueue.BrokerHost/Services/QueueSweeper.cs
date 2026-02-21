using FluxQueue.Core;

namespace FluxQueue.BrokerHost.Services;

public sealed class QueueSweeper : BackgroundService
{
    private readonly QueueEngine _engine;

    // MVP: static list. Later: store queues in RocksDB or config.
    private readonly string[] _queues = new[] { "test", "retry", "payments" };

    public QueueSweeper(QueueEngine engine) => _engine = engine;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var q in _queues)
            {
                try { await _engine.SweepExpiredAsync(q, ct: stoppingToken); }
                catch { /* TODO log */ }
            }

            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}