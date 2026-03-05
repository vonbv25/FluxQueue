using FluxQueue.Core;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Services;
public sealed class QueueSweeper : BackgroundService
{
    private readonly QueueEngine _engine;
    private readonly IOptions<QueueSweeperOptions> _opt;
    private readonly ILogger<QueueSweeper> _log;

    public QueueSweeper(QueueEngine engine, IOptions<QueueSweeperOptions> opt, ILogger<QueueSweeper> log)
    {
        _engine = engine;
        _opt = opt;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_opt.Value.Enabled) return;

        while (!stoppingToken.IsCancellationRequested)
        {
            var queues = _engine.KnownQueues;
            if (queues.Count == 0)
            {
                await Task.Delay(_opt.Value.IdleDelayMs, stoppingToken);
                continue;
            }

            int anyProcessed = 0;
            int swept = 0;

            foreach (var q in queues)
            {
                if (swept++ >= _opt.Value.MaxQueuesPerTick) break;

                try
                {
                    var p = await _engine.SweepExpiredAsync(q, _opt.Value.BatchSize, stoppingToken);
                    anyProcessed += p;
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Sweeper failed for queue {Queue}", q);
                }
            }

            await Task.Delay(anyProcessed > 0 ? _opt.Value.BusyDelayMs : _opt.Value.IdleDelayMs, stoppingToken);
        }
    }
}

public sealed class QueueSweeperOptions
{
    public bool Enabled { get; set; } = true;
    public int BatchSize { get; set; } = 2000;
    public int MaxQueuesPerTick { get; set; } = 50;
    public int IdleDelayMs { get; set; } = 250;
    public int BusyDelayMs { get; set; } = 10;
}