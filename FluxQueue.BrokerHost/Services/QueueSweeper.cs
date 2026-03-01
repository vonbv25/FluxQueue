using FluxQueue.Core;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Services;

public sealed class QueueSweeper : BackgroundService
{
    private readonly QueueEngine _engine;
    private readonly QueueSweeperOptions _opt;
    private readonly ILogger<QueueSweeper> _log;

    // Simple per-queue backoff when sweep errors occur (avoid log spam)
    private readonly Dictionary<string, DateTimeOffset> _skipUntil = [];

    public QueueSweeper(
        QueueEngine engine,
        IOptions<QueueSweeperOptions> options,
        ILogger<QueueSweeper> log)
    {
        _engine = engine;
        _opt = options.Value;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("QueueSweeper started. SweepInterval={SweepInterval} IdleInterval={IdleInterval} MaxToProcessPerQueue={Max}",
            _opt.SweepInterval, _opt.IdleInterval, _opt.MaxToProcessPerQueue);

        while (!stoppingToken.IsCancellationRequested)
        {
            var queues = _engine.KnownQueues;

            if (queues.Count == 0)
            {
                await Task.Delay(_opt.IdleInterval, stoppingToken);
                continue;
            }

            var now = DateTimeOffset.UtcNow;
            int totalProcessed = 0;

            foreach (var q in queues)
            {
                stoppingToken.ThrowIfCancellationRequested();

                // If a queue has been erroring, skip it briefly
                if (_skipUntil.TryGetValue(q, out var until) && until > now)
                    continue;

                try
                {
                    var processed = await _engine.SweepExpiredAsync(
                        queue: q,
                        maxToProcess: _opt.MaxToProcessPerQueue,
                        ct: stoppingToken);

                    totalProcessed += processed;

                    // Success: clear backoff
                    _skipUntil.Remove(q);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Back off this queue for a bit to avoid tight failure loops
                    var backoff = TimeSpan.FromSeconds(5);
                    _skipUntil[q] = now.Add(backoff);

                    _log.LogWarning(ex, "Sweep failed for queue {Queue}. Backing off for {BackoffSeconds}s", q, backoff.TotalSeconds);
                }
            }

            // If we did actual work, loop soon (keeps redelivery snappy).
            // If we did no work, pause (reduces CPU churn).
            var delay = totalProcessed > 0 ? _opt.SweepInterval : _opt.IdleInterval;
            await Task.Delay(delay, stoppingToken);
        }
    }
}

public sealed class QueueSweeperOptions
{
    /// <summary>How often to run a full sweep cycle.</summary>
    public TimeSpan SweepInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>When there are no known queues, wait this long before checking again.</summary>
    public TimeSpan IdleInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Max inflight entries to process per queue per sweep cycle.</summary>
    public int MaxToProcessPerQueue { get; set; } = 1000;
}