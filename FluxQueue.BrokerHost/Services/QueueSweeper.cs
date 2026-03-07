using FluxQueue.BrokerHost.Configuration;
using FluxQueue.Core;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Services;

public sealed class QueueSweeper : BackgroundService
{
    private readonly QueueEngine _engine;
    private readonly QueueSweeperOptions _opt;
    private readonly ILogger<QueueSweeper> _log;

    private readonly Dictionary<string, DateTimeOffset> _skipUntil = [];
    private int _roundRobinCursor = 0;

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
        if (!_opt.Enabled)
        {
            _log.LogInformation("QueueSweeper is disabled.");
            return;
        }

        _log.LogInformation(
            "QueueSweeper started. MaxQueuesPerTick={MaxQueuesPerTick}, MaxExpiredMessagesPerQueuePerTick={MaxExpiredPerQueue}, BusyDelayMs={BusyDelayMs}, IdleDelayMs={IdleDelayMs}, ErrorBackoffSeconds={ErrorBackoffSeconds}",
            _opt.MaxQueuesPerTick,
            _opt.MaxExpiredMessagesPerQueuePerTick,
            _opt.BusyDelayMs,
            _opt.IdleDelayMs,
            _opt.ErrorBackoffSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            var queues = _engine.KnownQueues.ToArray();

            CleanupSkipEntries(queues);

            if (queues.Length == 0)
            {
                await Task.Delay(_opt.IdleDelayMs, stoppingToken);
                continue;
            }

            var now = DateTimeOffset.UtcNow;
            var selectedQueues = SelectQueuesForThisTick(queues);

            int totalProcessed = 0;
            int visitedQueues = 0;

            foreach (var q in selectedQueues)
            {
                stoppingToken.ThrowIfCancellationRequested();

                if (_skipUntil.TryGetValue(q, out var until) && until > now)
                    continue;

                visitedQueues++;

                try
                {
                    var processed = await _engine.SweepExpiredAsync(
                        queue: q,
                        maxToProcess: _opt.MaxExpiredMessagesPerQueuePerTick,
                        ct: stoppingToken);

                    totalProcessed += processed;
                    _skipUntil.Remove(q);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    var backoff = TimeSpan.FromSeconds(_opt.ErrorBackoffSeconds);
                    _skipUntil[q] = now.Add(backoff);

                    _log.LogWarning(
                        ex,
                        "Sweep failed for queue {Queue}. Backing off for {BackoffSeconds}s",
                        q,
                        backoff.TotalSeconds);
                }
            }

            _roundRobinCursor = (_roundRobinCursor + visitedQueues) % Math.Max(queues.Length, 1);

            var delay = totalProcessed > 0 ? _opt.BusyDelayMs : _opt.IdleDelayMs;
            await Task.Delay(delay, stoppingToken);
        }
    }

    private IReadOnlyList<string> SelectQueuesForThisTick(string[] queues)
    {
        if (queues.Length <= _opt.MaxQueuesPerTick)
            return queues;

        var selected = new List<string>(_opt.MaxQueuesPerTick);

        for (int i = 0; i < _opt.MaxQueuesPerTick; i++)
        {
            var index = (_roundRobinCursor + i) % queues.Length;
            selected.Add(queues[index]);
        }

        return selected;
    }

    private void CleanupSkipEntries(string[] activeQueues)
    {
        if (_skipUntil.Count == 0)
            return;

        var active = new HashSet<string>(activeQueues, StringComparer.Ordinal);
        var expired = new List<string>();

        foreach (var kvp in _skipUntil)
        {
            if (!active.Contains(kvp.Key) || kvp.Value <= DateTimeOffset.UtcNow)
                expired.Add(kvp.Key);
        }

        foreach (var key in expired)
            _skipUntil.Remove(key);
    }
}