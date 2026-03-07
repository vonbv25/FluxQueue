using System.ComponentModel.DataAnnotations;

namespace FluxQueue.BrokerHost.Configuration;

public sealed class QueueSweeperOptions
{
    public bool Enabled { get; set; } = true;

    [Range(1, 100_000)]
    public int MaxExpiredMessagesPerQueuePerTick { get; set; } = 2_000;

    [Range(1, 10_000)]
    public int MaxQueuesPerTick { get; set; } = 50;

    [Range(1, 60_000)]
    public int IdleDelayMs { get; set; } = 250;

    [Range(1, 60_000)]
    public int BusyDelayMs { get; set; } = 10;

    [Range(1, 300)]
    public int ErrorBackoffSeconds { get; set; } = 5;
}