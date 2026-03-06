using System.ComponentModel.DataAnnotations;

namespace FluxQueue.BrokerHost.Configuration;

public sealed class QueueSweeperOptions
{
    public bool Enabled { get; set; } = true;
    [Range(1, 2000)]
    public int BatchSize { get; set; } = 2000;
    [Range(1, 50)]
    public int MaxQueuesPerTick { get; set; } = 50;
    [Range(1, 300)]
    public int IdleDelayMs { get; set; } = 250;
    [Range(1, 20)]
    public int BusyDelayMs { get; set; } = 10;
}
