using System.ComponentModel.DataAnnotations;

namespace FluxQueue.BrokerHost.Configuration;

public sealed class QueueReconcilerOptions
{
    public bool Enabled { get; set; } = true;

    [Range(1, 86400)]
    public int IntervalSeconds { get; set; } = 60;

    public bool WipeAndRebuildIndexes { get; set; } = false;

    [Range(1, 10_000_000)]
    public int MaxMessages { get; set; } = 5_000_000;

    [Range(1, 1_000_000)]
    public int SweepBatchSize { get; set; } = 20_000;
}
