using System.ComponentModel.DataAnnotations;

namespace FluxQueue.BrokerHost.Configuration;

public sealed class FluxQueueDefaultsOptions
{
    [Range(1, 100 * 1024 * 1024)]
    public int MaxMessageSizeBytes { get; set; } = 262_144;

    [Range(1, 1_000_000)]
    public int MaxInflightPerQueue { get; set; } = 1_000;

    [Range(1, 3600)]
    public int DefaultVisibilityTimeoutSeconds { get; set; } = 30;

    [Range(1, 3600)]
    public int MaxVisibilityTimeoutSeconds { get; set; } = 300;

    [Range(0, 3600)]
    public int DefaultWaitSeconds { get; set; } = 1;

    [Range(0, 3600)]
    public int MaxWaitSeconds { get; set; } = 30;

    [Range(1, 1_000)]
    public int MaxReceiveCount { get; set; } = 5;

    [Range(1, 1_000)]
    public int MaxBatchReceiveMessages { get; set; } = 50;
}