using System.ComponentModel.DataAnnotations;

namespace FluxQueue.BrokerHost.Configuration;

public sealed class FluxQueueAmqpOptions
{
    [Range(1, 65535)]
    public int Port { get; set; } = 5672;

    [Range(1, 3600)]
    public int DefaultVisibilityTimeoutSeconds { get; set; } = 30;

    [Range(0, 3600)]
    public int DefaultWaitSeconds { get; set; } = 1;

    [Range(1, 1000)]
    public int MaxBatch { get; set; } = 50;
}
