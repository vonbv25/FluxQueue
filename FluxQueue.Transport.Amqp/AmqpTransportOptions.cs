namespace FluxQueue.Transport.Amqp;

public sealed class AmqpTransportOptions
{
    public int Port { get; set; } = 5673;
    public string DefaultAddress { get; set; } = "127.0.0.1";
    public int DefaultVisibilityTimeoutSeconds { get; set; } = 60;
    public int DefaultWaitSeconds { get; set; } = 1;
    public int MaxBatch { get; set; } = 50;
}
