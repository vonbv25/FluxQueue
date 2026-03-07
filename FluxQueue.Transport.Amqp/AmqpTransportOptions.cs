namespace FluxQueue.Transport.Amqp;

public sealed class AmqpTransportOptions
{
    public int Port { get; set; } = 5673;
    public string DefaultAddress { get; set; } = "127.0.0.1";
    public int MaxBatch { get; set; } = 50;
    public int DefaultVisibilityTimeoutSeconds { get; set; } = 30;
    public int DefaultWaitSeconds { get; set; } = 5;

    // AMQP link-credit settings
    public int IngressInitialCredit { get; set; } = 200;
    public int EgressInitialCredit { get; set; } = 50;
}
