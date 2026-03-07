namespace FluxQueue.Transport.Abstractions;

public interface IQueueTransport
{
    string Name { get; }
    Task StartAsync(CancellationToken ct);
    Task StopAsync(CancellationToken ct);
}
