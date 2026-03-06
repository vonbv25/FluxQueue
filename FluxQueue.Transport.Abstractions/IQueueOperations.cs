using FluxQueue.Transport.Abstractions.Models;

namespace FluxQueue.Transport.Abstractions;

public interface IQueueOperations
{
    Task<string> SendAsync(SendRequest req, CancellationToken ct);
    Task<IReadOnlyList<TransportMessage>> ReceiveAsync(ReceiveRequest req, CancellationToken ct);
    Task<bool> AckAsync(string queue, string receiptHandle, CancellationToken ct);
    Task<bool> RejectAsync(string queue, string receiptHandle, CancellationToken ct);
}