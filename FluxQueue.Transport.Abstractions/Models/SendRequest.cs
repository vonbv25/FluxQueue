namespace FluxQueue.Transport.Abstractions.Models;

public sealed record SendRequest(
    string Queue,
    byte[] Payload,
    int DelaySeconds = 0,
    int MaxReceiveCount = 5,
    IReadOnlyDictionary<string, object?>? Headers = null
);
