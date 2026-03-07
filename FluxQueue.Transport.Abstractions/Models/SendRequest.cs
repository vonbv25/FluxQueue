namespace FluxQueue.Transport.Abstractions.Models;

public sealed record SendRequest(
    string Queue,
    byte[] Payload,
    int? DelaySeconds = null,
    int? MaxReceiveCount = null,
    IReadOnlyDictionary<string, string>? Headers = null
);