namespace FluxQueue.Transport.Abstractions.Models;

public sealed record ReceiveRequest(
    string Queue,
    int? MaxMessages = null,
    int? VisibilityTimeoutSeconds = null,
    int? WaitSeconds = null
);