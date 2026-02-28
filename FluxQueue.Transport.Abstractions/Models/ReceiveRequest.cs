namespace FluxQueue.Transport.Abstractions.Models;

public sealed record ReceiveRequest(
    string Queue,
    int MaxMessages = 1,
    int VisibilityTimeoutSeconds = 60,
    int WaitSeconds = 0
);