namespace FluxQueue.Transport.Http;

public sealed record SendDto(
    string? PayloadBase64,
    int? DelaySeconds,
    int? MaxReceiveCount);

public sealed record ReceiveDto(
    int? MaxMessages,
    int? VisibilityTimeoutSeconds,
    int? WaitSeconds);

public sealed record ErrorResponseDto(string Error);

public sealed record SendResponseDto(string MessageId);

public sealed record ReceivedMessageDto(
    string MessageId,
    string PayloadBase64,
    string ReceiptHandle,
    int ReceiveCount);

public sealed record ReceiveResponseDto(
    IReadOnlyList<ReceivedMessageDto> Messages);

public sealed record ReceiptActionResponseDto(
    bool Success,
    string Action,
    string Queue,
    string ReceiptHandle);
