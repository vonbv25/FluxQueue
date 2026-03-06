using FluxQueue.BrokerHost.Configuration;
using FluxQueue.Transport.Abstractions.Models;
using Microsoft.Extensions.Options;

namespace FluxQueue.BrokerHost.Services;
public interface IQueueRequestValidator
{
    void Validate(SendRequest req);
    void Validate(ReceiveRequest req);
    void ValidateAck(string queue, string receiptHandle);
}
public sealed class QueueRequestValidator(IOptions<FluxQueueOptions> options) : IQueueRequestValidator
{
    private readonly FluxQueueDefaultsOptions _defaults = options.Value.Defaults;

    public void Validate(SendRequest req)
    {
        if (!IsValidQueueName(req.Queue))
            throw new ArgumentException("Invalid queue name.", nameof(req.Queue));

        if (req.Payload is null)
            throw new ArgumentNullException(nameof(req.Payload));

        if (req.Payload.Length > _defaults.MaxMessageSizeBytes)
            throw new ArgumentOutOfRangeException(
                nameof(req.Payload),
                $"Payload exceeds max allowed size of {_defaults.MaxMessageSizeBytes} bytes.");

        if (req.DelaySeconds is null)
            throw new ArgumentNullException(nameof(req.DelaySeconds));

        if (req.DelaySeconds.Value < 0)
            throw new ArgumentOutOfRangeException(
                nameof(req.DelaySeconds),
                "DelaySeconds must be greater than or equal to 0.");

        if (req.MaxReceiveCount is null)
            throw new ArgumentNullException(nameof(req.MaxReceiveCount));

        if (req.MaxReceiveCount.Value <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(req.MaxReceiveCount),
                "MaxReceiveCount must be greater than 0.");
    }

    public void Validate(ReceiveRequest req)
    {
        if (!IsValidQueueName(req.Queue))
            throw new ArgumentException("Invalid queue name.", nameof(req.Queue));

        if (req.MaxMessages is null)
            throw new ArgumentNullException(nameof(req.MaxMessages));

        if (req.MaxMessages.Value <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(req.MaxMessages),
                "MaxMessages must be greater than 0.");

        if (req.MaxMessages.Value > _defaults.MaxBatchReceiveMessages)
            throw new ArgumentOutOfRangeException(
                nameof(req.MaxMessages),
                $"MaxMessages exceeds allowed limit of {_defaults.MaxBatchReceiveMessages}.");

        if (req.VisibilityTimeoutSeconds is null)
            throw new ArgumentNullException(nameof(req.VisibilityTimeoutSeconds));

        if (req.VisibilityTimeoutSeconds.Value <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(req.VisibilityTimeoutSeconds),
                "VisibilityTimeoutSeconds must be greater than 0.");

        if (req.VisibilityTimeoutSeconds.Value > _defaults.MaxVisibilityTimeoutSeconds)
            throw new ArgumentOutOfRangeException(
                nameof(req.VisibilityTimeoutSeconds),
                $"VisibilityTimeoutSeconds exceeds allowed limit of {_defaults.MaxVisibilityTimeoutSeconds}.");

        if (req.WaitSeconds is null)
            throw new ArgumentNullException(nameof(req.WaitSeconds));

        if (req.WaitSeconds.Value < 0)
            throw new ArgumentOutOfRangeException(
                nameof(req.WaitSeconds),
                "WaitSeconds must be greater than or equal to 0.");

        if (req.WaitSeconds.Value > _defaults.MaxWaitSeconds)
            throw new ArgumentOutOfRangeException(
                nameof(req.WaitSeconds),
                $"WaitSeconds exceeds allowed limit of {_defaults.MaxWaitSeconds}.");
    }

    public void ValidateAck(string queue, string receiptHandle)
    {
        if (!IsValidQueueName(queue))
            throw new ArgumentException("Invalid queue name.", nameof(queue));

        if (string.IsNullOrWhiteSpace(receiptHandle))
            throw new ArgumentException("Receipt handle is required.", nameof(receiptHandle));
    }

    private static bool IsValidQueueName(string queue) =>
        !string.IsNullOrWhiteSpace(queue) &&
        queue.Length <= 200 &&
        queue.All(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.');
}