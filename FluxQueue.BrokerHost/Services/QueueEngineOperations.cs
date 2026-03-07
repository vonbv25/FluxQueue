using FluxQueue.Core;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;

namespace FluxQueue.BrokerHost.Services;

public sealed class QueueEngineOperations : IQueueOperations
{
    private readonly QueueEngine _engine;
    private readonly IQueueRequestValidator _validator;
    private readonly IQueuePolicyProvider _policyProvider;

    public QueueEngineOperations(
        QueueEngine engine,
        IQueueRequestValidator validator,
        IQueuePolicyProvider policyProvider)
    {
        _engine = engine;
        _validator = validator;
        _policyProvider = policyProvider;
    }

    public async Task<string> SendAsync(SendRequest req, CancellationToken ct)
    {
        var defaults = _policyProvider.GetDefaults();

        var normalized = req with
        {
            DelaySeconds = req.DelaySeconds ?? 0,
            MaxReceiveCount = req.MaxReceiveCount ?? defaults.MaxReceiveCount
        };

        _validator.Validate(normalized);

        return await _engine.SendAsync(
            normalized.Queue,
            normalized.Payload,
            normalized.DelaySeconds!.Value,
            normalized.MaxReceiveCount!.Value,
            ct);
    }

    public async Task<IReadOnlyList<TransportMessage>> ReceiveAsync(ReceiveRequest req, CancellationToken ct)
    {
        var defaults = _policyProvider.GetDefaults();

        var normalized = req with
        {
            MaxMessages = req.MaxMessages ?? 1,
            VisibilityTimeoutSeconds = req.VisibilityTimeoutSeconds ?? defaults.DefaultVisibilityTimeoutSeconds,
            WaitSeconds = req.WaitSeconds ?? defaults.DefaultWaitSeconds
        };

        _validator.Validate(normalized);

        var msgs = await _engine.ReceiveAsync(
            normalized.Queue,
            normalized.MaxMessages!.Value,
            normalized.VisibilityTimeoutSeconds!.Value,
            normalized.WaitSeconds!.Value,
            ct);

        return msgs.Select(m => new TransportMessage(
            Queue: normalized.Queue,
            MessageId: m.MessageId,
            Payload: m.Payload,
            ReceiptHandle: m.ReceiptHandle,
            ReceiveCount: m.ReceiveCount
        )).ToArray();
    }

    public async Task<bool> AckAsync(string queue, string receiptHandle, CancellationToken ct)
    {
        _validator.ValidateAck(queue, receiptHandle);
        return await _engine.AckAsync(queue, receiptHandle, ct);
    }

    public async Task<bool> RejectAsync(string queue, string receiptHandle, CancellationToken ct)
    {
        _validator.ValidateAck(queue, receiptHandle);
        return await _engine.RejectAsync(queue, receiptHandle, ct);
    }
}