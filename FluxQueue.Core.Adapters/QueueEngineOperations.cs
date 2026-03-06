using FluxQueue.Core;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;

namespace FluxQueue.Core.Adapters;

//public sealed class QueueEngineOperations : IQueueOperations
//{
//    private readonly QueueEngine _engine;

//    public QueueEngineOperations(QueueEngine engine) => _engine = engine;

//    public Task<string> SendAsync(SendRequest req, CancellationToken ct)
//        => _engine.SendAsync(req.Queue, req.Payload, req.DelaySeconds, req.MaxReceiveCount);

//    public async Task<IReadOnlyList<TransportMessage>> ReceiveAsync(ReceiveRequest req, CancellationToken ct)
//    {
//        var msgs = await _engine.ReceiveAsync(req.Queue, req.MaxMessages, req.VisibilityTimeoutSeconds, req.WaitSeconds);
//        return msgs.Select(m => new TransportMessage(
//            Queue: req.Queue,
//            MessageId: m.MessageId,
//            Payload: m.Payload,
//            ReceiptHandle: m.ReceiptHandle,
//            ReceiveCount: m.ReceiveCount
//        )).ToArray();
//    }

//    public Task<bool> AckAsync(string queue, string receiptHandle, CancellationToken ct)
//        => _engine.AckAsync(queue, receiptHandle);

//    public Task<bool> RejectAsync(string queue, string receiptHandle, CancellationToken ct)
//        => Task.FromResult(false); // implement later if you add DLQ “reject now”
//}