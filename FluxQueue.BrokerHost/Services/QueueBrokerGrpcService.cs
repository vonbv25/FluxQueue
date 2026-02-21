using FluxQueue.Core;
using Grpc.Core;
using Google.Protobuf;
using FluxQueue.Broker; // this should match option csharp_namespace in your proto

namespace FluxQueue.BrokerHost.Services;

public sealed class QueueBrokerGrpcService : QueueBroker.QueueBrokerBase
{
    private readonly QueueEngine _engine;

    public QueueBrokerGrpcService(QueueEngine engine) => _engine = engine;

    public override async Task<SendResponse> Send(SendRequest request, ServerCallContext context)
    {
        var id = await _engine.SendAsync(
            queue: request.Queue,
            payload: request.Payload.ToByteArray(),
            delaySeconds: request.DelaySeconds,
            maxReceiveCount: request.MaxReceiveCount,
            ct: context.CancellationToken);

        return new SendResponse { MessageId = id };
    }

    public override async Task<ReceiveResponse> Receive(ReceiveRequest request, ServerCallContext context)
    {
        var msgs = await _engine.ReceiveAsync(
            queue: request.Queue,
            maxMessages: request.MaxMessages,
            visibilityTimeoutSeconds: request.VisibilityTimeoutSeconds,
            waitSeconds: request.WaitSeconds,
            ct: context.CancellationToken);

        var resp = new ReceiveResponse();
        foreach (var m in msgs)
        {
            resp.Messages.Add(new ReceivedMessage
            {
                MessageId = m.MessageId,
                Payload = ByteString.CopyFrom(m.Payload),
                ReceiptHandle = m.ReceiptHandle,
                ReceiveCount = m.ReceiveCount
            });
        }
        return resp;
    }

    public override async Task<AckResponse> Ack(AckRequest request, ServerCallContext context)
    {
        var ok = await _engine.AckAsync(
            queue: request.Queue,
            receiptHandle: request.ReceiptHandle,
            ct: context.CancellationToken);

        return new AckResponse { Acknowledged = ok };
    }
}
