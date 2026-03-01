using FluxQueue.Core;
using Grpc.Core;
using Google.Protobuf;
using FluxQueue.Broker;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;

namespace FluxQueue.BrokerHost.Services;
public sealed class QueueBrokerGrpcService : QueueBroker.QueueBrokerBase
{
    private readonly IQueueOperations _ops;

    public QueueBrokerGrpcService(IQueueOperations ops) => _ops = ops;

    public override async Task<SendGrpcResponse> Send(SendGrpcRequest request, ServerCallContext context)
    {
        var id = await _ops.SendAsync(new SendRequest(
            Queue: request.Queue,
            Payload: request.Payload.ToByteArray(),
            DelaySeconds: request.DelaySeconds,
            MaxReceiveCount: request.MaxReceiveCount
        ), context.CancellationToken);

        return new SendGrpcResponse { MessageId = id };
    }

    public override async Task<ReceiveGrpcResponse> Receive(ReceiveGrpcRequest request, ServerCallContext context)
    {
        var msgs = await _ops.ReceiveAsync(new ReceiveRequest(
            Queue: request.Queue,
            MaxMessages: request.MaxMessages,
            VisibilityTimeoutSeconds: request.VisibilityTimeoutSeconds,
            WaitSeconds: request.WaitSeconds
        ), context.CancellationToken);

        var resp = new ReceiveGrpcResponse();
        foreach (var m in msgs)
        {
            resp.Messages.Add(new ReceivedGrpcMessage
            {
                MessageId = m.MessageId,
                Payload = ByteString.CopyFrom(m.Payload),
                ReceiptHandle = m.ReceiptHandle,
                ReceiveCount = m.ReceiveCount
            });
        }

        return resp;
    }

    public override async Task<AckGrpcResponse> Ack(AckGrpcRequest request, ServerCallContext context)
    {
        var ok = await _ops.AckAsync(
            queue: request.Queue,
            receiptHandle: request.ReceiptHandle,
            ct: context.CancellationToken);

        return new AckGrpcResponse { Acknowledged = ok };
    }
}
