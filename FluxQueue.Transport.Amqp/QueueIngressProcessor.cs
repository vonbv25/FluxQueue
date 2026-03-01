using Amqp.Framing;
using Amqp.Listener;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;

namespace FluxQueue.Transport.Amqp;

internal sealed class QueueIngressProcessor : IMessageProcessor
{
    private readonly IQueueOperations _ops;
    private readonly string _queue;

    public QueueIngressProcessor(IQueueOperations ops, string queue)
    {
        _ops = ops;
        _queue = queue;
    }

    // Controls how much credit we issue for incoming transfers.
    public int Credit => 200;

    public void Process(MessageContext ctx)
    {
        try
        {
            var payload = AmqpMessageBody.ExtractPayload(ctx.Message);

            // Optional: read app props for delay/maxReceiveCount (keep minimal for now)
            // You can extend later:
            // var ap = ctx.Message.ApplicationProperties?.Map;

            _ops.SendAsync(new SendRequest(
                Queue: _queue,
                Payload: payload,
                DelaySeconds: 0,
                MaxReceiveCount: 5
            ), CancellationToken.None).GetAwaiter().GetResult();

            // Accepted
            ctx.Complete();
        }
        catch (Exception ex)
        {
            ctx.Complete(new Error
            {
                Condition = "amqp:internal-error",
                Description = ex.Message
            });
        }
    }
}