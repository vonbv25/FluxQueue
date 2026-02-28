using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;

namespace FluxQueue.Transport.Amqp;

internal sealed class QueueEgressSource : IMessageSource
{
    private readonly IQueueOperations _ops;
    private readonly string _queue;
    private readonly AmqpTransportOptions _opt;

    public QueueEgressSource(IQueueOperations ops, string queue, AmqpTransportOptions opt)
    {
        _ops = ops;
        _queue = queue;
        _opt = opt;
    }

    public async Task<ReceiveContext?> GetMessageAsync(ListenerLink link)
    {
        try
        {
            // Long-poll a bit (uses your QueueEngine waitSeconds semantics via adapter)
            var msgs = await _ops.ReceiveAsync(new ReceiveRequest(
                Queue: _queue,
                MaxMessages: 1,
                VisibilityTimeoutSeconds: _opt.DefaultVisibilityTimeoutSeconds,
                WaitSeconds: _opt.DefaultWaitSeconds
            ), CancellationToken.None);

            if (msgs.Count == 0)
                return null;

            var m = msgs[0];

            var amqpMsg = new Message
            {
                BodySection = new Data { Binary = m.Payload },
                Properties = new Properties
                {
                    MessageId = m.MessageId
                },
                ApplicationProperties = new ApplicationProperties()
            };

            // Add helpful metadata (optional)
            amqpMsg.ApplicationProperties.Map["x-receive-count"] = m.ReceiveCount;

            var rc = new ReceiveContext(link, amqpMsg)
            {
                // store receipt handle for settlement (ack)
                UserToken = m.ReceiptHandle
            };

            return rc;
        }
        catch
        {
            // Don't throw from GetMessageAsync; returning null means "no message right now"
            return null;
        }
    }

    public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
    {
        try
        {
            var receipt = receiveContext.UserToken as string;

            // If the client ACCEPTS, we ack in FluxQueue
            if (!string.IsNullOrWhiteSpace(receipt) &&
                dispositionContext.DeliveryState is Accepted)
            {
                _ops.AckAsync(_queue, receipt, CancellationToken.None)
                    .GetAwaiter().GetResult();
            }

            // Released/Rejected/Modified => do nothing:
            // message will become visible again after visibility timeout and be redriven by sweeper.

            dispositionContext.Complete();
        }
        catch
        {
            dispositionContext.Complete();
        }
    }
}