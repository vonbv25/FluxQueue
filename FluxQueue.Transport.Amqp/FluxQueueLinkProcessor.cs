using Amqp.Framing;
using Amqp.Listener;
using FluxQueue.Transport.Abstractions;

namespace FluxQueue.Transport.Amqp;

internal sealed class FluxQueueLinkProcessor : ILinkProcessor
{
    private readonly IQueueOperations _ops;
    private readonly AmqpTransportOptions _opt;

    public FluxQueueLinkProcessor(IQueueOperations ops, AmqpTransportOptions opt)
    {
        _ops = ops;
        _opt = opt;
    }

    public void Process(AttachContext context)
    {
        var attach = context.Attach;

        // role=false => peer is sender (it will send to our Target)
        // role=true  => peer is receiver (it will receive from our Source)
        if (attach.Role == false)
        {
            var target = attach.Target as Target;
            var queueAddress = target?.Address?.ToString();

            if (string.IsNullOrWhiteSpace(queueAddress))
            {
                context.Complete(new Error
                {
                    Condition = "amqp:invalid-field",
                    Description = "Target.Address (queue) is required for producer link."
                });
                return;
            }

            string queue;
            try { queue = AmqpAddressMapper.ToQueueName(queueAddress); }
            catch (Exception ex)
            {
                context.Complete(new Error
                {
                    Condition = "amqp:invalid-field",
                    Description = ex.Message
                });
                return;
            }

            // Handles incoming messages published to this queue
            var endpoint = new TargetLinkEndpoint(new QueueIngressProcessor(_ops, queue), context.Link);
            context.Complete(endpoint, initialCredit: 200);
            return;
        }
        else
        {
            var source = attach.Source as Source;
            var queueAddress = source?.Address?.ToString();

            if (string.IsNullOrWhiteSpace(queueAddress))
            {
                context.Complete(new Error
                {
                    Condition = "amqp:invalid-field",
                    Description = "Source.Address (queue) is required for consumer link."
                });
                return;
            }

            string queue;
            try { queue = AmqpAddressMapper.ToQueueName(queueAddress); }
            catch (Exception ex)
            {
                context.Complete(new Error
                {
                    Condition = "amqp:invalid-field",
                    Description = ex.Message
                });
                return;
            }

            // Sends messages from this queue to the consumer
            var endpoint = new SourceLinkEndpoint(new QueueEgressSource(_ops, queue, _opt), context.Link);
            context.Complete(endpoint, initialCredit: _opt.MaxBatch);
            return;
        }
    }
}
