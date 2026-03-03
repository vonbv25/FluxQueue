using Amqp;
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

            var delaySeconds = ExtractDelaySeconds(ctx.Message);
            var maxReceiveCount = ctx.Message.ApplicationProperties?.Map.TryGetValue("maxReceiveCount", out var v2) == true && int.TryParse(v2?.ToString(), out var mrc) ? mrc : 5;

            _ops.SendAsync(new SendRequest(
                Queue: _queue,
                Payload: payload,
                DelaySeconds: delaySeconds,
                MaxReceiveCount: maxReceiveCount
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

    private static int ExtractDelaySeconds(Message msg)
    {
        // 1) application property: delaySeconds (int/long/string)
        var ap = msg.ApplicationProperties;
        if (ap?.Map != null)
        {
            if (ap.Map.TryGetValue("delaySeconds", out var v) && v != null)
            {
                if (v is int i) return Math.Max(0, i);
                if (v is long l) return (int)Math.Max(0, Math.Min(int.MaxValue, l));
                if (v is string s && int.TryParse(s, out var si)) return Math.Max(0, si);
            }

            // 2) "x-delay" common pattern, in milliseconds
            if (ap.Map.TryGetValue("x-delay", out var xd) && xd != null)
            {
                long ms =
                    xd is int xi ? xi :
                    xd is long xl ? xl :
                    xd is string xs && long.TryParse(xs, out var xsl) ? xsl :
                    0;

                if (ms > 0)
                    return (int)Math.Max(0, Math.Min(int.MaxValue, (ms + 999) / 1000)); // round up to seconds
            }
        }

        return 0;
    }
}