using System;
using System.Threading;
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;
using Microsoft.Extensions.Logging;

namespace FluxQueue.Transport.Amqp;

internal sealed class QueueIngressProcessor : IMessageProcessor
{
    private const string DelaySecondsPropertyName = "delaySeconds";
    private const string XDelayPropertyName = "x-delay";
    private const string MaxReceiveCountPropertyName = "maxReceiveCount";

    private readonly IQueueOperations _ops;
    private readonly string _queue;
    private readonly int _credit;
    private readonly ILogger<QueueIngressProcessor> _log;

    public QueueIngressProcessor(
        IQueueOperations ops,
        string queue,
        int credit,
        ILogger<QueueIngressProcessor> log)
    {
        _ops = ops ?? throw new ArgumentNullException(nameof(ops));
        _queue = !string.IsNullOrWhiteSpace(queue)
            ? queue
            : throw new ArgumentException("Queue is required.", nameof(queue));
        _credit = credit > 0 ? credit : throw new ArgumentOutOfRangeException(nameof(credit));
        _log = log ?? throw new ArgumentNullException(nameof(log));
    }

    public int Credit => _credit;

    public void Process(MessageContext ctx)
    {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(ctx.Message);

        using var scope = _log.BeginScope("AMQP ingress queue {Queue}", _queue);

        try
        {
            var payload = AmqpMessageBody.ExtractPayload(ctx.Message);
            var delaySeconds = ExtractDelaySeconds(ctx.Message);
            var maxReceiveCount = ExtractMaxReceiveCount(ctx.Message, fallback: 5);

            _ops.SendAsync(
                    new SendRequest(
                        Queue: _queue,
                        Payload: payload,
                        DelaySeconds: delaySeconds,
                        MaxReceiveCount: maxReceiveCount),
                    CancellationToken.None)
                .GetAwaiter()
                .GetResult();

            _log.LogDebug(
                "Accepted AMQP message into queue {Queue}. PayloadBytes={PayloadBytes}, DelaySeconds={DelaySeconds}, MaxReceiveCount={MaxReceiveCount}",
                _queue,
                payload.Length,
                delaySeconds,
                maxReceiveCount);

            ctx.Complete();
        }
        catch (ArgumentException ex)
        {
            _log.LogWarning(
                ex,
                "Rejected AMQP ingress message for queue {Queue} due to invalid input.",
                _queue);

            ctx.Complete(new Error
            {
                Condition = "amqp:invalid-field",
                Description = ex.Message
            });
        }
        catch (Exception ex)
        {
            _log.LogError(
                ex,
                "Failed to process AMQP ingress message for queue {Queue}.",
                _queue);

            ctx.Complete(new Error
            {
                Condition = "amqp:internal-error",
                Description = "Failed to process message."
            });
        }
    }

    private static int ExtractDelaySeconds(Message msg)
    {
        var ap = msg.ApplicationProperties;
        if (ap?.Map is null)
            return 0;

        if (TryGetInt32(ap.Map, DelaySecondsPropertyName, out var delaySeconds))
            return Math.Max(0, delaySeconds);

        if (TryGetInt64(ap.Map, XDelayPropertyName, out var delayMs) && delayMs > 0)
        {
            var seconds = (delayMs + 999) / 1000; // round up
            return (int)Math.Min(int.MaxValue, seconds);
        }

        return 0;
    }

    private static int ExtractMaxReceiveCount(Message msg, int fallback)
    {
        var ap = msg.ApplicationProperties;
        if (ap?.Map is null)
            return fallback;

        if (TryGetInt32(ap.Map, MaxReceiveCountPropertyName, out var maxReceiveCount) && maxReceiveCount > 0)
            return maxReceiveCount;

        return fallback;
    }

    private static bool TryGetInt32(Map map, string key, out int value)
    {
        value = default;

        if (!map.TryGetValue(key, out var raw) || raw is null)
            return false;

        switch (raw)
        {
            case int i:
                value = i;
                return true;

            case long l when l >= int.MinValue && l <= int.MaxValue:
                value = (int)l;
                return true;

            case string s when int.TryParse(s, out var parsed):
                value = parsed;
                return true;

            default:
                return false;
        }
    }

    private static bool TryGetInt64(Map map, string key, out long value)
    {
        value = default;

        if (!map.TryGetValue(key, out var raw) || raw is null)
            return false;

        switch (raw)
        {
            case int i:
                value = i;
                return true;

            case long l:
                value = l;
                return true;

            case string s when long.TryParse(s, out var parsed):
                value = parsed;
                return true;

            default:
                return false;
        }
    }
}