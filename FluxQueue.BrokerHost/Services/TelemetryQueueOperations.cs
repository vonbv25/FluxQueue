using System.Diagnostics;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;

namespace FluxQueue.BrokerHost.Services;

public sealed class TelemetryQueueOperations : IQueueOperations
{
    private readonly QueueEngineOperations _inner;

    public TelemetryQueueOperations(QueueEngineOperations inner)
    {
        _inner = inner;
    }

    public async Task<string> SendAsync(SendRequest req, CancellationToken ct)
    {
        using var activity = StartActivity("fluxqueue.send", req.Queue);
        activity?.SetTag("messaging.operation", "publish");
        activity?.SetTag("messaging.message.body.size", req.Payload.Length);
        activity?.SetTag("fluxqueue.delay.seconds", req.DelaySeconds ?? 0);
        activity?.SetTag("fluxqueue.max_receive_count", req.MaxReceiveCount);

        var startedAt = Stopwatch.GetTimestamp();

        try
        {
            var messageId = await _inner.SendAsync(req, ct);
            activity?.SetTag("messaging.message.id", messageId);

            var tags = CreateTags(req.Queue, "send");
            FluxQueueTelemetry.SendOperations.Add(1, tags);
            FluxQueueTelemetry.OperationDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalSeconds, tags);

            return messageId;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            FluxQueueTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task<IReadOnlyList<TransportMessage>> ReceiveAsync(ReceiveRequest req, CancellationToken ct)
    {
        using var activity = StartActivity("fluxqueue.receive", req.Queue);
        activity?.SetTag("messaging.operation", "receive");
        activity?.SetTag("fluxqueue.max_messages", req.MaxMessages);
        activity?.SetTag("fluxqueue.visibility_timeout.seconds", req.VisibilityTimeoutSeconds);
        activity?.SetTag("fluxqueue.wait.seconds", req.WaitSeconds);

        var startedAt = Stopwatch.GetTimestamp();

        try
        {
            var messages = await _inner.ReceiveAsync(req, ct);
            activity?.SetTag("messaging.batch.message_count", messages.Count);

            var tags = CreateTags(req.Queue, "receive");
            FluxQueueTelemetry.ReceiveOperations.Add(1, tags);
            FluxQueueTelemetry.MessagesReceived.Add(messages.Count, tags);
            FluxQueueTelemetry.OperationDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalSeconds, tags);

            return messages;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            FluxQueueTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task<bool> AckAsync(string queue, string receiptHandle, CancellationToken ct)
    {
        using var activity = StartActivity("fluxqueue.ack", queue);
        activity?.SetTag("messaging.operation", "ack");

        var startedAt = Stopwatch.GetTimestamp();

        try
        {
            var acknowledged = await _inner.AckAsync(queue, receiptHandle, ct);
            activity?.SetTag("fluxqueue.acknowledged", acknowledged);

            var tags = CreateTags(queue, "ack");
            FluxQueueTelemetry.AckOperations.Add(1, tags);
            FluxQueueTelemetry.OperationDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalSeconds, tags);

            return acknowledged;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            FluxQueueTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    public async Task<bool> RejectAsync(string queue, string receiptHandle, CancellationToken ct)
    {
        using var activity = StartActivity("fluxqueue.reject", queue);
        activity?.SetTag("messaging.operation", "reject");

        var startedAt = Stopwatch.GetTimestamp();

        try
        {
            var rejected = await _inner.RejectAsync(queue, receiptHandle, ct);
            activity?.SetTag("fluxqueue.rejected", rejected);

            var tags = CreateTags(queue, "reject");
            FluxQueueTelemetry.RejectOperations.Add(1, tags);
            FluxQueueTelemetry.OperationDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalSeconds, tags);

            return rejected;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            FluxQueueTelemetry.RecordException(activity, ex);
            throw;
        }
    }

    private static Activity? StartActivity(string operationName, string queue)
    {
        var activity = FluxQueueTelemetry.ActivitySource.StartActivity(operationName, ActivityKind.Internal);
        activity?.SetTag("messaging.system", "fluxqueue");
        activity?.SetTag("messaging.destination.name", queue);
        activity?.SetTag("messaging.destination.kind", "queue");
        return activity;
    }

    private static TagList CreateTags(string queue, string operation)
    {
        return new TagList
        {
            { "messaging.system", "fluxqueue" },
            { "messaging.destination.name", queue },
            { "messaging.operation", operation }
        };
    }
}
