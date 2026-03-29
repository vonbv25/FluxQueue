using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using FluxQueue.Transport.Abstractions;
using FluxQueue.Transport.Abstractions.Models;
using Microsoft.Extensions.Logging;

namespace FluxQueue.Transport.Amqp;

internal sealed class QueueEgressSource : IMessageSource, IDisposable
{
    private const string QueuePropertyName = "x-queue";
    private const string ReceiveCountPropertyName = "x-receive-count";

    private readonly IQueueOperations _ops;
    private readonly string _queue;
    private readonly AmqpTransportOptions _opt;
    private readonly ILogger<QueueEgressSource> _log;

    private readonly ConcurrentDictionary<ListenerLink, CancellationTokenSource> _linkCts = new();
    private int _disposed;

    public QueueEgressSource(
        IQueueOperations ops,
        string queue,
        AmqpTransportOptions opt,
        ILogger<QueueEgressSource> log)
    {
        _ops = ops ?? throw new ArgumentNullException(nameof(ops));
        _queue = !string.IsNullOrWhiteSpace(queue)
            ? queue
            : throw new ArgumentException("Queue is required.", nameof(queue));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _log = log ?? throw new ArgumentNullException(nameof(log));
    }

    public async Task<ReceiveContext?> GetMessageAsync(ListenerLink link)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        ArgumentNullException.ThrowIfNull(link);

        using var scope = _log.BeginScope("AMQP egress queue {Queue}", _queue);
        using var activity = FluxQueueTelemetry.ActivitySource.StartActivity(
            "fluxqueue.amqp.egress.receive",
            ActivityKind.Producer);

        activity?.SetTag("messaging.system", "fluxqueue");
        activity?.SetTag("messaging.protocol", "amqp");
        activity?.SetTag("messaging.operation", "receive");
        activity?.SetTag("messaging.destination.name", _queue);

        var cts = GetOrCreateLinkCts(link);
        var token = cts.Token;

        try
        {
            if (IsLinkUnavailable(link, token))
                return null;

            var msgs = await _ops.ReceiveAsync(
                new ReceiveRequest(
                    Queue: _queue,
                    MaxMessages: 1,
                    VisibilityTimeoutSeconds: _opt.DefaultVisibilityTimeoutSeconds,
                    WaitSeconds: _opt.DefaultWaitSeconds),
                token);

            if (IsLinkUnavailable(link, token))
                return null;

            if (msgs.Count == 0)
                return null;

            var m = msgs[0];
            activity?.SetTag("messaging.message.id", m.MessageId);
            activity?.SetTag("messaging.message.body.size", m.Payload.Length);
            activity?.SetTag("fluxqueue.receive_count", m.ReceiveCount);

            var amqpMsg = new Message
            {
                BodySection = new Data { Binary = m.Payload },
                Properties = new Properties
                {
                    MessageId = m.MessageId
                },
                ApplicationProperties = new ApplicationProperties()
            };

            amqpMsg.ApplicationProperties.Map[QueuePropertyName] = _queue;
            amqpMsg.ApplicationProperties.Map[ReceiveCountPropertyName] = m.ReceiveCount;

            return new ReceiveContext(link, amqpMsg)
            {
                UserToken = m.ReceiptHandle
            };
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _log.LogDebug(
                "Canceled AMQP egress receive because link closed or detached. Queue={Queue}",
                _queue);

            return null;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            FluxQueueTelemetry.RecordException(activity, ex);

            _log.LogError(
                ex,
                "Failed AMQP egress receive for queue {Queue}. Returning no message.",
                _queue);

            return null;
        }
    }

    public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
    {
        ArgumentNullException.ThrowIfNull(receiveContext);
        ArgumentNullException.ThrowIfNull(dispositionContext);

        using var scope = _log.BeginScope("AMQP egress queue {Queue}", _queue);
        using var activity = FluxQueueTelemetry.ActivitySource.StartActivity(
            "fluxqueue.amqp.egress.settle",
            ActivityKind.Consumer);

        activity?.SetTag("messaging.system", "fluxqueue");
        activity?.SetTag("messaging.protocol", "amqp");
        activity?.SetTag("messaging.destination.name", _queue);

        var receipt = receiveContext.UserToken as string;
        var state = dispositionContext.DeliveryState;

        try
        {
            if (string.IsNullOrWhiteSpace(receipt))
            {
                _log.LogWarning(
                    "Settlement had no receipt handle. DeliveryState={DeliveryState}",
                    state?.GetType().Name ?? "<null>");

                dispositionContext.Complete();
                return;
            }

            switch (state)
            {
                case Accepted:
                    {
                        activity?.SetTag("messaging.operation", "ack");

                        var acked = _ops.AckAsync(_queue, receipt, CancellationToken.None)
                            .GetAwaiter()
                            .GetResult();

                        if (!acked)
                        {
                            _log.LogWarning(
                                "Accepted settlement did not ack any message. ReceiptHandle={ReceiptHandle}",
                                receipt);
                        }

                        break;
                    }

                case Rejected:
                    {
                        activity?.SetTag("messaging.operation", "reject");

                        var rejected = _ops.RejectAsync(_queue, receipt, CancellationToken.None)
                            .GetAwaiter()
                            .GetResult();

                        if (!rejected)
                        {
                            _log.LogWarning(
                                "Rejected settlement did not dead-letter any message. ReceiptHandle={ReceiptHandle}",
                                receipt);
                        }

                        break;
                    }

                case Released:
                case Modified:
                    {
                        activity?.SetTag("messaging.operation", "release");

                        _log.LogDebug(
                            "Settlement left message for visibility-timeout redelivery. ReceiptHandle={ReceiptHandle}, DeliveryState={DeliveryState}",
                            receipt,
                            state.GetType().Name);

                        break;
                    }

                default:
                    {
                        activity?.SetTag("messaging.operation", "settle");

                        _log.LogDebug(
                            "Unhandled AMQP delivery state treated as no-op. ReceiptHandle={ReceiptHandle}, DeliveryState={DeliveryState}",
                            receipt,
                            state?.GetType().Name ?? "<null>");

                        break;
                    }
            }

            dispositionContext.Complete();
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            FluxQueueTelemetry.RecordException(activity, ex);

            _log.LogError(
                ex,
                "Failed settlement for queue {Queue}. ReceiptHandle={ReceiptHandle}, DeliveryState={DeliveryState}",
                _queue,
                receipt ?? "<null>",
                state?.GetType().Name ?? "<null>");

            dispositionContext.Complete();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var kvp in _linkCts)
        {
            if (_linkCts.TryRemove(kvp.Key, out var cts))
            {
                try
                {
                    cts.Cancel();
                }
                catch
                {
                }
                finally
                {
                    cts.Dispose();
                }
            }
        }
    }

    private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    private static bool IsLinkUnavailable(ListenerLink link, CancellationToken token)
    {
        return token.IsCancellationRequested
               || link.IsClosed
               || link.LinkState == LinkState.DetachSent;
    }

    private CancellationTokenSource GetOrCreateLinkCts(ListenerLink link)
    {
        return _linkCts.GetOrAdd(link, CreateLinkCts);
    }

    private CancellationTokenSource CreateLinkCts(ListenerLink link)
    {
        var cts = new CancellationTokenSource();

        link.AddClosedCallback((_, error) =>
        {
            if (_linkCts.TryRemove(link, out var removed))
            {
                try
                {
                    removed.Cancel();
                }
                catch
                {
                }
                finally
                {
                    removed.Dispose();
                }
            }

            _log.LogDebug(
                "AMQP link closed for queue {Queue}. Error={Error}",
                _queue,
                error?.Description ?? "<none>");
        });

        return cts;
    }
}
