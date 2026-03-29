using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FluxQueue.Transport.Abstractions;

public static class FluxQueueTelemetry
{
    public const string ActivitySourceName = "FluxQueue";
    public const string MeterName = "FluxQueue";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    public static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> SendOperations = Meter.CreateCounter<long>(
        "fluxqueue.send.operations",
        "{operation}",
        "Number of send operations processed by FluxQueue.");

    public static readonly Counter<long> ReceiveOperations = Meter.CreateCounter<long>(
        "fluxqueue.receive.operations",
        "{operation}",
        "Number of receive operations processed by FluxQueue.");

    public static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>(
        "fluxqueue.receive.messages",
        "{message}",
        "Number of messages returned by receive operations.");

    public static readonly Counter<long> AckOperations = Meter.CreateCounter<long>(
        "fluxqueue.ack.operations",
        "{operation}",
        "Number of ack operations processed by FluxQueue.");

    public static readonly Counter<long> RejectOperations = Meter.CreateCounter<long>(
        "fluxqueue.reject.operations",
        "{operation}",
        "Number of reject operations processed by FluxQueue.");

    public static readonly Histogram<double> OperationDuration = Meter.CreateHistogram<double>(
        "fluxqueue.operation.duration",
        "s",
        "Duration of FluxQueue queue operations.");

    public static void RecordException(Activity? activity, Exception ex)
    {
        if (activity is null)
            return;

        activity.SetTag("exception.type", ex.GetType().FullName);
        activity.SetTag("exception.message", ex.Message);
        activity.SetTag("exception.stacktrace", ex.StackTrace);
    }
}
