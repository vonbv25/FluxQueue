namespace FluxQueue.Transport.Abstractions.Models;

public enum DeliveryDisposition
{
    Accept,     // success -> Ack
    Release,    // requeue (or let visibility expire)
    Reject,     // terminal failure (optional DLQ)
    Abandon     // TODO: for in-flight messages that are not acked within visibility timeout, should we have a separate disposition to distinguish from explicit release?
}