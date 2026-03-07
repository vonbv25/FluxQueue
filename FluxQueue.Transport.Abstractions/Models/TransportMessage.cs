using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FluxQueue.Transport.Abstractions.Models;

public sealed record TransportMessage(
    string Queue,
    string MessageId,
    byte[] Payload,
    string ReceiptHandle,
    int ReceiveCount,
    DateTimeOffset? EnqueuedAt = null,
    IReadOnlyDictionary<string, object?>? Headers = null
);
