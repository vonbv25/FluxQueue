# FluxQueue

FluxQueue is a lightweight message broker built with **.NET** and **RocksDB**.

It provides **durable messaging with minimal operational complexity**, making it suitable for:

- Local infrastructure
- Edge deployments
- Internal services
- Lightweight distributed systems

FluxQueue supports multiple transport protocols:

- HTTP
- gRPC
- AMQP 1.0

The goal of FluxQueue is to deliver **reliable messaging with simple operations** without requiring complex cluster infrastructure.

---

## Features

### Durable Messaging

Messages are persisted using **RocksDB**, providing fast embedded storage with crash-safe durability.

### Multiple Protocols

FluxQueue supports:

- HTTP API
- gRPC
- AMQP 1.0

All protocols share the same internal queue engine.

### Lease-Based Message Processing

FluxQueue uses **message leases** to prevent message loss.

Message lifecycle:

READY → INFLIGHT → ACKED / REJECTED → DLQ

If a consumer crashes or fails to acknowledge a message, the lease expires and the message returns to the queue.

### Dead Letter Queue

Messages that repeatedly fail processing can be moved to a **dead letter queue (DLQ)**.

### Crash Recovery

FluxQueue includes recovery mechanisms:

- Lease expiration sweeper
- Reconciliation engine that rebuilds indexes

---

## Architecture Overview

Place the high-level architecture diagram below.

[ARCHITECTURE_DIAGRAM_PLACEHOLDER]

For detailed system architecture see **ARCHITECTURE.md**.

---

## Message Lifecycle

Insert a state machine diagram here.

[STATE_MACHINE_DIAGRAM_PLACEHOLDER]

---

## Running FluxQueue

### Using Docker

```bash
docker compose up
```

Default ports:

| Protocol | Port |
|--------|------|
| HTTP | 8080 |
| gRPC | 5001 |
| AMQP | 5672 |

---

# Usage Examples

## HTTP Example

### Send Message
```
POST /queues/{queue}/messages
```
Example body:

```json
{
  "payloadBase64": "SGVsbG8="
}
```

### Receive Message
```
POST /queues/{queue}/receive
```
### Acknowledge Message
```
POST /queues/{queue}/ack
```
### Reject Message
```
POST /queues/{queue}/reject
```
---

## gRPC Example

Example pseudo client using .NET gRPC.

```csharp
var channel = GrpcChannel.ForAddress("http://localhost:5001");
var client = new QueueService.QueueServiceClient(channel);

var sendResponse = await client.SendAsync(new SendRequest
{
    Queue = "orders",
    Payload = ByteString.CopyFromUtf8("hello world")
});

var receiveResponse = await client.ReceiveAsync(new ReceiveRequest
{
    Queue = "orders"
});

await client.AckAsync(new AckRequest
{
    Queue = "orders",
    Receipt = receiveResponse.Receipt
});
```

---

## AMQP Example

Example using a generic AMQP client.

```python
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

class Sender(MessagingHandler):
    def on_start(self, event):
        conn = event.container.connect("amqp://localhost:5672")
        sender = event.container.create_sender(conn, "orders")
        sender.send(Message(body="hello world"))

Container(Sender()).run()
```

AMQP queues map directly to FluxQueue queues.

---

## Project Structure

```
FluxQueue
│
├── FluxQueue.BrokerHost
│   Broker host and protocol servers
│
├── FluxQueue.Core
│   Core queue engine and storage logic
│
├── FluxQueue.Transport.Abstractions
│   Transport interfaces
│
├── FluxQueue.Transport.Amqp
│   AMQP implementation
│
├── integration-tests
│   End-to-end protocol tests
│
└── unit-tests
    Queue engine unit tests
```

---

## Development

Build the project:

```bash
dotnet build
```

Run the broker:

```bash
dotnet run --project FluxQueue.BrokerHost
```

---

## Status

FluxQueue is currently **in active development (alpha)**.

Implemented capabilities:

- Durable message storage
- HTTP API
- gRPC support
- AMQP protocol support
- Message leasing
- Dead letter queues
- Reconciliation and crash recovery

---

## Roadmap

### Observability

- Queue metrics
- OpenTelemetry integration
- Queue inspection APIs

### Reliability

- Crash recovery tests
- Reconciliation stress testing
- AMQP edge case validation

### Security

- HTTP authentication
- Queue authorization

### Production Readiness

- Performance optimizations
- Operational tooling
- Stability improvements

---

## Contributing

Contributions are welcome.

Development work is tracked using:

- GitHub Issues
- Milestones
- Project board

---

## License

MIT License
