# FluxQueue

FluxQueue is a lightweight, durable message queue broker built in .NET, designed to explore and demonstrate the internal mechanics of modern messaging systems.

It provides persistent message storage using RocksDB and implements core messaging semantics including visibility timeouts, retries with exponential backoff, acknowledgements, and dead-letter queues. FluxQueue exposes HTTP and gRPC APIs and is designed with a protocol-agnostic architecture to support additional transports such as AMQP.

---

## Key Features

- Durable message persistence powered by RocksDB
- Visibility timeout support
- At-least-once delivery semantics
- Message acknowledgement via receipt handles
- Retry with exponential backoff + jitter
- Dead-letter queue (DLQ)
- HTTP API
- gRPC API
- Protocol adapter architecture (AMQP planned)
- Background sweeper for expired leases
- Designed for extensibility and experimentation

---

## Core Concepts

### Visibility Timeout

When a consumer receives a message, it becomes invisible to other consumers for a configurable period.  
If not acknowledged within this window, the message is requeued.

### Receipt Handle

Each delivery generates a unique receipt handle that must be used to acknowledge the message.  
This prevents accidental acknowledgement of stale deliveries.

### Retry Policy

Messages are retried using exponential backoff with jitter to prevent retry storms.

### Dead Letter Queue (DLQ)

Messages exceeding the maximum receive count are moved to the DLQ for inspection.

### At-Least-Once Delivery

FluxQueue prioritizes durability and reliability, meaning duplicate deliveries may occur and consumers should be idempotent.

---

## Getting Started

### Prerequisites

- .NET 8 SDK or later
- Supported OS (Windows, Linux, macOS)

### Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/fluxqueue.git
cd fluxqueue
```
Run the Broker
```bash
dotnet run --project FluxQueue.BrokerHost
```
The broker will start listening for HTTP and gRPC requests.

###📡 APIs
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /queues/{queue}/messages | Send message |
| POST | /queues/{queue}/messages:receive | Receive messages |
| DELETE | /queues/{queue}/receipts/{receipt} | Acknowledge message |

gRPC Service
```code
service QueueBroker {
  rpc Send (SendRequest) returns (SendResponse);
  rpc Receive (ReceiveRequest) returns (ReceiveResponse);
  rpc Ack (AckRequest) returns (AckResponse);
}
```
### Project Structure
```code
FluxQueue.Core        → Queue engine and persistence logic
FluxQueue.Test        → Unit tests
FluxQueue.BrokerHost  → HTTP and gRPC host
FluxQueue.Protocols   → Future protocol adapters (AMQP)
```
## Design Goals

- **Simplicity over completeness** — prioritize clarity and maintainability
- **Clear separation of concerns** — transport and storage are decoupled
- **Deterministic message lifecycle** — predictable state transitions
- **Extensible protocol layer** — support multiple transport adapters
- **Educational reference implementation** — easy to understand internals
- **Strong consistency per queue** — ordered and durable processing
- **Durable by default** — persistence is built-in

## Tradeoffs

| Decision | Reason |
|----------|--------|
| RocksDB storage | Fast embedded persistence with ordered keys |
| At-least-once delivery | Simpler reliability model |
| Single-node engine | MVP simplicity |
| Manual sweep process | Explicit control over lease expiration |
| No distributed coordination (yet) | Focus on core semantics first |

---

## Roadmap

- AMQP protocol adapter
- Queue registry and dynamic queue creation
- Metrics and observability (OpenTelemetry)
- Distributed clustering
- Partitioned queues
- Consumer groups
- Web dashboard
- Authentication and authorization
- Exactly-once processing experiments
- Rate limiting
- Message scheduling

---

## Why This Project Exists

FluxQueue was built as an exploration into how modern messaging brokers work internally — focusing on persistence, message lifecycle management, lease handling, and protocol abstraction.

It serves as both a learning project and a foundation for experimenting with broker design patterns.

## Contributing

Contributions, ideas, and feedback are welcome.  
Feel free to open issues or submit pull requests.

---

📜 License
MIT
