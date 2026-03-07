# FluxQueue

FluxQueue is a lightweight, durable message queue broker built in .NET
designed to explore the internal mechanics of modern messaging systems
such as RabbitMQ, Amazon SQS, and Kafka.

The project focuses on implementing the core primitives of a message
broker including durable storage, visibility timeouts, retries,
dead-letter queues, reconciliation, guardrails, and protocol
abstraction.

FluxQueue is both:

-   a **learning platform for distributed systems**
-   a **production-style experiment in broker architecture**

# Key Features

-   Durable message persistence using **RocksDB**
-   Visibility timeout based delivery
-   **At-least-once delivery semantics**
-   Message acknowledgement using **receipt handles**
-   Retry with **exponential backoff + jitter**
-   Dead-letter queue (DLQ)
-   **HTTP API**
-   **gRPC API**
-   **AMQP 1.0 protocol adapter**
-   Queue reconciliation and index rebuilding
-   Guardrails for corrupted or inconsistent state
-   Background sweeper for expired leases
-   GitHub Actions **CI/CD pipelines**
-   Docker container support
-   Designed for extensibility and experimentation

# Architecture Overview

FluxQueue separates responsibilities into clear layers.

    Client Applications
          │
          ├── HTTP API
          ├── gRPC API
          └── AMQP 1.0
                │
                ▼
          Broker Host
                │
                ▼
            Queue Engine
                │
                ▼
              RocksDB

### Responsibilities

**BrokerHost**

Handles transport protocols and converts requests into engine
operations.

Supported transports:

-   HTTP
-   gRPC
-   AMQP 1.0

**QueueEngine**

Responsible for the internal broker mechanics:

-   message persistence
-   visibility timeout management
-   retry logic
-   dead-letter routing
-   reconciliation
-   guardrails

**RocksDB**

Provides durable storage using **LSM trees** optimized for high write
throughput and sequential scans.

# Internal Storage Model

FluxQueue stores data across multiple RocksDB column families.

    msg        -> message payload + metadata
    ready      -> messages ready for delivery
    inflight   -> messages currently leased
    receipt    -> receipt handle mapping
    dlq        -> dead letter messages

This design allows efficient scans and deterministic message state
transitions.

# RocksDB Key Schema (Deep Dive)

FluxQueue encodes ordering and metadata directly inside RocksDB keys.

Example key format:

    msg:{queue}:{messageId}
    ready:{queue}:{timestamp}:{messageId}
    inflight:{queue}:{visibilityExpire}:{messageId}
    receipt:{receiptHandle}

### Why this works

-   Sorted keys enable **fast sequential scans**
-   FIFO ordering emerges naturally
-   Visibility expiration can be scanned efficiently
-   No complex secondary indexes are required

This pattern is commonly used in distributed systems where **storage
engines double as indexes**.

# Message Lifecycle

    SEND
     │
     ▼
    READY
     │
     ▼
    RECEIVE
     │
     ▼
    INFLIGHT (visibility timeout)
     │
     ├── ACK → COMPLETE
     │
     └── TIMEOUT
           │
           ▼
         RETRY
           │
           ▼
       MAX RETRIES
           │
           ▼
          DLQ

### State Descriptions

| State | Description |
|------|-------------|
| Ready | Message is available for delivery |
| Inflight | Message is leased to a consumer |
| Retry | Message returned to queue after timeout |
| Acked | Message successfully processed |
| Dead Letter | Message exceeded retry limit |


# Reconciliation

FluxQueue includes a **reconciliation process** capable of rebuilding
queue indexes from the durable message store.

This protects the system against:

-   partial writes
-   corrupted indexes
-   crash recovery scenarios

The reconcile process:

1.  Optionally wipes queue indexes
2.  Scans the message column family
3.  Rebuilds ready and inflight indexes
4.  Repairs orphaned messages

Example configuration:

    public sealed class ReconcileOptions
    {
        public bool WipeAndRebuildIndexes { get; set; } = true;
        public int MaxMessages { get; set; } = 5_000_000;
        public int SweepBatchSize { get; set; } = 20_000;
    }


# Guardrails

FluxQueue includes safety mechanisms that prevent inconsistent queue
states.

Examples include:

-   stale receipt handle detection
-   maximum receive count enforcement
-   inflight lease expiration handling
-   reconciliation based index repair
-   message scan limits during maintenance operations

These guardrails ensure message lifecycle integrity even after crashes
or partial writes.


# Ordering Guarantees

FluxQueue provides **FIFO ordering per queue**.

This is achieved through ordered keys stored in RocksDB which allow
deterministic scans of ready messages.


# Delivery Semantics

FluxQueue implements **at-least-once delivery**.

This guarantees that:

-   messages are never lost
-   duplicate deliveries may occur

Consumers must therefore be **idempotent**.


# APIs
## REST
| Method | Endpoint | Description |
|-------|----------|-------------|
| POST | `/queues/{queue}/messages` | Send message |
| POST | `/queues/{queue}/messages:receive` | Receive messages |
| DELETE | `/queues/{queue}/receipts/{receipt}` | Acknowledge message |

## gRPC

    service QueueBroker {
      rpc Send (SendRequest) returns (SendResponse);
      rpc Receive (ReceiveRequest) returns (ReceiveResponse);
      rpc Ack (AckRequest) returns (AckResponse);
    }

## AMQP 1.0

FluxQueue also supports the **AMQP 1.0 messaging protocol**, enabling
compatibility with enterprise messaging clients and tools.

This allows FluxQueue to integrate with ecosystems traditionally built
around enterprise brokers.

# Background Sweeper

A background worker periodically scans for:

-   expired visibility timeouts
-   messages stuck in inflight state

Expired leases are returned to the ready queue ensuring messages cannot
remain permanently locked.

# CI/CD

FluxQueue uses **GitHub Actions** for automated validation and releases.

### Pull Request Validation

Automatically runs:

-   build
-   unit tests
-   Docker build verification

### Release Pipeline

Triggered when merging into the **release branch**.

Steps include:

1.  Build application
2.  Run tests
3.  Build Docker image
4.  Publish container image

This ensures every release is reproducible and verified.

# Benchmarks (Planned)

Future versions will include benchmarking against other queue systems
including:

-   RabbitMQ
-   Redis Streams
-   Kafka (single partition)

Metrics will include:

-   messages per second
-   latency
-   disk usage
-   recovery time after crashes

# FluxQueue vs Traditional Brokers

| Feature | FluxQueue | RabbitMQ | Kafka |
|--------|-----------|----------|-------|
| Storage | RocksDB | Erlang + disk | Log segments |
| Ordering | Per queue | Per queue | Per partition |
| Delivery | At least once | At least once | At least once |
| Protocols | HTTP / gRPC / AMQP | AMQP / MQTT | Kafka protocol |
| Persistence | Embedded | Broker managed | Log based |
| Primary use | Experimental broker | Enterprise messaging | Streaming platform |

# Project Structure

    FluxQueue.Core
      Queue engine and persistence logic

    FluxQueue.Tests
      Unit tests

    FluxQueue.BrokerHost
      HTTP / gRPC / AMQP host

    FluxQueue.Protocols
      Protocol adapters

# Design Goals

FluxQueue follows several core principles.

**Simplicity over completeness**

Focus on clarity and maintainability rather than feature overload.

**Separation of concerns**

Transport protocols are isolated from the queue engine.

**Deterministic message lifecycle**

Message state transitions are explicit and predictable.

**Extensible protocol layer**

New transports can be added without modifying the engine.

**Durable by default**

Messages are persisted immediately to avoid loss.

# Tradeoffs

| Decision | Reason |
|----------|--------|
| RocksDB storage | High-performance embedded persistence |
| At-least-once delivery | Simpler reliability guarantees |
| Single node broker | Lower operational complexity |
| Manual reconciliation | Recovery from corruption |
| Embedded database | Easier local deployment |

# Roadmap

Future improvements planned:

-   distributed clustering
-   partitioned queues
-   consumer groups
-   OpenTelemetry metrics
-   authentication and authorization
-   web dashboard
-   exactly-once delivery experiments
-   rate limiting
-   scheduled messages

# Why This Project Exists

FluxQueue was created to explore the design of modern messaging systems.

Instead of treating brokers as black boxes, the project implements the
underlying mechanisms directly including:

-   durable storage
-   lease based delivery
-   retry management
-   dead letter handling
-   reconciliation
-   protocol abstraction

The goal is to better understand the engineering tradeoffs behind
distributed messaging systems.

# Contributing

Contributions, ideas, and feedback are welcome.

Feel free to open issues or submit pull requests.

# License

MIT
