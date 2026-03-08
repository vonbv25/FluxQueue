# FluxQueue

FluxQueue is a lightweight message broker built with .NET and RocksDB.

It is designed to provide **durable messaging with minimal operational complexity**, making it suitable for local deployments, edge environments, and lightweight infrastructure stacks.

FluxQueue supports multiple client protocols including:

- HTTP
- gRPC
- AMQP 1.0

The broker focuses on **predictable performance, durability, and operational simplicity**.

---

# Key Features

## Durable Messaging

Messages are persisted using **RocksDB**, providing fast local storage with crash-safe durability.

## Multiple Protocols

FluxQueue supports multiple client interfaces:

- HTTP API
- gRPC
- AMQP 1.0

All protocols operate through the same internal queue engine.

## Message Lifecycle Management

FluxQueue tracks message state transitions explicitly:

```
READY → INFLIGHT → ACKED / REJECTED / DLQ
```

This design enables reliable processing and recovery after crashes.

## Dead Letter Queues

Messages that cannot be successfully processed can be routed to a **dead letter queue (DLQ)** for inspection and replay.

## Crash Recovery

FluxQueue includes mechanisms to recover broker state:

- lease expiration sweeper
- reconciliation engine that rebuilds indexes from storage

This ensures queue consistency even after unexpected shutdowns.

---

# Architecture Overview

FluxQueue uses a layered architecture.

```
Clients
│
├── HTTP API
├── gRPC
└── AMQP 1.0
       │
       ▼
Broker Host
       │
       ▼
Queue Operations (IQueueOperations)
       │
       ▼
Queue Engine
       │
       ▼
RocksDB Storage
```

## Core Components

### BrokerHost

The service host responsible for:

- protocol servers
- configuration
- background workers
- lifecycle management

### QueueEngine

The core broker logic responsible for:

- enqueue / dequeue
- message leasing
- acknowledgements
- dead letter routing

### Storage Layer

FluxQueue uses RocksDB column families:

- `msg`
- `ready`
- `inflight`
- `receipt`
- `dlq`

These indexes allow efficient queue scanning and state transitions.

---

# Message Processing Model

Consumers receive messages using a **lease-based delivery model**.

1. Consumer receives message  
2. Message enters `INFLIGHT` state  
3. Consumer must either:

```
ACK     → message completed
REJECT  → message requeued
TIMEOUT → message returned to READY
```

This prevents message loss when consumers crash.

---

# Running FluxQueue

## Using Docker

```bash
docker compose up
```

Default ports:

```
HTTP   : 8080
gRPC   : 5001
AMQP   : 5672
```

---

# Example HTTP Usage

## Send a message

```
POST /queues/{queue}/messages
```

Body:

```json
{
  "payloadBase64": "SGVsbG8="
}
```

## Receive a message

```
POST /queues/{queue}/receive
```

## Acknowledge a message

```
POST /queues/{queue}/ack
```

## Reject a message

```
POST /queues/{queue}/reject
```

---

# Project Structure

```
FluxQueue
│
├── FluxQueue.BrokerHost
│   Broker service host
│
├── FluxQueue.Core
│   Queue engine and storage logic
│
├── FluxQueue.Transport.Abstractions
│   Transport interface contracts
│
├── FluxQueue.Transport.Amqp
│   AMQP protocol implementation
│
├── integration-tests
│   End-to-end protocol tests
│
└── unit-tests
    Core engine tests
```

---

# Development

## Build

```bash
dotnet build
```

## Run broker

```bash
dotnet run --project FluxQueue.BrokerHost
```

---

# Current Status

FluxQueue is currently **in active development (alpha)**.

Implemented capabilities include:

- durable message storage
- HTTP API
- AMQP protocol support
- message leasing and acknowledgement
- dead letter queues
- reconciliation and crash recovery

Current development focus:

- observability
- operational tooling
- reliability hardening

---

# Roadmap

## v0.3 — Observability & Operations

- queue metrics
- OpenTelemetry integration
- queue statistics API
- DLQ management APIs

## v0.4 — Reliability Hardening

- crash recovery tests
- reconciliation stress testing
- AMQP edge case validation

## v0.5 — Security

- HTTP authentication
- queue-level authorization
- secure protocol configuration

## v1.0 — Production Ready

- performance optimizations
- operational tooling
- stability improvements

---

# Contributing

Contributions are welcome.

Development work is tracked using:

- GitHub Issues
- Milestones
- Project board

If you would like to contribute, please open an issue or pull request.

---

# Why FluxQueue

FluxQueue aims to provide a messaging system that is:

- simple to deploy
- predictable in performance
- easy to operate
- suitable for local or embedded infrastructure

It is designed as a lightweight alternative to heavier broker systems when full cluster infrastructure is unnecessary.

---

# License

MIT License
