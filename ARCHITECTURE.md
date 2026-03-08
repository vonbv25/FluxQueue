# FluxQueue Architecture

This document describes the internal architecture of FluxQueue.

FluxQueue is designed to provide **durable messaging with simple operations** using a single-node broker backed by **RocksDB**.

---

## System Architecture

```mermaid
flowchart TD
    CLIENTS[Producers / Consumers]

    subgraph TRANSPORTS[Transport Protocols]
        HTTP[HTTP Endpoints]
        GRPC[gRPC Service]
        AMQP[AMQP Transport]
    end

    subgraph BROKER[Broker Host]
        STARTUP[Startup / Configuration]
        WORKERS[Background Workers]
        SWEEPER[Lease Expiration Sweeper]
        RECONCILER[Reconciliation Worker]
    end

    subgraph APP[Application Boundary]
        IQO[IQueueOperations]
    end

    subgraph CORE[Core Broker]
        QE[QueueEngine]
        QA[QueueActor per Queue]
        CMD[Single-Reader Command Mailbox]
    end

    subgraph ROCKSDB[RocksDB]
        CFMSG[(msg)]
        CFREADY[(ready)]
        CFINFLIGHT[(inflight)]
        CFRECEIPT[(receipt)]
        CFDLQ[(dlq)]
    end

    CLIENTS --> HTTP
    CLIENTS --> GRPC
    CLIENTS --> AMQP

    HTTP --> IQO
    GRPC --> IQO
    AMQP --> IQO

    IQO --> QE
    QE --> QA
    QA --> CMD

    STARTUP --> WORKERS
    WORKERS --> SWEEPER
    WORKERS --> RECONCILER

    SWEEPER --> QE
    RECONCILER --> QE

    QE --> CFMSG
    QE --> CFREADY
    QE --> CFINFLIGHT
    QE --> CFRECEIPT
    QE --> CFDLQ
```

Core components:

- Broker Host
- Transport Protocols (HTTP, gRPC, AMQP)
- Queue Engine
- Persistent Storage (RocksDB)

---

# Architectural Principles

FluxQueue follows several architectural principles:

- Simplicity over operational complexity
- Durable local persistence
- Deterministic message lifecycle
- Isolation of queue coordination
- Actor-style per-queue command serialization
- Log-structured storage for high write throughput

---

# Actor-Style Queue Coordination

FluxQueue uses an **actor-style per-queue coordination model** rather than a pure actor-system runtime.

Each queue can have a dedicated `QueueActor` that processes queue-local commands through a **single-reader mailbox**. This design serializes consumer-facing operations for a queue and reduces race conditions around message state transitions.

## Actor Coordination Model

```mermaid
flowchart TD
    CALLERS[Concurrent Callers]

    CALLERS --> SEND[SendAsync]
    CALLERS --> RECEIVE[ReceiveAsync]
    CALLERS --> ACK[AckAsync]
    CALLERS --> REJECT[RejectAsync]
    CALLERS --> SWEEP[SweepExpiredAsync]

    SEND --> ENGINE[QueueEngine direct write]
    RECEIVE --> ACTOR[QueueActor]
    ACK --> ACTOR
    REJECT --> ACTOR
    SWEEP --> ACTOR

    subgraph ACTOR_BOUNDARY[Actor-Style Per-Queue Coordination]
        ACTOR --> MAILBOX[Single-Reader Command Mailbox]
        MAILBOX --> SERIAL[Serialized queue-local processing]
    end

    SERIAL --> ENGINE
    ENGINE --> ROCKSDB[(RocksDB)]
    ENGINE --> SIGNAL[NotifyNewMessage]
```

## What the QueueActor Owns

The `QueueActor` is responsible for coordinating queue-local operations such as:

- receive
- ack
- reject
- lease expiration sweeping
- long-poll receive fulfillment

It maintains queue-local coordination state such as:

- pending receive requests
- new-message signaling
- actor lifecycle and idle cleanup
- serialized command processing

## Why This Design Was Chosen

Queue workloads involve concurrent operations from multiple callers:

- consumers receiving messages
- consumers acknowledging messages
- consumers rejecting messages
- background sweeps reclaiming expired leases

Without a serialization boundary, these operations could interleave in ways that make state transitions difficult to reason about.

The actor-style design provides:

- a **single logical coordinator per queue**
- **serialized processing** of queue-local commands
- a simpler mental model for consumer-side state transitions
- reduced lock contention compared to coarse global locking

## Important Precision

FluxQueue should **not** be described as a pure Actor Model implementation in the strict Erlang/Akka sense.

Not all queue mutations currently flow through `QueueActor` instances.

For example:

- producer writes in `SendAsync(...)` are performed directly by `QueueEngine`
- reconciliation also writes directly to RocksDB
- sequence generation is maintained in shared concurrent structures

Because of this, the most accurate description is:

> FluxQueue uses an **actor-inspired concurrency boundary** for per-queue coordination, especially for consumer-side operations, while still relying on shared RocksDB-backed storage and direct engine writes for some system-level operations.

## Benefits

This approach still provides meaningful advantages:

- predictable per-queue concurrency
- clearer ownership of consumer-side coordination
- deterministic handling of receive/ack/reject flows
- practical actor-style behavior without introducing a full actor framework

---

# RocksDB Architectural Principles

FluxQueue uses **RocksDB** as its storage engine. The choice of RocksDB is based on several architectural principles that align well with message broker workloads.

## RocksDB Write Path

```mermaid
flowchart LR
    WRITE[Broker write] --> WAL[Write Ahead Log]
    WAL --> MEM[Memtable]
    MEM --> FLUSH[Flush]
    FLUSH --> SST[SSTables]
    SST --> COMPACT[Compaction]
```

## Log-Structured Merge Tree (LSM)

RocksDB is built on the **LSM-tree architecture**, which optimizes for:

- high write throughput
- mostly sequential disk operations
- efficient write-heavy workloads

Message brokers are typically append-heavy systems, so this storage model fits well.

## Write Optimization

Instead of updating data in-place, RocksDB typically:

1. appends writes to a **Write Ahead Log (WAL)**
2. buffers writes in an in-memory **memtable**
3. flushes data into immutable **sorted SSTables**

This supports FluxQueue's goals of:

- durable writes
- low random-write overhead
- efficient batched updates

## Compaction

RocksDB periodically performs **compaction** to merge SSTables and remove obsolete data.

This is important for queue workloads because message lifecycle transitions naturally create stale versions and obsolete index entries over time.

Compaction helps:

- reclaim disk space
- maintain read performance
- keep storage healthy over long-running workloads

## Column Families

FluxQueue uses **column families** to separate message storage from queue indexes.

| Column Family | Purpose |
|---------------|--------|
| `msg` | Message payload storage |
| `ready` | Ready queue index |
| `inflight` | Leased message index |
| `receipt` | Receipt token mapping |
| `dlq` | Dead letter queue |

This separation supports:

- logical isolation of broker data structures
- targeted scans by queue state
- clearer mapping between broker lifecycle states and storage layout

## Column Family Rationale

```mermaid
flowchart TD
    BROKER[FluxQueue Broker State]

    BROKER --> MSG[msg CF]
    BROKER --> READY[ready CF]
    BROKER --> INFLIGHT[inflight CF]
    BROKER --> RECEIPT[receipt CF]
    BROKER --> DLQ[dlq CF]

    MSG --> MSG_USE[Payload durability]
    READY --> READY_USE[Ready queue scans]
    INFLIGHT --> INFLIGHT_USE[Lease expiry scans]
    RECEIPT --> RECEIPT_USE[Ack / reject validation]
    DLQ --> DLQ_USE[Dead-letter inspection and replay]
```

## Embedded Storage

RocksDB is an **embedded database**, which means:

- no external database service is required
- operational overhead stays low
- the broker can access storage directly in-process

That matches FluxQueue's architectural goal of **minimal infrastructure complexity**.

---

# Queue Engine

The `QueueEngine` is the core component responsible for:

- message persistence
- queue indexing
- message leasing
- receipt validation
- dead letter routing
- crash recovery

---

# Message Lifecycle

```mermaid
stateDiagram-v2
    [*] --> READY : Send

    READY --> INFLIGHT : Receive
    INFLIGHT --> ACKED : Ack
    INFLIGHT --> DLQ : Reject
    INFLIGHT --> READY : Lease expired and retry allowed
    INFLIGHT --> DLQ : Lease expired and maxReceiveCount reached

    DLQ --> READY : Replay
    DLQ --> [*] : Purge
    ACKED --> [*]
```

States:

| State | Description |
|------|-------------|
| `READY` | Message available for delivery |
| `INFLIGHT` | Message leased by consumer |
| `ACKED` | Successfully processed and removed |
| `DLQ` | Dead letter queue |

---

# Storage Layout

```mermaid
flowchart TD
    QE[QueueEngine]

    subgraph ROCKSDB[RocksDB Column Families]
        MSG[(msg CF)]
        READY[(ready CF)]
        INFLIGHT[(inflight CF)]
        RECEIPT[(receipt CF)]
        DLQ[(dlq CF)]
    end

    QE --> MSG
    QE --> READY
    QE --> INFLIGHT
    QE --> RECEIPT
    QE --> DLQ

    MSG_NOTE["Message payload + metadata
key: msg:{queue}:{messageId}"]
    READY_NOTE["Ready index
key: ready:{queue}:{visibleAt}:{enqueueSeq}:{messageId}"]
    INFLIGHT_NOTE["Inflight lease index
key: inflight:{queue}:{inflightUntil}:{messageId}"]
    RECEIPT_NOTE["Receipt mapping
key: receipt:{queue}:{receiptHandle}"]
    DLQ_NOTE["Dead-letter index
key: dlq:{queue}:{failedAt}:{messageId}"]

    MSG -.-> MSG_NOTE
    READY -.-> READY_NOTE
    INFLIGHT -.-> INFLIGHT_NOTE
    RECEIPT -.-> RECEIPT_NOTE
    DLQ -.-> DLQ_NOTE
```

## Storage Transitions

```mermaid
flowchart LR
    SEND[Send] --> MSG[(msg CF)]
    MSG --> READY[(ready CF)]

    READY -->|Receive| INFLIGHT[(inflight CF)]
    INFLIGHT -->|Issue receipt| RECEIPT[(receipt CF)]

    INFLIGHT -->|Ack| ACKED[Completed]
    RECEIPT -->|Used by Ack / Reject| INFLIGHT

    INFLIGHT -->|Reject| DLQ[(dlq CF)]
    INFLIGHT -->|Lease expired| READY
    INFLIGHT -->|Max receive count exceeded| DLQ

    DLQ -->|Replay| READY
```

---

# Long-Poll Receive Flow

```mermaid
flowchart TD
    RECV[Receive request] --> ACTOR[QueueActor receives command]
    ACTOR --> TRY[TryReceiveBatchNoLock]

    TRY --> FOUND{Messages available?}
    FOUND -->|Yes| RETURN[Return received messages]
    FOUND -->|No| DEADLINE{Wait deadline reached?}

    DEADLINE -->|Yes| EMPTY[Return empty result]
    DEADLINE -->|No| PENDING[Store request in pending receives]

    PENDING --> WAIT[Wait for command, signal, or visibility time]
    WAIT --> RETRY[Retry receive attempt]
    RETRY --> TRY
```

---

# Acknowledge and Reject Flow

```mermaid
flowchart TD
    RECEIPT[Receipt handle received] --> LOOKUP[Lookup receipt record]
    LOOKUP --> EXISTS{Receipt exists?}

    EXISTS -->|No| FAIL[Return false]
    EXISTS -->|Yes| LOAD[Load message]

    LOAD --> VALID{Message still inflight and lease matches?}
    VALID -->|No| FAIL
    VALID -->|Yes| ACTION{Operation}

    ACTION -->|Ack| ACKPATH[Delete receipt, inflight, and msg]
    ACTION -->|Reject| REJECTPATH[Delete receipt, inflight, and msg; write DLQ record]

    ACKPATH --> OK[Return true]
    REJECTPATH --> OK
```

---

# Lease Expiration

When a consumer receives a message:

1. Message moves from `READY` → `INFLIGHT`
2. A lease expiration timestamp is assigned
3. The consumer receives a receipt token

If the consumer does not acknowledge before the lease expires, the sweeper reclaims the message.

## Sweeper Flow

```mermaid
flowchart TD
    START[Start sweep cycle] --> SCAN[Scan inflight index for expired leases]
    SCAN --> FOUND{Expired inflight entry found?}

    FOUND -->|No| END[Finish sweep]
    FOUND -->|Yes| LOAD[Load message record]

    LOAD --> VALID{Message valid and still inflight?}

    VALID -->|No| CLEAN[Remove stale inflight entry]
    CLEAN --> NEXT[Continue scanning]

    VALID -->|Yes| RETRY{ReceiveCount >= MaxReceiveCount?}

    RETRY -->|Yes| MOVE_DLQ[Move message to DLQ]
    RETRY -->|No| REQUEUE[Move message back to READY with backoff]

    MOVE_DLQ --> CLEANUP[Delete inflight entry and receipt]
    REQUEUE --> CLEANUP

    CLEANUP --> NEXT
    NEXT --> FOUND
```

## Lease Timeout Recovery Sequence

```mermaid
sequenceDiagram
    participant Consumer
    participant QueueActor
    participant QueueEngine
    participant RocksDB
    participant Sweeper

    Consumer->>QueueActor: Receive
    QueueActor->>QueueEngine: TryReceiveBatchNoLock
    QueueEngine->>RocksDB: READY -> INFLIGHT
    QueueEngine->>RocksDB: Create receipt
    QueueActor-->>Consumer: Message + receipt

    Note over Consumer: No Ack / consumer crashes

    Sweeper->>QueueActor: SweepExpired
    QueueActor->>QueueEngine: SweepExpiredNoLock
    QueueEngine->>RocksDB: Scan expired INFLIGHT

    alt retry allowed
        QueueEngine->>RocksDB: INFLIGHT -> READY
        QueueEngine->>RocksDB: Delete receipt
    else max receive count exceeded
        QueueEngine->>RocksDB: INFLIGHT -> DLQ
        QueueEngine->>RocksDB: Delete receipt
    end
```

---

# Reconciliation

The reconciliation engine rebuilds indexes from the durable message store.

This ensures recovery after:

- broker crashes
- partial writes
- index corruption

## Reconciliation Flow

```mermaid
flowchart TD
    START[Start reconciliation] --> WIPE{Wipe and rebuild indexes?}

    WIPE -->|Yes| CLEAR[Clear ready / inflight / receipt column families]
    WIPE -->|No| SCAN
    CLEAR --> SCAN[Scan msg column family]

    SCAN --> READ[Read message envelope]
    READ --> OK{Envelope valid?}

    OK -->|No| DELETE[Delete corrupt message record]
    DELETE --> NEXT[Next message]

    OK -->|Yes| STATE{Message state?}

    STATE -->|Ready| REBUILD_READY[Rebuild ready index]
    STATE -->|Inflight| REBUILD_INFLIGHT[Rebuild inflight index]
    REBUILD_INFLIGHT --> RECEIPT{Has receipt handle?}
    RECEIPT -->|Yes| REBUILD_RECEIPT[Rebuild receipt mapping]
    RECEIPT -->|No| NEXT
    REBUILD_READY --> NEXT
    REBUILD_RECEIPT --> NEXT

    NEXT --> MORE{More messages?}
    MORE -->|Yes| SCAN
    MORE -->|No| SETSEQ[Restore max enqueue sequence per queue]
    SETSEQ --> SWEEP[Run post-reconcile expired sweep]
    SWEEP --> END[Finish reconciliation]
```

---

# Reliability Guarantees

FluxQueue provides:

### At-Least-Once Delivery

Messages may be delivered more than once but will not be lost.

### Crash Recovery

State can be reconstructed from durable storage.

### Message Durability

Messages are persisted before acknowledgement.

---

# Future Architecture Directions

### Observability

- Prometheus metrics
- OpenTelemetry tracing

### Security

- authentication
- authorization
- TLS transport security

### Distributed Mode

```mermaid
flowchart TD
    CLIENTS[Clients]

    subgraph CLUSTER[Future Distributed FluxQueue Cluster]
        LB[Client Routing / Front Door]

        subgraph NODE1[Node 1]
            BROKER1[Broker Host]
            STORE1[(Local RocksDB)]
        end

        subgraph NODE2[Node 2]
            BROKER2[Broker Host]
            STORE2[(Local RocksDB)]
        end

        subgraph NODE3[Node 3]
            BROKER3[Broker Host]
            STORE3[(Local RocksDB)]
        end

        META[Cluster Metadata / Coordination]
    end

    CLIENTS --> LB
    LB --> BROKER1
    LB --> BROKER2
    LB --> BROKER3

    BROKER1 --> STORE1
    BROKER2 --> STORE2
    BROKER3 --> STORE3

    BROKER1 -.-> META
    BROKER2 -.-> META
    BROKER3 -.-> META
```

---

# Summary

FluxQueue prioritizes:

- simplicity
- durability
- predictable performance

By combining **RocksDB log-structured storage**, **actor-style per-queue coordination**, and **lease-based delivery**, FluxQueue provides a reliable messaging system without requiring complex cluster infrastructure.
