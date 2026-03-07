using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RocksDbSharp;
using static RocksDbSharp.ColumnFamilies;

namespace FluxQueue.Core;

public sealed class ReconcileOptions
{
    public bool WipeAndRebuildIndexes { get; set; } = true;
    public int MaxMessages { get; set; } = 5_000_000;
    public int SweepBatchSize { get; set; } = 20_000;
}

public class QueueEngine : IDisposable
{
    private const string CF_MSG = "msg";
    private const string CF_READY = "ready";
    private const string CF_INFLIGHT = "inflight";
    private const string CF_RECEIPT = "receipt";
    private const string CF_DLQ = "dlq";

    private readonly RocksDb _db;
    private readonly ColumnFamilyHandle _cfMsg;
    private readonly ColumnFamilyHandle _cfReady;
    private readonly ColumnFamilyHandle _cfInflight;
    private readonly ColumnFamilyHandle _cfReceipt;
    private readonly ColumnFamilyHandle _cfDlq;

    private readonly string _nodeId;
    private readonly ConcurrentDictionary<string, QueueActor> _queueActors = new();
    private readonly ConcurrentDictionary<string, long> _enqueueSeq = new();
    private readonly ConcurrentDictionary<string, byte> _knownQueues = new();
    private readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);
    private readonly TimeSpan _actorIdleTtl = TimeSpan.FromMinutes(10);

    public IReadOnlyCollection<string> KnownQueues => [.. _knownQueues.Keys];

    public QueueEngine(string dbPath, string nodeId = "node-1")
    {
        _nodeId = nodeId;

        var opts = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true);

        var cfDescs = new ColumnFamilies
        {
            new Descriptor("default", new ColumnFamilyOptions()),
            new Descriptor(CF_MSG, new ColumnFamilyOptions()),
            new Descriptor(CF_READY, new ColumnFamilyOptions()),
            new Descriptor(CF_INFLIGHT, new ColumnFamilyOptions()),
            new Descriptor(CF_RECEIPT, new ColumnFamilyOptions()),
            new Descriptor(CF_DLQ, new ColumnFamilyOptions()),
        };

        _db = RocksDb.Open(opts, dbPath, cfDescs);

        _cfMsg = _db.GetColumnFamily(CF_MSG);
        _cfReady = _db.GetColumnFamily(CF_READY);
        _cfInflight = _db.GetColumnFamily(CF_INFLIGHT);
        _cfReceipt = _db.GetColumnFamily(CF_RECEIPT);
        _cfDlq = _db.GetColumnFamily(CF_DLQ);
    }

    public Task<string> SendAsync(
        string queue,
        byte[] payload,
        int delaySeconds = 0,
        int maxReceiveCount = 5,
        CancellationToken ct = default)
    {
        ValidateQueue(queue);
        RegisterQueue(queue);
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentOutOfRangeException.ThrowIfNegative(delaySeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxReceiveCount);

        ct.ThrowIfCancellationRequested();

        var nowMs = NowMs();
        var visibleAtMs = nowMs + delaySeconds * 1000L;
        var msgId = Guid.NewGuid().ToString("N");
        var seq = NextEnqueueSeq(queue);

        var record = new MessageRecord
        {
            Queue = queue,
            MessageId = msgId,
            VisibleAtMs = visibleAtMs,
            InflightUntilMs = 0,
            ReceiveCount = 0,
            MaxReceiveCount = maxReceiveCount,
            State = MessageState.Ready,
            CreatedAtMs = nowMs,
            UpdatedAtMs = nowMs,
            EnqueueSeq = seq
        };

        var msgKey = QueueEngineHelpers.MsgKey(queue, msgId);
        var readyKey = QueueEngineHelpers.ReadyKey(queue, visibleAtMs, seq, msgId);

        using var wb = new WriteBatch();
        wb.Put(msgKey, SerializeEnvelope(record, payload), _cfMsg);
        wb.Put(readyKey, Array.Empty<byte>(), _cfReady);
        _db.Write(wb);

        Signal(queue);
        return Task.FromResult(msgId);
    }

    public Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(
        string queue,
        int maxMessages = 1,
        int visibilityTimeoutSeconds = 30,
        int waitSeconds = 0,
        CancellationToken ct = default)
    {
        ValidateQueue(queue);
        RegisterQueue(queue);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessages);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(visibilityTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegative(waitSeconds);

        ct.ThrowIfCancellationRequested();

        var actor = GetOrCreateActor(queue);
        return actor.ReceiveAsync(maxMessages, visibilityTimeoutSeconds, waitSeconds, ct);
    }

    public Task<bool> AckAsync(string queue, string receiptHandle, CancellationToken ct = default)
    {
        ValidateQueue(queue);
        RegisterQueue(queue);

        if (string.IsNullOrWhiteSpace(receiptHandle))
            throw new ArgumentNullException(nameof(receiptHandle));

        ct.ThrowIfCancellationRequested();

        var actor = GetOrCreateActor(queue);
        return actor.AckAsync(receiptHandle, ct);
    }

    public Task<bool> RejectAsync(string queue, string receiptHandle, CancellationToken ct = default)
    {
        ValidateQueue(queue);
        RegisterQueue(queue);

        if (string.IsNullOrWhiteSpace(receiptHandle))
            throw new ArgumentNullException(nameof(receiptHandle));

        ct.ThrowIfCancellationRequested();

        var actor = GetOrCreateActor(queue);
        return actor.RejectAsync(receiptHandle, ct);
    }

    public Task<int> SweepExpiredAsync(
        string queue,
        int maxToProcess = 1000,
        CancellationToken ct = default)
    {
        ValidateQueue(queue);
        RegisterQueue(queue);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxToProcess);

        ct.ThrowIfCancellationRequested();

        var actor = GetOrCreateActor(queue);
        return actor.SweepExpiredAsync(maxToProcess, ct);
    }

    public async Task ReconcileAsync(ReconcileOptions opt, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (opt.WipeAndRebuildIndexes)
        {
            WipeColumnFamily(_cfReady, ct);
            WipeColumnFamily(_cfInflight, ct);
            WipeColumnFamily(_cfReceipt, ct);
        }

        int scanned = 0;
        var maxSeqByQueue = new Dictionary<string, long>(StringComparer.Ordinal);

        using var it = _db.NewIterator(_cfMsg);
        it.SeekToFirst();

        using var wb = new WriteBatch();

        while (it.Valid() && scanned < opt.MaxMessages)
        {
            ct.ThrowIfCancellationRequested();
            scanned++;

            var msgKey = it.Key();
            var msgVal = it.Value();

            MessageRecord meta;
            try
            {
                meta = DeserializeEnvelopeMeta<MessageRecord>(msgVal);
            }
            catch
            {
                wb.Delete(msgKey, _cfMsg);

                if (scanned % 10_000 == 0)
                {
                    _db.Write(wb);
                    wb.Clear();
                }

                it.Next();
                continue;
            }

            if (!string.IsNullOrWhiteSpace(meta.Queue))
            {
                RegisterQueue(meta.Queue);

                if (!maxSeqByQueue.TryGetValue(meta.Queue, out var currentMax) || meta.EnqueueSeq > currentMax)
                    maxSeqByQueue[meta.Queue] = meta.EnqueueSeq;
            }

            if (meta.State == MessageState.Ready)
            {
                var readyKey = QueueEngineHelpers.ReadyKey(
                    meta.Queue,
                    meta.VisibleAtMs,
                    meta.EnqueueSeq,
                    meta.MessageId);

                wb.Put(readyKey, Array.Empty<byte>(), _cfReady);
            }
            else if (meta.State == MessageState.Inflight)
            {
                var inflightKey = QueueEngineHelpers.InflightKey(
                    meta.Queue,
                    meta.InflightUntilMs,
                    meta.MessageId);

                wb.Put(inflightKey, Array.Empty<byte>(), _cfInflight);

                if (!string.IsNullOrEmpty(meta.CurrentReceiptHandle))
                {
                    var receiptKey = QueueEngineHelpers.ReceiptKey(meta.Queue, meta.CurrentReceiptHandle);

                    wb.Put(receiptKey, Serialize(new ReceiptRecord
                    {
                        Queue = meta.Queue,
                        ReceiptHandle = meta.CurrentReceiptHandle,
                        MessageId = meta.MessageId,
                        InflightUntilMs = meta.InflightUntilMs,
                        IssuedAtMs = meta.UpdatedAtMs
                    }), _cfReceipt);
                }
            }

            if (scanned % 10_000 == 0)
            {
                _db.Write(wb);
                wb.Clear();
            }

            it.Next();
        }

        if (wb.Count() > 0)
            _db.Write(wb);

        foreach (var kvp in maxSeqByQueue)
            _enqueueSeq[kvp.Key] = kvp.Value;

        foreach (var q in KnownQueues)
        {
            ct.ThrowIfCancellationRequested();

            for (int i = 0; i < 100; i++)
            {
                var processed = await SweepExpiredAsync(q, opt.SweepBatchSize, ct);
                if (processed == 0)
                    break;
            }
        }
    }

    public void Dispose()
    {
        foreach (var kv in _queueActors)
            kv.Value.Dispose();

        _db.Dispose();
    }

    // -------------------------
    // Internal no-lock methods
    // Only QueueActor should call these.
    // -------------------------

    private (int delivered, int cleaned) TryReceiveBatchNoLock(
        string queue,
        int maxMessages,
        int visibilityTimeoutSeconds,
        List<ReceivedMessage> output)
    {
        int delivered = 0;
        int cleaned = 0;

        var scanNowMs = NowMs();
        var prefix = QueueEngineHelpers.ReadyPrefix(queue);

        using var it = _db.NewIterator(_cfReady);
        it.Seek(prefix);

        var toClaim = new List<(byte[] readyKey, string msgId)>(maxMessages);

        while (it.Valid() && toClaim.Count < maxMessages)
        {
            var k = it.Key();
            if (!QueueEngineHelpers.StartsWith(k, prefix))
                break;

            var (msgId, visibleAt) = QueueEngineHelpers.ParseReadyKey(queue, k);
            if (visibleAt > scanNowMs)
                break;

            toClaim.Add(((byte[])k.Clone(), msgId));
            it.Next();
        }

        if (toClaim.Count == 0)
            return (0, 0);

        foreach (var (readyKeyBytes, msgId) in toClaim)
        {
            var nowMs = NowMs();
            var msgKey = QueueEngineHelpers.MsgKey(queue, msgId);
            var msgVal = _db.Get(msgKey, _cfMsg);

            if (msgVal is null)
            {
                _db.Remove(readyKeyBytes, _cfReady);
                cleaned++;
                continue;
            }

            MessageRecord msg;
            ReadOnlyMemory<byte> payloadMem;
            try
            {
                (msg, payloadMem) = DeserializeEnvelopeMetaAndPayload<MessageRecord>(msgVal);
            }
            catch
            {
                _db.Remove(readyKeyBytes, _cfReady);
                _db.Remove(msgKey, _cfMsg);
                cleaned++;
                continue;
            }

            if (msg.State != MessageState.Ready || msg.VisibleAtMs > nowMs)
            {
                _db.Remove(readyKeyBytes, _cfReady);
                cleaned++;
                continue;
            }

            var inflightUntil = nowMs + visibilityTimeoutSeconds * 1000L;
            var receiptHandle = QueueEngineHelpers.NewReceiptHandle();

            msg.State = MessageState.Inflight;
            msg.ReceiveCount += 1;
            msg.InflightUntilMs = inflightUntil;
            msg.UpdatedAtMs = nowMs;
            msg.CurrentReceiptHandle = receiptHandle;

            var inflightKey = QueueEngineHelpers.InflightKey(queue, inflightUntil, msgId);
            var receiptKey = QueueEngineHelpers.ReceiptKey(queue, receiptHandle);

            using var wb = new WriteBatch();
            wb.Delete(readyKeyBytes, _cfReady);
            wb.Put(msgKey, SerializeEnvelope(msg, payloadMem.Span), _cfMsg);
            wb.Put(inflightKey, Array.Empty<byte>(), _cfInflight);
            wb.Put(receiptKey, Serialize(new ReceiptRecord
            {
                Queue = queue,
                ReceiptHandle = receiptHandle,
                MessageId = msgId,
                InflightUntilMs = inflightUntil,
                IssuedAtMs = nowMs
            }), _cfReceipt);

            _db.Write(wb);

            output.Add(new ReceivedMessage(
                MessageId: msgId,
                Payload: payloadMem.ToArray(),
                ReceiptHandle: receiptHandle,
                ReceiveCount: msg.ReceiveCount));

            delivered++;
        }

        return (delivered, cleaned);
    }

    private bool AckNoLock(string queue, string receiptHandle)
    {
        var receiptKey = QueueEngineHelpers.ReceiptKey(queue, receiptHandle);
        var receiptVal = _db.Get(receiptKey, _cfReceipt);
        if (receiptVal is null)
            return false;

        var receipt = Deserialize<ReceiptRecord>(receiptVal);
        if (receipt is null)
            return false;

        var msgKey = QueueEngineHelpers.MsgKey(queue, receipt.MessageId);
        var msgVal = _db.Get(msgKey, _cfMsg);
        if (msgVal is null)
        {
            _db.Remove(receiptKey, _cfReceipt);
            return false;
        }

        MessageRecord msg;
        try
        {
            msg = DeserializeEnvelopeMeta<MessageRecord>(msgVal);
        }
        catch
        {
            return false;
        }

        if (msg.State != MessageState.Inflight || msg.InflightUntilMs != receipt.InflightUntilMs)
            return false;

        var inflightKey = QueueEngineHelpers.InflightKey(queue, msg.InflightUntilMs, msg.MessageId);

        using var wb = new WriteBatch();
        wb.Delete(receiptKey, _cfReceipt);
        wb.Delete(inflightKey, _cfInflight);
        wb.Delete(msgKey, _cfMsg);
        _db.Write(wb);

        return true;
    }

    private bool RejectNoLock(string queue, string receiptHandle)
    {
        var receiptKey = QueueEngineHelpers.ReceiptKey(queue, receiptHandle);
        var receiptVal = _db.Get(receiptKey, _cfReceipt);
        if (receiptVal is null)
            return false;

        var receipt = Deserialize<ReceiptRecord>(receiptVal);
        if (receipt is null)
            return false;

        var msgKey = QueueEngineHelpers.MsgKey(queue, receipt.MessageId);
        var msgVal = _db.Get(msgKey, _cfMsg);
        if (msgVal is null)
        {
            _db.Remove(receiptKey, _cfReceipt);
            return false;
        }

        MessageRecord msg;
        ReadOnlyMemory<byte> payloadMem;
        try
        {
            (msg, payloadMem) = DeserializeEnvelopeMetaAndPayload<MessageRecord>(msgVal);
        }
        catch
        {
            return false;
        }

        if (msg.State != MessageState.Inflight || msg.InflightUntilMs != receipt.InflightUntilMs)
            return false;

        var inflightKey = QueueEngineHelpers.InflightKey(queue, msg.InflightUntilMs, msg.MessageId);
        var failedAtMs = NowMs();
        var dlqKey = QueueEngineHelpers.DlqKey(queue, failedAtMs, msg.MessageId);

        var dlqMeta = new DeadLetterRecord
        {
            Queue = queue,
            MessageId = msg.MessageId,
            ReceiveCount = msg.ReceiveCount,
            FailedAtMs = failedAtMs,
            Reason = "rejected"
        };

        using var wb = new WriteBatch();
        wb.Delete(receiptKey, _cfReceipt);
        wb.Delete(inflightKey, _cfInflight);
        wb.Delete(msgKey, _cfMsg);
        wb.Put(dlqKey, SerializeEnvelope(dlqMeta, payloadMem.Span), _cfDlq);
        _db.Write(wb);

        return true;
    }

    private int SweepExpiredNoLock(string queue, int maxToProcess, CancellationToken ct)
    {
        var nowMs = NowMs();
        var prefix = QueueEngineHelpers.InflightPrefix(queue);
        var endKey = QueueEngineHelpers.InflightKey(queue, nowMs, "ffffffffffffffffffffffffffffffff");

        int processed = 0;

        using var it = _db.NewIterator(_cfInflight);
        it.Seek(prefix);

        var toHandle = new List<(byte[] inflightKey, string msgId, long untilMs)>(Math.Min(maxToProcess, 1024));

        while (it.Valid() && processed + toHandle.Count < maxToProcess)
        {
            ct.ThrowIfCancellationRequested();

            var k = it.Key();
            if (!QueueEngineHelpers.StartsWith(k, prefix))
                break;

            if (QueueEngineHelpers.CompareBytes(k, endKey) > 0)
                break;

            var (msgId, untilMs) = QueueEngineHelpers.ParseInflightKey(queue, k);
            toHandle.Add(((byte[])k.Clone(), msgId, untilMs));
            it.Next();
        }

        foreach (var (inflightKeyBytes, msgId, untilFromKey) in toHandle)
        {
            ct.ThrowIfCancellationRequested();

            var msgKey = QueueEngineHelpers.MsgKey(queue, msgId);
            var msgVal = _db.Get(msgKey, _cfMsg);

            if (msgVal is null)
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            MessageRecord msg;
            try
            {
                msg = DeserializeEnvelopeMeta<MessageRecord>(msgVal);
            }
            catch
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            if (msg.State != MessageState.Inflight)
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            if (msg.InflightUntilMs != untilFromKey)
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            if (msg.InflightUntilMs > nowMs)
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            ReadOnlyMemory<byte> payloadMem;
            try
            {
                (_, payloadMem) = DeserializeEnvelopeMetaAndPayload<MessageRecord>(msgVal);
            }
            catch
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            if (msg.ReceiveCount >= msg.MaxReceiveCount)
            {
                var failedAtMs = nowMs;
                var dlqKey = QueueEngineHelpers.DlqKey(queue, failedAtMs, msg.MessageId);
                var oldReceiptHandle = msg.CurrentReceiptHandle;

                using var wb = new WriteBatch();
                wb.Delete(inflightKeyBytes, _cfInflight);

                if (!string.IsNullOrEmpty(oldReceiptHandle))
                {
                    var receiptKey = QueueEngineHelpers.ReceiptKey(queue, oldReceiptHandle);
                    wb.Delete(receiptKey, _cfReceipt);
                }

                wb.Delete(msgKey, _cfMsg);

                var dlqMeta = new DeadLetterRecord
                {
                    Queue = queue,
                    MessageId = msg.MessageId,
                    ReceiveCount = msg.ReceiveCount,
                    FailedAtMs = failedAtMs,
                    Reason = "max_receive_count_exceeded"
                };

                wb.Put(dlqKey, SerializeEnvelope(dlqMeta, payloadMem.Span), _cfDlq);
                _db.Write(wb);
            }
            else
            {
                var delayMs = QueueEngineHelpers.ComputeBackoffMs(msg.ReceiveCount);
                var visibleAt = nowMs + delayMs;
                var oldReceiptHandle = msg.CurrentReceiptHandle;

                msg.State = MessageState.Ready;
                msg.VisibleAtMs = visibleAt;
                msg.InflightUntilMs = 0;
                msg.UpdatedAtMs = nowMs;
                msg.EnqueueSeq = NextEnqueueSeq(queue);
                msg.CurrentReceiptHandle = null;

                var readyKey = QueueEngineHelpers.ReadyKey(queue, visibleAt, msg.EnqueueSeq, msg.MessageId);

                using var wb = new WriteBatch();
                wb.Delete(inflightKeyBytes, _cfInflight);

                if (!string.IsNullOrEmpty(oldReceiptHandle))
                {
                    var receiptKey = QueueEngineHelpers.ReceiptKey(queue, oldReceiptHandle);
                    wb.Delete(receiptKey, _cfReceipt);
                }

                wb.Put(msgKey, SerializeEnvelope(msg, payloadMem.Span), _cfMsg);
                wb.Put(readyKey, Array.Empty<byte>(), _cfReady);
                _db.Write(wb);

                Signal(queue);
            }

            processed++;
        }

        return processed;
    }

    // -------------------------
    // Serialization
    // -------------------------

    private byte[] SerializeEnvelope<TMeta>(TMeta meta, ReadOnlySpan<byte> payload)
    {
        var metaJson = JsonSerializer.SerializeToUtf8Bytes(meta, _json);
        var buffer = new byte[4 + metaJson.Length + payload.Length];

        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), metaJson.Length);
        metaJson.CopyTo(buffer.AsSpan(4, metaJson.Length));
        payload.CopyTo(buffer.AsSpan(4 + metaJson.Length));

        return buffer;
    }
    private TMeta DeserializeEnvelopeMeta<TMeta>(byte[] value)
    {
        if (value.Length < 4)
            throw new InvalidOperationException("Corrupt envelope: too small.");

        int metaLen = BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(0, 4));
        if (metaLen < 0 || 4 + metaLen > value.Length)
            throw new InvalidOperationException("Corrupt envelope: invalid meta length.");

        var metaJson = value.AsSpan(4, metaLen);
        var meta = JsonSerializer.Deserialize<TMeta>(metaJson, _json);
        if (meta is null)
            throw new InvalidOperationException("Corrupt envelope: meta is null.");

        return meta;
    }
    private (TMeta meta, ReadOnlyMemory<byte> payload) DeserializeEnvelopeMetaAndPayload<TMeta>(byte[] value)
    {
        if (value.Length < 4)
            throw new InvalidOperationException("Corrupt envelope: too small.");

        int metaLen = BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(0, 4));
        if (metaLen < 0 || 4 + metaLen > value.Length)
            throw new InvalidOperationException("Corrupt envelope: invalid meta length.");

        var metaJson = value.AsSpan(4, metaLen);
        var meta = JsonSerializer.Deserialize<TMeta>(metaJson, _json);
        if (meta is null)
            throw new InvalidOperationException("Corrupt envelope: meta is null.");

        var payload = value.AsMemory(4 + metaLen);
        return (meta, payload);
    }
    private byte[] Serialize<T>(T obj) => JsonSerializer.SerializeToUtf8Bytes(obj, _json);
    private T? Deserialize<T>(byte[] bytes) => JsonSerializer.Deserialize<T>(bytes, _json);

    // -------------------------
    // Infrastructure helpers
    // -------------------------

    private QueueActor GetOrCreateActor(string queue) =>
        _queueActors.GetOrAdd(queue, q => new QueueActor(this, q));

    private void Signal(string queue)
    {
        if (_queueActors.TryGetValue(queue, out var actor))
            actor.NotifyNewMessage();
    }

    private static void ValidateQueue(string queue)
    {
        if (string.IsNullOrWhiteSpace(queue))
            throw new ArgumentNullException(nameof(queue));

        foreach (var ch in queue)
        {
            var ok = char.IsLetterOrDigit(ch) || ch is '-' or '_' or '.';
            if (!ok)
                throw new ArgumentException("Queue name contains invalid characters.", nameof(queue));
        }
    }
    private void RegisterQueue(string queue) => _knownQueues.TryAdd(queue, 0);
    private long NextEnqueueSeq(string queue) =>
        _enqueueSeq.AddOrUpdate(queue, 1, static (_, current) => checked(current + 1));
    private long? TryGetNextReadyVisibleAtMs(string queue)
    {
        var prefix = QueueEngineHelpers.ReadyPrefix(queue);

        using var it = _db.NewIterator(_cfReady);
        it.Seek(prefix);

        var nowMs = NowMs();

        for (int i = 0; i < 32; i++)
        {
            if (!it.Valid())
                return null;

            var k = it.Key();
            if (!QueueEngineHelpers.StartsWith(k, prefix))
                return null;

            var (_, visibleAt) = QueueEngineHelpers.ParseReadyKey(queue, k);

            if (visibleAt > nowMs)
                return visibleAt;

            it.Next();
        }

        return null;
    }
    private void RemoveActorReferenceIfCurrent(string queue, QueueActor actor)
    {
        _queueActors.TryRemove(new KeyValuePair<string, QueueActor>(queue, actor));
    }
    private void WipeColumnFamily(ColumnFamilyHandle cf, CancellationToken ct)
    {
        using var it = _db.NewIterator(cf);
        it.SeekToFirst();

        using var wb = new WriteBatch();
        int n = 0;

        while (it.Valid())
        {
            ct.ThrowIfCancellationRequested();

            wb.Delete(it.Key(), cf);
            n++;

            if (n % 50_000 == 0)
            {
                _db.Write(wb);
                wb.Clear();
            }

            it.Next();
        }

        if (wb.Count() > 0)
            _db.Write(wb);
    }
    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    // -------------------------
    // Types
    // -------------------------

    public readonly record struct ReceivedMessage(
        string MessageId,
        byte[] Payload,
        string ReceiptHandle,
        int ReceiveCount);
    private enum MessageState : byte
    {
        Ready = 1,
        Inflight = 2,
        Dead = 3
    }
    private sealed class MessageRecord
    {
        public string Queue { get; set; } = "";
        public string MessageId { get; set; } = "";
        public long EnqueueSeq { get; set; }
        public long VisibleAtMs { get; set; }
        public long InflightUntilMs { get; set; }
        public int ReceiveCount { get; set; }
        public int MaxReceiveCount { get; set; }
        public MessageState State { get; set; }
        public long CreatedAtMs { get; set; }
        public long UpdatedAtMs { get; set; }
        public string? CurrentReceiptHandle { get; set; }
    }
    private sealed class ReceiptRecord
    {
        public string Queue { get; set; } = "";
        public string ReceiptHandle { get; set; } = "";
        public string MessageId { get; set; } = "";
        public long InflightUntilMs { get; set; }
        public long IssuedAtMs { get; set; }
    }
    private sealed class DeadLetterRecord
    {
        public string Queue { get; set; } = "";
        public string MessageId { get; set; } = "";
        public int ReceiveCount { get; set; }
        public long FailedAtMs { get; set; }
        public string Reason { get; set; } = "";
    }

    // -------------------------
    // Queue Actor
    // -------------------------

    private sealed class QueueActor : IDisposable
    {
        private readonly QueueEngine _engine;
        private readonly string _queue;

        private readonly Channel<IQueueCommand> _commands =
            Channel.CreateUnbounded<IQueueCommand>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

        private readonly SemaphoreSlim _newMessageSignal = new(0, int.MaxValue);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _loop;

        private readonly Queue<ReceiveRequest> _pendingReceives = new();
        private long _lastActivityMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        public QueueActor(QueueEngine engine, string queue)
        {
            _engine = engine;
            _queue = queue;
            _loop = Task.Run(RunAsync);
        }

        public void NotifyNewMessage()
        {
            Touch();

            try
            {
                _newMessageSignal.Release();
            }
            catch (SemaphoreFullException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(
            int maxMessages,
            int visibilityTimeoutSeconds,
            int waitSeconds,
            CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<IReadOnlyList<ReceivedMessage>>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            var now = DateTimeOffset.UtcNow;
            var deadline = waitSeconds <= 0 ? now : now.AddSeconds(waitSeconds);

            CancellationTokenRegistration ctr = default;
            if (ct.CanBeCanceled)
                ctr = ct.Register(() => tcs.TrySetCanceled(ct));

            _ = tcs.Task.ContinueWith(_ => ctr.Dispose(), TaskScheduler.Default);

            var request = new ReceiveRequest(
                maxMessages,
                visibilityTimeoutSeconds,
                deadline,
                ct,
                tcs);

            Touch();

            if (!_commands.Writer.TryWrite(new ReceiveCommand(request)))
                tcs.TrySetException(new ObjectDisposedException(nameof(QueueActor)));

            return tcs.Task;
        }

        public Task<bool> AckAsync(string receiptHandle, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            CancellationTokenRegistration ctr = default;
            if (ct.CanBeCanceled)
                ctr = ct.Register(() => tcs.TrySetCanceled(ct));

            _ = tcs.Task.ContinueWith(_ => ctr.Dispose(), TaskScheduler.Default);

            Touch();

            if (!_commands.Writer.TryWrite(new AckCommand(receiptHandle, ct, tcs)))
                tcs.TrySetException(new ObjectDisposedException(nameof(QueueActor)));

            return tcs.Task;
        }

        public Task<bool> RejectAsync(string receiptHandle, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            CancellationTokenRegistration ctr = default;
            if (ct.CanBeCanceled)
                ctr = ct.Register(() => tcs.TrySetCanceled(ct));

            _ = tcs.Task.ContinueWith(_ => ctr.Dispose(), TaskScheduler.Default);

            Touch();

            if (!_commands.Writer.TryWrite(new RejectCommand(receiptHandle, ct, tcs)))
                tcs.TrySetException(new ObjectDisposedException(nameof(QueueActor)));

            return tcs.Task;
        }

        public Task<int> SweepExpiredAsync(int maxToProcess, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

            CancellationTokenRegistration ctr = default;
            if (ct.CanBeCanceled)
                ctr = ct.Register(() => tcs.TrySetCanceled(ct));

            _ = tcs.Task.ContinueWith(_ => ctr.Dispose(), TaskScheduler.Default);

            Touch();

            if (!_commands.Writer.TryWrite(new SweepExpiredCommand(maxToProcess, ct, tcs)))
                tcs.TrySetException(new ObjectDisposedException(nameof(QueueActor)));

            return tcs.Task;
        }

        private async Task RunAsync()
        {
            var stopCt = _stop.Token;

            try
            {
                while (!stopCt.IsCancellationRequested)
                {
                    while (_commands.Reader.TryRead(out var cmd))
                        ProcessCommand(cmd);

                    var didWork = TryFulfillPendingReceives();
                    if (didWork)
                    {
                        Touch();
                        continue;
                    }

                    if (_pendingReceives.Count == 0)
                    {
                        if (IdleFor() >= _engine._actorIdleTtl)
                        {
                            _engine.RemoveActorReferenceIfCurrent(_queue, this);
                            return;
                        }

                        var remaining = _engine._actorIdleTtl - IdleFor();
                        if (remaining < TimeSpan.FromSeconds(1))
                            remaining = TimeSpan.FromSeconds(1);

                        var waitCmd = _commands.Reader.WaitToReadAsync(stopCt).AsTask();
                        var waitTimeout = Task.Delay(remaining, stopCt);

                        await Task.WhenAny(waitCmd, waitTimeout);
                        continue;
                    }

                    var now = DateTimeOffset.UtcNow;
                    var earliestDeadline = GetEarliestDeadline();

                    if (earliestDeadline <= now)
                        continue;

                    var delay = earliestDeadline - now;

                    var nextVisibleAtMs = _engine.TryGetNextReadyVisibleAtMs(_queue);
                    if (nextVisibleAtMs is not null)
                    {
                        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        var untilVisibleMs = nextVisibleAtMs.Value - nowMs;

                        if (untilVisibleMs <= 0)
                        {
                            await Task.Delay(5, stopCt);
                            continue;
                        }

                        var visibleDelay = TimeSpan.FromMilliseconds(untilVisibleMs);
                        if (visibleDelay < delay)
                            delay = visibleDelay;
                    }

                    var waitCommandTask = _commands.Reader.WaitToReadAsync(stopCt).AsTask();
                    var waitSignalTask = _newMessageSignal.WaitAsync(delay, stopCt);

                    await Task.WhenAny(waitCommandTask, waitSignalTask);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                while (_pendingReceives.Count > 0)
                {
                    var req = _pendingReceives.Dequeue();
                    req.Tcs.TrySetException(ex);
                }

                while (_commands.Reader.TryRead(out var cmd))
                    FailCommand(cmd, ex);

                _commands.Writer.TryComplete(ex);
                throw;
            }
            finally
            {
                while (_pendingReceives.Count > 0)
                {
                    var req = _pendingReceives.Dequeue();
                    req.Tcs.TrySetCanceled(req.CancellationToken);
                }

                while (_commands.Reader.TryRead(out var cmd))
                    CancelCommand(cmd);
            }
        }

        private void ProcessCommand(IQueueCommand cmd)
        {
            switch (cmd)
            {
                case ReceiveCommand receive:
                    if (receive.Request.CancellationToken.IsCancellationRequested)
                    {
                        receive.Request.Tcs.TrySetCanceled(receive.Request.CancellationToken);
                    }
                    else
                    {
                        _pendingReceives.Enqueue(receive.Request);
                    }
                    break;

                case AckCommand ack:
                    if (ack.CancellationToken.IsCancellationRequested)
                    {
                        ack.Tcs.TrySetCanceled(ack.CancellationToken);
                        break;
                    }

                    try
                    {
                        var result = _engine.AckNoLock(_queue, ack.ReceiptHandle);
                        ack.Tcs.TrySetResult(result);
                    }
                    catch (OperationCanceledException)
                    {
                        ack.Tcs.TrySetCanceled(ack.CancellationToken);
                    }
                    catch (Exception ex)
                    {
                        ack.Tcs.TrySetException(ex);
                    }
                    break;

                case RejectCommand reject:
                    if (reject.CancellationToken.IsCancellationRequested)
                    {
                        reject.Tcs.TrySetCanceled(reject.CancellationToken);
                        break;
                    }

                    try
                    {
                        var result = _engine.RejectNoLock(_queue, reject.ReceiptHandle);
                        reject.Tcs.TrySetResult(result);
                    }
                    catch (OperationCanceledException)
                    {
                        reject.Tcs.TrySetCanceled(reject.CancellationToken);
                    }
                    catch (Exception ex)
                    {
                        reject.Tcs.TrySetException(ex);
                    }
                    break;

                case SweepExpiredCommand sweep:
                    if (sweep.CancellationToken.IsCancellationRequested)
                    {
                        sweep.Tcs.TrySetCanceled(sweep.CancellationToken);
                        break;
                    }

                    try
                    {
                        var processed = _engine.SweepExpiredNoLock(_queue, sweep.MaxToProcess, sweep.CancellationToken);
                        sweep.Tcs.TrySetResult(processed);
                    }
                    catch (OperationCanceledException)
                    {
                        sweep.Tcs.TrySetCanceled(sweep.CancellationToken);
                    }
                    catch (Exception ex)
                    {
                        sweep.Tcs.TrySetException(ex);
                    }
                    break;

                default:
                    throw new InvalidOperationException($"Unknown queue command: {cmd.GetType().FullName}");
            }
        }

        private bool TryFulfillPendingReceives()
        {
            bool didAnything = false;
            int count = _pendingReceives.Count;

            for (int i = 0; i < count; i++)
            {
                var req = _pendingReceives.Dequeue();

                if (req.CancellationToken.IsCancellationRequested)
                {
                    req.Tcs.TrySetCanceled(req.CancellationToken);
                    didAnything = true;
                    continue;
                }

                var now = DateTimeOffset.UtcNow;
                var results = new List<ReceivedMessage>(Math.Min(req.MaxMessages, 50));

                var (delivered, cleaned) = _engine.TryReceiveBatchNoLock(
                    _queue,
                    req.MaxMessages,
                    req.VisibilityTimeoutSeconds,
                    results);

                if (delivered > 0)
                {
                    req.Tcs.TrySetResult(results);
                    didAnything = true;
                    continue;
                }

                if (now >= req.Deadline)
                {
                    req.Tcs.TrySetResult([]);
                    didAnything = true;
                    continue;
                }

                if (cleaned > 0)
                {
                    didAnything = true;
                    _pendingReceives.Enqueue(req);
                    continue;
                }

                _pendingReceives.Enqueue(req);
            }

            return didAnything;
        }

        private DateTimeOffset GetEarliestDeadline()
        {
            var earliest = DateTimeOffset.MaxValue;

            foreach (var req in _pendingReceives)
            {
                if (req.Deadline < earliest)
                    earliest = req.Deadline;
            }

            return earliest;
        }

        private void Touch()
        {
            Volatile.Write(ref _lastActivityMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }

        private TimeSpan IdleFor()
        {
            var last = Volatile.Read(ref _lastActivityMs);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var deltaMs = Math.Max(0, now - last);
            return TimeSpan.FromMilliseconds(deltaMs);
        }

        public void Dispose()
        {
            _stop.Cancel();
            _commands.Writer.TryComplete();

            try
            {
                _loop.Wait(TimeSpan.FromSeconds(2));
            }
            catch
            {
            }

            _newMessageSignal.Dispose();
            _stop.Dispose();
        }

        private static void FailCommand(IQueueCommand cmd, Exception ex)
        {
            switch (cmd)
            {
                case ReceiveCommand receive:
                    receive.Request.Tcs.TrySetException(ex);
                    break;
                case AckCommand ack:
                    ack.Tcs.TrySetException(ex);
                    break;
                case RejectCommand reject:
                    reject.Tcs.TrySetException(ex);
                    break;
                case SweepExpiredCommand sweep:
                    sweep.Tcs.TrySetException(ex);
                    break;
            }
        }

        private static void CancelCommand(IQueueCommand cmd)
        {
            switch (cmd)
            {
                case ReceiveCommand receive:
                    receive.Request.Tcs.TrySetCanceled(receive.Request.CancellationToken);
                    break;
                case AckCommand ack:
                    ack.Tcs.TrySetCanceled(ack.CancellationToken);
                    break;
                case RejectCommand reject:
                    reject.Tcs.TrySetCanceled(reject.CancellationToken);
                    break;
                case SweepExpiredCommand sweep:
                    sweep.Tcs.TrySetCanceled(sweep.CancellationToken);
                    break;
            }
        }

        private interface IQueueCommand { }

        private readonly record struct ReceiveCommand(ReceiveRequest Request) : IQueueCommand;

        private readonly record struct AckCommand(
            string ReceiptHandle,
            CancellationToken CancellationToken,
            TaskCompletionSource<bool> Tcs) : IQueueCommand;

        private readonly record struct RejectCommand(
            string ReceiptHandle,
            CancellationToken CancellationToken,
            TaskCompletionSource<bool> Tcs) : IQueueCommand;

        private readonly record struct SweepExpiredCommand(
            int MaxToProcess,
            CancellationToken CancellationToken,
            TaskCompletionSource<int> Tcs) : IQueueCommand;

        private readonly record struct ReceiveRequest(
            int MaxMessages,
            int VisibilityTimeoutSeconds,
            DateTimeOffset Deadline,
            CancellationToken CancellationToken,
            TaskCompletionSource<IReadOnlyList<ReceivedMessage>> Tcs);
    }
}