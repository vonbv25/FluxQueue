using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RocksDbSharp;
using static RocksDbSharp.ColumnFamilies;

namespace FluxQueue.Core;

public class QueueEngine : IDisposable
{
    // Column families
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
    public IReadOnlyCollection<string> KnownQueues => [.. _knownQueues.Keys];

    public QueueEngine(string dbPath, string nodeId = "node-1")
    {
        _nodeId = nodeId;

        var opts = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true);

        var cfDescs = new ColumnFamilies
        {
            // Default CF is required by RocksDB; we don't use it.
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

    public async Task<string> SendAsync(
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
        await Task.CompletedTask;
        return msgId;
    }

    public async Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(
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

        var actor = _queueActors.GetOrAdd(queue, q => new QueueActor(this, q));
        return await actor.ReceiveAsync(maxMessages, visibilityTimeoutSeconds, waitSeconds, ct);
    }

    public Task<bool> AckAsync(string queue, string receiptHandle, CancellationToken ct = default)
    {
        ValidateQueue(queue);
        RegisterQueue(queue);
        if (string.IsNullOrWhiteSpace(receiptHandle)) throw new ArgumentNullException(nameof(receiptHandle));

        ct.ThrowIfCancellationRequested();

        var receiptKey = QueueEngineHelpers.ReceiptKey(queue, receiptHandle);
        var receiptVal = _db.Get(receiptKey, _cfReceipt);
        if (receiptVal is null) return Task.FromResult(false);

        var receipt = Deserialize<ReceiptRecord>(receiptVal);
        if (receipt is null) return Task.FromResult(false);

        var msgKey = QueueEngineHelpers.MsgKey(queue, receipt.MessageId);
        var msgVal = _db.Get(msgKey, _cfMsg);
        if (msgVal is null)
        {
            _db.Remove(receiptKey, _cfReceipt);
            return Task.FromResult(false);
        }

        MessageRecord msg;
        try
        {
            msg = DeserializeEnvelopeMeta<MessageRecord>(msgVal); // meta only
        }
        catch
        {
            return Task.FromResult(false);
        }

        if (msg.State != MessageState.Inflight || msg.InflightUntilMs != receipt.InflightUntilMs)
            return Task.FromResult(false);

        var inflightKey = QueueEngineHelpers.InflightKey(queue, msg.InflightUntilMs, msg.MessageId);

        using var wb = new WriteBatch();
        wb.Delete(receiptKey, _cfReceipt);
        wb.Delete(inflightKey, _cfInflight);
        wb.Delete(msgKey, _cfMsg);
        _db.Write(wb);

        return Task.FromResult(true);
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

        var nowMs = NowMs();
        var prefix = QueueEngineHelpers.InflightPrefix(queue);
        var endKey = QueueEngineHelpers.InflightKey(queue, nowMs, "ffffffffffffffffffffffffffffffff"); // inclusive upper bound within now

        int processed = 0;

        using var it = _db.NewIterator(_cfInflight);
        it.Seek(prefix);

        var toHandle = new List<(byte[] inflightKey, string msgId, long untilMs)>(Math.Min(maxToProcess, 1024));

        while (it.Valid() && processed + toHandle.Count < maxToProcess)
        {
            ct.ThrowIfCancellationRequested();

            var k = it.Key();
            if (!QueueEngineHelpers.StartsWith(k, prefix)) break;

            if (QueueEngineHelpers.CompareBytes(k, endKey) > 0) break;

            var (msgId, untilMs) = QueueEngineHelpers.ParseInflightKey(queue, k);

            // Defensive copy: iterator key buffer may be reused
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
                msg = DeserializeEnvelopeMeta<MessageRecord>(msgVal); // meta only
            }
            catch
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            // Only process truly expired + consistent inflight records
            if (msg.State != MessageState.Inflight) { _db.Remove(inflightKeyBytes, _cfInflight); processed++; continue; }
            if (msg.InflightUntilMs != untilFromKey) { _db.Remove(inflightKeyBytes, _cfInflight); processed++; continue; }
            if (msg.InflightUntilMs > nowMs) { processed++; continue; }

            // We need payload only if we will requeue or DLQ.
            ReadOnlyMemory<byte> payloadMem;
            try
            {
                (_, payloadMem) = DeserializeEnvelopeMetaAndPayload<MessageRecord>(msgVal);
            }
            catch
            {
                // Treat as corrupt: drop inflight index so we don't spin forever.
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            if (msg.ReceiveCount >= msg.MaxReceiveCount)
            {
                var failedAtMs = nowMs;
                var dlqKey = QueueEngineHelpers.DlqKey(queue, failedAtMs, msg.MessageId);

                using var wb = new WriteBatch();
                wb.Delete(inflightKeyBytes, _cfInflight);

                if (!string.IsNullOrEmpty(msg.CurrentReceiptHandle))
                {
                    var rk = QueueEngineHelpers.ReceiptKey(queue, msg.CurrentReceiptHandle);
                    wb.Delete(rk, _cfReceipt);
                    msg.CurrentReceiptHandle = null;
                }

                wb.Delete(msgKey, _cfMsg);

                var dlqMeta = new DeadLetterRecord
                {
                    Queue = queue,
                    MessageId = msg.MessageId,
                    ReceiveCount = msg.ReceiveCount,
                    FailedAtMs = failedAtMs
                };

                wb.Put(dlqKey, SerializeEnvelope(dlqMeta, payloadMem.Span), _cfDlq);
                _db.Write(wb);
            }
            else
            {
                var delayMs = ComputeBackoffMs(msg.ReceiveCount);
                var visibleAt = nowMs + delayMs;

                msg.State = MessageState.Ready;
                msg.VisibleAtMs = visibleAt;
                msg.InflightUntilMs = 0;
                msg.UpdatedAtMs = nowMs;
                msg.EnqueueSeq = NextEnqueueSeq(queue);

                var readyKey = QueueEngineHelpers.ReadyKey(queue, visibleAt, msg.EnqueueSeq, msg.MessageId);

                using var wb = new WriteBatch();
                wb.Delete(inflightKeyBytes, _cfInflight);

                if (!string.IsNullOrEmpty(msg.CurrentReceiptHandle))
                {
                    var rk = QueueEngineHelpers.ReceiptKey(queue, msg.CurrentReceiptHandle);
                    wb.Delete(rk, _cfReceipt);
                    msg.CurrentReceiptHandle = null;
                }

                wb.Put(msgKey, SerializeEnvelope(msg, payloadMem.Span), _cfMsg);
                wb.Put(readyKey, Array.Empty<byte>(), _cfReady);
                _db.Write(wb);

                Signal(queue);
            }

            processed++;
        }

        return Task.FromResult(processed);
    }

    public void Dispose()
    {
        _db?.Dispose();
        foreach (var kv in _queueActors) kv.Value.Dispose();
    }

    // -------- Internal receive logic --------
    private (int delivered, int cleaned) TryReceiveBatchNoLock(string queue, int maxMessages, int visibilityTimeoutSeconds, List<ReceivedMessage> output)
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
            if (!QueueEngineHelpers.StartsWith(k, prefix)) break;

            var (msgId, visibleAt) = QueueEngineHelpers.ParseReadyKey(queue, k);
            if (visibleAt > scanNowMs) break;

            // defensive copy in case iterator buffer is reused
            toClaim.Add(((byte[])k.Clone(), msgId));
            it.Next();
        }

        if (toClaim.Count == 0) return (0, 0);

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
                // READY index is stale or message isn't visible yet -> clean it up
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
                Payload: payloadMem.ToArray(), // copy only when returning to caller
                ReceiptHandle: receiptHandle,
                ReceiveCount: msg.ReceiveCount
            ));
            delivered++;
        }

        return (delivered, cleaned);
    }

    //--- Envelope ---//

    private byte[] SerializeEnvelope<TMeta>(TMeta meta, ReadOnlySpan<byte> payload)
    {
        var metaJson = JsonSerializer.SerializeToUtf8Bytes(meta, _json);
        var buf = new byte[4 + metaJson.Length + payload.Length];

        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(0, 4), metaJson.Length);
        metaJson.CopyTo(buf.AsSpan(4, metaJson.Length));
        payload.CopyTo(buf.AsSpan(4 + metaJson.Length));

        return buf;
    }

    private (TMeta meta, byte[] payload) DeserializeEnvelope<TMeta>(byte[] value)
    {
        if (value.Length < 4) throw new InvalidOperationException("Corrupt envelope: too small.");

        int metaLen = BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(0, 4));
        if (metaLen < 0 || 4 + metaLen > value.Length)
            throw new InvalidOperationException("Corrupt envelope: invalid meta length.");

        var metaJson = value.AsSpan(4, metaLen);
        var meta = JsonSerializer.Deserialize<TMeta>(metaJson, _json);
        if (meta is null) throw new InvalidOperationException("Corrupt envelope: meta is null.");

        var payloadSpan = value.AsSpan(4 + metaLen);
        var payload = payloadSpan.ToArray();

        return (meta, payload);
    }

    private TMeta DeserializeEnvelopeMeta<TMeta>(byte[] value)
    {
        if (value.Length < 4) throw new InvalidOperationException("Corrupt envelope: too small.");

        int metaLen = BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(0, 4));
        if (metaLen < 0 || 4 + metaLen > value.Length)
            throw new InvalidOperationException("Corrupt envelope: invalid meta length.");

        var metaJson = value.AsSpan(4, metaLen);
        var meta = JsonSerializer.Deserialize<TMeta>(metaJson, _json);
        if (meta is null) throw new InvalidOperationException("Corrupt envelope: meta is null.");
        return meta;
    }

    private (TMeta meta, ReadOnlyMemory<byte> payload) DeserializeEnvelopeMetaAndPayload<TMeta>(byte[] value)
    {
        if (value.Length < 4) throw new InvalidOperationException("Corrupt envelope: too small.");

        int metaLen = BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(0, 4));
        if (metaLen < 0 || 4 + metaLen > value.Length)
            throw new InvalidOperationException("Corrupt envelope: invalid meta length.");

        var metaJson = value.AsSpan(4, metaLen);
        var meta = JsonSerializer.Deserialize<TMeta>(metaJson, _json);
        if (meta is null) throw new InvalidOperationException("Corrupt envelope: meta is null.");

        var payload = value.AsMemory(4 + metaLen);
        return (meta, payload);
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static long ComputeBackoffMs(int receiveCount)
    {
        var exp = Math.Min(6, Math.Max(0, receiveCount));
        var baseMs = (long)(1000 * Math.Pow(2, exp));
        baseMs = Math.Min(baseMs, 60_000);

        Span<byte> b = stackalloc byte[2];
        RandomNumberGenerator.Fill(b);
        var jitter = BinaryPrimitives.ReadUInt16LittleEndian(b) % 500;

        return baseMs + jitter;
    }

    private void Signal(string queue)
    {
        if (_queueActors.TryGetValue(queue, out var actor))
            actor.NotifyNewMessage();
    }
    private static void ValidateQueue(string queue)
    {
        if (string.IsNullOrWhiteSpace(queue)) throw new ArgumentNullException(nameof(queue));
        foreach (var ch in queue)
        {
            var ok = char.IsLetterOrDigit(ch) || ch is '-' or '_' or '.';
            if (!ok) throw new ArgumentException("Queue name contains invalid characters.", nameof(queue));
        }
    }
    private long NextEnqueueSeq(string queue)
    {
        // First message => seq 1
        return _enqueueSeq.AddOrUpdate(queue, 1, static (_, current) => checked(current + 1));
    }
    private long? TryGetNextReadyVisibleAtMs(string queue)
    {
        var prefix = QueueEngineHelpers.ReadyPrefix(queue);

        using var it = _db.NewIterator(_cfReady);
        it.Seek(prefix);

        // Scan a few keys to find the earliest FUTURE visibleAt.
        // If the head keys are stale and <= now, they would cause 0ms waits.
        var nowMs = NowMs();

        for (int i = 0; i < 32; i++)
        {
            if (!it.Valid()) return null;

            var k = it.Key();
            if (!QueueEngineHelpers.StartsWith(k, prefix)) return null;

            var (_, visibleAt) = QueueEngineHelpers.ParseReadyKey(queue, k);

            if (visibleAt > nowMs)
                return visibleAt;

            it.Next();
        }

        return null;
    }
    private void RegisterQueue(string queue) => _knownQueues.TryAdd(queue, 0);
    private byte[] Serialize<T>(T obj) => JsonSerializer.SerializeToUtf8Bytes(obj, _json);
    private T? Deserialize<T>(byte[] bytes) => JsonSerializer.Deserialize<T>(bytes, _json);
    public readonly record struct ReceivedMessage(string MessageId, byte[] Payload, string ReceiptHandle, int ReceiveCount);
    private enum MessageState : byte { Ready = 1, Inflight = 2, Dead = 3 }

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
    }

    private readonly TimeSpan _actorIdleTtl = TimeSpan.FromMinutes(10);

    private void TryRemoveActor(string queue, QueueActor actor)
    {
        // Remove only if dictionary still points to THIS actor instance
        if (_queueActors.TryRemove(new KeyValuePair<string, QueueActor>(queue, actor)))
        {
            actor.Dispose();
        }
    }

    private sealed class QueueActor : IDisposable
    {
        private readonly QueueEngine _engine;
        private readonly string _queue;

        private readonly Channel<ReceiveRequest> _requests =
            Channel.CreateUnbounded<ReceiveRequest>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

        private readonly SemaphoreSlim _newMessageSignal = new(0, int.MaxValue);

        private readonly CancellationTokenSource _stop = new();
        private readonly Task _loop;
        private readonly Queue<ReceiveRequest> _pending = new();

        public QueueActor(QueueEngine engine, string queue)
        {
            _engine = engine;
            _queue = queue;
            _loop = Task.Run(RunAsync);
        }

        public void NotifyNewMessage()
        {
            Touch();
            _newMessageSignal.Release();
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
            {
                ctr = ct.Register(() => tcs.TrySetCanceled(ct));
            }

            // Ensure we dispose registration when completed (avoid leaks)
            _ = tcs.Task.ContinueWith(_ => ctr.Dispose(), TaskScheduler.Default);

            var req = new ReceiveRequest(maxMessages, visibilityTimeoutSeconds, deadline, ct, tcs);
            Touch();
            if (!_requests.Writer.TryWrite(req))
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
                    while (_requests.Reader.TryRead(out var req))
                        _pending.Enqueue(req);

                    var didWork = TryFulfillPending();
                    if (didWork) { Touch(); continue; }

                    if (_pending.Count == 0)
                    {
                        // If idle too long, self-terminate and let engine remove us.
                        if (IdleFor() >= _engine._actorIdleTtl)
                        {
                            _engine.TryRemoveActor(_queue, this);
                            return; // exit actor loop
                        }

                        // Wait for either a new request or until TTL check time
                        var remaining = _engine._actorIdleTtl - IdleFor();
                        if (remaining < TimeSpan.FromSeconds(1))
                            remaining = TimeSpan.FromSeconds(1);

                        var waitReq = _requests.Reader.WaitToReadAsync(stopCt).AsTask();
                        var waitTimeout = Task.Delay(remaining, stopCt);

                        await Task.WhenAny(waitReq, waitTimeout);
                        continue;
                    }

                    var now = DateTimeOffset.UtcNow;
                    var earliestDeadline = GetEarliestDeadline();

                    // If anything is already past its deadline, don't do a 0-delay wait.
                    // Just loop so TryFulfillPending() can complete it.
                    if (earliestDeadline <= now)
                    {
                        // Optional: Touch();  // not required
                        continue;
                    }

                    var delay = earliestDeadline - now;

                    // Also wake up when the next delayed message becomes visible
                    var nextVisibleAtMs = _engine.TryGetNextReadyVisibleAtMs(_queue);
                    if (nextVisibleAtMs is not null)
                    {
                        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        var untilVisibleMs = nextVisibleAtMs.Value - nowMs;

                        if (untilVisibleMs <= 0)
                        {
                            // Something claims to be visible now.
                            // Don't do WaitAsync(0) — just loop and try fulfill again.
                            // Add a tiny delay to avoid burning CPU if READY head is stale repeatedly.
                            await Task.Delay(5, stopCt);
                            continue;
                        }

                        var visibleDelay = TimeSpan.FromMilliseconds(untilVisibleMs);
                        if (visibleDelay < delay)
                            delay = visibleDelay;
                    }

                    // IMPORTANT: still wake on new requests or explicit signals
                    var waitRequestTask = _requests.Reader.WaitToReadAsync(stopCt).AsTask();
                    var waitSignalTask = _newMessageSignal.WaitAsync(delay, stopCt);

                    await Task.WhenAny(waitRequestTask, waitSignalTask);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                while (_pending.Count > 0)
                {
                    var req = _pending.Dequeue();
                    req.Tcs.TrySetException(ex);
                }

                _requests.Writer.TryComplete(ex);
                throw;
            }
            finally
            {
                while (_pending.Count > 0)
                {
                    var req = _pending.Dequeue();
                    req.Tcs.TrySetCanceled(req.CancellationToken);
                }
            }
        }

        private bool TryFulfillPending()
        {
            var didAnything = false;

            int count = _pending.Count;
            for (int i = 0; i < count; i++)
            {
                var req = _pending.Dequeue();

                if (req.CancellationToken.IsCancellationRequested)
                {
                    req.Tcs.TrySetCanceled(req.CancellationToken);
                    didAnything = true;
                    continue;
                }

                var now = DateTimeOffset.UtcNow;

                var results = new List<ReceivedMessage>(Math.Min(req.MaxMessages, 50));
                var (delivered, cleaned) = _engine.TryReceiveBatchNoLock(
                    _queue, req.MaxMessages, req.VisibilityTimeoutSeconds, results);

                if (delivered > 0)
                {
                    req.Tcs.TrySetResult(results);
                    didAnything = true;
                    continue;
                }

                // IMPORTANT: if caller is not waiting anymore, complete NOW
                // even if we just cleaned stale READY keys.
                if (now >= req.Deadline)
                {
                    req.Tcs.TrySetResult([]);
                    didAnything = true;
                    continue;
                }

                // If we cleaned something, we made progress: keep waiting, but loop again soon.
                if (cleaned > 0)
                {
                    didAnything = true;
                    _pending.Enqueue(req);
                    continue;
                }

                // Still waiting
                _pending.Enqueue(req);
            }

            return didAnything;
        }

        private DateTimeOffset GetEarliestDeadline()
        {
            var earliest = DateTimeOffset.MaxValue;
            foreach (var req in _pending)
                if (req.Deadline < earliest) earliest = req.Deadline;
            return earliest;
        }

        private long _lastActivityMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

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
            _requests.Writer.TryComplete();
            _newMessageSignal.Dispose();

            try { _loop.Wait(TimeSpan.FromSeconds(2)); } catch { }
            _stop.Dispose();
        }

        private readonly record struct ReceiveRequest(
            int MaxMessages,
            int VisibilityTimeoutSeconds,
            DateTimeOffset Deadline,
            CancellationToken CancellationToken,
            TaskCompletionSource<IReadOnlyList<ReceivedMessage>> Tcs);
    }
}