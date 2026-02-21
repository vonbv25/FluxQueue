using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
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
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _queueSignals = new();

    private readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web);

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

        // handles[0] is "default"
        _cfMsg = _db.GetColumnFamily(CF_MSG);
        _cfReady = _db.GetColumnFamily(CF_READY);
        _cfInflight = _db.GetColumnFamily(CF_INFLIGHT);
        _cfReceipt = _db.GetColumnFamily(CF_RECEIPT);
        _cfDlq = _db.GetColumnFamily(CF_DLQ);
    }

    // -------- Public API --------

    public async Task<string> SendAsync(
        string queue,
        byte[] payload,
        int delaySeconds = 0,
        int maxReceiveCount = 5,
        CancellationToken ct = default)
    {
        ValidateQueue(queue);
        if (payload is null) throw new ArgumentNullException(nameof(payload));
        if (delaySeconds < 0) throw new ArgumentOutOfRangeException(nameof(delaySeconds));
        if (maxReceiveCount <= 0) throw new ArgumentOutOfRangeException(nameof(maxReceiveCount));

        ct.ThrowIfCancellationRequested();

        var nowMs = NowMs();
        var visibleAtMs = nowMs + delaySeconds * 1000L;
        var msgId = Guid.NewGuid().ToString("N");

        var record = new MessageRecord
        {
            Queue = queue,
            MessageId = msgId,
            PayloadBase64 = Convert.ToBase64String(payload),
            VisibleAtMs = visibleAtMs,
            InflightUntilMs = 0,
            ReceiveCount = 0,
            MaxReceiveCount = maxReceiveCount,
            State = MessageState.Ready,
            CreatedAtMs = nowMs,
            UpdatedAtMs = nowMs
        };

        var msgKey = MsgKey(queue, msgId);
        var readyKey = ReadyKey(queue, visibleAtMs, msgId);

        using var wb = new WriteBatch();
        wb.Put(msgKey, Serialize(record), _cfMsg);
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
        if (maxMessages <= 0) throw new ArgumentOutOfRangeException(nameof(maxMessages));
        if (visibilityTimeoutSeconds <= 0) throw new ArgumentOutOfRangeException(nameof(visibilityTimeoutSeconds));
        if (waitSeconds < 0) throw new ArgumentOutOfRangeException(nameof(waitSeconds));

        var results = new List<ReceivedMessage>(Math.Min(maxMessages, 50));

        var deadline = waitSeconds == 0
            ? DateTimeOffset.UtcNow
            : DateTimeOffset.UtcNow.AddSeconds(waitSeconds);

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var gotAny = TryReceiveBatch(queue, maxMessages, visibilityTimeoutSeconds, results);

            if (gotAny || DateTimeOffset.UtcNow >= deadline || waitSeconds == 0)
                break;

            // Long-poll: wait for signal or timeout
            var remaining = deadline - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero) break;

            var sem = _queueSignals.GetOrAdd(queue, _ => new SemaphoreSlim(0, int.MaxValue));
            try
            {
                await sem.WaitAsync(remaining, ct);
            }
            catch (OperationCanceledException) { throw; }
        }

        return results;
    }

    public Task<bool> AckAsync(string queue, string receiptHandle, CancellationToken ct = default)
    {
        ValidateQueue(queue);
        if (string.IsNullOrWhiteSpace(receiptHandle)) throw new ArgumentNullException(nameof(receiptHandle));

        ct.ThrowIfCancellationRequested();

        var receiptKey = ReceiptKey(queue, receiptHandle);
        var receiptVal = _db.Get(receiptKey, _cfReceipt);
        if (receiptVal is null) return Task.FromResult(false);

        var receipt = Deserialize<ReceiptRecord>(receiptVal);
        if (receipt is null) return Task.FromResult(false);

        var msgKey = MsgKey(queue, receipt.MessageId);
        var msgVal = _db.Get(msgKey, _cfMsg);
        if (msgVal is null)
        {
            // Message already gone; cleanup receipt
            _db.Remove(receiptKey, _cfReceipt);
            return Task.FromResult(false);
        }

        var msg = Deserialize<MessageRecord>(msgVal);
        if (msg is null) return Task.FromResult(false);

        // Validate: ensure the receipt is for the current inflight lease
        if (msg.State != MessageState.Inflight || msg.InflightUntilMs != receipt.InflightUntilMs)
            return Task.FromResult(false);

        var inflightKey = InflightKey(queue, msg.InflightUntilMs, msg.MessageId);

        using var wb = new WriteBatch();
        wb.Delete(receiptKey, _cfReceipt);
        wb.Delete(inflightKey, _cfInflight);
        wb.Delete(msgKey, _cfMsg); // dequeue-on-ack keeps hot set small
        _db.Write(wb);

        return Task.FromResult(true);
    }

    /// <summary>
    /// Sweeps expired inflight messages for a queue and requeues or DLQs them.
    /// Run this on a timer in your host.
    /// </summary>
    public Task<int> SweepExpiredAsync(
        string queue,
        int maxToProcess = 1000,
        CancellationToken ct = default)
    {
        ValidateQueue(queue);
        if (maxToProcess <= 0) throw new ArgumentOutOfRangeException(nameof(maxToProcess));

        ct.ThrowIfCancellationRequested();

        var nowMs = NowMs();
        var prefix = InflightPrefix(queue);
        var endKey = InflightKey(queue, nowMs, "\uffff"); // upper bound within now

        int processed = 0;

        using var it = _db.NewIterator(_cfInflight);
        it.Seek(prefix);

        var toHandle = new List<(byte[] inflightKey, string msgId)>(Math.Min(maxToProcess, 1024));

        // Collect first, then process (avoid iterator invalidation surprises)
        while (it.Valid() && processed + toHandle.Count < maxToProcess)
        {
            ct.ThrowIfCancellationRequested();

            var k = it.Key();
            if (!StartsWith(k, prefix)) break;

            // Stop once inflightUntil > now
            if (CompareBytes(k, endKey) > 0) break;

            var (msgId, _) = ParseInflightKey(queue, k);
            toHandle.Add((k, msgId));
            it.Next();
        }

        foreach (var (inflightKeyBytes, msgId) in toHandle)
        {
            ct.ThrowIfCancellationRequested();

            var msgKey = MsgKey(queue, msgId);
            var msgVal = _db.Get(msgKey, _cfMsg);
            if (msgVal is null)
            {
                // cleanup orphan inflight
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            var msg = Deserialize<MessageRecord>(msgVal);
            if (msg is null)
            {
                _db.Remove(inflightKeyBytes, _cfInflight);
                processed++;
                continue;
            }

            if (msg.State != MessageState.Inflight || msg.InflightUntilMs > nowMs)
            {
                processed++;
                continue;
            }

            // Redrive policy
            if (msg.ReceiveCount >= msg.MaxReceiveCount)
            {
                // Move to DLQ
                var failedAtMs = nowMs;
                var dlqKey = DlqKey(queue, failedAtMs, msg.MessageId);

                using var wb = new WriteBatch();
                wb.Delete(inflightKeyBytes, _cfInflight);
                wb.Delete(msgKey, _cfMsg);
                wb.Put(dlqKey, Serialize(new DeadLetterRecord
                {
                    Queue = queue,
                    MessageId = msg.MessageId,
                    PayloadBase64 = msg.PayloadBase64,
                    ReceiveCount = msg.ReceiveCount,
                    FailedAtMs = failedAtMs
                }), _cfDlq);
                _db.Write(wb);
            }
            else
            {
                // Requeue with backoff + jitter
                var delayMs = ComputeBackoffMs(msg.ReceiveCount);
                var visibleAt = nowMs + delayMs;

                msg.State = MessageState.Ready;
                msg.VisibleAtMs = visibleAt;
                msg.InflightUntilMs = 0;
                msg.UpdatedAtMs = nowMs;

                var readyKey = ReadyKey(queue, visibleAt, msg.MessageId);

                using var wb = new WriteBatch();
                wb.Delete(inflightKeyBytes, _cfInflight);
                wb.Put(msgKey, Serialize(msg), _cfMsg);
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
        foreach (var kv in _queueSignals) kv.Value.Dispose();
    }

    // -------- Internal receive logic --------

    private bool TryReceiveBatch(string queue, int maxMessages, int visibilityTimeoutSeconds, List<ReceivedMessage> output)
    {
        var nowMs = NowMs();
        var prefix = ReadyPrefix(queue);

        using var it = _db.NewIterator(_cfReady);
        it.Seek(prefix);

        int claimed = 0;
        var toClaim = new List<(byte[] readyKey, long visibleAt, string msgId)>(maxMessages);

        while (it.Valid() && claimed < maxMessages)
        {
            var k = it.Key();
            if (!StartsWith(k, prefix)) break;

            var (msgId, visibleAt) = ParseReadyKey(queue, k);
            if (visibleAt > nowMs) break; // earliest visible item is in the future

            toClaim.Add((k, visibleAt, msgId));
            claimed++;
            it.Next();
        }

        if (toClaim.Count == 0) return false;

        foreach (var (readyKeyBytes, _, msgId) in toClaim)
        {
            var msgKey = MsgKey(queue, msgId);
            var msgVal = _db.Get(msgKey, _cfMsg);
            if (msgVal is null)
            {
                // orphan index
                _db.Remove(readyKeyBytes, _cfReady);
                continue;
            }

            var msg = Deserialize<MessageRecord>(msgVal);
            if (msg is null)
            {
                _db.Remove(readyKeyBytes, _cfReady);
                continue;
            }

            // Claim: Ready -> Inflight
            var inflightUntil = nowMs + visibilityTimeoutSeconds * 1000L;
            var receiptHandle = NewReceiptHandle();

            msg.State = MessageState.Inflight;
            msg.ReceiveCount += 1;
            msg.InflightUntilMs = inflightUntil;
            msg.UpdatedAtMs = nowMs;

            var inflightKey = InflightKey(queue, inflightUntil, msgId);
            var receiptKey = ReceiptKey(queue, receiptHandle);

            using var wb = new WriteBatch();
            wb.Delete(readyKeyBytes, _cfReady);
            wb.Put(msgKey, Serialize(msg), _cfMsg);
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
                Payload: Convert.FromBase64String(msg.PayloadBase64),
                ReceiptHandle: receiptHandle,
                ReceiveCount: msg.ReceiveCount
            ));
        }

        return output.Count > 0;
    }

    // -------- Key helpers (byte[] keys are faster than strings) --------

    private static byte[] MsgKey(string queue, string msgId) =>
        Utf8($"q:{queue}:m:{msgId}");

    private static byte[] ReadyPrefix(string queue) =>
        Utf8($"q:{queue}:r:");

    private static byte[] ReadyKey(string queue, long visibleAtMs, string msgId) =>
        Utf8($"q:{queue}:r:{visibleAtMs:D13}:{msgId}");

    private static byte[] InflightPrefix(string queue) =>
        Utf8($"q:{queue}:i:");

    private static byte[] InflightKey(string queue, long inflightUntilMs, string msgId) =>
        Utf8($"q:{queue}:i:{inflightUntilMs:D13}:{msgId}");

    private static byte[] ReceiptKey(string queue, string receiptHandle) =>
        Utf8($"q:{queue}:h:{receiptHandle}");

    private static byte[] DlqKey(string queue, long failedAtMs, string msgId) =>
        Utf8($"q:{queue}:d:{failedAtMs:D13}:{msgId}");

    private static (string msgId, long visibleAtMs) ParseReadyKey(string queue, byte[] key)
    {
        // q:{queue}:r:{visibleAt}:{msgId}
        var s = Encoding.UTF8.GetString(key);
        var parts = s.Split(':', 6);
        // ["q", queue, "r", visibleAt, msgId] but split count depends on queue text; safer:
        // We'll parse by finding the ":r:" marker.
        var marker = $":r:";
        var idx = s.IndexOf(marker, StringComparison.Ordinal);
        var rest = s[(idx + marker.Length)..]; // {visibleAt}:{msgId}
        var c = rest.IndexOf(':');
        var visibleAt = long.Parse(rest[..c]);
        var msgId = rest[(c + 1)..];
        return (msgId, visibleAt);
    }

    private static (string msgId, long inflightUntilMs) ParseInflightKey(string queue, byte[] key)
    {
        // q:{queue}:i:{inflightUntil}:{msgId}
        var s = Encoding.UTF8.GetString(key);
        var marker = $":i:";
        var idx = s.IndexOf(marker, StringComparison.Ordinal);
        var rest = s[(idx + marker.Length)..]; // {until}:{msgId}
        var c = rest.IndexOf(':');
        var until = long.Parse(rest[..c]);
        var msgId = rest[(c + 1)..];
        return (msgId, until);
    }

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static bool StartsWith(byte[] data, byte[] prefix)
    {
        if (data.Length < prefix.Length) return false;
        for (int i = 0; i < prefix.Length; i++)
            if (data[i] != prefix[i]) return false;
        return true;
    }

    private static int CompareBytes(byte[] a, byte[] b)
    {
        int len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int d = a[i].CompareTo(b[i]);
            if (d != 0) return d;
        }
        return a.Length.CompareTo(b.Length);
    }

    private static string NewReceiptHandle()
    {
        Span<byte> bytes = stackalloc byte[24];
        RandomNumberGenerator.Fill(bytes);
        return Base64Url(bytes);
    }

    private static string Base64Url(ReadOnlySpan<byte> data)
    {
        var s = Convert.ToBase64String(data);
        return s.Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static long ComputeBackoffMs(int receiveCount)
    {
        // Exponential backoff with jitter (bounded)
        // attempt 1 => ~1s, 2 => ~2s, 3 => ~4s ... max 60s + jitter
        var exp = Math.Min(6, Math.Max(0, receiveCount)); // cap at 2^6=64
        var baseMs = (long)(1000 * Math.Pow(2, exp));
        baseMs = Math.Min(baseMs, 60_000);

        // jitter 0..500ms
        Span<byte> b = stackalloc byte[2];
        RandomNumberGenerator.Fill(b);
        var jitter = BinaryPrimitives.ReadUInt16LittleEndian(b) % 500;

        return baseMs + jitter;
    }

    private void Signal(string queue)
    {
        if (_queueSignals.TryGetValue(queue, out var sem))
            sem.Release();
    }

    private static void ValidateQueue(string queue)
    {
        if (string.IsNullOrWhiteSpace(queue)) throw new ArgumentNullException(nameof(queue));
        // Keep it simple/safe for key building:
        // allow letters, numbers, dash, underscore, dot
        foreach (var ch in queue)
        {
            var ok = char.IsLetterOrDigit(ch) || ch is '-' or '_' or '.';
            if (!ok) throw new ArgumentException("Queue name contains invalid characters.", nameof(queue));
        }
    }

    private byte[] Serialize<T>(T obj) => JsonSerializer.SerializeToUtf8Bytes(obj, _json);
    private T? Deserialize<T>(byte[] bytes) => JsonSerializer.Deserialize<T>(bytes, _json);

    // -------- DTOs --------

    public readonly record struct ReceivedMessage(string MessageId, byte[] Payload, string ReceiptHandle, int ReceiveCount);

    private enum MessageState : byte { Ready = 1, Inflight = 2, Dead = 3 }

    private sealed class MessageRecord
    {
        public string Queue { get; set; } = "";
        public string MessageId { get; set; } = "";
        public string PayloadBase64 { get; set; } = "";
        public long VisibleAtMs { get; set; }
        public long InflightUntilMs { get; set; }
        public int ReceiveCount { get; set; }
        public int MaxReceiveCount { get; set; }
        public MessageState State { get; set; }
        public long CreatedAtMs { get; set; }
        public long UpdatedAtMs { get; set; }
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
        public string PayloadBase64 { get; set; } = "";
        public int ReceiveCount { get; set; }
        public long FailedAtMs { get; set; }
    }
}
