using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using RocksDbSharp;
using FluxQueue.Core;

namespace FluxQueue.Tests;

[TestFixture]
public class QueueEngineHardeningTests
{
    private string _dir = null!;

    [SetUp]
    public void SetUp()
    {
        _dir = Path.Combine(Path.GetTempPath(), "fluxqueue-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* ignore */ }
    }

    [Test]
    public async Task ReceiveAsync_concurrent_calls_should_not_double_claim_same_message()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        var payload = Encoding.UTF8.GetBytes("hello");
        await engine.SendAsync(queue, payload, delaySeconds: 0, maxReceiveCount: 5);

        // Run two concurrent receives. With the per-queue receive lock fix,
        // only one should get the message.
        var t1 = engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        var t2 = engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 30, waitSeconds: 0);

        await Task.WhenAll(t1, t2);

        var r1 = t1.Result;
        var r2 = t2.Result;

        var total = r1.Count + r2.Count;
        Assert.That(total, Is.EqualTo(1), "Expected exactly one delivery across concurrent receives.");

        // Optional: Ack it so the DB is clean
        var got = r1.Count == 1 ? r1[0] : r2[0];
        var ack = await engine.AckAsync(queue, got.ReceiptHandle);
        Assert.That(ack, Is.True);
    }

    [Test]
    public async Task ReceiveAsync_should_ignore_and_cleanup_stale_ready_index_when_msg_not_ready()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        var payload = Encoding.UTF8.GetBytes("stale-ready");
        var msgId = await engine.SendAsync(queue, payload, delaySeconds: 0, maxReceiveCount: 5);

        // First receive claims it (Ready -> Inflight)
        var first = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(first.Count, Is.EqualTo(1));
        var receipt = first[0].ReceiptHandle;

        // Inject a stale READY key for the same msgId (visibleAt <= now)
        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var staleReadyKey = Utf8($"q:{queue}:r:{nowMs:D13}:{msgId}");
        db.Put(staleReadyKey, Array.Empty<byte>(), cfReady);

        // Now receive again: if you added the guard
        //   if (msg.State != Ready) { Remove(readyKey); continue; }
        // it should NOT deliver it
        var second = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(second.Count, Is.EqualTo(0), "Should not deliver a message that is currently inflight.");

        // Verify stale READY entry was cleaned up
        Assert.That(CountKeysWithPrefix(db, cfReady, Utf8($"q:{queue}:r:")), Is.EqualTo(0),
            "Expected stale READY index entry to be removed.");

        // Cleanup: ack the original inflight lease
        var ack = await engine.AckAsync(queue, receipt);
        Assert.That(ack, Is.True);
    }

    [Test]
    public async Task SweepExpired_should_delete_stale_inflight_index_that_does_not_match_msg_lease()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        var payload = Encoding.UTF8.GetBytes("stale-inflight");
        var msgId = await engine.SendAsync(queue, payload, delaySeconds: 0, maxReceiveCount: 5);

        // Receive with a longer visibility timeout, so the real lease is in the future
        var received = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 10, waitSeconds: 0);
        Assert.That(received.Count, Is.EqualTo(1));

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfInflight = engine.GetPrivateField<ColumnFamilyHandle>("_cfInflight");

        // Inject a stale inflight key with an earlier "until" that does NOT match the message's actual lease
        var staleUntil = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 5_000; // already expired
        var staleInflightKey = Utf8($"q:{queue}:i:{staleUntil:D13}:{msgId}");
        db.Put(staleInflightKey, Array.Empty<byte>(), cfInflight);

        // Now call sweep. With the lease-match check:
        //   if (msg.InflightUntilMs != untilFromKey) { delete inflightKey; continue; }
        // it should remove stale inflight key and NOT requeue/DLQ the message.
        var processed = await engine.SweepExpiredAsync(queue, maxToProcess: 1000);
        Assert.That(processed, Is.GreaterThanOrEqualTo(1));

        // Verify stale inflight key got removed
        var prefix = Utf8($"q:{queue}:i:{staleUntil:D13}:{msgId}");
        Assert.That(db.Get(prefix, cfInflight), Is.Empty, "Expected stale inflight key to be deleted.");

        // The message should still be inflight (not receivable)
        var after = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 1, waitSeconds: 0);
        Assert.That(after.Count, Is.EqualTo(0), "Message should remain inflight under its real lease.");
    }

    [Test]
    public async Task SweepExpired_should_cleanup_receipt_so_old_receipt_cannot_ack_after_redrive()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        var payload = Encoding.UTF8.GetBytes("receipt-cleanup");

        await engine.SendAsync(queue, payload, delaySeconds: 0, maxReceiveCount: 5);

        // Receive with very short visibility
        var r1 = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 1, waitSeconds: 0);
        Assert.That(r1.Count, Is.EqualTo(1));
        var oldReceipt = r1[0].ReceiptHandle;

        // Let lease expire then sweep
        await Task.Delay(1200);
        await engine.SweepExpiredAsync(queue, maxToProcess: 1000);

        // Old receipt should NOT ack anymore (either because receipt was deleted or lease mismatch)
        var ackOld = await engine.AckAsync(queue, oldReceipt);
        Assert.That(ackOld, Is.False, "Old receipt must not ack after redrive.");

        // Message should be receivable again with a NEW receipt
        var r2 = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 10, waitSeconds: 0);
        Assert.That(r2.Count, Is.EqualTo(1));

        Assert.That(r2[0].ReceiptHandle, Is.Not.EqualTo(oldReceipt), "Expected a new receipt after redrive.");

        // Cleanup: ack new receipt
        var ackNew = await engine.AckAsync(queue, r2[0].ReceiptHandle);
        Assert.That(ackNew, Is.True);
    }

    // ---------------- helpers ----------------

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static int CountKeysWithPrefix(RocksDb db, ColumnFamilyHandle cf, byte[] prefix)
    {
        int count = 0;
        using var it = db.NewIterator(cf);
        it.Seek(prefix);
        while (it.Valid())
        {
            var k = it.Key();
            if (!StartsWith(k, prefix)) break;
            count++;
            it.Next();
        }
        return count;
    }

    private static bool StartsWith(byte[] data, byte[] prefix)
    {
        if (data.Length < prefix.Length) return false;
        for (int i = 0; i < prefix.Length; i++)
            if (data[i] != prefix[i]) return false;
        return true;
    }
}

internal static class ReflectionExtensions
{
    public static T GetPrivateField<T>(this object obj, string fieldName)
    {
        var f = obj.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        if (f is null) throw new InvalidOperationException($"Field not found: {fieldName}");
        return (T)f.GetValue(obj)!;
    }
}