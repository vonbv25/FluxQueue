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
using System.Buffers.Binary;

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
    [Explicit("Currently having some issues.")]
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
        var staleReadyKey = ReadyKey(queue, nowMs, msgId);
        db.Put(staleReadyKey, Array.Empty<byte>(), cfReady);

        // Should NOT deliver it because msg is inflight; stale ready index should be cleaned
        var second = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(second.Count, Is.EqualTo(0), "Should not deliver a message that is currently inflight.");

        // Verify stale READY entry was cleaned up (no ready keys at all in this queue right now)
        Assert.That(CountKeysWithPrefix(db, cfReady, ReadyPrefixKey(queue)), Is.EqualTo(0),
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

        // Inject a stale inflight key with an earlier "until" that does NOT match message's actual lease
        var staleUntil = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 5_000; // already expired
        var staleInflightKey = InflightKey(queue, staleUntil, msgId);
        db.Put(staleInflightKey, Array.Empty<byte>(), cfInflight);

        // Sweep should remove stale inflight key and NOT requeue/DLQ the message.
        var processed = await engine.SweepExpiredAsync(queue, maxToProcess: 1000);
        Assert.That(processed, Is.GreaterThanOrEqualTo(1));

        // Verify stale inflight key got removed
        var stillThere = db.Get(staleInflightKey, cfInflight);
        Assert.That(stillThere, Is.Null, "Expected stale inflight key to be deleted.");

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
        await Task.Delay(1500);
        await engine.SweepExpiredAsync(queue, maxToProcess: 1000);

        // Old receipt should NOT ack anymore
        var ackOld = await engine.AckAsync(queue, oldReceipt);
        Assert.That(ackOld, Is.False, "Old receipt must not ack after redrive.");

        // Message should be receivable again with a NEW receipt
        // Long-poll to cover backoff (~2s) + jitter
        var r2 = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 10, waitSeconds: 8);
        Assert.That(r2.Count, Is.EqualTo(1));

        Assert.That(r2[0].ReceiptHandle, Is.Not.EqualTo(oldReceipt), "Expected a new receipt after redrive.");

        // Cleanup: ack new receipt
        var ackNew = await engine.AckAsync(queue, r2[0].ReceiptHandle);
        Assert.That(ackNew, Is.True);
    }

    [Test]
    public async Task ReceiveAsync_should_return_empty_when_queue_is_empty()
    {
        using var engine = new QueueEngine(_dir);

        var r = await engine.ReceiveAsync("empty", maxMessages: 1, visibilityTimeoutSeconds: 5, waitSeconds: 0);
        Assert.That(r.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task ReceiveAsync_should_not_deliver_before_delay_and_should_deliver_after_delay()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "delay";
        var payload = Encoding.UTF8.GetBytes("later");
        await engine.SendAsync(queue, payload, delaySeconds: 2, maxReceiveCount: 5);

        // Immediately: should not deliver
        var r0 = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 5, waitSeconds: 0);
        Assert.That(r0.Count, Is.EqualTo(0));

        // Long poll: should deliver once visible
        var r1 = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 5, waitSeconds: 5);
        Assert.That(r1.Count, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(r1[0].Payload), Is.EqualTo("later"));

        // Cleanup
        Assert.That(await engine.AckAsync(queue, r1[0].ReceiptHandle), Is.True);
    }

    [Test]
    public async Task ReceiveAsync_long_poll_should_wake_up_when_message_arrives()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "lp";
        var started = DateTimeOffset.UtcNow;

        var receiveTask = engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 5, waitSeconds: 10);

        // Wait a bit then send
        await Task.Delay(250);
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("ping"));

        var r = await receiveTask;
        var elapsedMs = (DateTimeOffset.UtcNow - started).TotalMilliseconds;

        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(r[0].Payload), Is.EqualTo("ping"));
        Assert.That(elapsedMs, Is.LessThan(5000), "Long poll should return soon after message arrives.");

        Assert.That(await engine.AckAsync(queue, r[0].ReceiptHandle), Is.True);
    }

    [Test]
    public async Task ReceiveAsync_should_respect_maxMessages_and_not_overdeliver()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "batch";
        for (int i = 0; i < 5; i++)
            await engine.SendAsync(queue, Encoding.UTF8.GetBytes($"m{i}"));

        var r = await engine.ReceiveAsync(queue, maxMessages: 2, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(r.Count, Is.EqualTo(2));

        // Ack both
        foreach (var msg in r)
            Assert.That(await engine.AckAsync(queue, msg.ReceiptHandle), Is.True);

        // Still have 3 remaining
        var r2 = await engine.ReceiveAsync(queue, maxMessages: 10, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(r2.Count, Is.EqualTo(3));

        foreach (var msg in r2)
            Assert.That(await engine.AckAsync(queue, msg.ReceiptHandle), Is.True);
    }

    [Test]
    public async Task AckAsync_should_return_false_for_unknown_receipt()
    {
        using var engine = new QueueEngine(_dir);

        var ok = await engine.AckAsync("orders", "not-a-real-receipt");
        Assert.That(ok, Is.False);
    }

    [Test]
    public async Task AckAsync_should_fail_if_receipt_is_for_old_lease_even_if_message_still_exists()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "lease";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("x"));

        // Receive with short lease
        var r1 = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 1, waitSeconds: 0);
        Assert.That(r1.Count, Is.EqualTo(1));
        var oldReceipt = r1[0].ReceiptHandle;

        // Let lease expire + sweep (causes redrive with backoff)
        await Task.Delay(1500);
        await engine.SweepExpiredAsync(queue);

        // Old receipt must fail
        Assert.That(await engine.AckAsync(queue, oldReceipt), Is.False);

        // Eventually receivable again with new receipt (backoff ~2s + jitter)
        var r2 = await engine.ReceiveAsync(queue, maxMessages: 1, visibilityTimeoutSeconds: 10, waitSeconds: 8);
        Assert.That(r2.Count, Is.EqualTo(1));
        Assert.That(r2[0].ReceiptHandle, Is.Not.EqualTo(oldReceipt));

        Assert.That(await engine.AckAsync(queue, r2[0].ReceiptHandle), Is.True);
    }

    [Test]
    public async Task SweepExpired_should_redrive_until_MaxReceiveCount_then_move_to_DLQ()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "dlq";
        // maxReceiveCount = 2 => after 2 receives, next sweep should DLQ when expired
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("boom"), delaySeconds: 0, maxReceiveCount: 2);

        // 1st receive
        var r1 = await engine.ReceiveAsync(queue, 1, visibilityTimeoutSeconds: 1, waitSeconds: 0);
        Assert.That(r1.Count, Is.EqualTo(1));
        Assert.That(r1[0].ReceiveCount, Is.EqualTo(1));

        await Task.Delay(1500);
        await engine.SweepExpiredAsync(queue);

        // 2nd receive (after backoff)
        var r2 = await engine.ReceiveAsync(queue, 1, visibilityTimeoutSeconds: 1, waitSeconds: 8);
        Assert.That(r2.Count, Is.EqualTo(1));
        Assert.That(r2[0].ReceiveCount, Is.EqualTo(2));

        await Task.Delay(1500);
        await engine.SweepExpiredAsync(queue);

        // Now should NOT be receivable anymore (it should have DLQ’d)
        var r3 = await engine.ReceiveAsync(queue, 1, visibilityTimeoutSeconds: 1, waitSeconds: 2);
        Assert.That(r3.Count, Is.EqualTo(0));

        // Verify DLQ has at least one entry via RocksDB CF
        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfDlq = engine.GetPrivateField<ColumnFamilyHandle>("_cfDlq");

        // DLQ prefix is type 'd' with your binary key format
        var dlqPrefix = PrefixKey(queue, (byte)'d');
        Assert.That(CountKeysWithPrefix(db, cfDlq, dlqPrefix), Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task Queues_should_be_isolated_between_names()
    {
        using var engine = new QueueEngine(_dir);

        await engine.SendAsync("q1", Encoding.UTF8.GetBytes("a"));
        await engine.SendAsync("q2", Encoding.UTF8.GetBytes("b"));

        var r1 = await engine.ReceiveAsync("q1", 1, 30, 0);
        var r2 = await engine.ReceiveAsync("q2", 1, 30, 0);

        Assert.That(r1.Count, Is.EqualTo(1));
        Assert.That(r2.Count, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(r1[0].Payload), Is.EqualTo("a"));
        Assert.That(Encoding.UTF8.GetString(r2[0].Payload), Is.EqualTo("b"));

        Assert.That(await engine.AckAsync("q1", r1[0].ReceiptHandle), Is.True);
        Assert.That(await engine.AckAsync("q2", r2[0].ReceiptHandle), Is.True);
    }

    [Test]
    public void ReceiveAsync_should_honor_cancellation_token_while_waiting()
    {
        using var engine = new QueueEngine(_dir);

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(200);

        var ex = Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await engine.ReceiveAsync("cancel", maxMessages: 1, visibilityTimeoutSeconds: 5, waitSeconds: 10, ct: cts.Token);
        });

        Assert.That(ex!.CancellationToken, Is.EqualTo(cts.Token));
    }

    [Test]
    public async Task ReceiveAsync_should_cleanup_orphan_ready_index_when_msg_missing()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orph-ready";
        var payload = Encoding.UTF8.GetBytes("x");
        var msgId = await engine.SendAsync(queue, payload);

        // Delete the msg record but leave ready index
        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfMsg = engine.GetPrivateField<ColumnFamilyHandle>("_cfMsg");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");

        var msgKey = MsgKey(queue, msgId);
        db.Remove(msgKey, cfMsg);

        // Now receive should see ready key, discover msg missing, and delete ready key
        var r = await engine.ReceiveAsync(queue, 1, 30, 0);
        Assert.That(r.Count, Is.EqualTo(0));

        // Verify no ready keys remain
        Assert.That(CountKeysWithPrefix(db, cfReady, PrefixKey(queue, (byte)'r')), Is.EqualTo(0));
    }

    [Test]
    public async Task SweepExpired_should_cleanup_orphan_inflight_index_when_msg_missing()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orph-inflight";
        var msgId = await engine.SendAsync(queue, Encoding.UTF8.GetBytes("y"));

        // Receive so it creates inflight index
        var r = await engine.ReceiveAsync(queue, 1, visibilityTimeoutSeconds: 10, waitSeconds: 0);
        Assert.That(r.Count, Is.EqualTo(1));

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfMsg = engine.GetPrivateField<ColumnFamilyHandle>("_cfMsg");
        var cfInflight = engine.GetPrivateField<ColumnFamilyHandle>("_cfInflight");

        // Delete message record but leave inflight index
        db.Remove(MsgKey(queue, msgId), cfMsg);

        // Inject an expired inflight key (so sweep will visit it)
        var expiredUntil = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1000;
        var inflightKey = InflightKey(queue, expiredUntil, msgId);
        db.Put(inflightKey, Array.Empty<byte>(), cfInflight);

        await engine.SweepExpiredAsync(queue);

        // The orphan inflight key should be removed
        Assert.That(db.Get(inflightKey, cfInflight), Is.Null);
    }

    // ============================================================
    // (1) ORDERING TESTS
    // ============================================================

    [Test]
    public async Task Messages_Should_BeReceived_InSendOrder_When_VisibleAtMs_Differs()
    {
        // This test passes even with ReadyKey = [visibleAtMs][guid],
        // because the time component dominates ordering.
        using var engine = new QueueEngine(_dir);

        var q = "order-time";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("1"));
        await Task.Delay(3); // ensure different VisibleAtMs
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("2"));
        await Task.Delay(3);
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("3"));

        var r1 = await engine.ReceiveAsync(q, waitSeconds: 0);
        var r2 = await engine.ReceiveAsync(q, waitSeconds: 0);
        var r3 = await engine.ReceiveAsync(q, waitSeconds: 0);

        Assert.That(Encoding.UTF8.GetString(r1[0].Payload), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString(r2[0].Payload), Is.EqualTo("2"));
        Assert.That(Encoding.UTF8.GetString(r3[0].Payload), Is.EqualTo("3"));

        Assert.That(await engine.AckAsync(q, r1[0].ReceiptHandle), Is.True);
        Assert.That(await engine.AckAsync(q, r2[0].ReceiptHandle), Is.True);
        Assert.That(await engine.AckAsync(q, r3[0].ReceiptHandle), Is.True);
    }

    [Test]
    //[Explicit("Expected to FAIL until you add an enqueue/sequence component to ReadyKey for strict FIFO within same millisecond.")]
    public async Task Messages_Should_BeReceived_InStrictSendOrder_Even_When_SameVisibleAtMs()
    {
        // Why this is Explicit:
        // With ReadyKey = [visibleAtMs][guid], messages that share visibleAtMs will be ordered by GUID bytes,
        // NOT by send order.
        //
        // If you later change ReadyKey to [visibleAtMs][enqueueSeq][guid],
        // this test should pass and proves strict FIFO.
        using var engine = new QueueEngine(_dir);

        var q = "order-same-ms";

        // Try to land in same ms (best effort; in CI might not always).
        for (int i = 1; i <= 20; i++)
            await engine.SendAsync(q, Encoding.UTF8.GetBytes(i.ToString()));

        var got = new List<int>();
        for (int i = 0; i < 20; i++)
        {
            var r = await engine.ReceiveAsync(q, waitSeconds: 0);
            got.Add(int.Parse(Encoding.UTF8.GetString(r[0].Payload)));
            await engine.AckAsync(q, r[0].ReceiptHandle);
        }

        Assert.That(got, Is.EqualTo(Enumerable.Range(1, 20).ToArray()));
    }

    // ============================================================
    // (2) DELAY SEMANTICS
    // ============================================================

    [Test]
    public async Task DelayedMessage_Should_NotDeliver_BeforeDelay()
    {
        using var engine = new QueueEngine(_dir);

        var q = "delay";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("delayed"), delaySeconds: 2);

        // Immediate receive should not return it
        var r0 = await engine.ReceiveAsync(q, waitSeconds: 0);
        Assert.That(r0.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task DelayedMessage_Should_Deliver_AfterDelay_WithLongPoll()
    {
        using var engine = new QueueEngine(_dir);

        var q = "delay2";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("delayed"), delaySeconds: 2);

        // Long poll: if your actor wake logic doesn't handle "time becomes visible" by itself,
        // this may be flaky or fail until you add a timer/wakeup strategy.
        // It's still a valuable test: it codifies desired behavior.
        var r = await engine.ReceiveAsync(q, waitSeconds: 6, visibilityTimeoutSeconds: 5);
        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(r[0].Payload), Is.EqualTo("delayed"));

        Assert.That(await engine.AckAsync(q, r[0].ReceiptHandle), Is.True);
    }

    // ============================================================
    // (3) ORPHAN / PARTIAL-INDEX / ATOMICITY EXPECTATIONS
    // ============================================================

    [Test]
    public async Task ReceiveAsync_Should_CleanupReadyKey_When_MessageMissing()
    {
        using var engine = new QueueEngine(_dir);

        var q = "orphan-ready";
        var msgId = await engine.SendAsync(q, Encoding.UTF8.GetBytes("x"));

        // Delete msg record but leave READY index behind (orphan ready)
        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfMsg = engine.GetPrivateField<ColumnFamilyHandle>("_cfMsg");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");

        db.Remove(MsgKey(q, msgId), cfMsg);

        // Receive should notice msgVal == null and remove readyKey
        var r = await engine.ReceiveAsync(q, waitSeconds: 0);
        Assert.That(r.Count, Is.EqualTo(0));

        Assert.That(CountKeysWithPrefix(db, cfReady, ReadyPrefixKey(q)), Is.EqualTo(0),
            "Expected orphan READY index entry to be removed.");
    }

    [Test]
    public async Task AckAsync_Should_ReturnFalse_When_ReceiptOrphaned()
    {
        using var engine = new QueueEngine(_dir);

        var q = "orphan-receipt";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("x"));

        var r = await engine.ReceiveAsync(q, visibilityTimeoutSeconds: 10, waitSeconds: 0);
        Assert.That(r.Count, Is.EqualTo(1));

        var receipt = r[0].ReceiptHandle;

        // Delete msg but keep receipt (orphan receipt)
        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfMsg = engine.GetPrivateField<ColumnFamilyHandle>("_cfMsg");

        db.Remove(MsgKey(q, r[0].MessageId), cfMsg);

        var ok = await engine.AckAsync(q, receipt);
        Assert.That(ok, Is.False, "Ack should fail if msg is gone; receipt should be cleaned by engine.");
    }

    [Test]
    public async Task SweepExpired_Should_CleanupInflightKey_When_MessageMissing()
    {
        using var engine = new QueueEngine(_dir);

        var q = "orphan-inflight";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("x"));

        var r = await engine.ReceiveAsync(q, visibilityTimeoutSeconds: 1, waitSeconds: 0);
        Assert.That(r.Count, Is.EqualTo(1));

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfMsg = engine.GetPrivateField<ColumnFamilyHandle>("_cfMsg");
        var cfInflight = engine.GetPrivateField<ColumnFamilyHandle>("_cfInflight");

        // Delete msg record but leave inflight key
        db.Remove(MsgKey(q, r[0].MessageId), cfMsg);

        await Task.Delay(1200);
        await engine.SweepExpiredAsync(q);

        // Inflight index should be cleaned
        Assert.That(CountKeysWithPrefix(db, cfInflight, InflightPrefixKey(q)), Is.EqualTo(0));
    }

    // ============================================================
    // (4) MAX RECEIVE COUNT + DLQ
    // ============================================================

    [Test]
    public async Task Message_Should_MoveToDLQ_After_MaxReceiveCount_Reached()
    {
        using var engine = new QueueEngine(_dir);

        var q = "dlq";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("boom"), delaySeconds: 0, maxReceiveCount: 2);

        // Receive #1: do not ack -> expire -> sweep -> requeue
        var r1 = await engine.ReceiveAsync(q, visibilityTimeoutSeconds: 1, waitSeconds: 0);
        Assert.That(r1.Count, Is.EqualTo(1));
        await Task.Delay(1200);
        await engine.SweepExpiredAsync(q);

        // Long poll enough to cover backoff (~2s + jitter)
        var r2 = await engine.ReceiveAsync(q, visibilityTimeoutSeconds: 1, waitSeconds: 8);
        Assert.That(r2.Count, Is.EqualTo(1));
        Assert.That(r2[0].ReceiveCount, Is.EqualTo(2));
        await Task.Delay(1200);

        // Sweep again: now ReceiveCount >= MaxReceiveCount => DLQ
        await engine.SweepExpiredAsync(q);

        // Assert no READY/INFLIGHT left
        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");
        var cfInflight = engine.GetPrivateField<ColumnFamilyHandle>("_cfInflight");
        var cfDlq = engine.GetPrivateField<ColumnFamilyHandle>("_cfDlq");

        Assert.That(CountKeysWithPrefix(db, cfReady, ReadyPrefixKey(q)), Is.EqualTo(0));
        Assert.That(CountKeysWithPrefix(db, cfInflight, InflightPrefixKey(q)), Is.EqualTo(0));

        // Assert DLQ has at least one entry
        Assert.That(CountKeysWithPrefix(db, cfDlq, DlqPrefixKey(q)), Is.GreaterThanOrEqualTo(1),
            "Expected message to be in DLQ after max receive count reached.");
    }

    // ============================================================
    // (5) MULTI-QUEUE ISOLATION
    // ============================================================

    [Test]
    public async Task TwoQueues_Should_Not_Block_And_Not_CrossDeliver()
    {
        using var engine = new QueueEngine(_dir);

        var qa = "qa";
        var qb = "qb";

        await engine.SendAsync(qa, Encoding.UTF8.GetBytes("A1"));
        await engine.SendAsync(qb, Encoding.UTF8.GetBytes("B1"));

        // Concurrent receives on different queues
        var ta = engine.ReceiveAsync(qa, waitSeconds: 2);
        var tb = engine.ReceiveAsync(qb, waitSeconds: 2);

        await Task.WhenAll(ta, tb);

        Assert.That(ta.Result.Count, Is.EqualTo(1));
        Assert.That(tb.Result.Count, Is.EqualTo(1));

        Assert.That(Encoding.UTF8.GetString(ta.Result[0].Payload), Is.EqualTo("A1"));
        Assert.That(Encoding.UTF8.GetString(tb.Result[0].Payload), Is.EqualTo("B1"));

        Assert.That(await engine.AckAsync(qa, ta.Result[0].ReceiptHandle), Is.True);
        Assert.That(await engine.AckAsync(qb, tb.Result[0].ReceiptHandle), Is.True);
    }



    // ---------------- helpers ----------------

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static bool StartsWith(byte[] data, byte[] prefix)
    {
        if (data.Length < prefix.Length) return false;
        for (int i = 0; i < prefix.Length; i++)
            if (data[i] != prefix[i]) return false;
        return true;
    }
    private static byte[] DlqPrefixKey(string queue)
    {
        var qb = QueueBytes(queue);
        var key = new byte[4 + qb.Length];
        WriteHeader(key, (byte)'d', qb);
        return key;
    }
    private static byte[] InflightPrefixKey(string queue)
    {
        var qb = QueueBytes(queue);
        var key = new byte[4 + qb.Length];
        WriteHeader(key, (byte)'i', qb);
        return key;
    }
    // Must match QueueEngine key encoding
    private const byte KEYV = 1;

    private static byte[] QueueBytes(string queue) => Encoding.UTF8.GetBytes(queue);

    private static int WriteHeader(Span<byte> dst, byte type, ReadOnlySpan<byte> queueBytes)
    {
        dst[0] = KEYV;
        dst[1] = type;
        BinaryPrimitives.WriteUInt16BigEndian(dst.Slice(2, 2), (ushort)queueBytes.Length);
        queueBytes.CopyTo(dst.Slice(4));
        return 4 + queueBytes.Length;
    }

    private static void WriteInt64BE(Span<byte> dst, long value) =>
        BinaryPrimitives.WriteInt64BigEndian(dst, value);

    private static void WriteGuid(Span<byte> dst, Guid g)
    {
        Span<byte> tmp = stackalloc byte[16];
        g.TryWriteBytes(tmp);
        tmp.CopyTo(dst);
    }

    private static byte[] PrefixKey(string queue, byte type)
    {
        var qb = QueueBytes(queue);
        var key = new byte[4 + qb.Length];
        WriteHeader(key, type, qb);
        return key;
    }

    private static byte[] MsgKey(string queue, string msgId)
    {
        var qb = QueueBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        var key = new byte[4 + qb.Length + 16];
        var o = WriteHeader(key, (byte)'m', qb);
        WriteGuid(key.AsSpan(o, 16), gid);
        return key;
    }

    private static byte[] ReadyPrefixKey(string queue)
    {
        var qb = QueueBytes(queue);
        var key = new byte[4 + qb.Length];
        WriteHeader(key, (byte)'r', qb);
        return key;
    }

    private static byte[] ReadyKey(string queue, long visibleAtMs, string msgId)
    {
        var qb = QueueBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        var key = new byte[4 + qb.Length + 8 + 16];
        var o = WriteHeader(key, (byte)'r', qb);
        WriteInt64BE(key.AsSpan(o, 8), visibleAtMs); o += 8;
        WriteGuid(key.AsSpan(o, 16), gid);
        return key;
    }

    private static byte[] InflightKey(string queue, long inflightUntilMs, string msgId)
    {
        var qb = QueueBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        var key = new byte[4 + qb.Length + 8 + 16];
        var o = WriteHeader(key, (byte)'i', qb);
        WriteInt64BE(key.AsSpan(o, 8), inflightUntilMs); o += 8;
        WriteGuid(key.AsSpan(o, 16), gid);
        return key;
    }

    private static int CountKeysWithPrefix(RocksDb db, ColumnFamilyHandle cf, byte[] prefix)
    {
        int count = 0;
        using var it = db.NewIterator(cf);
        it.Seek(prefix);

        while (it.Valid())
        {
            var k = it.Key();
            if (k.Length < prefix.Length) break;

            bool match = true;
            for (int i = 0; i < prefix.Length; i++)
            {
                if (k[i] != prefix[i]) { match = false; break; }
            }

            if (!match) break;
            count++;
            it.Next();
        }

        return count;
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