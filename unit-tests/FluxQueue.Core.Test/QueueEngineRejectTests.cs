using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RocksDbSharp;

namespace FluxQueue.Core.Test;

[TestFixture]
public class QueueEngineRejectTests
{
    private string _dir = default!;

    [SetUp]
    public void SetUp()
    {
        _dir = Path.Combine(
            Path.GetTempPath(),
            "fluxqueue-tests",
            "reject",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(_dir);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch
        {
            // ignore cleanup errors on Windows file locking edge cases
        }
    }

    [Test]
    public async Task Reject_ValidInflightMessage_Should_MoveToDlq_And_RemoveFromQueue()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("hello"));

        var received = await engine.ReceiveAsync(
            queue,
            maxMessages: 1,
            visibilityTimeoutSeconds: 30,
            waitSeconds: 0);

        Assert.That(received, Has.Count.EqualTo(1));

        var msg = received[0];

        var rejected = await engine.RejectAsync(queue, msg.ReceiptHandle);
        Assert.That(rejected, Is.True);

        // No longer receivable
        var receiveAgain = await engine.ReceiveAsync(
            queue,
            maxMessages: 1,
            visibilityTimeoutSeconds: 30,
            waitSeconds: 0);

        Assert.That(receiveAgain, Is.Empty);

        // Ack on same receipt must now fail
        var ackAfterReject = await engine.AckAsync(queue, msg.ReceiptHandle);
        Assert.That(ackAfterReject, Is.False);

        // Verify exactly one DLQ record exists in isolated DB
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfDlq"), Is.EqualTo(1));

        // Message should be gone from msg CF
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfMsg"), Is.EqualTo(0));

        // No inflight / receipt should remain
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfInflight"), Is.EqualTo(0));
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfReceipt"), Is.EqualTo(0));
    }

    [Test]
    public async Task Reject_SameReceiptTwice_Should_ReturnFalse_SecondTime()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("hello"));

        var received = await engine.ReceiveAsync(queue);
        var msg = received.Single();

        var first = await engine.RejectAsync(queue, msg.ReceiptHandle);
        var second = await engine.RejectAsync(queue, msg.ReceiptHandle);

        Assert.That(first, Is.True);
        Assert.That(second, Is.False);
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfDlq"), Is.EqualTo(1));
    }

    [Test]
    public async Task Ack_AfterReject_Should_ReturnFalse()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("payload"));

        var received = await engine.ReceiveAsync(queue);
        var msg = received.Single();

        var rejected = await engine.RejectAsync(queue, msg.ReceiptHandle);
        Assert.That(rejected, Is.True);

        var ack = await engine.AckAsync(queue, msg.ReceiptHandle);
        Assert.That(ack, Is.False);
    }

    [Test]
    public async Task Reject_AfterAck_Should_ReturnFalse()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("payload"));

        var received = await engine.ReceiveAsync(queue);
        var msg = received.Single();

        var ack = await engine.AckAsync(queue, msg.ReceiptHandle);
        Assert.That(ack, Is.True);

        var reject = await engine.RejectAsync(queue, msg.ReceiptHandle);
        Assert.That(reject, Is.False);

        Assert.That(GetColumnFamilyEntryCount(engine, "_cfDlq"), Is.EqualTo(0));
    }

    [Test]
    public async Task Reject_StaleReceipt_AfterTimeoutAndSweep_Should_ReturnFalse()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("payload"));

        var firstReceive = await engine.ReceiveAsync(
            queue,
            maxMessages: 1,
            visibilityTimeoutSeconds: 1,
            waitSeconds: 0);

        var first = firstReceive.Single();

        await Task.Delay(TimeSpan.FromMilliseconds(1200));

        var swept = await engine.SweepExpiredAsync(queue, maxToProcess: 100);
        Assert.That(swept, Is.GreaterThanOrEqualTo(1));

        var rejectOldReceipt = await engine.RejectAsync(queue, first.ReceiptHandle);
        Assert.That(rejectOldReceipt, Is.False);

        var secondReceive = await engine.ReceiveAsync(
            queue,
            maxMessages: 1,
            visibilityTimeoutSeconds: 30,
            waitSeconds: 5);

        Assert.That(secondReceive, Has.Count.EqualTo(1));
        Assert.That(secondReceive[0].MessageId, Is.EqualTo(first.MessageId));
        Assert.That(secondReceive[0].ReceiptHandle, Is.Not.EqualTo(first.ReceiptHandle));
    }

    [Test]
    public async Task SweepExpired_AfterReject_Should_Not_Resurrect_Message()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("payload"));

        var received = await engine.ReceiveAsync(
            queue,
            maxMessages: 1,
            visibilityTimeoutSeconds: 1,
            waitSeconds: 0);

        var msg = received.Single();

        var rejected = await engine.RejectAsync(queue, msg.ReceiptHandle);
        Assert.That(rejected, Is.True);

        await Task.Delay(TimeSpan.FromMilliseconds(1200));

        var swept = await engine.SweepExpiredAsync(queue, maxToProcess: 100);

        // Might be zero because inflight entry is already gone, which is what we want.
        Assert.That(swept, Is.GreaterThanOrEqualTo(0));

        var receiveAgain = await engine.ReceiveAsync(
            queue,
            maxMessages: 1,
            visibilityTimeoutSeconds: 30,
            waitSeconds: 0);

        Assert.That(receiveAgain, Is.Empty);
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfDlq"), Is.EqualTo(1));
    }

    [Test]
    public async Task Concurrent_Ack_And_Reject_OnSameReceipt_Should_Have_ExactlyOneWinner()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("payload"));

        var received = await engine.ReceiveAsync(queue);
        var msg = received.Single();

        var ackTask = Task.Run(() => engine.AckAsync(queue, msg.ReceiptHandle));
        var rejectTask = Task.Run(() => engine.RejectAsync(queue, msg.ReceiptHandle));

        await Task.WhenAll(ackTask, rejectTask);

        var results = new[] { ackTask.Result, rejectTask.Result };

        Assert.That(results.Count(x => x), Is.EqualTo(1), "Exactly one of Ack or Reject should succeed.");
        Assert.That(results.Count(x => !x), Is.EqualTo(1), "Exactly one of Ack or Reject should fail.");

        // Queue should not have the message anymore regardless of who won.
        var receiveAgain = await engine.ReceiveAsync(queue);
        Assert.That(receiveAgain, Is.Empty);

        // If reject won, there will be one DLQ entry. If ack won, none.
        var dlqCount = GetColumnFamilyEntryCount(engine, "_cfDlq");
        Assert.That(dlqCount, Is.AnyOf(0, 1));

        var msgCount = GetColumnFamilyEntryCount(engine, "_cfMsg");
        Assert.That(msgCount, Is.EqualTo(0));

        var inflightCount = GetColumnFamilyEntryCount(engine, "_cfInflight");
        Assert.That(inflightCount, Is.EqualTo(0));

        var receiptCount = GetColumnFamilyEntryCount(engine, "_cfReceipt");
        Assert.That(receiptCount, Is.EqualTo(0));
    }

    [Test]
    public async Task Concurrent_TwoRejects_OnSameReceipt_Should_Have_ExactlyOneWinner()
    {
        using var engine = new QueueEngine(_dir);

        var queue = "orders";
        await engine.SendAsync(queue, Encoding.UTF8.GetBytes("payload"));

        var received = await engine.ReceiveAsync(queue);
        var msg = received.Single();

        var t1 = Task.Run(() => engine.RejectAsync(queue, msg.ReceiptHandle));
        var t2 = Task.Run(() => engine.RejectAsync(queue, msg.ReceiptHandle));

        await Task.WhenAll(t1, t2);

        var results = new[] { t1.Result, t2.Result };

        Assert.That(results.Count(x => x), Is.EqualTo(1));
        Assert.That(results.Count(x => !x), Is.EqualTo(1));

        Assert.That(GetColumnFamilyEntryCount(engine, "_cfDlq"), Is.EqualTo(1));
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfMsg"), Is.EqualTo(0));
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfInflight"), Is.EqualTo(0));
        Assert.That(GetColumnFamilyEntryCount(engine, "_cfReceipt"), Is.EqualTo(0));
    }

    [Test]
    public async Task Reject_Should_Not_Affect_Other_Queues()
    {
        using var engine = new QueueEngine(_dir);

        await engine.SendAsync("q1", Encoding.UTF8.GetBytes("a"));
        await engine.SendAsync("q2", Encoding.UTF8.GetBytes("b"));

        var q1Received = await engine.ReceiveAsync("q1");
        var q1Msg = q1Received.Single();

        var rejected = await engine.RejectAsync("q1", q1Msg.ReceiptHandle);
        Assert.That(rejected, Is.True);

        var q1Again = await engine.ReceiveAsync("q1");
        var q2Again = await engine.ReceiveAsync("q2");

        Assert.That(q1Again, Is.Empty);
        Assert.That(q2Again, Has.Count.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(q2Again[0].Payload), Is.EqualTo("b"));

        Assert.That(GetColumnFamilyEntryCount(engine, "_cfDlq"), Is.EqualTo(1));
    }

    // -------------------------
    // Reflection helpers
    // -------------------------

    private static int GetColumnFamilyEntryCount(QueueEngine engine, string cfFieldName)
    {
        var db = GetPrivateField<RocksDb>(engine, "_db");
        var cf = GetPrivateField<ColumnFamilyHandle>(engine, cfFieldName);

        int count = 0;
        using var it = db.NewIterator(cf);
        it.SeekToFirst();

        while (it.Valid())
        {
            count++;
            it.Next();
        }

        return count;
    }

    private static T GetPrivateField<T>(object instance, string fieldName)
    {
        var field = instance.GetType().GetField(
            fieldName,
            BindingFlags.Instance | BindingFlags.NonPublic);

        if (field is null)
            throw new InvalidOperationException($"Field '{fieldName}' was not found on {instance.GetType().FullName}.");

        return (T)field.GetValue(instance)!;
    }
}