using FluxQueue.Tests;
using NUnit.Framework.Legacy;
using RocksDbSharp;
using System.Buffers.Binary;
using System.Reflection;
using System.Text;

namespace FluxQueue.Core.Test;

[TestFixture]
public class QueueEngineReconcileTests
{
    private string _dir = default!;

    [SetUp]
    public void SetUp()
    {
        _dir = Path.Combine(Path.GetTempPath(), "fluxqueue-tests", "reconcile", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* ignore */ }
    }

    [Test]
    public async Task Reconcile_WipeTrue_Should_ClearIndexCFs_And_RebuildFromMsg()
    {
        using var engine = new QueueEngine(_dir);

        var q = "q1";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("a"));
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("b"));

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");
        var cfInflight = engine.GetPrivateField<ColumnFamilyHandle>("_cfInflight");
        var cfReceipt = engine.GetPrivateField<ColumnFamilyHandle>("_cfReceipt");

        // Add junk keys to each CF to prove wipe happens
        db.Put(Encoding.UTF8.GetBytes("junk-ready"), Array.Empty<byte>(), cfReady);
        db.Put(Encoding.UTF8.GetBytes("junk-inflight"), Array.Empty<byte>(), cfInflight);
        db.Put(Encoding.UTF8.GetBytes("junk-receipt"), Array.Empty<byte>(), cfReceipt);

        Assert.That(CountAllKeys(db, cfReady), Is.GreaterThan(0));
        Assert.That(CountAllKeys(db, cfInflight), Is.GreaterThan(0));
        Assert.That(CountAllKeys(db, cfReceipt), Is.GreaterThan(0));

        await engine.ReconcileAsync(new ReconcileOptions
        {
            WipeAndRebuildIndexes = true,
            MaxMessages = 5_000_000,
            SweepBatchSize = 1_000
        });

        // junk keys must be gone
        Assert.That(KeyExists(db, cfReady, Encoding.UTF8.GetBytes("junk-ready")), Is.False);
        Assert.That(KeyExists(db, cfInflight, Encoding.UTF8.GetBytes("junk-inflight")), Is.False);
        Assert.That(KeyExists(db, cfReceipt, Encoding.UTF8.GetBytes("junk-receipt")), Is.False);

        // and messages should still be receivable (indexes rebuilt)
        var got = await engine.ReceiveAsync(q, maxMessages: 10, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(got.Count, Is.EqualTo(2));

        // Cleanup
        foreach (var m in got)
            Assert.That(await engine.AckAsync(q, m.ReceiptHandle), Is.True);
    }

    [Test]
    public async Task Reconcile_Should_RecreateReadyIndex_ForReadyMessages()
    {
        using var engine = new QueueEngine(_dir);

        var q = "qready";
        var id = await engine.SendAsync(q, Encoding.UTF8.GetBytes("x"));

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");

        // Delete READY CF entirely to simulate lost index
        WipeCF(db, cfReady);

        // Sanity: should not deliver because index is gone
        var before = await engine.ReceiveAsync(q, 1, 30, 0);
        Assert.That(before.Count, Is.EqualTo(0));

        await engine.ReconcileAsync(new ReconcileOptions { WipeAndRebuildIndexes = false });

        var after = await engine.ReceiveAsync(q, 1, 30, 0);
        Assert.That(after.Count, Is.EqualTo(1));
        Assert.That(after[0].MessageId, Is.EqualTo(id));

        Assert.That(await engine.AckAsync(q, after[0].ReceiptHandle), Is.True);
    }

    [Test]
    public async Task Reconcile_Should_RecreateInflightAndReceipt_ForUnexpiredInflightMessages()
    {
        using var engine = new QueueEngine(_dir);

        var q = "qinflight";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("x"));

        // Put into inflight
        var got = await engine.ReceiveAsync(q, 1, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(got.Count, Is.EqualTo(1));
        var msgId = got[0].MessageId;
        var receipt = got[0].ReceiptHandle;

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfInflight = engine.GetPrivateField<ColumnFamilyHandle>("_cfInflight");
        var cfReceipt = engine.GetPrivateField<ColumnFamilyHandle>("_cfReceipt");

        // Simulate index loss (wipe inflight & receipt CF)
        WipeCF(db, cfInflight);
        WipeCF(db, cfReceipt);

        // Ack should fail (receipt missing)
        Assert.That(await engine.AckAsync(q, receipt), Is.False);

        // Reconcile should rebuild inflight + receipt because it's not expired yet
        await engine.ReconcileAsync(new ReconcileOptions { WipeAndRebuildIndexes = false });

        // Ack should work again (receipt rebuilt)
        Assert.That(await engine.AckAsync(q, receipt), Is.True);

        // Message removed => receive empty
        var again = await engine.ReceiveAsync(q, 1, 30, 0);
        Assert.That(again.Count, Is.EqualTo(0));
        Assert.That(again, Is.Empty);
    }

    [Test]
    public async Task Reconcile_Should_DeleteCorruptMsgEnvelopes_FromMsgCF()
    {
        using var engine = new QueueEngine(_dir);

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfMsg = engine.GetPrivateField<ColumnFamilyHandle>("_cfMsg");

        // Put a corrupt msg record in CF_MSG
        var corruptKey = Encoding.UTF8.GetBytes("corrupt-key");
        db.Put(corruptKey, new byte[] { 1, 2, 3 }, cfMsg);

        await engine.ReconcileAsync(new ReconcileOptions { WipeAndRebuildIndexes = false, MaxMessages = 10_000 });

        Assert.That(KeyExists(db, cfMsg, corruptKey), Is.False, "Corrupt msg should be deleted from CF_MSG.");
    }

    [Test]
    public async Task Reconcile_Should_PopulateKnownQueues_FromMsgMeta()
    {
        using var engine = new QueueEngine(_dir);

        await engine.SendAsync("qa", Encoding.UTF8.GetBytes("a"));
        await engine.SendAsync("qb", Encoding.UTF8.GetBytes("b"));

        // If KnownQueues was empty (fresh engine), reconcile should register queues from msg meta.
        await engine.ReconcileAsync(new ReconcileOptions { WipeAndRebuildIndexes = true });

        CollectionAssert.IsSupersetOf(engine.KnownQueues, new[] { "qa", "qb" });
    }

    [Test]
    public async Task Reconcile_Should_Respect_MaxMessages_Cap()
    {
        using var engine = new QueueEngine(_dir);

        var q = "qcap";
        for (int i = 0; i < 10; i++)
            await engine.SendAsync(q, Encoding.UTF8.GetBytes(i.ToString()));

        // Wipe READY so reconcile must rebuild it
        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");
        WipeCF(db, cfReady);

        await engine.ReconcileAsync(new ReconcileOptions
        {
            WipeAndRebuildIndexes = false,
            MaxMessages = 3, // only rebuild from 3 msg records
        });

        // We expect only up to 3 messages to become receivable.
        // (Others remain stuck because READY index wasn't rebuilt for them.)
        var got = await engine.ReceiveAsync(q, maxMessages: 50, visibilityTimeoutSeconds: 30, waitSeconds: 0);
        Assert.That(got.Count, Is.InRange(0, 3));

        // cleanup any received
        foreach (var m in got)
            await engine.AckAsync(q, m.ReceiptHandle);
    }

    [Test]
    public void Reconcile_Should_Honor_Cancellation()
    {
        using var engine = new QueueEngine(_dir);

        var q = "qcancel";
        // enough messages so reconcile has something to do
        for (int i = 0; i < 5_000; i++)
            engine.SendAsync(q, Encoding.UTF8.GetBytes("x")).GetAwaiter().GetResult();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await engine.ReconcileAsync(new ReconcileOptions
            {
                WipeAndRebuildIndexes = true,
                MaxMessages = 5_000_000,
                SweepBatchSize = 100
            }, cts.Token);
        });
    }

    [Test]
    public async Task Reconcile_WipeTrue_Should_NotThrow_On_LegacyReadyKeysPresent()
    {
        using var engine = new QueueEngine(_dir);

        var q = "qlegacy";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("x"));

        var db = engine.GetPrivateField<RocksDb>("_db");
        var cfReady = engine.GetPrivateField<ColumnFamilyHandle>("_cfReady");

        // Insert an OLD-format READY key (without seq) to simulate leftover from earlier version
        // This should not matter because WipeAndRebuildIndexes=true wipes READY CF first.
        var legacyKey = LegacyReadyKey_NoSeq(q, NowMs() - 1, Guid.NewGuid().ToString("N"));
        db.Put(legacyKey, Array.Empty<byte>(), cfReady);

        Assert.DoesNotThrowAsync(async () =>
        {
            await engine.ReconcileAsync(new ReconcileOptions { WipeAndRebuildIndexes = true });
        });
    }

    [Test]
    public async Task Reconcile_WipeTrue_Should_RequeueExpiredInflight_AfterSweep()
    {
        using var engine = new QueueEngine(_dir);

        var q = "qexpired";
        await engine.SendAsync(q, Encoding.UTF8.GetBytes("x"));

        var got = await engine.ReceiveAsync(q, 1, visibilityTimeoutSeconds: 1, waitSeconds: 0);
        Assert.That(got.Count, Is.EqualTo(1));

        await Task.Delay(TimeSpan.FromSeconds(3)); // let inflight expire

        await engine.ReconcileAsync(new ReconcileOptions
        {
            WipeAndRebuildIndexes = true,
            SweepBatchSize = 1000
        });

        // Backoff on redelivery is ~2.0–2.5s for receiveCount=1, so wait.
        var again = await engine.ReceiveAsync(q, 1, visibilityTimeoutSeconds: 30, waitSeconds: 5);

        Assert.That(again.Count, Is.EqualTo(1),
            "Expected expired inflight to be swept and requeued after reconcile (may be delayed by backoff).");
    }

    // -------------------------
    // Helpers
    // -------------------------

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static bool KeyExists(RocksDb db, ColumnFamilyHandle cf, byte[] key)
        => db.Get(key, cf) is not null;

    private static int CountAllKeys(RocksDb db, ColumnFamilyHandle cf)
    {
        int n = 0;
        using var it = db.NewIterator(cf);
        it.SeekToFirst();
        while (it.Valid())
        {
            n++;
            it.Next();
        }
        return n;
    }

    private static void WipeCF(RocksDb db, ColumnFamilyHandle cf)
    {
        using var it = db.NewIterator(cf);
        it.SeekToFirst();
        var batch = new WriteBatch();
        int i = 0;

        while (it.Valid())
        {
            batch.Delete(it.Key(), cf);
            it.Next();

            if (++i % 10_000 == 0)
            {
                db.Write(batch);
                batch.Clear();
            }
        }

        if (batch.Count() > 0)
            db.Write(batch);
    }

    // Builds a legacy ready key in the OLD format: header + visibleAt + guid (no seq)
    // Only used to simulate "old keys exist on disk".
    private static byte[] LegacyReadyKey_NoSeq(string queue, long visibleAtMs, string msgId)
    {
        var qb = Encoding.UTF8.GetBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        // header(4+q) + visibleAt(8) + guid(16)
        var key = new byte[4 + qb.Length + 8 + 16];
        key[0] = 1; // KEYV
        key[1] = (byte)'r';
        BinaryPrimitives.WriteUInt16BigEndian(key.AsSpan(2, 2), (ushort)qb.Length);
        qb.CopyTo(key.AsSpan(4));

        int o = 4 + qb.Length;
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(o, 8), visibleAtMs); o += 8;
        gid.TryWriteBytes(key.AsSpan(o, 16));
        return key;
    }
}