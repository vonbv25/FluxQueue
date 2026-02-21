using NUnit.Framework;
using NUnit.Framework.Legacy;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FluxQueue.Core.Test;

[TestFixture]
public class QueueEngineTest
{
    private string _dbPath = null!;
    private QueueEngine _engine = null!;

    [SetUp]
    public void Setup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "queueengine-test-" + Guid.NewGuid());
        Directory.CreateDirectory(_dbPath);

        _engine = new QueueEngine(_dbPath);
    }

    [TearDown]
    public void TearDown()
    {
        _engine.Dispose();

        if (Directory.Exists(_dbPath))
            Directory.Delete(_dbPath, recursive: true);
    }

    [Test]
    public async Task Send_Receive_Ack_Should_Remove_Message()
    {
        var payload = Encoding.UTF8.GetBytes("hello world");

        var msgId = await _engine.SendAsync("test", payload);

        var received = await _engine.ReceiveAsync("test", maxMessages: 1, visibilityTimeoutSeconds: 30);

        Assert.That(received.Count, Is.EqualTo(1));
        Assert.That(received[0].MessageId, Is.EqualTo(msgId));
        Assert.That(received[0].Payload, Is.EqualTo(payload));

        var ack = await _engine.AckAsync("test", received[0].ReceiptHandle);

        Assert.That(ack, Is.True);

        // Ensure message no longer available
        var receivedAgain = await _engine.ReceiveAsync("test", maxMessages: 1);

        Assert.That(receivedAgain.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task Message_Should_Redeliver_After_Visibility_Timeout()
    {
        var payload = Encoding.UTF8.GetBytes("retry me");

        await _engine.SendAsync("retry", payload);

        var received = await _engine.ReceiveAsync("retry", visibilityTimeoutSeconds: 1);

        Assert.That(received.Count, Is.EqualTo(1));

        // Wait for lease to expire
        await Task.Delay(2500);

        await _engine.SweepExpiredAsync("retry");

        var receivedAgain = await _engine.ReceiveAsync("retry");

        Assert.That(receivedAgain.Count, Is.EqualTo(1));
        Assert.That(receivedAgain[0].ReceiveCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Message_Should_Move_To_DLQ_After_Max_Retries()
    {
        var payload = Encoding.UTF8.GetBytes("fail me");

        await _engine.SendAsync("dlq", payload, maxReceiveCount: 1);

        var received = await _engine.ReceiveAsync("dlq", visibilityTimeoutSeconds: 1);

        Assert.That(received.Count, Is.EqualTo(1));

        await Task.Delay(1200);

        await _engine.SweepExpiredAsync("dlq");

        // Should not be receivable anymore
        var receivedAgain = await _engine.ReceiveAsync("dlq");

        Assert.That(receivedAgain.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task Multiple_Messages_Should_Be_Received_In_Batch()
    {
        await _engine.SendAsync("batch", Encoding.UTF8.GetBytes("1"));
        await _engine.SendAsync("batch", Encoding.UTF8.GetBytes("2"));
        await _engine.SendAsync("batch", Encoding.UTF8.GetBytes("3"));

        var received = await _engine.ReceiveAsync("batch", maxMessages: 3);

        Assert.That(received.Count, Is.EqualTo(3));

        var payloads = received.Select(m => Encoding.UTF8.GetString(m.Payload)).ToList();

        CollectionAssert.AreEquivalent(new[] { "1", "2", "3" }, payloads);
    }
}
