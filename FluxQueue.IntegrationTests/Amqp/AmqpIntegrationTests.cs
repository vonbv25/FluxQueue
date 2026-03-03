using Amqp;
using FluxQueue.IntegrationTests.TestHost;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FluxQueue.IntegrationTests.Amqp;

[TestFixture]
public sealed class AmqpIntegrationTests
{
    private FluxQueueTestHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _host = new FluxQueueTestHost();
        await _host.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _host.DisposeAsync();
    }
    [Test]
    public async Task MultipleMessages_CanBeReceived_AndAcked()
    {
        var queue = "batchq";

        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("m1"));
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("m2"));

        var (a, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        Assert.That(a, Is.Not.Null);
        rx.Accept(a!);

        // Ask for another message on same receiver
        rx.SetCredit(1, autoRestore: false);
        var b = await rx.ReceiveAsync(TimeSpan.FromSeconds(3));

        Assert.That(b, Is.Not.Null);
        rx.Accept(b!);

        await AmqpClient.CloseReceiverAsync(rx);
    }

    [Test]
    public async Task Messages_Should_BeReceived_InSendOrder_OnSingleReceiver()
    {
        var queue = "orderq";
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("1"));
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("2"));
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("3"));

        var (m1, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));
        Assert.That(m1, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString((byte[])m1!.Body), Is.EqualTo("1"));
        rx.Accept(m1);

        rx.SetCredit(1, autoRestore: false);
        var m2 = await rx.ReceiveAsync(TimeSpan.FromSeconds(3));
        Assert.That(m2, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString((byte[])m2!.Body), Is.EqualTo("2"));
        rx.Accept(m2);

        rx.SetCredit(1, autoRestore: false);
        var m3 = await rx.ReceiveAsync(TimeSpan.FromSeconds(3));
        Assert.That(m3, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString((byte[])m3!.Body), Is.EqualTo("3"));
        rx.Accept(m3);

        await AmqpClient.CloseReceiverAsync(rx);
    }

    [Test]
    public async Task AcceptingSameDeliveryTwice_ShouldNotBreak()
    {
        var queue = "idempotentq";
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("dup-accept"));

        var (msg, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));
        Assert.That(msg, Is.Not.Null);

        // First accept
        rx.Accept(msg!);

        // Second accept (should be no-op-ish; transport may ignore / engine should just return false)
        Assert.DoesNotThrow(() => rx.Accept(msg!));

        await AmqpClient.CloseReceiverAsync(rx);

        // Ensure no re-delivery
        var (msg2, rx2) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(1));
        Assert.That(msg2, Is.Null);
        await AmqpClient.CloseReceiverAsync(rx2);
    }
    [Test]
    public async Task UnsettledDelivery_Should_Redeliver_ToAnotherConsumer_AfterSweep()
    {
        var queue = "handoffq";
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("handoff"));

        // Consumer A receives but doesn't accept
        var (m1, rx1) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));
        Assert.That(m1, Is.Not.Null);

        await AmqpClient.CloseReceiverAsync(rx1);

        // Wait past visibility timeout and sweep
        await Task.Delay(TimeSpan.FromSeconds(3));
        await _host.SweepQueueAsync(queue);

        // Consumer B should receive it
        var (m2, rx2) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));
        Assert.That(m2, Is.Not.Null);

        rx2.Accept(m2!);
        await AmqpClient.CloseReceiverAsync(rx2);
    }
    [Test]
    public async Task LongPoll_Should_ReturnMessage_WhenMessageArrives()
    {
        var queue = "longpollq";

        // Start a long poll receive (3s)
        var receiveTask = AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        // Send after a short delay
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("late"));

        var (msg, rx) = await receiveTask;
        Assert.That(msg, Is.Not.Null);

        rx.Accept(msg!);
        await AmqpClient.CloseReceiverAsync(rx);
    }
    [Test]
    public async Task Receive_WhenQueueEmpty_Should_ReturnNull_AndNotThrow()
    {
        var queue = "emptyq";

        var (msg, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(1));

        Assert.That(msg, Is.Null);
        await AmqpClient.CloseReceiverAsync(rx);
    }

    [Test]
    public async Task Send_Receive_Accepted_AcksMessage()
    {
        var queue = "orders";
        var payload = Encoding.UTF8.GetBytes("hello");

        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, payload);

        var (msg, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        Assert.That(msg, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(AmqpClient.GetPayload(msg!)), Is.EqualTo("hello"));

        rx.Accept(msg!);
        await AmqpClient.CloseReceiverAsync(rx);

        // should be empty now
        var (msg2, rx2) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(1));

        Assert.That(msg2, Is.Null);
        await AmqpClient.CloseReceiverAsync(rx2);
    }
    [Test]
    public async Task Receive_WithoutAccept_Redelivers_AfterVisibilityTimeout()
    {
        var queue = "retryq";
        var payload = Encoding.UTF8.GetBytes("will-redeliver");

        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, payload);

        var (msg, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        Assert.That(msg, Is.Not.Null);

        // close without accept => remains inflight until sweep
        await AmqpClient.CloseReceiverAsync(rx);

        await Task.Delay(TimeSpan.FromSeconds(3));
        await _host.SweepQueueAsync(queue);

        var (msg2, rx2) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        Assert.That(msg2, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(AmqpClient.GetPayload(msg2!)), Is.EqualTo("will-redeliver"));

        rx2.Accept(msg2!);
        await AmqpClient.CloseReceiverAsync(rx2);
    }

    [Test]
    public async Task CompetingConsumers_Should_NotBothReceiveSameMessage()
    {
        var queue = "competeq";
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("x"));

        var t1 = AmqpClient.ReceiveOnceAsync("127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));
        var t2 = AmqpClient.ReceiveOnceAsync("127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        await Task.WhenAll(t1, t2);

        var (m1, rx1) = t1.Result;
        var (m2, rx2) = t2.Result;

        var count = (m1 is null ? 0 : 1) + (m2 is null ? 0 : 1);
        Assert.That(count, Is.EqualTo(1));

        if (m1 != null) rx1.Accept(m1);
        if (m2 != null) rx2.Accept(m2);

        await AmqpClient.CloseReceiverAsync(rx1);
        await AmqpClient.CloseReceiverAsync(rx2);
    }
    [Test]
    public async Task CompetingConsumers_Should_NotReceiveSameMessage()
    {
        var queue = "competeq";
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("x"));

        // Start two concurrent receives (two independent links)
        var t1 = AmqpClient.ReceiveOnceAsync("127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));
        var t2 = AmqpClient.ReceiveOnceAsync("127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        await Task.WhenAll(t1, t2);

        var (m1, rx1) = t1.Result;
        var (m2, rx2) = t2.Result;

        var count = (m1 is null ? 0 : 1) + (m2 is null ? 0 : 1);
        Assert.That(count, Is.EqualTo(1), "Expected exactly one consumer to receive the message.");

        // Cleanup receiver links
        if (m1 != null) rx1.Accept(m1);
        if (m2 != null) rx2.Accept(m2);

        await AmqpClient.CloseReceiverAsync(rx1);
        await AmqpClient.CloseReceiverAsync(rx2);
    }

    [Test]
    public async Task Messages_Should_BeReceived_InSendOrder_OnSameReceiverLink()
    {
        var queue = "orderq";

        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("1"));
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("2"));
        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, Encoding.UTF8.GetBytes("3"));

        var addr = new Address("127.0.0.1", _host.AmqpPort, null, null, scheme: "amqp");
        var conn = await Connection.Factory.CreateAsync(addr);
        var sess = new Session(conn);
        var rx = new ReceiverLink(sess, "rx-" + Guid.NewGuid().ToString("N"), queue);

        try
        {
            rx.SetCredit(1, autoRestore: false);
            var m1 = await rx.ReceiveAsync(TimeSpan.FromSeconds(3));
            Assert.That(m1, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(AmqpClient.GetPayload(m1!)), Is.EqualTo("1"));
            rx.Accept(m1!);

            rx.SetCredit(1, autoRestore: false);
            var m2 = await rx.ReceiveAsync(TimeSpan.FromSeconds(3));
            Assert.That(m2, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(AmqpClient.GetPayload(m2!)), Is.EqualTo("2"));
            rx.Accept(m2!);

            rx.SetCredit(1, autoRestore: false);
            var m3 = await rx.ReceiveAsync(TimeSpan.FromSeconds(3));
            Assert.That(m3, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(AmqpClient.GetPayload(m3!)), Is.EqualTo("3"));
            rx.Accept(m3!);
        }
        finally
        {
            try { await rx.CloseAsync(); } catch { }
            try { await sess.CloseAsync(); } catch { }
            try { await conn.CloseAsync(); } catch { }
        }
    }

    [Test]
    public async Task Receive_WhenQueueEmpty_Should_ReturnNull()
    {
        var (msg, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, "emptyq", TimeSpan.FromSeconds(1));

        Assert.That(msg, Is.Null);
        await AmqpClient.CloseReceiverAsync(rx);
    }

    //[Test]
    //public async Task DelayedMessage_Should_NotDeliverBeforeDelay_ButDeliverAfter()
    //{
    //    var queue = "delayq";
    //    await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue,
    //        Encoding.UTF8.GetBytes("delayed"), delaySeconds: 2);

    //    // Should not arrive in 1s
    //    var (m0, rx0) = await AmqpClient.ReceiveOnceAsync(
    //        "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(1));

    //    Assert.That(m0, Is.Null);
    //    await AmqpClient.CloseReceiverAsync(rx0);

    //    // Wait >2s plus a buffer
    //    await Task.Delay(TimeSpan.FromSeconds(3));

    //    var (m1, rx1) = await AmqpClient.ReceiveOnceAsync(
    //        "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

    //    Assert.That(m1, Is.Not.Null);
    //    Assert.That(Encoding.UTF8.GetString(AmqpClient.GetPayload(m1!)), Is.EqualTo("delayed"));

    //    rx1.Accept(m1!);
    //    await AmqpClient.CloseReceiverAsync(rx1);
    //}
}
