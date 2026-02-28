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
    public async Task Send_Receive_Accepted_AcksMessage()
    {
        var queue = "orders";
        var payload = Encoding.UTF8.GetBytes("hello");

        await AmqpClient.SendAsync("127.0.0.1", _host.AmqpPort, queue, payload);

        var (msg, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        Assert.That(msg, Is.Not.Null);

        // Accept -> should call AckAsync via your AMQP adapter
        rx.Accept(msg!);
        await AmqpClient.CloseReceiverAsync(rx);

        // Try receive again - should be empty
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

        // Receive but DO NOT accept
        var (msg, rx) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        Assert.That(msg, Is.Not.Null);

        // Close receiver without accepting (no settlement)
        await AmqpClient.CloseReceiverAsync(rx);

        // Wait longer than visibility timeout configured in test host (2s)
        await Task.Delay(TimeSpan.FromSeconds(3));

        // Should redeliver
        var (msg2, rx2) = await AmqpClient.ReceiveOnceAsync(
            "127.0.0.1", _host.AmqpPort, queue, TimeSpan.FromSeconds(3));

        Assert.That(msg2, Is.Not.Null);

        // Now accept to clean up
        rx2.Accept(msg2!);
        await AmqpClient.CloseReceiverAsync(rx2);
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
}
