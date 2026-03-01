using Amqp.Framing;
using Amqp;

namespace FluxQueue.IntegrationTests.Amqp;

public static class AmqpClient
{
    public static async Task SendAsync(string host, int port, string queue, byte[] payload)
    {
        var addr = new Address(host, port, null, null, scheme: "amqp");
        var conn = await Connection.Factory.CreateAsync(addr);
        var sess = new Session(conn);

        var sender = new SenderLink(sess, "sender-" + Guid.NewGuid().ToString("N"),
            queue);

        var msg = new Message
        {
            BodySection = new Data { Binary = payload },
            Properties = new Properties { MessageId = Guid.NewGuid().ToString("N") }
        };

        await sender.SendAsync(msg);
        await sender.CloseAsync();
    }

    public static async Task<(Message? Msg, ReceiverLink Rx)> ReceiveOnceAsync(
        string host, int port, string queue, TimeSpan timeout)
    {
        var addr = new Address(host, port, null, null, scheme: "amqp");
        var conn = await Connection.Factory.CreateAsync(addr);
        var sess = new Session(conn);

        // receiver link: Source.Address is the queue
        var rx = new ReceiverLink(sess, "rx-" + Guid.NewGuid().ToString("N"), queue);

        // Credit 1
        rx.SetCredit(1, autoRestore: false);

        var msg = await rx.ReceiveAsync(timeout);
        // If msg == null => no message
        return (msg, rx);
    }

    public static async Task CloseReceiverAsync(ReceiverLink rx)
    {
        try
        {
            var session = rx.Session;
            var conn = session.Connection;

            await rx.CloseAsync();
            await session.CloseAsync();
            await conn.CloseAsync();
        }
        catch { /* ignore */ }
    }
}
