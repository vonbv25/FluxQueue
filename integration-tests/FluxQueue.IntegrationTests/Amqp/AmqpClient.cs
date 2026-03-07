using Amqp.Framing;
using Amqp;

namespace FluxQueue.IntegrationTests.Amqp;

public static class AmqpClient
{
    public static async Task SendAsync(
        string host,
        int port,
        string queue,
        byte[] payload,
        int delaySeconds = 0,
        int? maxReceiveCount = null,
        IDictionary<string, object?>? applicationProperties = null)
    {
        var addr = new Address(host, port, null, null, scheme: "amqp");
        var conn = await Connection.Factory.CreateAsync(addr);
        var sess = new Session(conn);
        var sender = new SenderLink(sess, "sender-" + Guid.NewGuid().ToString("N"), queue);

        var msg = new Message
        {
            BodySection = new Data { Binary = payload },
            Properties = new Properties
            {
                MessageId = Guid.NewGuid().ToString("N")
            }
        };

        if (delaySeconds > 0 || maxReceiveCount.HasValue || applicationProperties is not null)
        {
            msg.ApplicationProperties = new ApplicationProperties();
        }

        if (delaySeconds > 0)
        {
            msg.ApplicationProperties!["delaySeconds"] = delaySeconds;
        }

        if (maxReceiveCount.HasValue)
        {
            msg.ApplicationProperties!["maxReceiveCount"] = maxReceiveCount.Value;
        }

        if (applicationProperties is not null)
        {
            foreach (var kvp in applicationProperties)
            {
                msg.ApplicationProperties![kvp.Key] = kvp.Value;
            }
        }

        try
        {
            await sender.SendAsync(msg);
        }
        finally
        {
            try { await sender.CloseAsync(); } catch { }
            try { await sess.CloseAsync(); } catch { }
            try { await conn.CloseAsync(); } catch { }
        }
    }

    public static async Task<(Message? Msg, ReceiverLink Rx)> ReceiveOnceAsync(
        string host,
        int port,
        string queue,
        TimeSpan timeout)
    {
        var addr = new Address(host, port, null, null, scheme: "amqp");
        var conn = await Connection.Factory.CreateAsync(addr);
        var sess = new Session(conn);

        var rx = new ReceiverLink(sess, "rx-" + Guid.NewGuid().ToString("N"), queue);

        rx.SetCredit(1, autoRestore: false);

        var msg = await rx.ReceiveAsync(timeout);
        return (msg, rx);
    }

    public static async Task AcceptAsync(ReceiverLink rx, Message msg)
    {
        ArgumentNullException.ThrowIfNull(rx);
        ArgumentNullException.ThrowIfNull(msg);

        try
        {
            rx.Accept(msg);
        }
        finally
        {
            await CloseReceiverAsync(rx);
        }
    }

    public static async Task RejectAsync(ReceiverLink rx, Message msg, string? errorDescription = null)
    {
        ArgumentNullException.ThrowIfNull(rx);
        ArgumentNullException.ThrowIfNull(msg);

        try
        {
            rx.Reject(msg, new Error
            {
                Condition = "amqp:internal-error",
                Description = errorDescription ?? "Rejected by integration test."
            });
        }
        finally
        {
            await CloseReceiverAsync(rx);
        }
    }

    public static async Task ReleaseAsync(ReceiverLink rx, Message msg)
    {
        ArgumentNullException.ThrowIfNull(rx);
        ArgumentNullException.ThrowIfNull(msg);

        try
        {
            rx.Release(msg);
        }
        finally
        {
            await CloseReceiverAsync(rx);
        }
    }

    public static async Task CloseReceiverAsync(ReceiverLink rx)
    {
        if (rx is null)
            return;

        try
        {
            var session = rx.Session;
            var conn = session.Connection;

            try { await rx.CloseAsync(); } catch { }
            try { await session.CloseAsync(); } catch { }
            try { await conn.CloseAsync(); } catch { }
        }
        catch
        {
            // ignore cleanup failures
        }
    }

    public static byte[] GetPayload(Message msg)
    {
        var data = msg.BodySection as Data;
        return data?.Binary ?? Array.Empty<byte>();
    }

    public static string GetPayloadAsString(Message msg)
    {
        return System.Text.Encoding.UTF8.GetString(GetPayload(msg));
    }

    public static int? TryGetApplicationInt(Message msg, string key)
    {
        var map = msg.ApplicationProperties?.Map;
        if (map is null || !map.TryGetValue(key, out var value) || value is null)
            return null;

        return value switch
        {
            int i => i,
            long l when l >= int.MinValue && l <= int.MaxValue => (int)l,
            string s when int.TryParse(s, out var parsed) => parsed,
            _ => null
        };
    }
}