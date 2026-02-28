using Amqp.Framing;
using Amqp;
using System.Text;

namespace FluxQueue.Transport.Amqp;

internal static class AmqpMessageBody
{
    public static byte[] ExtractPayload(Message msg)
    {
        // Preferred: Data section (binary)
        if (msg.BodySection is Data data && data.Binary is not null)
            return data.Binary;

        // Common fallback: AmqpValue
        if (msg.BodySection is AmqpValue v && v.Value is not null)
        {
            return v.Value switch
            {
                byte[] b => b,
                ArraySegment<byte> seg => seg.ToArray(),
                string str => Encoding.UTF8.GetBytes(str),
                _ => Encoding.UTF8.GetBytes(v.Value.ToString() ?? string.Empty)
            };
        }

        // If no body section exists, treat as empty payload
        if (msg.BodySection is null)
            return Array.Empty<byte>();

        throw new InvalidOperationException($"Unsupported AMQP body section type: {msg.BodySection.GetType().Name}");
    }
}