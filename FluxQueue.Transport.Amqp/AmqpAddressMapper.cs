namespace FluxQueue.Transport.Amqp;

internal static class AmqpAddressMapper
{
    // Accept: "orders", "/queue/orders", "queue://orders"
    public static string ToQueueName(string? address)
    {
        if (string.IsNullOrWhiteSpace(address))
            throw new ArgumentException("AMQP address is required.");

        var s = address.Trim();

        if (s.StartsWith("queue://", StringComparison.OrdinalIgnoreCase))
            s = s["queue://".Length..];

        if (s.StartsWith("/queue/", StringComparison.OrdinalIgnoreCase))
            s = s["/queue/".Length..];

        // Some clients send leading "/" with plain address
        s = s.TrimStart('/');

        if (string.IsNullOrWhiteSpace(s))
            throw new ArgumentException("Queue name is empty after address normalization.");

        return s;
    }
}
