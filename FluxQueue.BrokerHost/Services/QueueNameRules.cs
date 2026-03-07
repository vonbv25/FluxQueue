namespace FluxQueue.BrokerHost.Services;

internal static class QueueNameRules
{
    public const int MaxLength = 200;

    public static bool IsValid(string? queue)
    {
        if (string.IsNullOrWhiteSpace(queue))
            return false;

        if (queue.Length > MaxLength)
            return false;

        foreach (var ch in queue)
        {
            var ok = char.IsLetterOrDigit(ch) || ch is '-' or '_' or '.';
            if (!ok)
                return false;
        }

        return true;
    }

    public static void ThrowIfInvalid(string? queue, string paramName)
    {
        if (string.IsNullOrWhiteSpace(queue))
            throw new ArgumentNullException(paramName);

        if (queue.Length > MaxLength)
            throw new ArgumentOutOfRangeException(
                paramName,
                $"Queue name must not exceed {MaxLength} characters.");

        foreach (var ch in queue)
        {
            var ok = char.IsLetterOrDigit(ch) || ch is '-' or '_' or '.';
            if (!ok)
            {
                throw new ArgumentException(
                    "Queue name contains invalid characters. Only letters, digits, '-', '_' and '.' are allowed.",
                    paramName);
            }
        }
    }
}
