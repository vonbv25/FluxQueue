using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

public static class QueueEngineHelpers
{
    // -------- Key helpers (byte[] keys are faster than strings) --------

    private const byte KEYV = 1;
    public static string Base64Url(ReadOnlySpan<byte> data)
    {
        var s = Convert.ToBase64String(data);
        return s.Replace('+', '-').Replace('/', '_').TrimEnd('=');
    }

    public static int CompareBytes(byte[] a, byte[] b)
    {
        int len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int d = a[i].CompareTo(b[i]);
            if (d != 0) return d;
        }
        return a.Length.CompareTo(b.Length);
    }

    public static byte[] DlqKey(string queue, long failedAtMs, string msgId)
    {
        var qb = QueueBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        var key = new byte[4 + qb.Length + 8 + 16];
        var o = WriteHeader(key, (byte)'d', qb);
        WriteInt64BE(key.AsSpan(o, 8), failedAtMs); o += 8;
        WriteGuid(key.AsSpan(o, 16), gid);
        return key;
    }

    public static byte[] InflightKey(string queue, long inflightUntilMs, string msgId)
    {
        var qb = QueueBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        var key = new byte[4 + qb.Length + 8 + 16];
        var o = WriteHeader(key, (byte)'i', qb);
        WriteInt64BE(key.AsSpan(o, 8), inflightUntilMs); o += 8;
        WriteGuid(key.AsSpan(o, 16), gid);
        return key;
    }

    public static byte[] InflightPrefix(string queue)
    {
        var qb = QueueBytes(queue);
        var key = new byte[4 + qb.Length];
        WriteHeader(key, (byte)'i', qb);
        return key;
    }

    public static byte[] MsgKey(string queue, string msgId)
    {
        var qb = QueueBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        var key = new byte[4 + qb.Length + 16];
        var o = WriteHeader(key, (byte)'m', qb);
        WriteGuid(key.AsSpan(o, 16), gid);
        return key;
    }

    public static string NewReceiptHandle()
    {
        Span<byte> bytes = stackalloc byte[24];
        RandomNumberGenerator.Fill(bytes);
        return Base64Url(bytes);
    }

    public static (string msgId, long inflightUntilMs) ParseInflightKey(string queue, byte[] key)
    {
        int queueLen = BinaryPrimitives.ReadUInt16BigEndian(key.AsSpan(2, 2));
        int o = 4 + queueLen;

        long until = ReadInt64BE(key.AsSpan(o, 8)); o += 8;
        var gid = ReadGuid(key.AsSpan(o, 16));
        return (gid.ToString("N"), until);
    }

    public static (string msgId, long visibleAtMs) ParseReadyKey(string queue, byte[] key)
    {

        int queueLen = BinaryPrimitives.ReadUInt16BigEndian(key.AsSpan(2, 2));
        int o = 4 + queueLen;

        long time = ReadInt64BE(key.AsSpan(o, 8)); o += 8;

        // skip seq
        o += 8;

        var gid = ReadGuid(key.AsSpan(o, 16));
        return (gid.ToString("N"), time);
    }

    public static byte[] QueueBytes(string queue) => Encoding.UTF8.GetBytes(queue);

    public static Guid ReadGuid(ReadOnlySpan<byte> src) => new Guid(src.Slice(0, 16));

    public static long ReadInt64BE(ReadOnlySpan<byte> src) =>
        BinaryPrimitives.ReadInt64BigEndian(src);

    public static byte[] ReadyKey(string queue, long visibleAtMs, long enqueueSeq, string msgId)
    {
        var qb = QueueBytes(queue);
        var gid = Guid.ParseExact(msgId, "N");

        // header + visibleAt(8) + seq(8) + guid(16)
        var key = new byte[4 + qb.Length + 8 + 8 + 16];
        var o = WriteHeader(key, (byte)'r', qb);

        WriteInt64BE(key.AsSpan(o, 8), visibleAtMs); o += 8;
        WriteInt64BE(key.AsSpan(o, 8), enqueueSeq); o += 8;
        WriteGuid(key.AsSpan(o, 16), gid);

        return key;
    }

    public static byte[] ReadyPrefix(string queue)
    {
        var qb = QueueBytes(queue);
        var key = new byte[4 + qb.Length];
        WriteHeader(key, (byte)'r', qb);
        return key;
    }

    public static byte[] ReceiptKey(string queue, string receiptHandle) => Utf8($"q:{queue}:h:{receiptHandle}");

    public static bool StartsWith(byte[] data, byte[] prefix)
    {
        if (data.Length < prefix.Length) return false;
        for (int i = 0; i < prefix.Length; i++)
            if (data[i] != prefix[i]) return false;
        return true;
    }

    public static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    public static void WriteGuid(Span<byte> dst, Guid g)
    {
        Span<byte> tmp = stackalloc byte[16];
        g.TryWriteBytes(tmp);
        tmp.CopyTo(dst);
    }

    public static int WriteHeader(Span<byte> dst, byte type, ReadOnlySpan<byte> queueBytes)
    {
        dst[0] = KEYV;
        dst[1] = type;
        BinaryPrimitives.WriteUInt16BigEndian(dst.Slice(2, 2), (ushort)queueBytes.Length);
        queueBytes.CopyTo(dst.Slice(4));
        return 4 + queueBytes.Length;
    }

    public static void WriteInt64BE(Span<byte> dst, long value) =>
        BinaryPrimitives.WriteInt64BigEndian(dst, value);

    public static long ComputeBackoffMs(int receiveCount)
    {
        var exp = Math.Min(6, Math.Max(0, receiveCount));
        var baseMs = (long)(1000 * Math.Pow(2, exp));
        baseMs = Math.Min(baseMs, 60_000);

        Span<byte> b = stackalloc byte[2];
        RandomNumberGenerator.Fill(b);
        var jitter = BinaryPrimitives.ReadUInt16LittleEndian(b) % 500;

        return baseMs + jitter;
    }
}