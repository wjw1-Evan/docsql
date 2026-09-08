// docsql wire protocol v1 client.
// Frame: magic "DSQ1" | flags:u16 | type:u16 | topology_version:u64 | len:u32 | payload

using System.Buffers.Binary;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace Docsql.Client;

public enum FrameType : ushort
{
    ReqSql = 0x0001,
    ReqKv = 0x0002,
    ReqPing = 0x0006,
    RespRows = 0x0101,
    RespAffected = 0x0102,
    RespError = 0x0103,
    RespPong = 0x0105,
}

public readonly record struct Frame(FrameType Type, ushort Flags, ulong TopologyVersion, byte[] Payload)
{
    public byte[] Encode()
    {
        var buf = new byte[20 + Payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(0, 4), 0x31515344);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(4, 2), Flags);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(6, 2), (ushort)Type);
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(8, 8), TopologyVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(16, 4), (uint)Payload.Length);
        Payload.CopyTo(buf, 20);
        return buf;
    }
}

public sealed class ProtocolConnection : IDisposable
{
    private const ushort FlagEncrypted = 0x0004;

    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;
    private readonly byte[] _header = new byte[20];
    private readonly byte[]? _key;

    public ProtocolConnection(string host, int port, byte[]? key = null)
    {
        _tcp = new TcpClient(host, port);
        _stream = _tcp.GetStream();
        _key = key;
    }

    /// SQL text payload: length-prefixed UTF-8, tag 4 (string).
    public static byte[] EncodeSql(string sql)
    {
        var utf8 = Encoding.UTF8.GetBytes(sql);
        var buf = new byte[5 + utf8.Length];
        buf[0] = 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1, 4), utf8.Length);
        utf8.CopyTo(buf, 5);
        return buf;
    }

    public Frame Send(Frame request)
    {
        if (_key is not null)
        {
            request = request with
            {
                Flags = (ushort)(request.Flags | FlagEncrypted),
                Payload = Seal(_key, request.Payload),
            };
        }
        _stream.Write(request.Encode());
        _stream.Flush();
        return ReadFrame();
    }

    /// <summary>AES-256-GCM:nonce(12) ‖ 密文 ‖ tag(16)。</summary>
    private static byte[] Seal(byte[] key, byte[] plaintext)
    {
        var nonce = new byte[12];
        RandomNumberGenerator.Fill(nonce);
        using var gcm = new AesGcm(key, 16);
        var ct = new byte[plaintext.Length];
        var tag = new byte[16];
        gcm.Encrypt(nonce, plaintext, ct, tag);
        var sealed_ = new byte[12 + ct.Length + 16];
        nonce.CopyTo(sealed_, 0);
        ct.CopyTo(sealed_, 12);
        tag.CopyTo(sealed_, 12 + ct.Length);
        return sealed_;
    }

    private static byte[] Unseal(byte[] key, byte[] sealed_)
    {
        if (sealed_.Length < 12 + 16)
            throw new DocsqlException("加密载荷过短");
        var nonce = sealed_[..12];
        var ct = sealed_[12..^16];
        var tag = sealed_[^16..];
        using var gcm = new AesGcm(key, 16);
        var pt = new byte[ct.Length];
        try
        {
            gcm.Decrypt(nonce, ct, tag, pt);
        }
        catch (CryptographicException)
        {
            throw new DocsqlException("解密失败(密钥错误或数据被篡改)");
        }
        return pt;
    }

    private Frame ReadFrame()
    {
        ReadExact(_header);
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(_header.AsSpan(0, 4));
        if (magic != 0x31515344)
        {
            throw new IOException("bad frame magic");
        }
        var type = (FrameType)BinaryPrimitives.ReadUInt16LittleEndian(_header.AsSpan(6, 2));
        var flags = BinaryPrimitives.ReadUInt16LittleEndian(_header.AsSpan(4, 2));
        var topo = BinaryPrimitives.ReadUInt64LittleEndian(_header.AsSpan(8, 8));
        int len = (int)BinaryPrimitives.ReadUInt32LittleEndian(_header.AsSpan(16, 4));
        // Mirror the server's inbound cap: a corrupt or hostile length must
        // not drive a multi-GB allocation.
        if (len is < 0 or > 64 * 1024 * 1024)
        {
            throw new IOException($"frame length {len} out of range");
        }
        var payload = new byte[len];
        ReadExact(payload);
        if ((flags & FlagEncrypted) != 0)
        {
            if (_key is null)
                throw new DocsqlException("服务端返回加密帧但客户端未配置 key");
            payload = Unseal(_key, payload);
        }
        return new Frame(type, flags, topo, payload);
    }

    private void ReadExact(byte[] buf)
    {
        int off = 0;
        while (off < buf.Length)
        {
            int n = _stream.Read(buf, off, buf.Length - off);
            if (n == 0)
            {
                throw new IOException("connection closed");
            }
            off += n;
        }
    }

    public void Dispose()
    {
        _stream.Dispose();
        _tcp.Dispose();
    }
}
