// DocSQL wire protocol v1 client.
// Frame: magic "DSQ1" | flags:u16 | type:u16 | topology_version:u64 | len:u32 | payload

using System.Buffers.Binary;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace Docsql.Client;

public enum FrameType : ushort
{
    ReqSql = 0x0001,
    /// <summary>会话认证:载荷为 token 原始字节。</summary>
    ReqAuth = 0x0002,
    ReqPrepare = 0x0003,
    ReqExecute = 0x0004,
    ReqCloseStmt = 0x0005,
    ReqPing = 0x0006,
    /// <summary>故障转移提升:清除只读副本模式(需已认证会话)。</summary>
    ReqPromote = 0x0007,
    ReqStatus = 0x0008,
    /// <summary>订阅频道:JSON {"channel","from"};确认 RESP_AFFECTED,历史以 RESP_PUSH 回放。</summary>
    ReqSubscribe = 0x0009,
    /// <summary>模式订阅(glob):JSON {"pattern","from"}。</summary>
    ReqPsubscribe = 0x000A,
    /// <summary>退订:JSON 数组(空 = 全部);回剩余订阅数。</summary>
    ReqUnsubscribe = 0x000B,
    ReqPunsubscribe = 0x000C,
    /// <summary>发布:JSON {"channel","payload"};先落盘再推送,回 RESP_ROWS [id, receivers]。</summary>
    ReqPublish = 0x000D,
    /// <summary>内省/保留:JSON {"sub":"channels|numsub|numpat|trim",...}。</summary>
    ReqPubsub = 0x000E,
    /// <summary>日志报告:查询日志 + 同步日志两环最新条目(需认证)。</summary>
    ReqLogs = 0x000F,
    /// <summary>cluster join:请求全量快照(节点间复制帧)。</summary>
    ReqSync = 0x0010,
    /// <summary>cluster join:冻结写路径(排空在途写)。</summary>
    ReqHold = 0x0011,
    /// <summary>cluster join:解除冻结。</summary>
    ReqRelease = 0x0012,
    /// <summary>对象浏览器元数据(与 web /api/meta 同构)。</summary>
    ReqMeta = 0x0013,
    RespRows = 0x0101,
    RespAffected = 0x0102,
    RespError = 0x0103,
    RespRedirect = 0x0104,
    RespPong = 0x0105,
    RespStatus = 0x0106,
    /// <summary>服务端主动推送(pub/sub):JSON {"kind","pattern"?,"channel","id","ts","payload"}。</summary>
    RespPush = 0x0107,
    /// <summary>REQ_LOGS 的应答载荷。</summary>
    RespLogs = 0x0108,
    /// <summary>REQ_SYNC 的分块应答(≤4MB 流式回传)。</summary>
    RespSync = 0x0109,
    /// <summary>REQ_META 的应答载荷。</summary>
    RespMeta = 0x010A,
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

public static class FrameExtensions
{
    /// <summary>RESP_ERROR 帧 → DocsqlException:所有"发送后检查应答"路径共用的骨架。</summary>
    public static Frame EnsureOk(this Frame f, string? prefix = null)
    {
        if (f.Type == FrameType.RespError)
        {
            var msg = Encoding.UTF8.GetString(f.Payload);
            throw new DocsqlException(string.IsNullOrEmpty(prefix) ? msg : prefix + msg);
        }
        return f;
    }
}

public sealed class ProtocolConnection : IDisposable
{
    private const ushort FlagEncrypted = 0x0004;

    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;
    private readonly byte[] _header = new byte[20];
    private readonly byte[]? _key;

    public ProtocolConnection(
        string host, int port, byte[]? key = null, int connectTimeoutMs = 15_000)
    {
        // The synchronous TcpClient ctor blocks for the OS connect timeout
        // (often 75s+) on unreachable hosts; bound it so callers fail fast.
        _tcp = new TcpClient();
        try
        {
            var connecting = _tcp.ConnectAsync(host, port);
            try
            {
                if (!connecting.Wait(connectTimeoutMs))
                {
                    throw new System.IO.IOException(
                        $"connect to {host}:{port} timed out after {connectTimeoutMs} ms");
                }
            }
            catch (AggregateException ae)
            {
                throw ae.GetBaseException();
            }
            _stream = _tcp.GetStream();
            _key = key;
        }
        catch
        {
            _tcp.Dispose();
            throw;
        }
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
        Write(request);
        return Receive();
    }

    /// <summary>仅发送一帧(订阅连接拆开用:响应帧与推送帧需分别接收)。</summary>
    public void Write(Frame request)
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
    }

    /// <summary>仅接收一帧(已解密);阻塞直至一帧完整到达。</summary>
    public Frame Receive() => ReadFrame();

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
