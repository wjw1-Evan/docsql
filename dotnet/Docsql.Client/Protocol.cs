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
    /// <summary>用户名/密码登录(REQ_AUTH_USER):载荷 JSON {"user","password"}。</summary>
    ReqAuthUser = 0x0018,
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
    /// <summary>REQ_PREPARE 的应答:JSON {"handle":n} — 句柄按连接隔离,随连接生死。</summary>
    RespPrepared = 0x010E,
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

    /// <summary>
    /// 服务端 prepared statement 句柄缓存:模板 SQL → REQ_PREPARE 句柄。
    /// 句柄按物理连接隔离且随连接生死 —— 池化复用同一物理连接即缓存有效;
    /// 超过容量上限时逐个 REQ_CLOSE_STMT 后清空(服务器侧无自动逐出)。
    /// </summary>
    private readonly Dictionary<string, ulong> _prepared = new();
    private const int PreparedCapacity = 96;

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

    private ProtocolConnection(TcpClient connected, byte[]? key)
    {
        _tcp = connected;
        _stream = connected.GetStream();
        _key = key;
    }

    /// <summary>异步建连(真异步,不占线程):超时与外部取消共用一个令牌。
    /// 连接超时抛 <see cref="System.IO.IOException"/>,外部取消抛
    /// OperationCanceledException —— 两者可据此区分。</summary>
    public static async Task<ProtocolConnection> ConnectAsync(
        string host, int port, byte[]? key = null, int connectTimeoutMs = 15_000,
        CancellationToken cancellationToken = default)
    {
        var tcp = new TcpClient();
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(connectTimeoutMs);
            await tcp.ConnectAsync(host, port, cts.Token);
            return new ProtocolConnection(tcp, key);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            tcp.Dispose();
            throw new System.IO.IOException(
                $"connect to {host}:{port} timed out after {connectTimeoutMs} ms");
        }
        catch
        {
            tcp.Dispose();
            throw;
        }
    }

    /// <summary>
    /// 取模板的 prepared 句柄(缓存命中不发帧);未缓存则 REQ_PREPARE 注册。
    /// 返回 (句柄, 是否命中缓存)。注册失败(语法错误等)直接抛出,不入缓存。
    /// </summary>
    internal (ulong Handle, bool Cached) GetOrPrepare(string template)
    {
        if (_prepared.TryGetValue(template, out var cached))
        {
            return (cached, true);
        }
        if (_prepared.Count >= PreparedCapacity)
        {
            ClearPrepared();
        }
        var resp = Send(new Frame(FrameType.ReqPrepare, 0, 0, EncodeSql(template)))
            .EnsureOk("prepare failed: ");
        return FinishPrepare(resp, template);
    }

    /// <summary><see cref="GetOrPrepare"/> 的真异步形态。</summary>
    internal async Task<(ulong Handle, bool Cached)> GetOrPrepareAsync(
        string template, CancellationToken cancellationToken = default)
    {
        if (_prepared.TryGetValue(template, out var cached))
        {
            return (cached, true);
        }
        if (_prepared.Count >= PreparedCapacity)
        {
            ClearPrepared();
        }
        var resp = await SendAsync(
                new Frame(FrameType.ReqPrepare, 0, 0, EncodeSql(template)), cancellationToken)
            .ConfigureAwait(false);
        resp.EnsureOk("prepare failed: ");
        return FinishPrepare(resp, template);
    }

    private (ulong, bool) FinishPrepare(Frame resp, string template)
    {
        if (resp.Type != FrameType.RespPrepared)
        {
            throw new DocsqlException($"unexpected response to PREPARE: {resp.Type}");
        }
        using var doc = System.Text.Json.JsonDocument.Parse(
            System.Text.Encoding.UTF8.GetString(resp.Payload));
        var handle = doc.RootElement.GetProperty("handle").GetUInt64();
        _prepared[template] = handle;
        return (handle, false);
    }

    /// <summary>显式关闭全部缓存句柄(容量逐出/连接归还前清扫)。</summary>
    internal void ClearPrepared()
    {
        foreach (var h in _prepared.Values)
        {
            try
            {
                Send(new Frame(
                    FrameType.ReqCloseStmt, 0, 0,
                    System.Text.Encoding.UTF8.GetBytes($"{{\"handle\":{h}}}")));
            }
            catch
            {
                // 连接已坏:句柄随物理连接消亡,无需逐个关闭。
                break;
            }
        }
        _prepared.Clear();
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

    /// <summary><see cref="Send"/> 的真异步形态。取消只到语句边界:一帧发到
    /// 一半作废会错位帧流(该连接只能弃用),语句级取消由服务端语句超时承担,
    /// ct 在发送前检查。</summary>
    public async Task<Frame> SendAsync(Frame request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await WriteAsync(request).ConfigureAwait(false);
        return await ReadFrameAsync().ConfigureAwait(false);
    }

    private Frame SealFrame(Frame request)
    {
        if (_key is null)
        {
            return request;
        }
        return request with
        {
            Flags = (ushort)(request.Flags | FlagEncrypted),
            Payload = Seal(_key, request.Payload),
        };
    }

    /// <summary>仅发送一帧(订阅连接拆开用:响应帧与推送帧需分别接收)。</summary>
    public void Write(Frame request)
    {
        var sealedFrame = SealFrame(request);
        _stream.Write(sealedFrame.Encode());
        _stream.Flush();
    }

    /// <summary><see cref="Write"/> 的真异步形态。</summary>
    public async Task WriteAsync(Frame request)
    {
        var sealedFrame = SealFrame(request);
        await _stream.WriteAsync(sealedFrame.Encode()).ConfigureAwait(false);
        await _stream.FlushAsync().ConfigureAwait(false);
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
        var (type, flags, topo, len) = ParseHeader(_header);
        var payload = new byte[len];
        ReadExact(payload);
        return Assemble(flags, type, topo, payload);
    }

    /// <summary><see cref="ReadFrame"/> 的真异步形态。</summary>
    private async Task<Frame> ReadFrameAsync()
    {
        await ReadExactAsync(_header).ConfigureAwait(false);
        var (type, flags, topo, len) = ParseHeader(_header);
        var payload = new byte[len];
        await ReadExactAsync(payload).ConfigureAwait(false);
        return Assemble(flags, type, topo, payload);
    }

    private (FrameType Type, ushort Flags, ulong TopologyVersion, int Len) ParseHeader(byte[] header)
    {
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
        if (magic != 0x31515344)
        {
            throw new IOException("bad frame magic");
        }
        var type = (FrameType)BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(6, 2));
        var flags = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(4, 2));
        var topo = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(8, 8));
        int len = (int)BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
        // Mirror the server's inbound cap: a corrupt or hostile length must
        // not drive a multi-GB allocation.
        if (len is < 0 or > 64 * 1024 * 1024)
        {
            throw new IOException($"frame length {len} out of range");
        }
        return (type, flags, topo, len);
    }

    private Frame Assemble(ushort flags, FrameType type, ulong topo, byte[] payload)
    {
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

    private async Task ReadExactAsync(byte[] buf)
    {
        int off = 0;
        while (off < buf.Length)
        {
            int n = await _stream.ReadAsync(buf.AsMemory(off, buf.Length - off)).ConfigureAwait(false);
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
