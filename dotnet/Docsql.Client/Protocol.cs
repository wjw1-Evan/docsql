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
    /// Read budget per frame. A dead peer / black-holed NAT mapping leaves a
    /// synchronous read blocked forever otherwise (the old code had no read
    /// timeout at all: Open()'s pooled PING and every statement could hang
    /// the caller permanently). Overridable per statement via
    /// <see cref="SendAsync(Frame, CancellationToken, int)"/> and the ADO.NET
    /// CommandTimeout. 0 = infinite (tests/diagnostics only).
    /// </summary>
    /// <remarks>
    /// The setter pushes the value into the socket: the synchronous path
    /// blocks in <c>_stream.Read</c>, which is governed by
    /// <c>ReceiveTimeout</c> — an auto-property here made a per-statement
    /// CommandTimeout a silent no-op on the sync path (the async path passes
    /// the budget explicitly and was unaffected).
    /// </remarks>
    private int _readTimeoutMs = 30_000;
    public int ReadTimeoutMs
    {
        get => _readTimeoutMs;
        set
        {
            _readTimeoutMs = value;
            if (_tcp is { Client: not null } && _tcp.Connected)
            {
                _tcp.ReceiveTimeout = value;
            }
        }
    }

    /// <summary>True after a timeout/IO failure left the frame stream in an
    /// unknown position: the connection must be discarded, never pooled.</summary>
    public bool Broken { get; private set; }

    /// <summary>
    /// 服务端 prepared statement 句柄缓存:模板 SQL → REQ_PREPARE 句柄。
    /// 句柄按物理连接隔离且随连接生死 —— 池化复用同一物理连接即缓存有效;
    /// 超过容量上限时逐个 REQ_CLOSE_STMT 后清空(服务器侧无自动逐出)。
    /// </summary>
    private readonly Dictionary<string, ulong> _prepared = new();

    // 入向重放闸(与服务端 ReplayGuard 对称):同连接的加密帧必须共享对端
    // 进程前缀且计数器严格递增;重放/乱序帧虽可解密但在此拒绝。
    private byte[]? _peerPrefix;
    private long _peerLastCounter;
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
            _tcp.ReceiveTimeout = ReadTimeoutMs;
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
        _tcp.ReceiveTimeout = ReadTimeoutMs;
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
        try
        {
            Write(request);
            return Receive();
        }
        catch (Exception e) when (e is IOException or SocketException)
        {
            // 镜像 SendAsync 的收尾:同步读路径上的任何失败(超时表现为
            // socket 超时的 IOException,断流表现为 IOException)都要毒化
            // 连接。服务端随后仍可能送出迟到应答——被复用的连接会把那帧
            // 当成下一条语句的应答(TCP 有序),静默错配数据;关闭 socket
            // 让 Close() 的弃用判定与后续使用都拦得住。同步超时与异步
            // 路径一样转译为 TimeoutException,两条路径异常形状一致。
            bool timedOut = e is SocketException { SocketErrorCode: SocketError.TimedOut }
                || e.InnerException is SocketException { SocketErrorCode: SocketError.TimedOut };
            Broken = true;
            try { _tcp.Dispose(); } catch { /* already gone */ }
            if (timedOut)
            {
                throw new TimeoutException($"statement read timed out after {ReadTimeoutMs} ms");
            }
            throw;
        }
    }

    /// <summary><see cref="Send"/> 的真异步形态。取消只到语句边界:一帧发到
    /// 一半作废会错位帧流(该连接只能弃用),语句级取消由服务端语句超时承担,
    /// ct 在发送前检查。</summary>
    public async Task<Frame> SendAsync(
        Frame request, CancellationToken cancellationToken = default, int readTimeoutMs = -1)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await WriteAsync(request).ConfigureAwait(false);
        int budget = readTimeoutMs >= 0 ? readTimeoutMs : ReadTimeoutMs;
        if (budget <= 0)
        {
            return await ReadFrameAsync(cancellationToken).ConfigureAwait(false);
        }
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(budget);
        try
        {
            return await ReadFrameAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            Broken = true;
            try { _tcp.Dispose(); } catch { /* already gone */ }
            throw new TimeoutException($"statement read timed out after {budget} ms");
        }
    }

    private Frame SealFrame(Frame request)
    {
        if (_key is null)
        {
            return request;
        }
        var flags = (ushort)(request.Flags | FlagEncrypted);
        return request with
        {
            Flags = flags,
            Payload = Seal(_key, request.Type, flags, request.Payload),
        };
    }

    /// <summary>Associated data for the GCM tag: the transmitted header
    /// fields a receiver routes on (type + flags), bound little-endian like
    /// the wire form. A MITM flipping a flag invalidates the frame instead
    /// of re-routing it (the server does the same).</summary>
    private static byte[] Aad(FrameType type, ushort flags)
    {
        var aad = new byte[4];
        BinaryPrimitives.WriteUInt16LittleEndian(aad.AsSpan(0, 2), (ushort)type);
        BinaryPrimitives.WriteUInt16LittleEndian(aad.AsSpan(2, 2), flags);
        return aad;
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

    /// <summary>进程级 nonce 素材,与服务端 crypto.rs 对称:随机 4 字节前缀 +
    /// 进程级单调 64 位计数器。纯随机 96 位 nonce 受 NIST SP 800-38D 生日界
    /// (每密钥 2^32 次加密)约束,高吞吐扇出集群数天即越界;前缀+计数器在进程内
    /// 永不重复,跨进程仅在「同前缀且同计数器」时碰撞,概率可忽略。计数器结构
    /// 同时让接收端可做重放判定(同连接内计数器必须严格递增)。</summary>
    private static readonly byte[] NoncePrefix = CreateNoncePrefix();
    private static long _nonceCounter;

    private static byte[] CreateNoncePrefix()
    {
        var prefix = new byte[4];
        RandomNumberGenerator.Fill(prefix);
        return prefix;
    }

    /// <summary>AES-256-GCM:nonce(12)=前缀(4)+计数器(8,LE) ‖ 密文 ‖ tag(16),
    /// 头部字段作为 AAD。</summary>
    private static byte[] Seal(byte[] key, FrameType type, ushort flags, byte[] plaintext)
    {
        var nonce = new byte[12];
        NoncePrefix.CopyTo(nonce, 0);
        BinaryPrimitives.WriteInt64LittleEndian(nonce.AsSpan(4, 8),
            Interlocked.Increment(ref _nonceCounter));
        using var gcm = new AesGcm(key, 16);
        var ct = new byte[plaintext.Length];
        var tag = new byte[16];
        gcm.Encrypt(nonce, plaintext, ct, tag, Aad(type, flags));
        var sealed_ = new byte[12 + ct.Length + 16];
        nonce.CopyTo(sealed_, 0);
        ct.CopyTo(sealed_, 12);
        tag.CopyTo(sealed_, 12 + ct.Length);
        return sealed_;
    }

    private static byte[] Unseal(byte[] key, FrameType type, ushort flags, byte[] sealed_)
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
            gcm.Decrypt(nonce, ct, tag, pt, Aad(type, flags));
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
    private async Task<Frame> ReadFrameAsync(CancellationToken cancellationToken = default)
    {
        await ReadExactAsync(_header, cancellationToken).ConfigureAwait(false);
        var (type, flags, topo, len) = ParseHeader(_header);
        var payload = new byte[len];
        await ReadExactAsync(payload, cancellationToken).ConfigureAwait(false);
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
            CheckReplay(payload);
            payload = Unseal(_key, type, flags, payload);
        }
        return new Frame(type, flags, topo, payload);
    }

    /// <summary>校验入向密封帧的 nonce(前缀 + 计数器)满足重放约束。</summary>
    private void CheckReplay(byte[] sealed_)
    {
        if (sealed_.Length < 12)
            throw new DocsqlException("加密载荷过短");
        var counter = BinaryPrimitives.ReadInt64LittleEndian(sealed_.AsSpan(4, 8));
        if (_peerPrefix is null)
        {
            _peerPrefix = sealed_[..4];
            _peerLastCounter = counter;
            return;
        }
        if (!sealed_.AsSpan(0, 4).SequenceEqual(_peerPrefix))
            throw new DocsqlException("加密帧 nonce 前缀在连接中途改变");
        if (counter <= _peerLastCounter)
            throw new DocsqlException("拒绝重放或乱序的加密帧");
        _peerLastCounter = counter;
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

    private async Task ReadExactAsync(byte[] buf, CancellationToken cancellationToken = default)
    {
        int off = 0;
        while (off < buf.Length)
        {
            int n = await _stream.ReadAsync(buf.AsMemory(off, buf.Length - off), cancellationToken)
                .ConfigureAwait(false);
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
