// ADO.NET provider surface for DocSQL.

using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;

namespace Docsql.Client;

public static class EndpointExtensions
{
    public static DocsqlConnectionStringBuilder ToBuilder(
        this (string host, int port, string token) ep) =>
        new() { Host = ep.host, Port = ep.port, Token = ep.token };
}

public sealed class DocsqlConnectionStringBuilder : DbConnectionStringBuilder
{
    public string Host
    {
        get => TryGetValue("host", out var v) ? (string)v : "127.0.0.1";
        set => this["host"] = value;
    }

    public int Port
    {
        get => TryGetValue("port", out var v) && int.TryParse((string)v, out var n)
            ? n
            : (ContainsKey("port")
                ? throw new ArgumentException($"port '{v}' is not a valid integer")
                : 7600);
        set => this["port"] = value.ToString();
    }

    /// <summary>DateTime 参数的线协议形态:<c>ts</c>(默认)发引擎原生
    /// {"$ts":ms} 标记,精度无损、与 TIMESTAMP 列同带比较;<c>iso</c> 发
    /// ISO-8601 文本——对 79a94ba 之前的服务端(不认 $ts,参数会退化成
    /// 字符串字面量匹配不到行)与存量文本时间列的过渡期显式兼容开关。</summary>
    public bool TimestampIso
    {
        get => TryGetValue("timestampformat", out var v)
            && (v as string ?? "").Equals("iso", StringComparison.OrdinalIgnoreCase);
        set => this["timestampformat"] = value ? "iso" : "ts";
    }

    public string Token
    {
        get => TryGetValue("token", out var v) ? (string)v : "";
        set => this["token"] = value;
    }

    /// <summary>传输加密密钥(64 位 hex 字符,32 字节 AES-256-GCM)。</summary>
    public string Key
    {
        get => TryGetValue("key", out var v) ? (string)v : "";
        set => this["key"] = value;
    }

    /// <summary>数据库用户名(REQ_AUTH_USER 登录;与 token 二选一,同时给出时优先用户登录)。</summary>
    public string User
    {
        get => TryGetValue("user", out var v) ? (string)v : "";
        set => this["user"] = value;
    }

    /// <summary>数据库用户密码。</summary>
    public string Password
    {
        get => TryGetValue("password", out var v) ? (string)v : "";
        set => this["password"] = value;
    }

    /// <summary>连接池(默认开启;false = 每次 Open 物理建连、Close 即断)。</summary>
    public bool Pooling
    {
        get => TryGetValue("pooling", out var v)
            ? !(v as string ?? "").Equals("false", StringComparison.OrdinalIgnoreCase)
            : true;
        set => this["pooling"] = value.ToString();
    }

    /// <summary>每个池的物理连接总数上限(借出 + 空闲):池满时 Open 等待
    /// connect timeout 秒后报错(与 SqlClient 语义一致)。</summary>
    public int MaxPoolSize
    {
        get => TryGetValue("max pool size", out var v) && int.TryParse((string)v, out var n)
            ? Math.Max(1, n)
            : 100;
        set => this["max pool size"] = value.ToString();
    }

    /// <summary>连接/借池等待超时秒数(默认 15):TCP 建连与池满等待共用。</summary>
    public int ConnectTimeout
    {
        get => TryGetValue("connect timeout", out var v) && int.TryParse((string)v, out var n)
            ? Math.Max(1, n)
            : 15;
        set => this["connect timeout"] = value.ToString();
    }

    /// <summary>ADO.NET 语义:连接打开后 ConnectionString 是否仍返回凭据
    /// (默认 false —— Open 之后读取 ConnectionString 得到的是掩码副本,
    /// 防止诊断代码/日志经属性带出口令与 token)。</summary>
    public bool PersistSecurityInfo
    {
        get => TryGetValue("persist security info", out var v)
            && (v as string ?? "").Equals("true", StringComparison.OrdinalIgnoreCase);
        set => this["persist security info"] = value.ToString();
    }

    /// <summary>关键字不区分大小写地存在性检查(含同义写法)。</summary>
    public bool HasCredential =>
        ContainsKey("password") || ContainsKey("token") || ContainsKey("key");
}

/// <summary>
/// 物理连接池:按"唯一确定一条已认证会话"的键(host/port/user/password/token/key)池化。
/// <see cref="ConnectionPool.Slot.Permits"/> 封顶并发借出数(Max Pool Size,与 SqlClient
/// 同语义):Rent 先取借出名额(池满等待 connect timeout,超时抛 TimeoutException),
/// 再取空闲连接或新建 —— 新建只在空闲为空时发生,加上归还时的超额裁剪,物理连接总数
/// 因此不超上限。归还/丢弃都释放借出名额,等待者随之被唤醒后重查空闲队列。
/// 借出前用 PING 往返证明连接活着(死连接直接丢弃换新建);事务未了结的连接绝不归还
/// (Close 即物理断开 —— 服务器对断连自动 ROLLBACK,残留事务不可能泄漏给下一个借出者)。
/// 服务端 prepared 句柄缓存挂在物理连接上:同键复用即缓存有效,无需失效。
/// </summary>
internal static class ConnectionPool
{
    internal sealed class Slot
    {
        public readonly ConcurrentQueue<ProtocolConnection> Idle = new();
        public readonly int MaxSize;
        public readonly SemaphoreSlim Permits;

        /// <summary>Set by ClearSlot/ClearAll: a borrowed connection returned
        /// after the slot left the registry must be closed, not enqueued
        /// into the now-unreachable queue (it would leak its socket).</summary>
        public volatile bool Closed;

        public Slot(int maxSize)
        {
            MaxSize = Math.Max(1, maxSize);
            Permits = new SemaphoreSlim(MaxSize, MaxSize);
        }
    }

    private static readonly ConcurrentDictionary<string, Slot> Pools = new();

    /// <summary>池命中 / 未命中(新建) / 丢弃(死连接、超容量裁剪) 计数,测试可断言。</summary>
    internal static long Hits, Misses, Discarded;

    internal static string KeyOf(DocsqlConnectionStringBuilder p, string? keyOverride)
    {
        // The pool registry lives for the process lifetime: keying it on the
        // plaintext password/token retains every credential ever used (and
        // exposes it to memory dumps / telemetry enumerating pools). A
        // SHA-256 digest identifies the session just as well.
        var creds = string.Join('\u0001', p.User, p.Password, p.Token, keyOverride ?? p.Key);
        var digest = Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(creds)));
        return string.Join('\u0001', p.Host, p.Port, digest, p.MaxPoolSize);
    }

    internal static Slot SlotOf(DocsqlConnectionStringBuilder p, string? keyOverride) =>
        Pools.GetOrAdd(KeyOf(p, keyOverride), _ => new Slot(p.MaxPoolSize));

    private static readonly Frame PingFrame =
        new(FrameType.ReqPing, 0, 0, Array.Empty<byte>());

    /// <summary>借出前验活:PING 一次往返通过才算命中;死连接就地丢弃(其借出
    /// 名额早已在归还时释放,这里无需动信号量)。</summary>
    private static ProtocolConnection? TryDequeueLive(Slot slot)
    {
        while (slot.Idle.TryDequeue(out var proto))
        {
            try
            {
                // PING 在服务器上无需认证:一次往返即可证明 TCP 活着且帧通路完好。
                var pong = proto.Send(PingFrame);
                if (pong.Type == FrameType.RespPong)
                {
                    Interlocked.Increment(ref Hits);
                    return proto;
                }
            }
            catch
            {
                // 死连接(对端重启/网络断/超时)。
            }
            Interlocked.Increment(ref Discarded);
            proto.Dispose();
        }
        return null;
    }

    internal static ProtocolConnection Rent(
        Slot slot, DocsqlConnectionStringBuilder p, string? keyOverride, int timeoutMs)
    {
        if (!slot.Permits.Wait(timeoutMs))
        {
            throw new TimeoutException(
                $"pool exhausted: all {slot.MaxSize} connection(s) to {p.Host}:{p.Port} " +
                $"were still in use after {timeoutMs} ms");
        }
        try
        {
            var live = TryDequeueLive(slot);
            if (live is not null)
            {
                return live;
            }
            Interlocked.Increment(ref Misses);
            return DocsqlConnection.ConnectAndAuth(p, keyOverride, timeoutMs);
        }
        catch
        {
            // 新建失败(建连/认证错):借出名额必须还回,否则池永久缩容。
            slot.Permits.Release();
            throw;
        }
    }

    /// <summary><see cref="Rent"/> 的真异步形态(等待名额与 PING 验活均不占线程)。</summary>
    internal static async Task<ProtocolConnection> RentAsync(
        Slot slot, DocsqlConnectionStringBuilder p, string? keyOverride,
        int timeoutMs, CancellationToken cancellationToken)
    {
        if (!await slot.Permits.WaitAsync(TimeSpan.FromMilliseconds(timeoutMs), cancellationToken)
                .ConfigureAwait(false))
        {
            throw new TimeoutException(
                $"pool exhausted: all {slot.MaxSize} connection(s) to {p.Host}:{p.Port} " +
                $"were still in use after {timeoutMs} ms");
        }
        try
        {
            while (slot.Idle.TryDequeue(out var proto))
            {
                try
                {
                    var pong = await proto.SendAsync(PingFrame, cancellationToken).ConfigureAwait(false);
                    if (pong.Type == FrameType.RespPong)
                    {
                        Interlocked.Increment(ref Hits);
                        return proto;
                    }
                }
                catch
                {
                    // 死连接(对端重启/网络断/超时)。
                }
                Interlocked.Increment(ref Discarded);
                proto.Dispose();
            }
            Interlocked.Increment(ref Misses);
            return await DocsqlConnection.ConnectAndAuthAsync(p, keyOverride, timeoutMs, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            slot.Permits.Release();
            throw;
        }
    }

    internal static void Return(Slot? slot, ProtocolConnection proto)
    {
        if (slot is null || slot.Closed)
        {
            Interlocked.Increment(ref Discarded);
            proto.Dispose();
            return;
        }
        slot.Idle.Enqueue(proto);
        slot.Permits.Release();
        // 并发归还可能让空闲数越过 MaxSize:自愈裁剪,物理连接总数不超上限。
        while (slot.Idle.Count > slot.MaxSize && slot.Idle.TryDequeue(out var excess))
        {
            Interlocked.Increment(ref Discarded);
            excess.Dispose();
        }
    }

    /// <summary>事务未了结等不可归还的路径:物理关闭并还回借出名额。</summary>
    internal static void Discard(Slot? slot, ProtocolConnection proto)
    {
        Interlocked.Increment(ref Discarded);
        proto.Dispose();
        slot?.Permits.Release();
    }

    /// <summary>清空全部池(物理关闭所有空闲连接)。进程退出/测试隔离用。</summary>
    public static void ClearAll()
    {
        foreach (var slot in Pools.Values)
        {
            // Mark closed BEFORE draining: a connection borrowed right now
            // will be disposed on Return instead of enqueued into a queue
            // nobody owns.
            slot.Closed = true;
            while (slot.Idle.TryDequeue(out var proto))
            {
                proto.Dispose();
            }
        }
        Pools.Clear();
    }

    public static void ClearSlot(string key)
    {
        if (Pools.TryRemove(key, out var slot))
        {
            slot.Closed = true;
            while (slot.Idle.TryDequeue(out var proto))
            {
                proto.Dispose();
            }
        }
    }
}

public sealed class DocsqlConnection : DbConnection
{
    private ConnectionState _state = ConnectionState.Closed;
    private ProtocolConnection? _proto;
    private ConnectionPool.Slot? _poolSlot;

    /// <summary>事务打开期间为 true:此时 Close 物理断开(服务器断连自动回滚),连接绝不归还池。</summary>
    internal bool InTransaction { get; set; }

    public DocsqlConnection() { }

    /// <param name="connectionString">DocSQL endpoint ("host=..;port=..;token=..") or,
    /// for EF-compatible callers, any string whose endpoint parts live in EndpointOverride.</param>
    public DocsqlConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// Optional explicit endpoint; when set it wins over ConnectionString
    /// (lets hosts like EF's SQLite layer rewrite ConnectionString freely).
    public (string host, int port, string token)? EndpointOverride { get; set; }

    /// Optional explicit transport key (hex); wins over ConnectionString's
    /// key= when EF rewrites the connection string.
    public string? KeyOverride { get; set; }

    /// <summary>hex 密钥 → 32 字节;空串返回 null(明文模式)。</summary>
    internal static byte[]? ParseKey(string hex)
    {
        hex = hex.Trim();
        if (hex.Length == 0) return null;
        if (hex.Length != 64)
            throw new ArgumentException("key 必须是 64 位 hex(32 字节)");
        return Convert.FromHexString(hex);
    }

    // Raw backing store — internal reads (endpoint parsing, pool keys) must
    // always see the credentials, while the public getter masks them after
    // Open (ADO.NET Persist Security Info semantics, see below).
    private string _connectionString = "";

    internal DocsqlConnectionStringBuilder Parsed => new() { ConnectionString = _connectionString };

    /// <summary>连接打开后默认掩去 password/token/key(ADO.NET Persist
    /// Security Info 语义);连接串写 "persist security info=true" 才返回
    /// 原文。诊断代码读到的串交给日志或异常不再携带凭据。</summary>
    public override string ConnectionString
    {
        get => ShouldMaskCredentials ? MaskCredentials(_connectionString) : _connectionString;
        set => _connectionString = value ?? "";
    }

    private bool ShouldMaskCredentials
    {
        get
        {
            if (_state != ConnectionState.Open) return false;
            return !new DocsqlConnectionStringBuilder { ConnectionString = _connectionString }
                .PersistSecurityInfo;
        }
    }

    internal static string MaskCredentials(string raw)
    {
        var b = new DocsqlConnectionStringBuilder { ConnectionString = raw };
        foreach (var k in new[] { "password", "token", "key" })
        {
            if (b.ContainsKey(k)) b[k] = "***";
        }
        return b.ConnectionString;
    }

    public override string Database => "docsql";

    public override string DataSource => $"{Parsed.Host}:{Parsed.Port}";

    public override string ServerVersion => "0.1";

    /// <summary>连接/借池等待超时秒数(连接串 connect timeout,默认 15)。</summary>
    public override int ConnectionTimeout => Parsed.ConnectTimeout;

    public override ConnectionState State => _state;

    internal ProtocolConnection Proto =>
        _proto ?? throw new InvalidOperationException("connection is closed");

    public override void ChangeDatabase(string databaseName) { }

    /// <summary>
    /// 故障转移提升(REQ_PROMOTE):把只读副本提升为可写节点。
    /// </summary>
    public void Promote()
    {
        Proto.Send(new Frame(FrameType.ReqPromote, 0, 0, Array.Empty<byte>())).EnsureOk();
    }

    /// <summary>
    /// 发布一条持久化 pub/sub 消息(REQ_PUBLISH):服务端先落盘(WAL)再向
    /// 订阅者推送。返回 (消息 id, 实时收到的连接数);id 单调递增,可作为
    /// 断线续传的游标(DocsqlSubscriber 的 from 参数)。
    /// </summary>
    public (long Id, long Receivers) Publish(string channel, string payload)
    {
        var body = JsonSerializer.Serialize(new { channel, payload });
        var resp = Proto.Send(new Frame(FrameType.ReqPublish, 0, 0, Encoding.UTF8.GetBytes(body)))
            .EnsureOk();
        using var doc = JsonDocument.Parse(Encoding.UTF8.GetString(resp.Payload));
        var row = doc.RootElement.GetProperty("rows")[0];
        return (row[0].GetInt64(), row[1].GetInt64());
    }

    /// <summary>
    /// 保留某频道最新 keep 条消息,更早的删除(REQ_PUBSUB trim,随写复制)。
    /// 返回删除的条数。keep 必须 ≥ 1(清空会破坏 id 单调性)。
    /// </summary>
    public long PubsubTrim(string channel, long keep)
    {
        var body = JsonSerializer.Serialize(new { sub = "trim", channel, keep });
        var resp = Proto.Send(new Frame(FrameType.ReqPubsub, 0, 0, Encoding.UTF8.GetBytes(body)));
        return resp.Type switch
        {
            FrameType.RespAffected => DecodeLong(resp.Payload),
            _ => throw new DocsqlException(Encoding.UTF8.GetString(resp.Payload)),
        };
    }

    internal static long DecodeLong(byte[] payload) =>
        payload.Length >= 8 ? BitConverter.ToInt64(payload, 0) : 0;

    /// <summary>建连 + 认证(用户名/密码走 REQ_AUTH_USER,否则 token 走 REQ_AUTH):
    /// DocsqlConnection.Open 与 DocsqlSubscriber 共用的握手骨架。</summary>
    internal static ProtocolConnection ConnectAndAuth(
        DocsqlConnectionStringBuilder p, string? keyOverride, int timeoutMs)
    {
        var proto = new ProtocolConnection(p.Host, p.Port, ParseKey(keyOverride ?? p.Key), timeoutMs);
        try
        {
            SendAuth(proto, p);
            return proto;
        }
        catch
        {
            // Never leak the socket on a failed handshake (a retry loop would
            // orphan it).
            proto.Dispose();
            throw;
        }
    }

    /// <summary><see cref="ConnectAndAuth"/> 的真异步形态。</summary>
    internal static async Task<ProtocolConnection> ConnectAndAuthAsync(
        DocsqlConnectionStringBuilder p, string? keyOverride, int timeoutMs,
        CancellationToken cancellationToken)
    {
        var proto = await ProtocolConnection.ConnectAsync(
                p.Host, p.Port, ParseKey(keyOverride ?? p.Key), timeoutMs, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await SendAuthAsync(proto, p, cancellationToken).ConfigureAwait(false);
            return proto;
        }
        catch
        {
            proto.Dispose();
            throw;
        }
    }

    private static void SendAuth(ProtocolConnection proto, DocsqlConnectionStringBuilder p)
    {
        foreach (var frame in AuthFrames(p))
        {
            proto.Send(frame).EnsureOk("auth failed: ");
        }
    }

    private static async Task SendAuthAsync(
        ProtocolConnection proto, DocsqlConnectionStringBuilder p, CancellationToken ct)
    {
        foreach (var frame in AuthFrames(p))
        {
            (await proto.SendAsync(frame, ct).ConfigureAwait(false)).EnsureOk("auth failed: ");
        }
    }

    /// <summary>认证帧序列:用户登录优先,退回 token;两者皆空 = 匿名。</summary>
    private static IEnumerable<Frame> AuthFrames(DocsqlConnectionStringBuilder p)
    {
        if (!string.IsNullOrEmpty(p.User))
        {
            var body = JsonSerializer.Serialize(new { user = p.User, password = p.Password });
            yield return new Frame(
                FrameType.ReqAuthUser, 0, 0, Encoding.UTF8.GetBytes(body));
        }
        else if (!string.IsNullOrEmpty(p.Token))
        {
            yield return new Frame(
                FrameType.ReqAuth, 0, 0, Encoding.UTF8.GetBytes(p.Token));
        }
    }

    public override void Open()
    {
        if (_state == ConnectionState.Open)
        {
            return;
        }
        var p = EndpointOverride is { } ep ? ep.ToBuilder() : Parsed;
        try
        {
            if (p.Pooling)
            {
                var slot = ConnectionPool.SlotOf(p, KeyOverride);
                _proto = ConnectionPool.Rent(slot, p, KeyOverride, p.ConnectTimeout * 1000);
                _poolSlot = slot;
            }
            else
            {
                _proto = ConnectAndAuth(p, KeyOverride, p.ConnectTimeout * 1000);
            }
        }
        catch
        {
            _proto = null;
            _poolSlot = null;
            _state = ConnectionState.Broken;
            throw;
        }
        _state = ConnectionState.Open;
    }

    /// <summary><see cref="Open"/> 的真异步形态:建连、认证与池等待均不占线程。</summary>
    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state == ConnectionState.Open)
        {
            return;
        }
        var p = EndpointOverride is { } ep ? ep.ToBuilder() : Parsed;
        try
        {
            if (p.Pooling)
            {
                var slot = ConnectionPool.SlotOf(p, KeyOverride);
                _proto = await ConnectionPool.RentAsync(slot, p, KeyOverride, p.ConnectTimeout * 1000, cancellationToken)
                    .ConfigureAwait(false);
                _poolSlot = slot;
            }
            else
            {
                _proto = await ConnectAndAuthAsync(p, KeyOverride, p.ConnectTimeout * 1000, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch
        {
            _proto = null;
            _poolSlot = null;
            _state = ConnectionState.Broken;
            throw;
        }
        _state = ConnectionState.Open;
    }

    public override void Close()
    {
        if (_proto is not null)
        {
            var proto = _proto;
            _proto = null;
            if (InTransaction || proto.Broken)
            {
                // 事务未了结就 Close:物理断开(服务器断连自动 ROLLBACK)。
                // 归还一个带着开放事务的连接,会把事务泄漏给下一个借出者。
                // Broken 同理:读超时/IO 失败后帧流停在未知位置。
                ConnectionPool.Discard(_poolSlot, proto);
            }
            else
            {
                ConnectionPool.Return(_poolSlot, proto);
            }
        }
        _poolSlot = null;
        _state = ConnectionState.Closed;
    }

    /// <summary>清空与当前连接串对应的池(物理关闭空闲连接)。</summary>
    public void ClearPool() =>
        ConnectionPool.ClearSlot(ConnectionPool.KeyOf(
            EndpointOverride is { } ep ? ep.ToBuilder() : Parsed, KeyOverride));

    /// <summary>清空全部池(物理关闭所有空闲连接)。</summary>
    public static void ClearAllPools() => ConnectionPool.ClearAll();

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        new DocsqlTransaction(this, isolationLevel);

    protected override DbCommand CreateDbCommand() =>
        new DocsqlCommand { Connection = this };

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }
}

public sealed class DocsqlParameter : DbParameter
{
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }
    public override string? ParameterName { get; set; }
    public override int Size { get; set; }
    public override string SourceColumn { get; set; } = "";
    public override bool SourceColumnNullMapping { get; set; }
    public override object? Value { get; set; }

    public override void ResetDbType() => DbType = DbType.Object;
}

public sealed class DocsqlCommand : DbCommand
{
    public DocsqlCommand() { }

    public new DocsqlConnection? Connection { get; set; }

    protected override DbConnection? DbConnection
    {
        get => Connection;
        set => Connection = value as DocsqlConnection ?? throw new ArgumentException("wrong connection type");
    }

    public override string CommandText { get; set; } = "";

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override System.Data.UpdateRowSource UpdatedRowSource { get; set; } = System.Data.UpdateRowSource.None;

    public override bool DesignTimeVisible { get; set; }

    protected override bool CanRaiseEvents => false;

    public new DocsqlParameterCollection Parameters { get; } = new();

    protected override DbParameterCollection DbParameterCollection => Parameters;

    /// <summary>
    /// ADO.NET 的 Cancel 语义在本客户端由语句超时承担:帧流是一问一答的
    /// 二进制流,单方面中止读取会错位帧边界(连接只能作废)。设置
    /// CommandTimeout 即可获得有界的语句执行时间。
    /// </summary>
    public override void Cancel() { }

    public override int ExecuteNonQuery()
    {
        var frame = Execute();
        return frame.Type switch
        {
            FrameType.RespAffected => DecodeAffected(frame.Payload),
            FrameType.RespRows => throw new DocsqlException("statement returned rows"),
            _ => throw new DocsqlException(ErrorText(frame)),
        };
    }

    public override object? ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (!reader.Read() || reader.FieldCount == 0)
        {
            return null;
        }
        return NarrowScalar(reader.GetValue(0));
    }

    /// <summary>标量窄化:整数值返回 long,小数保留原类型。
    /// decimal 用自身比较判断整数性——先转 double 会让 17 位大数因精度丢失去整,
    /// 静默截断金额;double/float 仍按各自的精确整数性处理(3.5、AVG 结果保持浮点)。</summary>
    private static object? NarrowScalar(object? v)
    {
        if (v is decimal m)
        {
            if (m == decimal.Truncate(m) && m >= long.MinValue && m <= long.MaxValue)
            {
                return (long)m;
            }
            return v;
        }
        if (v is double or float)
        {
            var d = Convert.ToDouble(v, CultureInfo.InvariantCulture);
            if (Math.Floor(d) == d)
            {
                try
                {
                    return Convert.ToInt64(d);
                }
                catch (OverflowException)
                {
                    // out-of-range doubles (1e300, SUM overflow) return as-is
                }
            }
        }
        return v;
    }

    public new DocsqlDataReader ExecuteReader() => (DocsqlDataReader)base.ExecuteReader();

    protected override DbTransaction? DbTransaction { get; set; }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var frame = Execute();
        return frame.Type switch
        {
            FrameType.RespRows => new DocsqlDataReader(frame.Payload),
            FrameType.RespAffected => new DocsqlDataReader(
                Array.Empty<byte>(), DecodeAffected(frame.Payload)),
            _ => throw new DocsqlException(ErrorText(frame)),
        };
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var frame = await ExecuteAsync(cancellationToken).ConfigureAwait(false);
        return frame.Type switch
        {
            FrameType.RespRows => new DocsqlDataReader(frame.Payload),
            FrameType.RespAffected => new DocsqlDataReader(
                Array.Empty<byte>(), DecodeAffected(frame.Payload)),
            _ => throw new DocsqlException(ErrorText(frame)),
        };
    }

    private Frame SqlFrame() =>
        new(FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql(CommandText));

    private static Frame ExecuteBody(
        ulong handle, List<object?> values, bool timestampIso)
    {
        var sb = new StringBuilder(values.Count * 8 + 32);
        sb.Append("{\"handle\":").Append(handle).Append(",\"params\":[");
        for (int i = 0; i < values.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }
            sb.Append(JsonOf(values[i], timestampIso));
        }
        sb.Append("]}");
        return new Frame(FrameType.ReqExecute, 0, 0, Encoding.UTF8.GetBytes(sb.ToString()));
    }

    private Frame Execute()
    {
        if (Connection is not { State: ConnectionState.Open } conn)
        {
            throw new InvalidOperationException("connection is not open");
        }
        // CommandTimeout 驱动读取预算(0 = ADO.NET 默认无限制,这里落到连接
        // 默认的 30s 读超时,而不是原来的永久阻塞)。
        conn.Proto.ReadTimeoutMs = CommandTimeout > 0 ? CommandTimeout * 1000 : 30_000;
        if (Parameters.Count == 0)
        {
            return conn.Proto.Send(SqlFrame());
        }
        // 参数化语句走服务端绑定:占位符改写为 ?,模板注册 REQ_PREPARE(物理连接
        // 内按句柄缓存),参数数组经 REQ_EXECUTE 执行 —— 值由服务器渲染为类型化
        // 字面量(引号感知、字符串翻倍转义),任何取值都无法逃逸字面量;授权/审计
        // 与 REQ_SQL 同路径。响应帧形状与 REQ_SQL 完全一致。
        var (template, values) = RewriteParameters();
        var (handle, _) = conn.Proto.GetOrPrepare(template);
        return conn.Proto.Send(ExecuteBody(handle, values, conn.Parsed.TimestampIso));
    }

    /// <summary><see cref="Execute"/> 的真异步形态(同一服务端绑定路径)。</summary>
    private async Task<Frame> ExecuteAsync(CancellationToken cancellationToken)
    {
        if (Connection is not { State: ConnectionState.Open } conn)
        {
            throw new InvalidOperationException("connection is not open");
        }
        int readTimeoutMs = CommandTimeout > 0 ? CommandTimeout * 1000 : 30_000;
        if (Parameters.Count == 0)
        {
            return await conn.Proto
                .SendAsync(SqlFrame(), cancellationToken, readTimeoutMs)
                .ConfigureAwait(false);
        }
        var (template, values) = RewriteParameters();
        var (handle, _) = await conn.Proto
            .GetOrPrepareAsync(template, cancellationToken)
            .ConfigureAwait(false);
        return await conn.Proto
            .SendAsync(
                ExecuteBody(handle, values, conn.Parsed.TimestampIso),
                cancellationToken, readTimeoutMs)
            .ConfigureAwait(false);
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var frame = await ExecuteAsync(cancellationToken).ConfigureAwait(false);
        return frame.Type switch
        {
            FrameType.RespAffected => DecodeAffected(frame.Payload),
            FrameType.RespRows => throw new DocsqlException("statement returned rows"),
            _ => throw new DocsqlException(ErrorText(frame)),
        };
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        using var reader = (DocsqlDataReader)await ExecuteDbDataReaderAsync(
            CommandBehavior.Default, cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.FieldCount == 0)
        {
            return null;
        }
        return NarrowScalar(reader.GetValue(0));
    }

    /// Substitute @name parameters with `?` marks, returning the values in
    /// marker order. A single scanner pass skips '...' string literals and
    /// matches whole identifiers, so <c>@id</c> never rewrites <c>@id2</c>,
    /// literals containing <c>@name</c> stay intact, and a repeated name
    /// produces one marker per occurrence (duplicated value).
    private (string Template, List<object?> Values) RewriteParameters()
    {
        var sql = CommandText;
        var values = new List<object?>(Parameters.Count);
        var sb = new StringBuilder(sql.Length);
        int i = 0;
        while (i < sql.Length)
        {
            char c = sql[i];
            if (c == '\'')
            {
                // Copy the string literal verbatim ('' is an escaped quote).
                int j = i + 1;
                while (j < sql.Length)
                {
                    if (sql[j] == '\'')
                    {
                        if (j + 1 < sql.Length && sql[j + 1] == '\'')
                        {
                            j += 2;
                            continue;
                        }
                        j++;
                        break;
                    }
                    j++;
                }
                sb.Append(sql[i..j]);
                i = j;
                continue;
            }
            if (c == '"' || c == '`')
            {
                // Quoted identifier: copy verbatim — an @-lookalike inside
                // "some@ident" / `some@ident` is part of the name, not a
                // parameter. (The server-side placeholder scanner skips the
                // same contexts, so both scans agree.)
                char quote = c;
                int j = i + 1;
                while (j < sql.Length)
                {
                    if (sql[j] == quote)
                    {
                        if (j + 1 < sql.Length && sql[j + 1] == quote)
                        {
                            j += 2;
                            continue;
                        }
                        j++;
                        break;
                    }
                    j++;
                }
                sb.Append(sql[i..j]);
                i = j;
                continue;
            }
            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                // Line comment: verbatim through the newline.
                int j = sql.IndexOf('\n', i);
                j = j < 0 ? sql.Length : j + 1;
                sb.Append(sql[i..j]);
                i = j;
                continue;
            }
            if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                // Block comment: verbatim through the closing star-slash.
                int close = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
                int j = close < 0 ? sql.Length : close + 2;
                sb.Append(sql[i..j]);
                i = j;
                continue;
            }
            if (c == '@')
            {
                int k = i + 1;
                while (k < sql.Length && (char.IsLetterOrDigit(sql[k]) || sql[k] == '_'))
                {
                    k++;
                }
                if (k > i + 1)
                {
                    var name = sql[(i + 1)..k];
                    var p = FindParameter(name);
                    if (p is not null)
                    {
                        sb.Append('?');
                        values.Add(p.Value);
                        i = k;
                        continue;
                    }
                }
            }
            sb.Append(c);
            i++;
        }
        return (sb.ToString(), values);
    }

    private DocsqlParameter? FindParameter(string name) =>
        Parameters.Cast<DocsqlParameter>().FirstOrDefault(
            p => (p.ParameterName?.TrimStart('@') ?? "") == name);

    /// <summary>参数值 → JSON(REQ_EXECUTE 的 params 数组元素)。字符串值在
    /// 服务端转义绑定;decimal 与 byte[] 用带类型标记的对象载荷($dec/$bytes),
    /// 精确值不经过 IEEE double,服务端解码为引擎原生 DECIMAL/BLOB 值。
    /// DateTime/DateTimeOffset 走 $ts 标记:引擎原生 TIMESTAMP(UTC 毫秒,
    /// 毫秒精度),比较/排序按时间带执行;本地值转为 UTC 后存储,旧库中的
    /// ISO 文本值读取路径兼容(Convert.ToDateTime 双向解析)。</summary>
    private static string JsonOf(object? v, bool timestampIso = false) => v switch
    {
        null or DBNull => "null",
        bool b => b ? "true" : "false",
        int or long or short or byte or sbyte
            => JsonSerializer.Serialize(Convert.ToInt64(v, CultureInfo.InvariantCulture)),
        uint or ushort => JsonSerializer.Serialize(Convert.ToInt64(v, CultureInfo.InvariantCulture)),
        ulong u => JsonSerializer.Serialize(u),
        // 非有限浮点:JSON 数字装不下,发 $float 标记(引擎双向对称)。
        // System.Text.Json 对 NaN/∞ 的 Serialize 直接抛异常,不发标记就发不出去。
        double d when !double.IsFinite(d) => FloatJson(d),
        double d => JsonSerializer.Serialize(d),
        float f when !float.IsFinite(f) => FloatJson(f),
        float f => JsonSerializer.Serialize((double)f),
        // 精确小数文本走 $dec 标记,服务端渲染 CAST(... AS DECIMAL)。
        decimal m => "{\"$dec\":\"" + m.ToString(CultureInfo.InvariantCulture) + "\"}",
        // DateTime 族走 $ts 标记(UTC 毫秒):引擎原生 TIMESTAMP 值,
        // 与时间列的时间带比较/排序一致。Kind=Unspecified 按 UTC 存取
        // (不偷偷做本地时区换算);存量 ISO 文本值的读取路径不变。
        DateTime dt when timestampIso => IsoJson(dt.Kind == DateTimeKind.Local
                ? dt.ToUniversalTime()
                : DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
        DateTime dt => "{\"$ts\":" + new DateTimeOffset(dt.Kind == DateTimeKind.Local
                ? dt.ToUniversalTime()
                : DateTime.SpecifyKind(dt, DateTimeKind.Utc),
            TimeSpan.Zero).ToUnixTimeMilliseconds() + "}",
        DateTimeOffset dto when timestampIso => IsoJson(dto.UtcDateTime),
        DateTimeOffset dto => "{\"$ts\":" + dto.ToUnixTimeMilliseconds() + "}",
        TimeSpan ts => JsonSerializer.Serialize(
            ts.ToString("c", CultureInfo.InvariantCulture)),
        DateOnly d => JsonSerializer.Serialize(
            d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        TimeOnly t => JsonSerializer.Serialize(
            t.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture)),
        string s => JsonSerializer.Serialize(s),
        char c => JsonSerializer.Serialize(c.ToString()),
        Guid g => JsonSerializer.Serialize(g.ToString()),
        // BLOB:整数数组的 $bytes 标记,服务端解码后渲染 x'..' 十六进制字面量。
        byte[] b => BytesJson(b),
        // Enums have no wire form; their ToString() name would be quoted as
        // text and silently match nothing.
        Enum => throw new NotSupportedException(
            "enum parameters are not supported; convert to the underlying integer first"),
        _ => throw new NotSupportedException(
            $"parameter type {v.GetType().Name} is not supported"),
    };

    private static string FloatJson(double d) => d switch
    {
        _ when double.IsNaN(d) => "{\"$float\":\"NaN\"}",
        _ when d > 0 => "{\"$float\":\"inf\"}",
        _ => "{\"$float\":\"-inf\"}",
    };

    /// <summary>ISO-8601 文本参数(timestampformat=iso):对不认 $ts 标记的
    /// 旧服务端与存量文本时间列的过渡期兼容形态;引擎的比较路径会把可解析
    /// 的时间文本按时间语义比较。</summary>
    private static string IsoJson(DateTime utc) =>
        JsonSerializer.Serialize(utc.ToString("O", CultureInfo.InvariantCulture));

    private static string BytesJson(byte[] b)
    {
        var sb = new StringBuilder(b.Length * 4 + 12);
        sb.Append("{\"$bytes\":[");
        for (int i = 0; i < b.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }
            sb.Append(b[i]);
        }
        sb.Append("]}");
        return sb.ToString();
    }

    private static string ErrorText(Frame f) => Encoding.UTF8.GetString(f.Payload);

    internal static int DecodeAffected(byte[] payload)
    {
        if (payload.Length < 8)
        {
            return 0;
        }
        // The server sends a u64 count; BitConverter.ToInt32 truncated
        // counts above int.MaxValue into negative/garbage values.
        ulong n = BitConverter.ToUInt64(payload, 0);
        return n > int.MaxValue ? int.MaxValue : (int)n;
    }

    /// <summary>预注册服务端 prepared statement(REQ_PREPARE,句柄按物理连接
    /// 缓存):命令首次执行即省一次注册往返。无参数命令为 no-op(不走绑定路径)。</summary>
    public override void Prepare()
    {
        if (Connection is not { State: ConnectionState.Open })
        {
            throw new InvalidOperationException("connection is not open");
        }
        if (Parameters.Count > 0)
        {
            var (template, _) = RewriteParameters();
            _ = Connection.Proto.GetOrPrepare(template);
        }
    }

    /// <summary><see cref="Prepare"/> 的真异步形态。</summary>
    public override async Task PrepareAsync(CancellationToken cancellationToken)
    {
        if (Connection is not { State: ConnectionState.Open })
        {
            throw new InvalidOperationException("connection is not open");
        }
        if (Parameters.Count > 0)
        {
            var (template, _) = RewriteParameters();
            _ = await Connection.Proto.GetOrPrepareAsync(template, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    protected override DbParameter CreateDbParameter() => new DocsqlParameter();
}

public sealed class DocsqlParameterCollection : DbParameterCollection
{
    private readonly List<DocsqlParameter> _list = new();

    public override int Count => _list.Count;
    public override object SyncRoot => _list;

    public DocsqlParameter AddWithValue(string name, object? value)
    {
        var p = new DocsqlParameter { ParameterName = name, Value = value };
        _list.Add(p);
        return p;
    }

    public override int Add(object value)
    {
        _list.Add((DocsqlParameter)value);
        return _list.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var v in values)
        {
            Add(v);
        }
    }

    public override void Clear() => _list.Clear();

    public override bool Contains(object value) => _list.Contains((DocsqlParameter)value);

    public override bool Contains(string value) => _list.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index)
    {
        for (int i = 0; i < _list.Count; i++)
        {
            array.SetValue(_list[i], index + i);
        }
    }

    public override System.Collections.IEnumerator GetEnumerator() => _list.GetEnumerator();

    public override int IndexOf(object value) => _list.IndexOf((DocsqlParameter)value);

    public override int IndexOf(string parameterName) =>
        _list.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value) => _list.Insert(index, (DocsqlParameter)value);

    public override void Remove(object value) => _list.Remove((DocsqlParameter)value);

    public override void RemoveAt(int index) => _list.RemoveAt(index);

    public override void RemoveAt(string parameterName) => _list.RemoveAt(IndexOf(parameterName));

    protected override DbParameter GetParameter(int index) => _list[index];

    protected override DbParameter GetParameter(string parameterName) => _list[IndexOf(parameterName)];

    protected override void SetParameter(int index, DbParameter value) => _list[index] = (DocsqlParameter)value;

    protected override void SetParameter(string parameterName, DbParameter value) =>
        _list[IndexOf(parameterName)] = (DocsqlParameter)value;
}

public sealed class DocsqlDataReader : DbDataReader
{
    private readonly List<string> _columns = new();
    private readonly List<object?[]> _rows = new();
    private readonly List<Type> _types = new();
    private int _pos = -1;
    private bool _closed;

    private readonly int _recordsAffected;

    internal DocsqlDataReader(byte[] payload, int recordsAffected = 0)
    {
        _recordsAffected = recordsAffected;
        if (payload.Length == 0)
        {
            return;
        }
        using var doc = JsonDocument.Parse(Encoding.UTF8.GetString(payload));
        foreach (var c in doc.RootElement.GetProperty("columns").EnumerateArray())
        {
            _columns.Add(c.GetString() ?? "");
        }
        foreach (var r in doc.RootElement.GetProperty("rows").EnumerateArray())
        {
            _rows.Add(r.EnumerateArray().Select(ElemToValue).ToArray());
        }
        // Column type from the first non-null value (schemaless storage).
        for (int i = 0; i < _columns.Count; i++)
        {
            _types.Add(_rows.FirstOrDefault(r => r[i] is not null)?[i]?.GetType() ?? typeof(object));
        }
    }

    private static object? ElemToValue(JsonElement e) => e.ValueKind switch
    {
        // (object) cast: without it the ternary's type is the common type
        // double — every JSON integer silently became a Double, losing
        // precision past 2^53 (int64 IDs from other stores corrupt).
        JsonValueKind.Number => e.TryGetInt64(out var l) ? (object)l : e.GetDouble(),
        JsonValueKind.String => e.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        // Typed markers from the server: exact DECIMAL / BLOB scalars.
        JsonValueKind.Object => MarkerToValue(e),
        _ => e.GetRawText(),
    };

    private static object? MarkerToValue(JsonElement e)
    {
        if (e.TryGetProperty("$dec", out var dec) && dec.ValueKind == JsonValueKind.String)
        {
            return decimal.Parse(dec.GetString()!, CultureInfo.InvariantCulture);
        }
        // TIMESTAMP: UTC milliseconds (engine stores i64 ms, year 0001-9999).
        // Surfaced as UTC DateTime so existing readers keep working; the
        // engine is millisecond-precision, sub-ms digits do not round-trip.
        if (e.TryGetProperty("$ts", out var ts) && ts.ValueKind == JsonValueKind.Number)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(ts.GetInt64()).UtcDateTime;
        }
        if (e.TryGetProperty("$bytes", out var bytes) && bytes.ValueKind == JsonValueKind.Array)
        {
            var b = new byte[bytes.GetArrayLength()];
            int i = 0;
            foreach (var x in bytes.EnumerateArray())
            {
                b[i++] = x.GetByte();
            }
            return b;
        }
        // Non-finite floats ride the same marker family (the engine cannot
        // emit them as JSON numbers): NaN/inf/-inf map straight onto the
        // double constants so GetDouble works instead of handing the caller
        // the raw marker string.
        if (e.TryGetProperty("$float", out var f) && f.ValueKind == JsonValueKind.String)
        {
            return f.GetString() switch
            {
                "NaN" => double.NaN,
                "inf" or "+inf" or "Infinity" => double.PositiveInfinity,
                "-inf" or "-Infinity" => double.NegativeInfinity,
                _ => e.GetRawText(),
            };
        }
        return e.GetRawText();
    }

    public override int FieldCount => _columns.Count;
    public override bool HasRows => _rows.Count > 0;
    public override bool IsClosed => _closed;

    public override void Close() => _closed = true;

    // ADO.NET contract: IsClosed flips on Dispose — EF's RelationalDataReader
    // double-dispose guard and Dapper read the flag to dedupe cleanup.
    protected override void Dispose(bool disposing)
    {
        _closed = true;
        base.Dispose(disposing);
    }
    public override int RecordsAffected => _recordsAffected;

    public override bool Read()
    {
        if (_pos + 1 >= _rows.Count)
        {
            return false;
        }
        _pos++;
        return true;
    }

    private object?[] CurrentRow => _pos >= 0 && _pos < _rows.Count
        ? _rows[_pos]
        : throw new InvalidOperationException("no current row (call Read first)");

    // ADO.NET contract: NULL columns surface as DBNull.Value, not null.
    public override object GetValue(int ordinal) => CurrentRow[ordinal] ?? DBNull.Value;
    public override bool IsDBNull(int ordinal) => CurrentRow[ordinal] is null;
    public override string GetName(int ordinal) => _columns[ordinal];
    public override int GetOrdinal(string name)
    {
        var idx = _columns.IndexOf(name);
        return idx >= 0 ? idx : throw new IndexOutOfRangeException($"no column named '{name}'");
    }

    public override long GetInt64(int ordinal) => Convert.ToInt64(CurrentRow[ordinal]);
    public override int GetInt32(int ordinal) => Convert.ToInt32(CurrentRow[ordinal]);
    public override double GetDouble(int ordinal) => Convert.ToDouble(CurrentRow[ordinal]);
    public override string GetString(int ordinal) => Convert.ToString(CurrentRow[ordinal])!;
    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(CurrentRow[ordinal]);

    /// <summary>EF 提供程序按类型映射读取;泛型读取覆盖 DECIMAL/BLOB/DATE/TIME 文本。</summary>
    public override T GetFieldValue<T>(int ordinal)
    {
        var v = CurrentRow[ordinal];
        if (v is T typed)
        {
            return typed;
        }
        if (v is null)
        {
            throw new InvalidCastException(
                $"column {_columns[ordinal]} is NULL; check IsDBNull before GetFieldValue");
        }
        var t = typeof(T);
        if (t == typeof(byte[]))
        {
            var bytes = v switch
            {
                byte[] b => b,
                string s => Convert.FromBase64String(s),
                _ => throw new InvalidCastException(
                    $"column {_columns[ordinal]} is not a BLOB value"),
            };
            return (T)(object)bytes;
        }
        if (t == typeof(DateOnly))
        {
            return (T)(object)DateOnly.Parse(
                Convert.ToString(v, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture);
        }
        if (t == typeof(TimeOnly))
        {
            return (T)(object)TimeOnly.Parse(
                Convert.ToString(v, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture);
        }
        if (t == typeof(decimal))
        {
            return (T)(object)Convert.ToDecimal(v, CultureInfo.InvariantCulture);
        }
        if (t == typeof(DateTime))
        {
            return (T)(object)Convert.ToDateTime(v, CultureInfo.InvariantCulture);
        }
        if (t == typeof(Guid))
        {
            return (T)(object)Guid.Parse(Convert.ToString(v, CultureInfo.InvariantCulture)!);
        }
        return (T)Convert.ChangeType(v, t, CultureInfo.InvariantCulture);
    }

    public override int GetValues(object[] values)
    {
        int n = Math.Min(values.Length, _columns.Count);
        for (int i = 0; i < n; i++)
        {
            // Contract: NULL must come back as DBNull.Value.
            values[i] = CurrentRow[i] ?? DBNull.Value;
        }
        return n;
    }

    public override System.Collections.IEnumerator GetEnumerator() => _rows.GetEnumerator();

    public override object this[int ordinal] => GetValue(ordinal)!;

    public override object this[string name] => GetValue(GetOrdinal(name))!;

    public override short GetInt16(int ordinal) => Convert.ToInt16(CurrentRow[ordinal]);


    public override System.Data.DataTable GetSchemaTable()
    {
        var t = new System.Data.DataTable();
        t.Columns.Add("ColumnName", typeof(string));
        t.Columns.Add("ColumnOrdinal", typeof(int));
        t.Columns.Add("DataType", typeof(Type));
        t.Columns.Add("AllowDBNull", typeof(bool));
        for (int i = 0; i < _columns.Count; i++)
        {
            int ordinal = i;
            bool nullable = _rows.Any(r => r[ordinal] is null);
            t.Rows.Add(_columns[i], ordinal, _types[i], nullable);
        }
        return t;
    }

    #region Not needed for basic flows

    public override int Depth => 0;
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    public override Type GetFieldType(int ordinal) => _types[ordinal];
    public override char GetChar(int ordinal) => Convert.ToChar(CurrentRow[ordinal]);
    public override byte GetByte(int ordinal) => Convert.ToByte(CurrentRow[ordinal]);
    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));
    public override float GetFloat(int ordinal) => Convert.ToSingle(CurrentRow[ordinal]);
    public override decimal GetDecimal(int ordinal) =>
        Convert.ToDecimal(CurrentRow[ordinal], CultureInfo.InvariantCulture);
    public override DateTime GetDateTime(int ordinal) =>
        Convert.ToDateTime(CurrentRow[ordinal], CultureInfo.InvariantCulture);
    public override long GetBytes(
        int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var v = CurrentRow[ordinal];
        var bytes = v switch
        {
            byte[] b => b,
            string s => Convert.FromBase64String(s),
            _ => throw new InvalidCastException($"column {_columns[ordinal]} is not a BLOB value"),
        };
        if (buffer is null)
        {
            return bytes.Length;
        }
        if (dataOffset < 0 || dataOffset > bytes.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(dataOffset));
        }
        int n = (int)Math.Min(length, bytes.Length - dataOffset);
        Array.Copy(bytes, dataOffset, buffer, bufferOffset, n);
        return n;
    }
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
    public override bool NextResult() => false;

    #endregion
}

public sealed class DocsqlTransaction : DbTransaction
{
    private readonly DocsqlConnection _conn;
    private bool _done;

    public DocsqlTransaction(DocsqlConnection conn, IsolationLevel iso)
    {
        _conn = conn;
        IsolationLevel = iso;
        Run("BEGIN");
        // 事务归属这条物理连接:InTransaction 期间 Close 把连接物理丢弃
        // 而不是归还池(残留事务不可能泄漏给下一个借出者)。
        conn.InTransaction = true;
    }

    public override IsolationLevel IsolationLevel { get; }

    // 保存点在引擎是显式 SQL(SAVEPOINT/ROLLBACK TO/RELEASE):ADO.NET 侧如实暴露。
    public override bool SupportsSavepoints => true;

    protected override DbConnection DbConnection => _conn;

    /// <summary>建保存点。</summary>
    public override void Save(string savePointName) => Run($"SAVEPOINT {QuoteIdent(savePointName)}");

    /// <summary>回滚到保存点。注意引擎语义:ROLLBACK TO 会把命名保存点自身也
    /// 丢弃(异于 SQLite/PG)—— 回滚后该保存点已消费,勿再 Release 同名保存点;
    /// 需要再次回滚就先重新 Save。</summary>
    public override void Rollback(string savePointName) => Run($"ROLLBACK TO {QuoteIdent(savePointName)}");

    /// <summary>释放保存点。</summary>
    public override void Release(string savePointName) => Run($"RELEASE SAVEPOINT {QuoteIdent(savePointName)}");

    public override async Task SaveAsync(
        string savePointName, CancellationToken cancellationToken = default) =>
        await RunAsync($"SAVEPOINT {QuoteIdent(savePointName)}", cancellationToken).ConfigureAwait(false);

    /// <summary>同 <see cref="Rollback(string)"/>:引擎把保存点自身一并丢弃。</summary>
    public override async Task RollbackAsync(
        string savePointName, CancellationToken cancellationToken = default) =>
        await RunAsync($"ROLLBACK TO {QuoteIdent(savePointName)}", cancellationToken).ConfigureAwait(false);

    public override async Task ReleaseAsync(
        string savePointName, CancellationToken cancellationToken = default) =>
        await RunAsync($"RELEASE SAVEPOINT {QuoteIdent(savePointName)}", cancellationToken).ConfigureAwait(false);

    public override void Commit()
    {
        if (_done)
        {
            throw new InvalidOperationException("transaction already finished");
        }
        Run("COMMIT");
        _done = true;
        _conn.InTransaction = false;
    }

    public override void Rollback()
    {
        if (_done)
        {
            throw new InvalidOperationException("transaction already finished");
        }
        Run("ROLLBACK");
        _done = true;
        _conn.InTransaction = false;
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_done)
        {
            throw new InvalidOperationException("transaction already finished");
        }
        await RunAsync("COMMIT", cancellationToken).ConfigureAwait(false);
        _done = true;
        _conn.InTransaction = false;
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_done)
        {
            throw new InvalidOperationException("transaction already finished");
        }
        await RunAsync("ROLLBACK", cancellationToken).ConfigureAwait(false);
        _done = true;
        _conn.InTransaction = false;
    }

    // ADO.NET contract: disposing an unfinished transaction rolls it back —
    // otherwise `using (var tx = ...)` without Commit would leak an open
    // server-side transaction (every later BEGIN fails, writes buffer).
    protected override void Dispose(bool disposing)
    {
        if (!_done)
        {
            _done = true;
            try
            {
                _conn.Proto.Send(new Frame(
                    FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql("ROLLBACK")));
            }
            catch
            {
                // Connection already broken; nothing to roll back.
            }
            finally
            {
                _conn.InTransaction = false;
            }
        }
        base.Dispose(disposing);
    }

    private void Run(string sql)
    {
        Check(_conn.Proto.Send(
            new Frame(FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql(sql))));
    }

    private async Task RunAsync(string sql, CancellationToken cancellationToken)
    {
        Check(await _conn.Proto.SendAsync(
            new Frame(FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql(sql)),
            cancellationToken).ConfigureAwait(false));
    }

    private static void Check(Frame resp)
    {
        if (resp.Type == FrameType.RespError)
        {
            // A failed BEGIN/COMMIT/ROLLBACK must surface: reporting success
            // would tell the caller data is durable when it is not.
            throw new DocsqlException(Encoding.UTF8.GetString(resp.Payload));
        }
    }

    private static string QuoteIdent(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("savepoint name must not be empty", nameof(name));
        }
        return '"' + name.Replace("\"", "\"\"") + '"';
    }
}

public sealed class DocsqlFactory : DbProviderFactory
{
    public static readonly DocsqlFactory Instance = new();

    public override DbConnection CreateConnection() => new DocsqlConnection();
    public override DbCommand CreateCommand() => new DocsqlCommand();
    public override DbParameter CreateParameter() => new DocsqlParameter();
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() =>
        new DocsqlConnectionStringBuilder();
}

/// Concrete DbException for DocSQL errors.
public sealed class DocsqlException : DbException
{
    public DocsqlException(string message) : base(message) { }

    /// <summary>服务端为唯一约束冲突返回的错误(UNIQUE constraint failed[: …])。
    /// 幂等写入路径(webhook 重放等)可据此分支,等价于 Mongo 的 DuplicateKey 判定。</summary>
    public bool IsUniqueViolation =>
        Message.StartsWith("UNIQUE constraint failed", StringComparison.OrdinalIgnoreCase)
        || Message.Contains(": UNIQUE constraint failed", StringComparison.OrdinalIgnoreCase);

    /// <summary>服务端为语法/解析错误返回的消息前缀。</summary>
    public bool IsSyntaxError =>
        Message.StartsWith("parse error", StringComparison.OrdinalIgnoreCase);
}
