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
        get => TryGetValue("port", out var v) ? int.Parse((string)v) : 7600;
        set => this["port"] = value.ToString();
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

        public Slot(int maxSize)
        {
            MaxSize = Math.Max(1, maxSize);
            Permits = new SemaphoreSlim(MaxSize, MaxSize);
        }
    }

    private static readonly ConcurrentDictionary<string, Slot> Pools = new();

    /// <summary>池命中 / 未命中(新建) / 丢弃(死连接、超容量裁剪) 计数,测试可断言。</summary>
    internal static long Hits, Misses, Discarded;

    internal static string KeyOf(DocsqlConnectionStringBuilder p, string? keyOverride) =>
        string.Join('\u0001', p.Host, p.Port, p.User, p.Password, p.Token,
            keyOverride ?? p.Key, p.MaxPoolSize);

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
        if (slot is null)
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
    /// key= when EF rewrites the connection string.</summary>
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

    private DocsqlConnectionStringBuilder Parsed => new() { ConnectionString = ConnectionString };

    public override string ConnectionString { get; set; } = "";

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
            if (InTransaction)
            {
                // 事务未了结就 Close:物理断开(服务器断连自动 ROLLBACK)。
                // 归还一个带着开放事务的连接,会把事务泄漏给下一个借出者。
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
        var v = reader.GetValue(0);
        if (v is double or float or decimal)
        {
            // Only narrow to long when the value is an exact integer;
            // fractional doubles (3.5, AVG results) must keep their type.
            // Comparing two roundings of the same value — the previous
            // check — was always true and rounded 3.5 to 4.
            var d = Convert.ToDouble(v);
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

    private static Frame ExecuteBody(ulong handle, List<object?> values)
    {
        var sb = new StringBuilder(values.Count * 8 + 32);
        sb.Append("{\"handle\":").Append(handle).Append(",\"params\":[");
        for (int i = 0; i < values.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }
            sb.Append(JsonOf(values[i]));
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
        return conn.Proto.Send(ExecuteBody(handle, values));
    }

    /// <summary><see cref="Execute"/> 的真异步形态(同一服务端绑定路径)。</summary>
    private async Task<Frame> ExecuteAsync(CancellationToken cancellationToken)
    {
        if (Connection is not { State: ConnectionState.Open } conn)
        {
            throw new InvalidOperationException("connection is not open");
        }
        if (Parameters.Count == 0)
        {
            return await conn.Proto.SendAsync(SqlFrame(), cancellationToken).ConfigureAwait(false);
        }
        var (template, values) = RewriteParameters();
        var (handle, _) = await conn.Proto.GetOrPrepareAsync(template, cancellationToken)
            .ConfigureAwait(false);
        return await conn.Proto.SendAsync(ExecuteBody(handle, values), cancellationToken)
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
        var v = reader.GetValue(0);
        if (v is double or float or decimal)
        {
            // Only narrow to long when the value is an exact integer;
            // fractional doubles (3.5, AVG results) must keep their type.
            // Comparing two roundings of the same value — the previous
            // check — was always true and rounded 3.5 to 4.
            var d = Convert.ToDouble(v);
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
    /// 服务端转义绑定;DateTime 族沿用 culture-invariant 可排序文本形态。</summary>
    private static string JsonOf(object? v) => v switch
    {
        null or DBNull => "null",
        bool b => b ? "true" : "false",
        int or long or short or byte or sbyte
            => JsonSerializer.Serialize(Convert.ToInt64(v, CultureInfo.InvariantCulture)),
        uint or ushort => JsonSerializer.Serialize(Convert.ToInt64(v, CultureInfo.InvariantCulture)),
        ulong u => JsonSerializer.Serialize(u),
        double d => JsonSerializer.Serialize(d),
        float f => JsonSerializer.Serialize((double)f),
        // Numeric literal: the engine stores decimals as f64 — big values
        // lose precision beyond ~15-16 significant digits (no decimal type).
        decimal m => JsonSerializer.Serialize((double)m),
        // Date/time values keep the culture-invariant, lexicographically
        // sortable text form the engine stores (GetDateTime parses it back).
        DateTime dt => JsonSerializer.Serialize(
            dt.ToString("O", CultureInfo.InvariantCulture)),
        DateTimeOffset dto => JsonSerializer.Serialize(
            dto.ToString("O", CultureInfo.InvariantCulture)),
        TimeSpan ts => JsonSerializer.Serialize(
            ts.ToString("c", CultureInfo.InvariantCulture)),
        string s => JsonSerializer.Serialize(s),
        char c => JsonSerializer.Serialize(c.ToString()),
        Guid g => JsonSerializer.Serialize(g.ToString()),
        // No BLOB storage in the engine; storing ToString() would corrupt
        // data silently — refuse loudly instead.
        byte[] => throw new NotSupportedException(
            "byte[] parameters are not supported (no BLOB storage); serialize to TEXT/Base64"),
        // Enums have no wire form; their ToString() name would be quoted as
        // text and silently match nothing.
        Enum => throw new NotSupportedException(
            "enum parameters are not supported; convert to the underlying integer first"),
        _ => throw new NotSupportedException(
            $"parameter type {v.GetType().Name} is not supported"),
    };

    private static string ErrorText(Frame f) => Encoding.UTF8.GetString(f.Payload);

    internal static int DecodeAffected(byte[] payload) =>
        payload.Length >= 8 ? BitConverter.ToInt32(payload, 0) : 0;

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
        _ => e.GetRawText(),
    };

    public override int FieldCount => _columns.Count;
    public override bool HasRows => _rows.Count > 0;
    public override bool IsClosed => false;
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
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(CurrentRow[ordinal]);
    public override DateTime GetDateTime(int ordinal) =>
        Convert.ToDateTime(CurrentRow[ordinal], CultureInfo.InvariantCulture);
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
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
}
