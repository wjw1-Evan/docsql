// 持久化发布订阅:订阅端(专用连接 + 后台读线程)。
//
// 普通连接(DocsqlConnection)是严格一问一答,无法安全承接服务端主动
// 推送;DocsqlSubscriber 用相同连接串独立开一条 TCP 连接,Subscribe 后
// 进入订阅模式:后台线程持续收帧,RESP_PUSH 解析后分发到回调,其余帧
// (订阅/退订确认)经信号交回调用线程。消息语义 at-least-once:断线重连
// 后用最后收到的 id 作为 from 重新订阅即可补齐缺口。

using System.Linq;
using System.Text;
using System.Text.Json;

namespace Docsql.Client;

/// <summary>一条推送消息(Redis 形态:"message" 或 "pmessage")。</summary>
public sealed record DocsqlMessage(
    string Kind,
    string? Pattern,
    string Channel,
    long Id,
    long Ts,
    string Payload)
{
    /// <summary>模式订阅投递(携带命中的 pattern)。</summary>
    public bool IsPattern => Kind == "pmessage";

    internal static DocsqlMessage Parse(byte[] payload)
    {
        using var doc = JsonDocument.Parse(Encoding.UTF8.GetString(payload));
        var r = doc.RootElement;
        return new(
            r.TryGetProperty("kind", out var k) ? k.GetString() ?? "" : "",
            r.TryGetProperty("pattern", out var p) ? p.GetString() : null,
            r.TryGetProperty("channel", out var c) ? c.GetString() ?? "" : "",
            r.TryGetProperty("id", out var i) && i.TryGetInt64(out var id) ? id : 0,
            r.TryGetProperty("ts", out var t) && t.TryGetInt64(out var ts) ? ts : 0,
            r.TryGetProperty("payload", out var pl) ? pl.GetString() ?? "" : "");
    }
}

/// <summary>
/// pub/sub 订阅者:与 DocsqlConnection 相同的连接串(host/port/token/key),
/// 但独占一条专用连接。<paramref name="from"/> 起点:"latest"(默认,只收
/// 新消息)、"earliest"(全量回放)、数字 id(从该 id 之后续传)。
/// 消息在回调线程上分发;回调抛出的异常被吞掉(不杀分发线程)。
/// </summary>
public sealed class DocsqlSubscriber : IDisposable
{
    private readonly ProtocolConnection _proto;
    private readonly Thread _reader;
    /// <summary>串行化整个控制面往返(发送→等确认→取回应答):并发的
    /// Subscribe/Unsubscribe 各自等待同一个无关联邮箱,应答会被错配或吞掉。</summary>
    private readonly object _ctrlLock = new();
    private readonly AutoResetEvent _respReady = new(false);
    private readonly Dictionary<(bool Pattern, string Name), Action<DocsqlMessage>>
        _handlers = new();
    private volatile bool _running = true;
    private volatile bool _dead;
    private volatile string? _deadReason;
    private Frame? _pendingResp;

    /// <summary>
    /// 连接断开或读取失败时触发(在后台读线程上,Dispose 主动关闭不触发)。
    /// 触发后订阅停止投递且不会自愈,Subscribe/Unsubscribe 立即抛错;
    /// 应用据此重连并用最后收到的 id 续传。回调抛出的异常被吞掉。
    /// </summary>
    public Action<Exception>? OnError { get; set; }

    public DocsqlSubscriber(string connectionString)
    {
        var b = new DocsqlConnectionStringBuilder { ConnectionString = connectionString };
        _proto = DocsqlConnection.ConnectAndAuth(b, null);
        _reader = new Thread(ReadLoop) { IsBackground = true, Name = "docsql-subscriber" };
        _reader.Start();
    }

    /// <summary>订阅频道;返回该连接的活跃订阅总数。</summary>
    public int Subscribe(string channel, Action<DocsqlMessage> onMessage, string from = "latest")
        => Sub(FrameType.ReqSubscribe, pattern: false, channel, onMessage, from);

    /// <summary>模式订阅(glob:* ? [...]);返回该连接的活跃订阅总数。</summary>
    public int Psubscribe(string pattern, Action<DocsqlMessage> onMessage, string from = "latest")
        => Sub(FrameType.ReqPsubscribe, pattern: true, pattern, onMessage, from);

    /// <summary>退订一个频道。</summary>
    public int Unsubscribe(string channel)
        => Unsub(FrameType.ReqUnsubscribe, pattern: false, channel);

    /// <summary>退订一个模式。</summary>
    public int Punsubscribe(string pattern)
        => Unsub(FrameType.ReqPunsubscribe, pattern: true, pattern);

    /// <summary>退订全部(true = 全部模式订阅,false = 全部频道订阅);返回剩余订阅数。</summary>
    public int UnsubscribeAll(bool patterns = false)
        => Unsub(patterns ? FrameType.ReqPunsubscribe : FrameType.ReqUnsubscribe,
            patterns, null);

    private int Sub(
        FrameType type, bool pattern, string name, Action<DocsqlMessage> onMessage, string from)
    {
        ArgumentNullException.ThrowIfNull(onMessage);
        object bodyObj = pattern ? new { pattern = name, from } : new { channel = name, from };
        var body = JsonSerializer.Serialize(bodyObj);
        // Handler first: the server replays history right after the
        // confirmation, so the dispatcher must already know where to send it.
        lock (_handlers)
        {
            _handlers[(pattern, name)] = onMessage;
        }
        var resp = RoundTrip(new Frame(type, 0, 0, Encoding.UTF8.GetBytes(body)));
        if (resp.Type == FrameType.RespError)
        {
            // 服务端明确拒绝:handler 必须移除,否则回放会被静默吞掉。
            // 传输层失败(超时/断线)不在此列 —— 订阅可能已在服务端生效。
            lock (_handlers)
            {
                _handlers.Remove((pattern, name));
            }
            resp.EnsureOk();
        }
        return (int)Math.Clamp(DocsqlConnection.DecodeLong(resp.Payload), 0, int.MaxValue);
    }

    private int Unsub(FrameType type, bool pattern, string? name)
    {
        var body = name is null ? "[]" : JsonSerializer.Serialize(new[] { name });
        var resp = RoundTrip(new Frame(type, 0, 0, Encoding.UTF8.GetBytes(body)));
        resp.EnsureOk();
        lock (_handlers)
        {
            if (name is null)
            {
                var doomed = _handlers.Keys.Where(k => k.Pattern == pattern).ToList();
                foreach (var k in doomed)
                {
                    _handlers.Remove(k);
                }
            }
            else
            {
                _handlers.Remove((pattern, name));
            }
        }
        return (int)Math.Clamp(DocsqlConnection.DecodeLong(resp.Payload), 0, int.MaxValue);
    }

    /// <summary>发送订阅请求并等待后台读线程交回的确认帧。整个往返串行在
    /// _ctrlLock 内:并发的控制请求不会交错抢占同一个响应邮箱。</summary>
    private Frame RoundTrip(Frame req)
    {
        if (_dead)
        {
            throw new DocsqlException("连接已断开: " + _deadReason);
        }
        // 消息回调跑在读线程上;在回调里调用 Subscribe/Unsubscribe 会在
        // 唯一能收到确认帧的线程上等它自己 —— 必死锁 30s 后失败,直接拒绝。
        if (ReferenceEquals(Thread.CurrentThread, _reader))
        {
            throw new DocsqlException(
                "不能在消息回调内调用订阅控制方法(死锁);请把调用移出回调线程");
        }
        lock (_ctrlLock)
        {
            // 清空上一次往返可能遗留的邮箱状态(如超时后才迟到的那条应答),
            // 否则它会被下一个请求错认为自己的确认。
            _pendingResp = null;
            _respReady.Reset();
            _proto.Write(req);
            if (!_respReady.WaitOne(TimeSpan.FromSeconds(30)))
            {
                if (_dead)
                {
                    throw new DocsqlException("连接已断开: " + _deadReason);
                }
                throw new DocsqlException("订阅请求超时(服务器无响应或连接已断开)");
            }
            if (_dead && _pendingResp is null)
            {
                // 断线唤醒:这不是任何请求的应答。
                throw new DocsqlException("连接已断开: " + _deadReason);
            }
            var resp = _pendingResp ?? default;
            _pendingResp = null;
            return resp;
        }
    }

    private void ReadLoop()
    {
        Exception? fatal = null;
        while (_running)
        {
            Frame f;
            try
            {
                f = _proto.Receive();
            }
            catch (Exception ex)
            {
                fatal = ex; // connection closed or corrupted — stop dispatching
                break;
            }
            if (f.Type == FrameType.RespPush)
            {
                Dispatch(f);
                continue;
            }
            _pendingResp = f;
            _respReady.Set();
        }
        if (!_running)
        {
            // Dispose 主动关闭不是故障,但在途 RoundTrip 仍需被唤醒
            // (否则要挂满 30s 超时);标记 _dead 让后续控制调用立即失败。
            _deadReason = "disposed";
            _dead = true;
            _respReady.Set();
            return;
        }
        // 连接丢失必须可见:静默死线程会让应用以为订阅还活着。同时唤醒
        // 在途 RoundTrip 让它立即失败。
        _deadReason = fatal?.Message ?? "connection closed";
        _dead = true;
        try
        {
            OnError?.Invoke(fatal ?? new System.IO.IOException("connection closed"));
        }
        catch
        {
            // 观察者异常不影响断线流程
        }
        _respReady.Set();
    }

    private void Dispatch(Frame f)
    {
        DocsqlMessage msg;
        try
        {
            msg = DocsqlMessage.Parse(f.Payload);
        }
        catch
        {
            return; // malformed push must not kill the dispatch thread
        }
        List<Action<DocsqlMessage>> targets = new();
        lock (_handlers)
        {
            if (msg.IsPattern)
            {
                if (msg.Pattern is not null
                    && _handlers.TryGetValue((true, msg.Pattern), out var ph))
                {
                    targets.Add(ph);
                }
            }
            else if (_handlers.TryGetValue((false, msg.Channel), out var h))
            {
                targets.Add(h);
            }
        }
        foreach (var h in targets)
        {
            try
            {
                h(msg);
            }
            catch
            {
                // user callback errors never kill the dispatch thread
            }
        }
    }

    public void Dispose()
    {
        _running = false;
        _proto.Dispose(); // unblocks the reader's Receive
        _reader.Join(TimeSpan.FromSeconds(2));
    }
}
