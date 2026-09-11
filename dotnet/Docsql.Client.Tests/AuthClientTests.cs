// 客户端认证(REQ_AUTH)契约测试:不启动真实 server(服务器侧的 token/
// 只读/锁定行为由 Rust e2e 覆盖),用进程内 TCP 假服务端按协议 v1 帧格式
// 应答,验证 DocsqlConnection / DocsqlSubscriber 的认证请求构造与应答处理:
// 正确应答放行且载荷为 token 原始字节、错误应答在 Open 即失败并置 Broken、
// 未带 token 时首条命令(REQ_SQL)原样上抛服务端错误。

using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Docsql.Client;
using Xunit;

public sealed class AuthClientTests
{
    /// <summary>收一帧(20 字节头 + 载荷),回 (类型, 载荷)。</summary>
    private static (FrameType Type, byte[] Payload) ReadFrame(NetworkStream s)
    {
        var header = new byte[20];
        ReadExact(s, header);
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
        Assert.Equal(0x31515344u, magic); // "DSQ1"
        var len = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(16, 4));
        var payload = new byte[len];
        ReadExact(s, payload);
        var type = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(6, 2));
        return ((FrameType)type, payload);
    }

    private static void ReadExact(NetworkStream s, byte[] buf)
    {
        int off = 0;
        while (off < buf.Length)
        {
            var n = s.Read(buf, off, buf.Length - off);
            if (n == 0)
            {
                throw new IOException("unexpected EOF");
            }
            off += n;
        }
    }

    /// <summary>
    /// 单应答假服务端:校验收到的第一帧类型后回 (reply, replyText),随后
    /// 读到 EOF 为止。返回监听端口与首帧类型的任务(供断言请求构造)。
    /// </summary>
    private static (int Port, Task<FrameType> FirstFrame) ServeOnce(
        FrameType reply, string replyText)
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        var first = new TaskCompletionSource<FrameType>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task.Run(() =>
        {
            try
            {
                using var client = l.AcceptTcpClient();
                using var s = client.GetStream();
                var (type, _) = ReadFrame(s);
                first.TrySetResult(type);
                s.Write(new Frame(reply, 0, 0, Encoding.UTF8.GetBytes(replyText)).Encode());
                s.Flush();
                var buf = new byte[256];
                while (true)
                {
                    try
                    {
                        if (s.Read(buf, 0, buf.Length) == 0)
                        {
                            break;
                        }
                    }
                    catch
                    {
                        break;
                    }
                }
            }
            catch
            {
                first.TrySetCanceled();
            }
            finally
            {
                l.Stop();
            }
        });
        return (port, first.Task);
    }

    [Fact]
    public async Task Correct_token_is_sent_as_req_auth_and_accepted()
    {
        var (port, first) = ServeOnce(FrameType.RespAffected, "ok");
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={port};token=secret-token");
        conn.Open();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
        Assert.Equal(FrameType.ReqAuth, await first.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void Error_reply_fails_open_and_marks_connection_broken()
    {
        var (port, _) = ServeOnce(FrameType.RespError, "bad token");
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={port};token=wrong-token");
        var ex = Assert.Throws<DocsqlException>(conn.Open);
        Assert.Contains("auth failed", ex.Message);
        Assert.Contains("bad token", ex.Message);
        Assert.Equal(System.Data.ConnectionState.Broken, conn.State);
    }

    [Fact]
    public async Task Missing_token_surfaces_the_servers_unauthorized_reply()
    {
        // 连接串未带 token:Open 不发任何帧,首条命令(REQ_SQL)被服务端
        // 拒绝,错误原样上抛。
        var (port, first) = ServeOnce(FrameType.RespError, "unauthorized");
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={port}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var ex = Assert.Throws<DocsqlException>(() => cmd.ExecuteScalar());
        Assert.Contains("unauthorized", ex.Message);
        Assert.Equal(FrameType.ReqSql, await first.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Subscriber_authenticates_with_the_configured_token()
    {
        var (port, first) = ServeOnce(FrameType.RespAffected, "ok");
        using var sub = new DocsqlSubscriber($"host=127.0.0.1;port={port};token=sub-token");
        Assert.Equal(FrameType.ReqAuth, await first.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void Subscriber_auth_failure_throws_instead_of_half_open()
    {
        var (port, _) = ServeOnce(FrameType.RespError, "bad token");
        Assert.Throws<DocsqlException>(
            () => new DocsqlSubscriber($"host=127.0.0.1;port={port};token=wrong-token"));
    }

    /// <summary>同 ServeOnce,但把首帧载荷一并交回(校验 REQ_AUTH_USER 的 JSON 构造)。</summary>
    private static (int Port, Task<(FrameType Type, byte[] Payload)> First) ServeOnceFull(
        FrameType reply, string replyText)
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        var port = ((IPEndPoint)l.LocalEndpoint).Port;
        var first = new TaskCompletionSource<(FrameType, byte[])>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Task.Run(() =>
        {
            try
            {
                using var client = l.AcceptTcpClient();
                using var s = client.GetStream();
                var (type, payload) = ReadFrame(s);
                first.TrySetResult((type, payload));
                s.Write(new Frame(reply, 0, 0, Encoding.UTF8.GetBytes(replyText)).Encode());
                s.Flush();
                var buf = new byte[256];
                while (true)
                {
                    try
                    {
                        if (s.Read(buf, 0, buf.Length) == 0)
                        {
                            break;
                        }
                    }
                    catch
                    {
                        break;
                    }
                }
            }
            catch
            {
                // 测试结束后连接关闭属预期
            }
            finally
            {
                l.Stop();
            }
        });
        return (port, first.Task);
    }

    [Fact]
    public async Task User_login_sends_req_auth_user_with_json_body()
    {
        var (port, first) = ServeOnceFull(FrameType.RespAffected, "ok(user:dana)");
        using var conn = new DocsqlConnection(
            "host=127.0.0.1;port=" + port + ";user=dana;password=" + "pw" + "12345" + "678");
        conn.Open();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
        var (type, payload) = await first.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(FrameType.ReqAuthUser, type);
        var body = System.Text.Json.JsonDocument.Parse(Encoding.UTF8.GetString(payload)).RootElement;
        Assert.Equal("dana", body.GetProperty("user").GetString());
        Assert.Equal("pw12345" + "678", body.GetProperty("password").GetString());
    }

    [Fact]
    public void User_login_rejection_fails_open()
    {
        var (port, _) = ServeOnceFull(FrameType.RespError, "bad username or password");
        using var conn = new DocsqlConnection(
            "host=127.0.0.1;port=" + port + ";user=ghost;password=whatever12");
        var ex = Assert.Throws<DocsqlException>(conn.Open);
        Assert.Contains("auth failed", ex.Message);
        Assert.Equal(System.Data.ConnectionState.Broken, conn.State);
    }

    [Fact]
    public async Task Subscriber_logs_in_with_username_password()
    {
        var (port, first) = ServeOnceFull(FrameType.RespAffected, "ok(user:dana)");
        using var sub = new DocsqlSubscriber(
            "host=127.0.0.1;port=" + port + ";user=dana;password=pw123456");
        var (type, _) = await first.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(FrameType.ReqAuthUser, type);
    }
}
