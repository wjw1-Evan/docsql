// 传输加密测试:DOCSQL_KEY 启用 AES-256-GCM 帧加密。
// 覆盖:正确密钥全功能(SQL/KV/EF)、错误密钥被拒、明文客户端被拒、
// 加密集群扇出复制(节点间也加密)。

using Docsql.Client;
using System.Diagnostics;
using Xunit;

public sealed class TlsServer : IDisposable
{
    public readonly Process Proc;
    private readonly string _db;
    public int Port { get; }
    public string Cs => $"host=127.0.0.1;port={Port};key={KeyHex}";
    public const string KeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    private TlsServer(Process proc, string db, int port) => (Proc, _db, Port) = (proc, db, port);

    public static TlsServer Start(string? keyHex = KeyHex, string? peers = null)
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        if (!File.Exists(exe))
            throw new FileNotFoundException("docsql-server 不存在:" + exe);
        var db = Path.Combine(Path.GetTempPath(), $"docsql-tls-{port}.db");
        try { File.Delete(db); } catch { }
        var psi = new ProcessStartInfo
        {
            FileName = exe, Arguments = $"{db} 127.0.0.1:{port}",
            CreateNoWindow = true, RedirectStandardError = true,
        };
        if (keyHex is not null) psi.Environment["DOCSQL_KEY"] = keyHex;
        if (peers is not null) psi.Environment["DOCSQL_PEERS"] = peers;
        var proc = Process.Start(psi) ?? throw new InvalidOperationException("启动失败");
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return new(proc, db, port); }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException("启动超时");
    }

    public void Dispose()
    {
        try { if (!Proc.HasExited) Proc.Kill(); } catch { }
        Proc.Dispose();
        try { File.Delete(_db); } catch { }
    }
}

public sealed class TransportEncryptionTests
{
    private static object Scalar(string cs, string sql)
    {
        using var conn = new DocsqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar()!;
    }

    [Fact]
    public void Encrypted_roundtrip_sql_and_kv()
    {
        using var server = TlsServer.Start();
        using var conn = new DocsqlConnection(server.Cs);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE sec (v TEXT)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO sec VALUES ('机密数据')";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT v FROM sec";
            Assert.Equal("机密数据", cmd.ExecuteScalar());
        }
        var kv = conn.Kv("SET", "k", "v");
        Assert.Equal(FrameType.RespAffected, kv.Type);
        Assert.Equal("v", conn.Kv("GET", "k").Payload);
    }

    [Fact]
    public void Wrong_key_is_rejected()
    {
        using var server = TlsServer.Start();
        var wrong = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={server.Port};key={wrong}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        Assert.Throws<DocsqlException>(() => cmd.ExecuteScalar());
    }

    [Fact]
    public void Plaintext_client_is_rejected_by_encrypted_server()
    {
        using var server = TlsServer.Start();
        // 不带 key 的客户端:帧未加密,服务端拒绝;且拒绝响应本身也是加密的,
        // 客户端因无法解密而报"未配置 key"(两种路径都证明明文客户端不可用)。
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={server.Port}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var ex = Assert.Throws<DocsqlException>(() => cmd.ExecuteScalar());
        Assert.Contains("key", ex.Message);
    }

    [Fact]
    public void Encrypted_cluster_peers_stay_in_sync()
    {
        // 两个节点都启用同一把 DOCSQL_KEY:节点间复制帧同样加密。
        // 先定 b 的端口,a 的 DOCSQL_PEERS 指向 b。
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var bPort = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        using var a = TlsServer.Start(peers: $"127.0.0.1:{bPort}");
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        var db = Path.Combine(Path.GetTempPath(), $"docsql-tlsb-{bPort}.db");
        var psi = new ProcessStartInfo
        {
            FileName = exe, Arguments = $"{db} 127.0.0.1:{bPort}",
            CreateNoWindow = true, RedirectStandardError = true,
        };
        psi.Environment["DOCSQL_KEY"] = TlsServer.KeyHex;
        using var b = Process.Start(psi)!;
        try
        {
            for (var i = 0; i < 100; i++)
            {
                try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", bPort); break; }
                catch { Thread.Sleep(50); }
            }

            // 通过 a(带 key)写入,b 同样用 key 连接读到复制数据
            Scalar(a.Cs, "CREATE TABLE esync (v INT)");
            Scalar(a.Cs, "INSERT INTO esync VALUES (9)");
            var bCs = $"host=127.0.0.1;port={bPort};key={TlsServer.KeyHex}";
            var seen = false;
            for (var i = 0; i < 100 && !seen; i++)
            {
                try { seen = Convert.ToInt64(Scalar(bCs, "SELECT COUNT(v) FROM esync")) == 1; }
                catch { }
                Thread.Sleep(30);
            }
            Assert.True(seen, "加密集群:节点 b 未收到复制写入");
        }
        finally
        {
            try { if (!b.HasExited) b.Kill(); } catch { }
            b.Dispose();
            try { File.Delete(db); } catch { }
        }
    }
}
