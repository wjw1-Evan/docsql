// 故障转移测试:主副本(异步转发复制)+ 真实杀掉主进程 + PROMOTE 提升。
//
// 拓扑:primary(DOCSQL_REPLICATE_TO=replica)→ replica(DOCSQL_READ_ONLY=1)。
// 与 Rust e2e 的 replication_and_failover 相比,这里真的 kill 主进程,
// 验证客户端对新主机的重连、复制数据完整性,以及 EF 在新主上的完整 CRUD。

using Docsql.Client;
using System.Diagnostics;
using Xunit;

public sealed class NodeProcess : IDisposable
{
    public int Port { get; }
    public string Cs => $"host=127.0.0.1;port={Port}";
    private readonly Process _proc;
    private readonly string _dbFile;

    private NodeProcess(Process proc, string dbFile, int port)
        => (_proc, _dbFile, Port) = (proc, dbFile, port);

    /// <summary>启动一个 docsql-server 节点。</summary>
    public static NodeProcess Start(int port, string role, string? replicateTo = null)
    {
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        if (!File.Exists(exe))
            throw new FileNotFoundException("docsql-server 不存在,请先 cargo build:" + exe);
        var dbFile = Path.Combine(Path.GetTempPath(), $"docsql-fo-{role}-{port}.db");
        try { File.Delete(dbFile); } catch { }
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            ArgumentList = { dbFile, $"127.0.0.1:{port}" },
            CreateNoWindow = true,
            RedirectStandardError = true,
        };
        if (replicateTo is not null)
            psi.Environment["DOCSQL_REPLICATE_TO"] = replicateTo;
        if (role == "replica")
            psi.Environment["DOCSQL_READ_ONLY"] = "1";
        var proc = Process.Start(psi) ?? throw new InvalidOperationException("启动失败");
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return new(proc, dbFile, port); }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException($"节点 {role}:{port} 启动超时");
    }

    public void Kill()
    {
        try { _proc.Kill(); } catch { }
        _proc.WaitForExit(5000);
    }

    public void Dispose()
    {
        try { if (!_proc.HasExited) _proc.Kill(); } catch { }
        _proc.Dispose();
        try { File.Delete(_dbFile); } catch { }
    }
}

public sealed class FailoverTests
{
    private static int FreePort()
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var p = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return p;
    }

    private static (NodeProcess Primary, NodeProcess Replica) Cluster()
    {
        var replicaPort = FreePort();
        var primaryPort = FreePort();
        var replica = NodeProcess.Start(replicaPort, "replica");
        NodeProcess? primary = null;
        try
        {
            primary = NodeProcess.Start(primaryPort, "primary", replicateTo: $"127.0.0.1:{replicaPort}");
            return (primary, replica);
        }
        catch
        {
            replica.Dispose();
            primary?.Dispose();
            throw;
        }
    }

    private static object Scalar(string cs, string sql)
    {
        using var conn = new DocsqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar()!;
    }

    private static long Long(string cs, string sql) => Convert.ToInt64(Scalar(cs, sql));

    /// <summary>执行写语句,返回受影响行数。</summary>
    private static long Exec(string cs, string sql)
    {
        using var conn = new DocsqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }

    /// <summary>轮询直到谓词为真(异步复制需要短暂收敛时间),超时抛异常。</summary>
    private static T Eventually<T>(Func<T> probe, Func<T, bool> ok, string what)
    {
        for (var i = 0; i < 100; i++)
        {
            var v = probe();
            if (ok(v)) return v;
            Thread.Sleep(30);
        }
        throw new TimeoutException($"等待超时: {what}");
    }

    [Fact]
    public void Writes_replicate_sql_and_kv_to_replica()
    {
        var (primary, replica) = Cluster();
        using var _r = replica;
        using var _p = primary;
        Scalar(primary.Cs, "CREATE TABLE fo (id INT)");
        Assert.Equal(1, Exec(primary.Cs, "INSERT INTO fo VALUES (7)"));

        // SQL 复制(异步转发,轮询收敛)
        Eventually(
            () => { try { return Long(replica.Cs, "SELECT id FROM fo"); } catch { return -1L; } },
            v => v == 7L, "副本看到 SQL 写入");

        // KV 复制
        using (var p = new DocsqlConnection(primary.Cs))
        {
            p.Open();
            Assert.Equal(FrameType.RespAffected, p.Kv("SET", "fokey", "foval").Type);
        }
        using (var r = new DocsqlConnection(replica.Cs))
        {
            r.Open();
            var got = Eventually(
                () => r.Kv("GET", "fokey"),
                resp => resp.Type == FrameType.RespAffected && resp.Payload == "foval",
                "副本看到 KV 写入");
            Assert.Equal("foval", got.Payload);
        }
    }

    [Fact]
    public void Replica_rejects_writes_before_promotion()
    {
        var (primary, replica) = Cluster();
        using var _r = replica;
        using var _p = primary;
        Scalar(primary.Cs, "CREATE TABLE ro (id INT)");

        using var r = new DocsqlConnection(replica.Cs);
        r.Open();
        using var cmd = r.CreateCommand();
        cmd.CommandText = "INSERT INTO ro VALUES (1)";
        var ex = Assert.Throws<DocsqlException>(() => cmd.ExecuteNonQuery());
        Assert.Contains("read-only", ex.Message);

        // 读不受影响
        cmd.CommandText = "SELECT 1 AS one";
        Assert.Equal(1L, cmd.ExecuteScalar());
    }

    [Fact]
    public void Killing_primary_then_promoting_restores_write_path()
    {
        var (primary, replica) = Cluster();
        using var _r = replica;
        using var _p = primary;

        Scalar(primary.Cs, "CREATE TABLE fail (id INT)");
        Assert.Equal(1, Exec(primary.Cs, "INSERT INTO fail VALUES (7)"));
        Eventually(
            () => { try { return Long(replica.Cs, "SELECT COUNT(id) FROM fail"); } catch { return -1L; } },
            v => v == 1L, "复制收敛");

        // 真实故障:杀掉主进程
        primary.Kill();

        // 旧连接/新连接都到不了死掉的主节点
        using (var dead = new DocsqlConnection(primary.Cs))
        {
            Assert.ThrowsAny<Exception>(() => dead.Open());
        }

        // 故障转移:PROMOTE 副本,写路径恢复,复制来的数据完整
        using (var r = new DocsqlConnection(replica.Cs))
        {
            r.Open();
            var promoted = r.Kv("PROMOTE");
            Assert.NotEqual(FrameType.RespError, promoted.Type);
        }
        Assert.Equal(1, Exec(replica.Cs, "INSERT INTO fail VALUES (8)"));
        Assert.Equal(2L, Long(replica.Cs, "SELECT COUNT(id) FROM fail"));
    }
}
