// 对称集群测试:三个对等节点(DOCSQL_PEERS),不分主从 ——
// 连接任意节点都可读写,写自动扇出到其余节点。

using Docsql.Client;
using System.Diagnostics;
using Xunit;

public sealed class PeerNode : IDisposable
{
    public readonly Process Proc;
    private readonly string _db;
    public int Port { get; }
    public string Cs => $"host=127.0.0.1;port={Port}";
    /// <summary>该节点的本地数据文件(每个节点独立存储)。</summary>
    public string DbFile => _db;

    private PeerNode(Process proc, string db, int port) => (Proc, _db, Port) = (proc, db, port);

    public static PeerNode Start(int port, string peers, string? dbFile = null)
    {
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        if (!File.Exists(exe))
            throw new FileNotFoundException("docsql-server 不存在,请先 cargo build:" + exe);
        var db = dbFile ?? Path.Combine(Path.GetTempPath(), $"docsql-peer-{port}.db");
        if (dbFile is null)
        {
            try { File.Delete(db); } catch { }
        }
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            Arguments = $"{db} 127.0.0.1:{port}",
            CreateNoWindow = true,
            RedirectStandardError = true,
        };
        psi.Environment["DOCSQL_PEERS"] = peers;
        var proc = Process.Start(psi) ?? throw new InvalidOperationException("启动失败");
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return new(proc, db, port); }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException($"节点 {port} 启动超时");
    }

    public void Kill()
    {
        try { Proc.Kill(); } catch { }
        Proc.WaitForExit(5000);
    }

    public void Dispose()
    {
        try { if (!Proc.HasExited) Proc.Kill(); } catch { }
        Proc.Dispose();
    }
}

public sealed class SymmetricClusterTests
{
    private static List<PeerNode> Cluster(int n)
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        var ports = new List<int>();
        for (var i = 0; i < n; i++)
        {
            l.Start();
            ports.Add(((System.Net.IPEndPoint)l.LocalEndpoint).Port);
            l.Stop();
        }
        var all = ports.Select(p => $"127.0.0.1:{p}").ToList();
        return ports.Select(p => PeerNode.Start(
            p, string.Join(",", all.Where(a => a != $"127.0.0.1:{p}")))).ToList();
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

    private static long Exec(string cs, string sql)
    {
        using var conn = new DocsqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }

    /// <summary>轮询直到谓词为真(扇出复制是异步尽力而为,需要短暂收敛)。</summary>
    private static void Eventually(Action probe, string what)
    {
        for (var i = 0; i < 100; i++)
        {
            try { probe(); return; } catch { }
            Thread.Sleep(30);
        }
        throw new TimeoutException($"等待超时: {what}");
    }

    [Fact]
    public void Any_node_accepts_writes_and_all_see_them()
    {
        var nodes = Cluster(3);
        try
        {
            var (a, b, c) = (nodes[0], nodes[1], nodes[2]);

            // 连 A 建表写入
            Scalar(a.Cs, "CREATE TABLE sym (id INT, src INT)");
            Exec(a.Cs, "INSERT INTO sym VALUES (1, 0)");
            // 连 B 写入(没有只读限制)
            Exec(b.Cs, "INSERT INTO sym VALUES (2, 1)");
            // 连 C 读到两条
            Eventually(() => Assert.Equal(2L, Long(c.Cs, "SELECT COUNT(id) FROM sym")), "C 看到 A/B 的写入");
            Assert.Equal(0L, Long(c.Cs, "SELECT src FROM sym WHERE id = 1"));
            Assert.Equal(1L, Long(c.Cs, "SELECT src FROM sym WHERE id = 2"));

            // KV 同样扇出:连 B SET,连 A GET
            using (var bc = new DocsqlConnection(b.Cs))
            {
                bc.Open();
                Assert.Equal(FrameType.RespAffected, bc.Kv("SET", "symkey", "fromB").Type);
            }
            Eventually(() =>
            {
                using var ac = new DocsqlConnection(a.Cs);
                ac.Open();
                Assert.Equal("fromB", ac.Kv("GET", "symkey").Payload);
            }, "A 看到 B 的 KV 写入");

            // 连 C 写,连 A 读
            Exec(c.Cs, "INSERT INTO sym VALUES (3, 2)");
            Eventually(() => Assert.Equal(3L, Long(a.Cs, "SELECT COUNT(id) FROM sym")), "A 看到 C 的写入");
        }
        finally
        {
            nodes.ForEach(n => n.Dispose());
        }
    }

    [Fact]
    public void Cluster_tolerates_a_dead_node_for_reads_and_writes()
    {
        var nodes = Cluster(3);
        try
        {
            var (a, b, c) = (nodes[0], nodes[1], nodes[2]);
            Scalar(a.Cs, "CREATE TABLE tol (id INT)");
            Exec(a.Cs, "INSERT INTO tol VALUES (1)");
            Eventually(() => Assert.Equal(1L, Long(b.Cs, "SELECT COUNT(id) FROM tol")), "B 收到初始写入");

            // C 宕机:A、B 依旧可写可读(扇出对死节点只记日志)
            c.Kill();
            Exec(b.Cs, "INSERT INTO tol VALUES (2)");
            Assert.Equal(2L, Long(a.Cs, "SELECT COUNT(id) FROM tol"));

            // C 复活(独立进程拉起会丢内存态,这里验证的是其余节点不受影响)
            Assert.Equal(2L, Long(b.Cs, "SELECT COUNT(id) FROM tol"));
        }
        finally
        {
            nodes.ForEach(n => n.Dispose());
            nodes.ForEach(n => { try { File.Delete(n.DbFile); } catch { } });
        }
    }

    [Fact]
    public void Each_node_has_its_own_storage_and_restart_keeps_local_data()
    {
        var nodes = Cluster(3);
        try
        {
            var (a, b, c) = (nodes[0], nodes[1], nodes[2]);

            // 三个节点各自独立的本地文件
            Assert.Equal(3, nodes.Select(n => n.DbFile).Distinct().Count());

            // 经 A 写入,同步到 B、C
            Scalar(a.Cs, "CREATE TABLE sto (id INT)");
            Exec(a.Cs, "INSERT INTO sto VALUES (42)");
            Eventually(() => Assert.Equal(1L, Long(b.Cs, "SELECT COUNT(id) FROM sto")), "B 同步");
            Eventually(() => Assert.Equal(1L, Long(c.Cs, "SELECT COUNT(id) FROM sto")), "C 同步");

            // 同步完成后,每个节点自己的存储文件都落了盘且非空
            Thread.Sleep(200); // WAL checkpoint/写盘
            foreach (var n in nodes)
            {
                Assert.True(File.Exists(n.DbFile), $"缺少存储文件 {n.DbFile}");
                Assert.True(new FileInfo(n.DbFile).Length > 0, $"{n.DbFile} 是空文件");
            }

            // 节点 C 用同一份存储文件重启:本地数据仍在(持久化)
            var cDb = c.DbFile;
            var cPort = c.Port;
            var cPeers = $"127.0.0.1:{a.Port},127.0.0.1:{b.Port}";
            c.Dispose();
            using var c2 = PeerNode.Start(cPort, cPeers, dbFile: cDb);
            Assert.Equal(1L, Long(c2.Cs, "SELECT COUNT(id) FROM sto"));

            // 重启后的 C 还能继续参与集群:写入扇出到 A、B
            Exec(c2.Cs, "INSERT INTO sto VALUES (43)");
            Eventually(() => Assert.Equal(2L, Long(a.Cs, "SELECT COUNT(id) FROM sto")), "A 收到 C 重启后的写入");
            Eventually(() => Assert.Equal(2L, Long(b.Cs, "SELECT COUNT(id) FROM sto")), "B 收到 C 重启后的写入");
        }
        finally
        {
            nodes.ForEach(n => n.Dispose());
            nodes.ForEach(n => { try { File.Delete(n.DbFile); } catch { } });
        }
    }
}
