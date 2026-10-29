// hosts= 多主机池测试:连接建立轮换分摊 + 节点死亡自动切换。
//
// 拓扑:三节点对等集群(真实 docsql-server 子进程,ArgumentList 形态,
// 与 SymmetricClusterTests/FailoverTests 同规)。断言锚点:
// - DataSource 打开后返回实际连上的节点(轮换可观测);
// - 连接串列表内分隔符必须用逗号(分号是连接串的配对分隔符);
// - 池按候选端点分槽:死节点验活/拨号失败 → 轮换到下一候选;
// - 全部候选不可用 → 合并错误,每个端点一条原因。

using Docsql.Client;
using Xunit;

public sealed class MultiHostTests
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

    /// <summary>hosts= 连接串(书写顺序 = 轮换基序)。</summary>
    private static string Cs(List<PeerNode> nodes, string extra = "") =>
        "hosts=" + string.Join(",", nodes.Select(x => $"127.0.0.1:{x.Port}")) + extra;

    private static void Exec(DocsqlConnection c, string sql)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>轮询直到谓词为真(对称扇出复制异步尽力而为,需要收敛窗口)。</summary>
    private static void Eventually(Action probe, string what)
    {
        for (var i = 0; i < 100; i++)
        {
            try { probe(); return; } catch { }
            Thread.Sleep(30);
        }
        throw new TimeoutException($"等待超时: {what}");
    }

    /// <summary>在指定节点上直接单点读(不经 hosts 轮换,断言确定性独立)。</summary>
    private static long CountOn(PeerNode node, string table)
    {
        using var conn = new DocsqlConnection(node.Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(id) FROM {table}";
        return Convert.ToInt64(cmd.ExecuteScalar()!);
    }

    private static int PortOf(DocsqlConnection c) =>
        int.Parse(c.DataSource["127.0.0.1:".Length..]);

    // ---- 解析与校验(不启动进程) ----

    [Fact]
    public void Hosts_parses_entries_and_bare_hosts()
    {
        var p = new DocsqlConnectionStringBuilder
        {
            ConnectionString = "hosts= a:1 , b:2 ,c:3",
        };
        Assert.True(p.IsMultiHost);
        Assert.Equal(
            new[] { ("a", 1), ("b", 2), ("c", 3) },
            p.HostList);
        Assert.Equal("a:1,b:2,c:3", p.Hosts);
        // 空/缺 hosts 键 = 非 multi-host,退回单 host 语义。
        Assert.False(
            new DocsqlConnectionStringBuilder { ConnectionString = "host=a;port=1" }.IsMultiHost);
        // 裸 host 条目(无冒号)与单 host 形态同默认端口 7600。
        Assert.Equal(
            new[] { ("only", 7600) },
            new DocsqlConnectionStringBuilder { ConnectionString = "hosts=only" }.HostList);
        // DataSource 打开前 = hosts 规范化形态;打开后(观测落点,由真集群测试断言)。
        Assert.Equal("a:1,b:2,c:3", new DocsqlConnection("hosts=a:1,b:2,c:3").DataSource);
    }

    [Fact]
    public void Hosts_rejects_bad_lists_and_conflicts()
    {
        foreach (var bad in new[]
        {
            "hosts=",                                  // 空
            "hosts=  ",                                // 只有空白
            "hosts=a:0",                               // 端口越界下
            "hosts=a:99999",                           // 端口越界上
            "hosts=a:x",                               // 端口非整数
            "hosts=:1",                                // 缺 host
            "host=a;hosts=b:1",                        // 与 host= 互斥
            "port=5;hosts=b:1",                        // 与 port= 互斥
        })
        {
            var p = new DocsqlConnectionStringBuilder { ConnectionString = bad };
            Assert.Throws<ArgumentException>(() => p.HostList);
        }
    }

    [Fact]
    public void Subscriber_rejects_hosts_form()
    {
        Assert.Throws<ArgumentException>(() =>
            new DocsqlSubscriber("hosts=127.0.0.1:1,127.0.0.1:2"));
    }

    // ---- 轮换分摊(真实集群,pooling=false 保证每次 Open 都拨号) ----

    [Fact]
    public void Multi_host_rotates_new_connections_across_nodes()
    {
        var nodes = Cluster(3);
        try
        {
            var cs = Cs(nodes, ";pooling=false");
            var conns = new List<DocsqlConnection>();
            try
            {
                // 游标为该 hosts 键新建:第 i 次 Open 轮换到第 i%3 个节点。
                for (var i = 0; i < 6; i++)
                {
                    var c = new DocsqlConnection(cs);
                    c.Open();
                    conns.Add(c);
                    Assert.Equal(nodes[i % 3].Port, PortOf(c));
                }
                // 各连接落在不同节点上写入:轮换不能改变复制语义,全网收敛。
                Exec(conns[0], "CREATE TABLE spin (id INT, src TEXT)");
                for (var i = 0; i < 6; i++)
                {
                    Exec(conns[i], $"INSERT INTO spin VALUES ({i}, 'via{i}')");
                }
                foreach (var node in nodes)
                {
                    Eventually(
                        () => Assert.Equal(6, CountOn(node, "spin")),
                        $"node {node.Port} 未收敛");
                }
            }
            finally
            {
                foreach (var c in conns) c.Dispose();
            }
        }
        finally
        {
            foreach (var node in nodes) node.Dispose();
        }
    }

    // ---- 故障转移(真实杀进程):死节点的每次轮换出场切到下一候选 ----

    [Fact]
    public void Multi_host_fails_over_to_survivors_when_one_dies()
    {
        var nodes = Cluster(3);
        try
        {
            using (var bootstrap = new DocsqlConnection(nodes[0].Cs))
            {
                bootstrap.Open();
                Exec(bootstrap, "CREATE TABLE fo (id INT)");
            }
            // 中间节点下线:候选列表里它每次出现都自动换下一个节点。
            nodes[1].Kill();
            var alive = new HashSet<int> { nodes[0].Port, nodes[2].Port };
            var conns = new List<DocsqlConnection>();
            try
            {
                for (var i = 0; i < 4; i++)
                {
                    var c = new DocsqlConnection(Cs(nodes, ";pooling=false"));
                    c.Open();
                    conns.Add(c);
                    Assert.True(alive.Contains(PortOf(c)),
                        $"落在死节点上了: {c.DataSource}");
                    Exec(c, $"INSERT INTO fo VALUES ({i})");
                }
                // 两个幸存节点都看到全部 4 行(切换后的写入照常扇出)。
                foreach (var node in new[] { nodes[0], nodes[2] })
                {
                    Eventually(
                        () => Assert.Equal(4, CountOn(node, "fo")),
                        "幸存节点未收敛");
                }
            }
            finally
            {
                foreach (var c in conns) c.Dispose();
            }
        }
        finally
        {
            foreach (var node in nodes) node.Dispose();
        }
    }

    // ---- 池化形态:死连接验活/拨号失败 → 轮换 + 池按端点分槽 ----

    [Fact]
    public void Multi_host_pooled_reopen_fails_over_and_keeps_writing()
    {
        var nodes = Cluster(3);
        try
        {
            var cs = Cs(nodes);
            var conns = new List<DocsqlConnection>();
            try
            {
                // 池化默认开:前 6 次 Open 都是新建拨号,轮转覆盖三节点
                // (序列 [0,1,2,0,1,2]),每条物理连接投给各自端点的池槽。
                for (var i = 0; i < 6; i++)
                {
                    var c = new DocsqlConnection(cs);
                    c.Open();
                    conns.Add(c);
                    Assert.Equal(nodes[i % 3].Port, PortOf(c));
                }
                var victim = PortOf(conns[0]);
                Exec(conns[0], "CREATE TABLE pooled (id INT)");
                Exec(conns[0], "INSERT INTO pooled VALUES (1)");
                // 杀掉 conns[0]/conns[3] 所在节点,然后重开这两条连接:
                // 原端点的池验活(死连接 PING)与拨号都必须失败并轮换过去。
                nodes.First(n => n.Port == victim).Kill();
                foreach (var i in new[] { 0, 3 })
                {
                    conns[i].Close();
                    conns[i].Open();
                    Assert.NotEqual(victim, PortOf(conns[i]));
                    Assert.Contains(PortOf(conns[i]),
                        new HashSet<int> { nodes[1].Port, nodes[2].Port });
                    Exec(conns[i], $"INSERT INTO pooled VALUES ({i + 2})");
                }
                // 幸存节点最终各持有 3 行(1 = 失效前写入,2/5 = 切换后写入)。
                foreach (var node in new[] { nodes[1], nodes[2] })
                {
                    Eventually(
                        () => Assert.Equal(3, CountOn(node, "pooled")),
                        $"幸存节点 {node.Port} 未收敛");
                }
            }
            finally
            {
                foreach (var c in conns) c.Dispose();
            }
        }
        finally
        {
            foreach (var node in nodes) node.Dispose();
        }
    }

    // ---- 全部候选不可用:合并错误(每个端点一条原因) ----

    [Fact]
    public void Multi_host_all_endpoints_down_reports_each_endpoint()
    {
        var nodes = Cluster(3);
        try
        {
            var cs = Cs(nodes, ";pooling=false");
            foreach (var node in nodes) node.Kill();

            var ex = Assert.Throws<DocsqlException>(() => new DocsqlConnection(cs).Open());
            Assert.Contains("全部不可用", ex.Message);
            foreach (var node in nodes)
            {
                Assert.Contains($"127.0.0.1:{node.Port}", ex.Message);
            }
            // 内因 = 最后尝试的原始异常,诊断链不断。
            Assert.NotNull(ex.InnerException);
        }
        finally
        {
            foreach (var node in nodes) node.Dispose();
        }
    }
}
