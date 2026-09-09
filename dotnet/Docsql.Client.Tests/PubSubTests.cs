// 持久化发布订阅测试:实时投递、离线回放(earliest)、断线续传(from id)、
// 模式订阅、保留(trim + docsql_pubsub 视图)、加密传输下的推送、跨节点投递。

using Docsql.Client;
using System.Collections.Concurrent;
using System.Diagnostics;
using Xunit;

public sealed class PubSubTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public PubSubTests(ServerFixture fx) => _fx = fx;

    private string Cs => $"host=127.0.0.1;port={_fx.Port}";

    private static DocsqlMessage WaitOne(ConcurrentBag<DocsqlMessage> bag, int ms = 5000)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < ms)
        {
            if (bag.TryTake(out var m))
            {
                return m;
            }
            Thread.Sleep(20);
        }
        throw new TimeoutException("未收到推送");
    }

    /// <summary>断言静默:一段时间内不应有任何推送(离线消息不实时补投)。</summary>
    private static void AssertSilent(ConcurrentBag<DocsqlMessage> bag, int ms = 400)
    {
        Thread.Sleep(ms);
        Assert.True(bag.IsEmpty, $"不应收到推送,却有 {bag.Count} 条");
    }

    [Fact]
    public void Live_delivery_between_connections()
    {
        var bag = new ConcurrentBag<DocsqlMessage>();
        using var sub = new DocsqlSubscriber(Cs);
        Assert.Equal(1, sub.Subscribe("news", bag.Add));

        using var pub = new DocsqlConnection(Cs);
        pub.Open();
        var (id, receivers) = pub.Publish("news", "hello world");
        Assert.True(id >= 1, "持久化消息 id");
        Assert.Equal(1, receivers);

        var msg = WaitOne(bag);
        Assert.Equal("message", msg.Kind);
        Assert.Equal("news", msg.Channel);
        Assert.Equal("hello world", msg.Payload);
        Assert.Equal(id, msg.Id);
        Assert.True(msg.Ts > 0);
    }

    [Fact]
    public void Replay_earliest_after_offline_publishes()
    {
        // 无订阅者时发布:消息持久化,receivers = 0。
        using (var pub = new DocsqlConnection(Cs))
        {
            pub.Open();
            for (var i = 1; i <= 3; i++)
            {
                var (_, r) = pub.Publish("offline", $"m{i}");
                Assert.Equal(0, r);
            }
        }

        var bag = new ConcurrentBag<DocsqlMessage>();
        using var sub = new DocsqlSubscriber(Cs);
        sub.Subscribe("offline", bag.Add, "earliest");
        long last = 0;
        for (var i = 1; i <= 3; i++)
        {
            var msg = WaitOne(bag);
            Assert.Equal("offline", msg.Channel);
            Assert.Equal($"m{i}", msg.Payload);
            Assert.True(msg.Id > last, "回放按 id 递增");
            last = msg.Id;
        }
        AssertSilent(bag);
    }

    [Fact]
    public void Resume_from_id_after_reconnect()
    {
        var bag = new ConcurrentBag<DocsqlMessage>();
        long lastId;
        using (var sub = new DocsqlSubscriber(Cs))
        {
            sub.Subscribe("resume", bag.Add);
            using var pub = new DocsqlConnection(Cs);
            pub.Open();
            pub.Publish("resume", "kept-1");
            pub.Publish("resume", "kept-2");
            var m1 = WaitOne(bag);
            var m2 = WaitOne(bag);
            lastId = Math.Max(m1.Id, m2.Id);
        }

        // 断线期间错过的发布。
        using (var pub = new DocsqlConnection(Cs))
        {
            pub.Open();
            pub.Publish("resume", "missed");
        }

        // 从最后收到的 id 续传:恰好补回错过的那条。
        var resumed = new ConcurrentBag<DocsqlMessage>();
        using var sub2 = new DocsqlSubscriber(Cs);
        sub2.Subscribe("resume", resumed.Add, lastId.ToString());
        var msg = WaitOne(resumed);
        Assert.Equal("missed", msg.Payload);
        Assert.True(msg.Id > lastId);
        AssertSilent(resumed);
    }

    [Fact]
    public void Pattern_delivery_and_non_matching_silent()
    {
        var bag = new ConcurrentBag<DocsqlMessage>();
        using var sub = new DocsqlSubscriber(Cs);
        sub.Psubscribe("news.*", bag.Add);

        using var pub = new DocsqlConnection(Cs);
        pub.Open();
        pub.Publish("news.tech", "deep dive");
        pub.Publish("sports", "ignored");

        var msg = WaitOne(bag);
        Assert.Equal("pmessage", msg.Kind);
        Assert.Equal("news.*", msg.Pattern);
        Assert.Equal("news.tech", msg.Channel);
        Assert.Equal("deep dive", msg.Payload);
        AssertSilent(bag, 500);
    }

    [Fact]
    public void Trim_keeps_newest_and_view_counts()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        for (var i = 1; i <= 5; i++)
        {
            conn.Publish("trimch", $"m{i}");
        }

        // docsql_pubsub 视图可见该频道全部 5 条(其它用例的频道互不干扰)。
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM docsql_pubsub WHERE channel = 'trimch'";
            Assert.Equal(5L, (long)cmd.ExecuteScalar()!);
        }

        Assert.Equal(3, conn.PubsubTrim("trimch", 2));

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM docsql_pubsub WHERE channel = 'trimch'";
            Assert.Equal(2L, (long)cmd.ExecuteScalar()!);
        }

        // 保留的是最新的两条。
        var bag = new ConcurrentBag<DocsqlMessage>();
        using var sub = new DocsqlSubscriber(Cs);
        sub.Subscribe("trimch", bag.Add, "earliest");
        Assert.Equal("m4", WaitOne(bag).Payload);
        Assert.Equal("m5", WaitOne(bag).Payload);
        AssertSilent(bag);

        // keep=0 被拒(会破坏 id 单调性)。
        Assert.Throws<DocsqlException>(() => conn.PubsubTrim("trimch", 0));
    }

    [Fact]
    public void Encrypted_transport_delivers_pushes()
    {
        using var server = TlsServer.Start();
        var bag = new ConcurrentBag<DocsqlMessage>();
        using var sub = new DocsqlSubscriber(server.Cs);
        sub.Subscribe("secret", bag.Add);

        using var pub = new DocsqlConnection(server.Cs);
        pub.Open();
        var (id, receivers) = pub.Publish("secret", "sealed");
        Assert.Equal(1, receivers);

        var msg = WaitOne(bag);
        Assert.Equal("secret", msg.Channel);
        Assert.Equal("sealed", msg.Payload);
        Assert.Equal(id, msg.Id);
    }

    [Fact]
    public void Cross_node_delivery_and_persistence()
    {
        // 两节点对称集群(仿 SymmetricClusterTests)。
        using var l = new System.Net.Sockets.TcpListener(
            System.Net.IPAddress.Loopback, 0);
        var ports = new List<int>();
        for (var i = 0; i < 2; i++)
        {
            l.Start();
            ports.Add(((System.Net.IPEndPoint)l.LocalEndpoint).Port);
            l.Stop();
        }
        var all = ports.Select(p => $"127.0.0.1:{p}").ToList();
        var nodes = ports
            .Select(p => PeerNode.Start(p, string.Join(",", all.Where(a => a != $"127.0.0.1:{p}"))))
            .ToList();
        try
        {
            var bag = new ConcurrentBag<DocsqlMessage>();
            using var sub = new DocsqlSubscriber(nodes[0].Cs);
            sub.Subscribe("cluster", bag.Add);

            // 在节点 1 发布,节点 0 的订阅者应实时收到。
            using var pub = new DocsqlConnection(nodes[1].Cs);
            pub.Open();
            var (_, receivers) = pub.Publish("cluster", "from-node1");
            Assert.True(receivers == 1, $"发布方应计入对端节点的订阅者,got {receivers}");

            var msg = WaitOne(bag);
            Assert.Equal("cluster", msg.Channel);
            Assert.Equal("from-node1", msg.Payload);

            // 复制的发布在节点 0 也已持久化。
            long count = 0;
            for (var i = 0; i < 100; i++)
            {
                using var c = new DocsqlConnection(nodes[0].Cs);
                c.Open();
                using var cmd = c.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM docsql_pubsub";
                count = (long)cmd.ExecuteScalar()!;
                if (count == 1)
                {
                    break;
                }
                Thread.Sleep(30);
            }
            Assert.Equal(1L, count);
        }
        finally
        {
            foreach (var n in nodes)
            {
                n.Dispose();
            }
        }
    }
}
