using System.Diagnostics;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

// 运行期连接串工厂:同一宿主内不同上下文可连不同节点,模型/服务提供程序
// 只建一次(扩展哈希恒为 0,连接信息不参与缓存键)。测试夹具按用例路由
// 到独立节点的机制依赖于此。
public sealed class ConnectionFactoryTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public ConnectionFactoryTests(EfServerFixture fx) => _fx = fx;

    private static readonly AsyncLocal<string?> Route = new();

    private static (Process Proc, string Cs, string File) StartNode()
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var file = Path.Combine(Path.GetTempPath(), $"docsql-efcf-{port}.db");
        try { File.Delete(file); } catch { }
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        var proc = Process.Start(new ProcessStartInfo
        {
            FileName = exe,
            ArgumentList = { file, $"127.0.0.1:{port}" },
            UseShellExecute = false,
            CreateNoWindow = true,
        })!;
        for (var i = 0; ; i++)
        {
            try
            {
                using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port);
                break;
            }
            catch when (i < 100)
            {
                Thread.Sleep(50);
            }
        }
        return (proc, $"host=127.0.0.1;port={port}", file);
    }

    [Fact]
    public void Factory_routes_each_context_to_the_current_node()
    {
        var (procB, csB, fileB) = StartNode();
        try
        {
            var csA = $"host=127.0.0.1;port={_fx.Port}";

            Route.Value = csA;
            using (var db = new RoutedDb(() => Route.Value!))
            {
                db.Rows.Add(new RoutedRow { Label = "a" });
                db.SaveChanges();
            }

            Route.Value = csB;
            using (var db = new RoutedDb(() => Route.Value!))
            {
                db.Rows.Add(new RoutedRow { Label = "b" });
                db.SaveChanges();
            }

            // 两台节点数据互不可见:工厂按上下文运行,而非在 options 构建时定型。
            Route.Value = csA;
            using (var db = new RoutedDb(() => Route.Value!))
            {
                Assert.Equal("a", db.Rows.Single().Label);
            }
            Route.Value = csB;
            using (var db = new RoutedDb(() => Route.Value!))
            {
                Assert.Equal("b", db.Rows.Single().Label);
            }
        }
        finally
        {
            try { if (!procB.HasExited) procB.Kill(); } catch { }
            procB.Dispose();
            try { File.Delete(fileB); } catch { }
        }
    }

    public class RoutedRow
    {
        public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    public class RoutedDb : DbContext
    {
        private readonly Func<string> _factory;
        public RoutedDb(Func<string> factory) => _factory = factory;
        public DbSet<RoutedRow> Rows => Set<RoutedRow>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_factory);
    }
}
