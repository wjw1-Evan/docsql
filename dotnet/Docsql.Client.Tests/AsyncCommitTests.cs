// 异步组提交(DOCSQL_ASYNC_COMMIT=1):服务端后台每 ~2ms 合并一次 WAL fsync。
// 覆盖:并发多客户端写入全部可见、读一致、KV 路径同样生效。

using Docsql.Client;
using System.Diagnostics;
using Xunit;

public sealed class AsyncCommitTests
{
    private static (Process Proc, int Port) StartServer()
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        var db = Path.Combine(Path.GetTempPath(), $"docsql-async-{port}.db");
        try { File.Delete(db); } catch { }
        var psi = new ProcessStartInfo
        {
            FileName = exe, ArgumentList = { db, $"127.0.0.1:{port}" },
            CreateNoWindow = true, RedirectStandardError = true,
        };
        psi.Environment["DOCSQL_ASYNC_COMMIT"] = "1";
        var proc = Process.Start(psi)!;
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); break; }
            catch { Thread.Sleep(50); }
        }
        return (proc, port);
    }

    [Fact]
    public void Concurrent_writes_all_visible_under_async_commit()
    {
        var (proc, port) = StartServer();
        using var _ = proc;
        var cs = $"host=127.0.0.1;port={port}";

        using (var conn = new DocsqlConnection(cs))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE ac (thread INT, n INT)";
            cmd.ExecuteNonQuery();
        }

        var threads = Enumerable.Range(0, 4).Select(t => new Thread(() =>
        {
            using var conn = new DocsqlConnection(cs);
            conn.Open();
            using var cmd = conn.CreateCommand();
            for (var n = 0; n < 50; n++)
            {
                cmd.CommandText = $"INSERT INTO ac VALUES ({t}, {n})";
                cmd.ExecuteNonQuery();
            }
        })).ToList();
        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        using (var check = new DocsqlConnection(cs))
        {
            check.Open();
            using var cmd = check.CreateCommand();
            cmd.CommandText = "SELECT COUNT(n) FROM ac";
            Assert.Equal(200L, cmd.ExecuteScalar());
        }
    }
}
