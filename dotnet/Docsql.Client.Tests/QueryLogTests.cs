// 查询日志:docsql_log 系统视图 + DOCSQL_LOG_FILE JSONL 落盘。

using Docsql.Client;
using System.Diagnostics;
using Xunit;

public sealed class QueryLogTests
{
    private static (Process Proc, int Port, string LogFile) StartServer()
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        var db = Path.Combine(Path.GetTempPath(), $"docsql-qlog-{port}.db");
        var logFile = Path.Combine(Path.GetTempPath(), $"docsql-qlog-{port}.jsonl");
        try { File.Delete(db); File.Delete(logFile); } catch { }
        var psi = new ProcessStartInfo
        {
            FileName = exe, ArgumentList = { db, $"127.0.0.1:{port}" },
            CreateNoWindow = true, RedirectStandardError = false,
        };
        psi.Environment["DOCSQL_LOG_FILE"] = logFile;
        var proc = Process.Start(psi)!;
        var up = false;
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); up = true; break; }
            catch { Thread.Sleep(50); }
        }
        if (!up) throw new InvalidOperationException($"server on port {port} never came up");
        return (proc, port, logFile);
    }

    [Fact]
    public void Docsql_log_view_and_jsonl_file_record_statements()
    {
        var (proc, port, logFile) = StartServer();
        using var _ = proc;
        try
        {
            var cs = $"host=127.0.0.1;port={port}";
            using (var conn = new DocsqlConnection(cs))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE q (id INT)";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "INSERT INTO q VALUES (1), (2), (3)";
                cmd.ExecuteNonQuery();
            }

            // 视图可查询,含语句与延迟列
            using (var conn = new DocsqlConnection(cs))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT sql, ms, affected FROM docsql_log";
                using var reader = cmd.ExecuteReader();
                var seen = new List<string>();
                var sqlCol = reader.GetOrdinal("sql");
                while (reader.Read()) seen.Add(reader.GetString(sqlCol));
                Assert.Contains(seen, s => s.Contains("CREATE TABLE"));
                Assert.Contains(seen, s => s.Contains("INSERT INTO q"));
            }

            // JSONL 文件落盘:每行一条记录
            for (var i = 0; i < 50 && !File.Exists(logFile); i++) Thread.Sleep(50);
            Assert.True(File.Exists(logFile), "日志文件未生成");
            var lines = File.ReadAllLines(logFile);
            Assert.Contains(lines, l => l.Contains("INSERT INTO q"));
            Assert.Contains(lines, l => l.Contains("\"affected\":3"));
        }
        finally
        {
            try { proc.Kill(); } catch { }
            try { File.Delete(logFile); } catch { }
        }
    }
}
