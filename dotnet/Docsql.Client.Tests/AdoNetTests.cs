using Docsql.Client;
using System.Data.Common;
using Xunit;

public sealed class ServerFixture : IDisposable
{
    public int Port { get; }
    private readonly System.Diagnostics.Process _proc;

    public ServerFixture()
    {
        // 系统分配空闲端口:测试类并行运行时随机区间端口会碰撞。
        using var l = new System.Net.Sockets.TcpListener(
            System.Net.IPAddress.Loopback, 0);
        l.Start();
        Port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var tmp = Path.Combine(Path.GetTempPath(), $"docsql-adonet-{Port}.db");
        try { File.Delete(tmp); } catch { }
        var exe = FindServer();
        _proc = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
        {
            FileName = exe,
            Arguments = $"{tmp} 127.0.0.1:{Port}",
            CreateNoWindow = true,
            RedirectStandardError = true,
        })!;
        // wait for port
        for (int i = 0; i < 100; i++)
        {
            try
            {
                using var c = new System.Net.Sockets.TcpClient("127.0.0.1", Port);
                return;
            }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException("server did not start");
    }

    private static string FindServer()
    {
        var exe = Path.GetFullPath(
            Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..",
                "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        return exe;
    }

    public void Dispose()
    {
        try { _proc.Kill(); } catch { }
        _proc.Dispose();
    }
}

public sealed class AdoNetTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public AdoNetTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var c = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        c.Open();
        return c;
    }

    [Fact]
    public void Create_insert_select_roundtrip()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS people (id INT, name TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM people";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO people (id, name) VALUES (1, 'ann'), (2, 'bob')";
        Assert.Equal(2, cmd.ExecuteNonQuery());

        cmd.CommandText = "SELECT id, name FROM people ORDER BY id";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("ann", reader.GetString(1));
        Assert.True(reader.Read());
        Assert.Equal("bob", reader.GetString(1));
        Assert.False(reader.Read());
    }

    [Fact]
    public void ExecuteScalar_returns_first_column()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS scalar_t (v INT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM scalar_t";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO scalar_t VALUES (42)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT v FROM scalar_t";
        Assert.Equal(42L, cmd.ExecuteScalar());
    }

    [Fact]
    public void Parameters_are_bound_and_escaped()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS para (name TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM para";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO para (name) VALUES (@name)";
        ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("name", "o'brien");
        Assert.Equal(1, cmd.ExecuteNonQuery());
        cmd.CommandText = "SELECT name FROM para WHERE name = @n";
        ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("n", "o'brien");
        Assert.Equal("o'brien", cmd.ExecuteScalar());
    }

    [Fact]
    public void Transactions_commit_and_rollback()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS tx (a INT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM tx";
        cmd.ExecuteNonQuery();

        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "INSERT INTO tx VALUES (1)";
            cmd.ExecuteNonQuery();
            tx.Commit();
        }
        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "INSERT INTO tx VALUES (2)";
            cmd.ExecuteNonQuery();
            tx.Rollback();
        }
        cmd.CommandText = "SELECT COUNT(a) FROM tx";
        Assert.Equal(1L, cmd.ExecuteScalar());
    }

    [Fact]
    public void Errors_surface_as_DbException()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM definitely_missing";
        Assert.Throws<DocsqlException>(() => cmd.ExecuteReader());
        // connection still usable
        cmd.CommandText = "SELECT 1 + 1 AS two";
        Assert.Equal(2L, cmd.ExecuteScalar());
    }
}
