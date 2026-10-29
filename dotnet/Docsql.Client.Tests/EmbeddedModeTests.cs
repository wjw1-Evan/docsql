// Data Source=app.db 嵌入式模式(SQLite 式进程内引擎)的行为契约:
// 无 server、无端口 —— 语句经 C ABI(crates/docsql-ffi)落到与官方 server
// 同源的引擎,含全部 SQL、事务与参数绑定。原生库由 `cargo build -p docsql-ffi`
// 产出(target/debug);EmbeddedNative 的解析器会从测试输出目录向上查找仓库的
// target/{debug,release}(CI 的 dotnet job 已先行构建)。
//
// 本文件不启动任何进程:与 ServerFixture 那套远程测试完全独立。

using Docsql.Client;
using System.Data;
using System.Data.Common;
using Xunit;

public sealed class EmbeddedModeTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("docsql-embed-").FullName;

    private string NewDb([System.Runtime.CompilerServices.CallerMemberName] string name = "") =>
        Path.Combine(_dir, name + ".db");

    private static string Cs(string path) => $"Data Source={path}";

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public void Crud_ThroughTheRegularAdoNetSurface()
    {
        var path = NewDb();
        using var conn = new DocsqlConnection(Cs(path));
        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.Equal(path, conn.DataSource);

        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE todos (id INT AUTOINCREMENT PRIMARY KEY, title VARCHAR, done BOOLEAN)";
            Assert.Equal(0, cmd.ExecuteNonQuery());
        }
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO todos (title, done) VALUES (@t, false)";
            cmd.Parameters.AddWithValue("@t", "写方案");
            Assert.Equal(1, cmd.ExecuteNonQuery());
        }
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "SELECT id, title, done FROM todos WHERE done = false";
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal("写方案", r.GetString(1));
            Assert.False(r.GetBoolean(2));
            Assert.False(r.Read());
        }
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "UPDATE todos SET done = true WHERE id = 1";
            Assert.Equal(1, cmd.ExecuteNonQuery());
        }
    }

    [Fact]
    public void BoundParameters_AreDataOnly()
    {
        var path = NewDb();
        using var conn = new DocsqlConnection(Cs(path));
        conn.Open();
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v VARCHAR)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO t (v) VALUES (@v)";
            cmd.Parameters.AddWithValue("@v", "x'; DROP TABLE t; --");
            cmd.ExecuteNonQuery();
        }
        using var q = (DocsqlCommand)conn.CreateCommand();
        q.CommandText = "SELECT COUNT(*) FROM t";
        Assert.Equal(1L, Convert.ToInt64(q.ExecuteScalar()));
    }

    [Fact]
    public void TwoConnections_OnOneFile_ShareTheEngine()
    {
        var path = NewDb();
        using var a = new DocsqlConnection(Cs(path));
        using var b = new DocsqlConnection(Cs(path));
        a.Open();
        b.Open();
        using (var cmd = a.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE shared (id INT AUTOINCREMENT PRIMARY KEY, v INT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = a.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO shared (v) VALUES (42)";
            cmd.ExecuteNonQuery();
        }
        using var q = (DocsqlCommand)b.CreateCommand();
        q.CommandText = "SELECT v FROM shared";
        Assert.Equal(42L, Convert.ToInt64(q.ExecuteScalar()));
    }

    [Fact]
    public void ConnectionStringBuilder_RecognizesDataSource()
    {
        var b = new DocsqlConnectionStringBuilder { ConnectionString = "Data Source=app.db" };
        Assert.True(b.Embedded);
        Assert.Equal("app.db", b.DataSource);
        var remote = new DocsqlConnectionStringBuilder { ConnectionString = "host=127.0.0.1;port=7600" };
        Assert.False(remote.Embedded);
    }

    [Fact]
    public void MemoryMode_IsIsolatedPerOpen()
    {
        using var a = new DocsqlConnection(Cs(":memory:"));
        using var b = new DocsqlConnection(Cs(":memory:"));
        a.Open();
        b.Open();
        using (var cmd = a.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE only_a (id INT)";
            cmd.ExecuteNonQuery();
        }
        using var q = (DocsqlCommand)b.CreateCommand();
        q.CommandText = "SELECT * FROM only_a";
        var ex = Assert.Throws<DocsqlException>(() => q.ExecuteNonQuery());
        Assert.Contains("only_a", ex.Message);
    }

    [Fact]
    public void ExactScalars_RoundTrip_ThroughParameters()
    {
        var path = NewDb();
        using var conn = new DocsqlConnection(Cs(path));
        conn.Open();
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE nums (d DECIMAL, ts TIMESTAMP, b BLOB)";
            cmd.ExecuteNonQuery();
        }
        var ts = new DateTime(2026, 10, 29, 12, 34, 56, DateTimeKind.Utc);
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO nums (d, ts, b) VALUES (@d, @ts, @b)";
            cmd.Parameters.AddWithValue("@d", 1.25m);
            cmd.Parameters.AddWithValue("@ts", ts);
            cmd.Parameters.AddWithValue("@b", new byte[] { 1, 2, 255 });
            cmd.ExecuteNonQuery();
        }
        using var q = (DocsqlCommand)conn.CreateCommand();
        q.CommandText = "SELECT d, ts, b FROM nums";
        using var r = q.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal(1.25m, r.GetDecimal(0));
        Assert.Equal(ts, r.GetDateTime(1));
        Assert.Equal(new byte[] { 1, 2, 255 }, (byte[])r.GetValue(2));
    }

    [Fact]
    public void Transactions_CommitPersists_RollbackDiscards()
    {
        var path = NewDb();
        using var conn = new DocsqlConnection(Cs(path));
        conn.Open();
        using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v INT)";
            cmd.ExecuteNonQuery();
        }
        using (var tx = conn.BeginTransaction())
        {
            using var cmd = (DocsqlCommand)conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO t (v) VALUES (1)";
            cmd.ExecuteNonQuery();
            // The owning session sees its own uncommitted write.
            cmd.CommandText = "SELECT COUNT(*) FROM t";
            Assert.Equal(1L, Convert.ToInt64(cmd.ExecuteScalar()));
            tx.Commit();
        }
        using (var tx = conn.BeginTransaction())
        {
            using var cmd = (DocsqlCommand)conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO t (v) VALUES (2)";
            cmd.ExecuteNonQuery();
            tx.Rollback();
        }
        using var q = (DocsqlCommand)conn.CreateCommand();
        q.CommandText = "SELECT COUNT(*) FROM t";
        Assert.Equal(1L, Convert.ToInt64(q.ExecuteScalar()));
    }

    [Fact]
    public void ConcurrentWriter_WaitsForTheOpenTransaction_ThenFailsLoudly()
    {
        var path = NewDb();
        using var owner = new DocsqlConnection(Cs(path));
        using var other = new DocsqlConnection(Cs(path));
        owner.Open();
        other.Open();
        using (var cmd = owner.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INT AUTOINCREMENT PRIMARY KEY, v INT)";
            cmd.ExecuteNonQuery();
        }
        using var tx = owner.BeginTransaction();
        using (var cmd = owner.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO t (v) VALUES (1)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = (DocsqlCommand)other.CreateCommand())
        {
            cmd.CommandTimeout = 1; // seconds — the native wait budget
            cmd.CommandText = "INSERT INTO t (v) VALUES (2)";
            var ex = Assert.Throws<DocsqlException>(() => cmd.ExecuteNonQuery());
            Assert.Contains("transaction", ex.Message);
        }
        tx.Commit();
        using var q = owner.CreateCommand();
        q.CommandText = "SELECT COUNT(*) FROM t";
        Assert.Equal(1L, Convert.ToInt64(q.ExecuteScalar()));
    }

    [Fact]
    public void PreparedPath_IsReusedAcrossExecutions()
    {
        var path = NewDb();
        using var conn = new DocsqlConnection(Cs(path));
        conn.Open();
        using var cmd = (DocsqlCommand)conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE p (id INT AUTOINCREMENT PRIMARY KEY, v VARCHAR)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO p (v) VALUES (@v)";
        cmd.Parameters.AddWithValue("@v", "a");
        cmd.ExecuteNonQuery();
        cmd.Parameters["@v"].Value = "b";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT COUNT(*) FROM p WHERE v IN ('a', 'b')";
        cmd.Parameters.Clear();
        Assert.Equal(2L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    [Fact]
    public void ServerOnlyFeatures_FailLoudly()
    {
        var path = NewDb();
        using var conn = new DocsqlConnection(Cs(path));
        conn.Open();
        var ex = Assert.Throws<DocsqlException>(() => conn.Publish("chan", "payload"));
        Assert.Contains("server feature", ex.Message);
    }

    [Fact]
    public async Task AsyncSurface_Works()
    {
        var path = NewDb();
        await using var conn = new DocsqlConnection(Cs(path));
        await conn.OpenAsync();
        await using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE a (id INT AUTOINCREMENT PRIMARY KEY, v INT)";
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var cmd = (DocsqlCommand)conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO a (v) VALUES (@v)";
            cmd.Parameters.AddWithValue("@v", 5);
            await cmd.ExecuteNonQueryAsync();
        }
        await using var q = (DocsqlCommand)conn.CreateCommand();
        q.CommandText = "SELECT v FROM a";
        Assert.Equal(5L, Convert.ToInt64(await q.ExecuteScalarAsync()));
    }

    [Fact]
    public void DataSurvivesCloseAndReopen_ThroughTheSameProcess()
    {
        var path = NewDb();
        using (var conn = new DocsqlConnection(Cs(path)))
        {
            conn.Open();
            using var cmd = (DocsqlCommand)conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persist (id INT AUTOINCREMENT PRIMARY KEY, v VARCHAR)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO persist (v) VALUES ('kept')";
            cmd.ExecuteNonQuery();
        }
        using (var conn = new DocsqlConnection(Cs(path)))
        {
            conn.Open();
            using var cmd = (DocsqlCommand)conn.CreateCommand();
            cmd.CommandText = "SELECT v FROM persist";
            Assert.Equal("kept", cmd.ExecuteScalar());
        }
    }
}
