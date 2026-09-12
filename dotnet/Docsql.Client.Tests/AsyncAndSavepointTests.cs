// 真异步执行链路 + ADO.NET 保存点 API + 池容量封顶的行为契约。
// ServerFixture 在 AdoNetTests.cs(全测试项目共用的进程启动辅助类);
// 本文件只引用它,不自行启动进程。

using Docsql.Client;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Xunit;

public sealed class AsyncClientTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public AsyncClientTests(ServerFixture fx) => _fx = fx;

    [Fact]
    public async Task Async_open_and_execute_roundtrip()
    {
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        await conn.OpenAsync();
        Assert.Equal(ConnectionState.Open, conn.State);

        using (var create = new DocsqlCommand
        {
            Connection = conn,
            CommandText = "CREATE TABLE IF NOT EXISTS async_t (id INT PRIMARY KEY, note TEXT)",
        })
        {
            await create.ExecuteNonQueryAsync();
        }
        using (var clear = new DocsqlCommand { Connection = conn, CommandText = "DELETE FROM async_t" })
        {
            await clear.ExecuteNonQueryAsync();
        }
        using (var ins = new DocsqlCommand
        {
            Connection = conn,
            CommandText = "INSERT INTO async_t VALUES (1, 'one'), (2, 'two')",
        })
        {
            Assert.Equal(2, await ins.ExecuteNonQueryAsync());
        }

        using var query = conn.CreateCommand();
        query.CommandText = "SELECT id, note FROM async_t ORDER BY id";
        using var reader = await query.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("one", reader.GetString(1));
        Assert.True(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task Async_parameterized_query_uses_server_binding()
    {
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        await conn.OpenAsync();
        using var cmd = (DocsqlCommand)conn.CreateCommand();
        cmd.CommandText = "SELECT @v";
        cmd.Parameters.AddWithValue("v", "it's async");
        Assert.Equal("it's async", await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Async_scalar_keeps_fractional_doubles()
    {
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        await conn.OpenAsync();
        using var cmd = (DocsqlCommand)conn.CreateCommand();
        cmd.CommandText = "SELECT @d";
        cmd.Parameters.AddWithValue("d", 3.5d);
        Assert.Equal(3.5d, await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Async_transaction_commit_and_rollback()
    {
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        await conn.OpenAsync();
        new DocsqlCommand { Connection = conn, CommandText = "CREATE TABLE IF NOT EXISTS async_tx (id INT)" }
            .ExecuteNonQuery();
        new DocsqlCommand { Connection = conn, CommandText = "DELETE FROM async_tx" }.ExecuteNonQuery();

        await using (var tx = await conn.BeginTransactionAsync())
        {
            using var ins = conn.CreateCommand();
            ins.CommandText = "INSERT INTO async_tx VALUES (1)";
            ins.Transaction = tx;
            await ins.ExecuteNonQueryAsync();
            await tx.CommitAsync();
        }
        await using (var tx = await conn.BeginTransactionAsync())
        {
            using var ins = conn.CreateCommand();
            ins.CommandText = "INSERT INTO async_tx VALUES (2)";
            ins.Transaction = tx;
            await ins.ExecuteNonQueryAsync();
            await tx.RollbackAsync();
        }

        Assert.Equal(1L, Convert.ToInt64(new DocsqlCommand
        {
            Connection = conn,
            CommandText = "SELECT COUNT(*) FROM async_tx",
        }.ExecuteScalar()));
    }
}

public sealed class SavepointApiTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public SavepointApiTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        return conn;
    }

    private static void Exec(DocsqlConnection conn, DbTransaction tx, string sql)
    {
        using var cmd = new DocsqlCommand { Connection = conn, CommandText = sql, Transaction = tx };
        cmd.ExecuteNonQuery();
    }

    private static long Scalar(DocsqlConnection conn, string sql) =>
        Convert.ToInt64(new DocsqlCommand { Connection = conn, CommandText = sql }.ExecuteScalar());

    [Fact]
    public void Savepoint_rollback_consumes_then_recreate_roundtrip()
    {
        using var conn = Open();
        new DocsqlCommand { Connection = conn, CommandText = "CREATE TABLE IF NOT EXISTS sp_t (id INT)" }
            .ExecuteNonQuery();
        new DocsqlCommand { Connection = conn, CommandText = "DELETE FROM sp_t" }.ExecuteNonQuery();

        using var tx = conn.BeginTransaction();
        Assert.True(tx.SupportsSavepoints);

        Exec(conn, tx, "INSERT INTO sp_t VALUES (1)");
        tx.Save("s1");
        Exec(conn, tx, "INSERT INTO sp_t VALUES (2)");
        // 回滚到 s1:事务内读可见自己的分阶段写,2 已撤销。
        tx.Rollback("s1");
        Assert.Equal(1L, Scalar(conn, "SELECT COUNT(*) FROM sp_t"));
        // 引擎语义:ROLLBACK TO 把命名保存点自身也丢弃 —— 重新 Save 同名后继续用。
        tx.Save("s1");
        Exec(conn, tx, "INSERT INTO sp_t VALUES (3)");
        tx.Release("s1");
        tx.Commit();

        Assert.Equal(2L, Scalar(conn, "SELECT COUNT(*) FROM sp_t"));
        Assert.Equal(4L, Scalar(conn, "SELECT SUM(id) FROM sp_t"));
    }

    [Fact]
    public async Task Async_savepoint_roundtrip()
    {
        using var conn = Open();
        new DocsqlCommand { Connection = conn, CommandText = "CREATE TABLE IF NOT EXISTS sp_a (id INT)" }
            .ExecuteNonQuery();
        new DocsqlCommand { Connection = conn, CommandText = "DELETE FROM sp_a" }.ExecuteNonQuery();

        await using var tx = await conn.BeginTransactionAsync();
        await tx.SaveAsync("sp");
        using (var ins = conn.CreateCommand())
        {
            ins.CommandText = "INSERT INTO sp_a VALUES (10)";
            ins.Transaction = tx;
            await ins.ExecuteNonQueryAsync();
        }
        await tx.RollbackAsync("sp");
        await tx.CommitAsync();

        Assert.Equal(0L, Scalar(conn, "SELECT COUNT(*) FROM sp_a"));
    }
}

public sealed class PoolCapacityTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public PoolCapacityTests(ServerFixture fx) => _fx = fx;

    private string Cs(string extra) => $"host=127.0.0.1;port={_fx.Port};{extra}";

    [Fact]
    public void Pool_full_blocks_then_times_out_and_recovers()
    {
        ConnectionPool.ClearAll();
        var cs = Cs("max pool size=1;connect timeout=1");

        var a = new DocsqlConnection(cs);
        a.Open();
        try
        {
            using var b = new DocsqlConnection(cs);
            var sw = Stopwatch.StartNew();
            var ex = Assert.Throws<TimeoutException>(() => b.Open());
            Assert.True(sw.ElapsedMilliseconds >= 900,
                $"pool wait returned too early: {sw.ElapsedMilliseconds} ms");
            Assert.Contains("pool exhausted", ex.Message);
        }
        finally
        {
            a.Close(); // 归还后池立即可用
        }

        using var c = new DocsqlConnection(cs);
        c.Open();
        Assert.Equal(1L, Convert.ToInt64(
            new DocsqlCommand { Connection = c, CommandText = "SELECT 1" }.ExecuteScalar()));
    }

    [Fact]
    public async Task Async_open_waits_for_returned_connection()
    {
        ConnectionPool.ClearAll();
        var cs = Cs("max pool size=1;connect timeout=5");

        var a = new DocsqlConnection(cs);
        await a.OpenAsync();
        var borrower = Task.Run(async () =>
        {
            using var b = new DocsqlConnection(cs);
            await b.OpenAsync(); // 等 a 归还,不新建
            using var cmd = new DocsqlCommand { Connection = b, CommandText = "SELECT 1" };
            return Convert.ToInt64(await cmd.ExecuteScalarAsync());
        });
        await Task.Delay(200);
        a.Close();
        Assert.Equal(1L, await borrower);
    }
}
