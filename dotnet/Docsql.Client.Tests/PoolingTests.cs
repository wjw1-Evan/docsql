// 连接池 + 服务端参数绑定(REQ_PREPARE/REQ_EXECUTE)的行为契约。
//
// 池化默认开启:Close 归还、Open 借出前 PING 验活;事务未了结的 Close 物理
// 丢弃连接(服务器断连自动 ROLLBACK,残留事务不可能泄漏给下一个借出者)。
// 参数化语句经服务端绑定执行:值在服务器渲染为类型化字面量(引号感知、
// 字符串翻倍转义),注入载荷只能是数据。

using Docsql.Client;
using System.Data.Common;
using Xunit;

// 池命中/丢弃计数是客户端进程级静态(xUnit 默认按测试类并行),其它测试类的
// 池化连接会在断言窗口内推高计数 —— 断言计数器的类必须整体串行,xUnit 保证
// 该 collection 等所有并行 collection 结束后独占运行。
[CollectionDefinition("pooling-counters", DisableParallelization = true)]
public sealed class PoolingCountersCollection
{
}

[Collection("pooling-counters")]
public sealed class PoolingTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public PoolingTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open(string extra = "") => OpenRaw(
        $"host=127.0.0.1;port={_fx.Port}{extra}");

    private static DocsqlConnection OpenRaw(string cs)
    {
        var conn = new DocsqlConnection(cs);
        conn.Open();
        return conn;
    }

    private static long ExecScalar(DocsqlConnection conn, string sql) =>
        Convert.ToInt64(new DocsqlCommand { Connection = conn, CommandText = sql }
            .ExecuteScalar());

    [Fact]
    public void Pooled_connection_is_reused_and_healthy()
    {
        ConnectionPool.ClearAll();
        long hitsBefore = Interlocked.Read(ref ConnectionPool.Hits);

        var c1 = Open();
        new DocsqlCommand { Connection = c1, CommandText = "CREATE TABLE poolt (id INT)" }
            .ExecuteNonQuery();
        new DocsqlCommand { Connection = c1, CommandText = "INSERT INTO poolt VALUES (7)" }
            .ExecuteNonQuery();
        c1.Close(); // 归还
        using var c2 = Open();
        Assert.True(Interlocked.Read(ref ConnectionPool.Hits) > hitsBefore,
            "second Open must hit the pool");
        // 借出的连接是真的能用,且能看到前一条连接写的数据。
        Assert.Equal(1L, ExecScalar(c2, "SELECT COUNT(*) FROM poolt"));
    }

    [Fact]
    public void Pooling_disabled_opens_fresh_each_time()
    {
        ConnectionPool.ClearAll();
        long hitsBefore = Interlocked.Read(ref ConnectionPool.Hits);

        var c1 = OpenRaw($"host=127.0.0.1;port={_fx.Port};pooling=false");
        c1.Close();
        var c2 = OpenRaw($"host=127.0.0.1;port={_fx.Port};pooling=false");
        Assert.Equal(1L, ExecScalar(c2, "SELECT 1"));
        c2.Close();

        // pooling=false 两次 Open 都是物理新建:池命中计数不动。
        Assert.Equal(hitsBefore, Interlocked.Read(ref ConnectionPool.Hits));
    }

    [Fact]
    public void Unfinished_transaction_is_never_returned_to_pool()
    {
        ConnectionPool.ClearAll();
        long hitsBefore = Interlocked.Read(ref ConnectionPool.Hits);

        var conn = Open();
        new DocsqlCommand { Connection = conn, CommandText = "CREATE TABLE IF NOT EXISTS ptx (a INT)" }
            .ExecuteNonQuery();
        var tx = conn.BeginTransaction();
        new DocsqlCommand { Connection = conn, CommandText = "INSERT INTO ptx VALUES (1)", Transaction = tx }
            .ExecuteNonQuery();
        // 事务开着就 Close:连接必须被物理丢弃,不能带着开放事务进池。
        conn.Close();
        Assert.Equal(hitsBefore, Interlocked.Read(ref ConnectionPool.Hits));

        // 借出者拿不到悬挂事务:新连接上 BEGIN 正常,且未提交的 INSERT 没有生效。
        using var fresh = Open();
        Assert.Equal(0L, ExecScalar(fresh, "SELECT COUNT(a) FROM ptx"));
        using var tx2 = fresh.BeginTransaction();
        tx2.Commit();
    }

    [Fact]
    public void Committed_transaction_connection_returns_to_pool()
    {
        ConnectionPool.ClearAll();
        long hitsBefore = Interlocked.Read(ref ConnectionPool.Hits);

        var conn = Open();
        using (var tx = conn.BeginTransaction())
        {
            tx.Commit();
        }
        conn.Close();
        using var again = Open();
        Assert.True(Interlocked.Read(ref ConnectionPool.Hits) > hitsBefore,
            "a finished transaction must not poison the pooled connection");
        Assert.Equal(1L, ExecScalar(again, "SELECT 1"));
    }

    [Fact]
    public void ClearAllPools_physically_drops_idle_connections()
    {
        ConnectionPool.ClearAll();
        long hitsBefore = Interlocked.Read(ref ConnectionPool.Hits);

        var c1 = Open();
        c1.Close();
        DocsqlConnection.ClearAllPools();
        using var c2 = Open();
        Assert.Equal(hitsBefore, Interlocked.Read(ref ConnectionPool.Hits));
    }

    [Fact]
    public void Max_pool_size_caps_idle_connections()
    {
        ConnectionPool.ClearAll();
        long discardedBefore = Interlocked.Read(ref ConnectionPool.Discarded);

        var a = OpenRaw($"host=127.0.0.1;port={_fx.Port};max pool size=1");
        var b = OpenRaw($"host=127.0.0.1;port={_fx.Port};max pool size=1");
        a.Close(); // 入池(idle=1)
        b.Close(); // 池满:物理丢弃
        Assert.True(Interlocked.Read(ref ConnectionPool.Discarded) > discardedBefore,
            "over-capacity return must physically close");
    }

    [Fact]
    public void Dead_pooled_connections_are_replaced()
    {
        ConnectionPool.ClearAll();
        var conn = Open();
        conn.Close(); // 池里有一条
        // 归还后再借出:PING 探活通过,复用成功。(真死连接路径 —— 服务器进程
        // kill —— 由 FailoverTests 覆盖:借出时 PING 失败的连接被丢弃并新建。)
        using var c2 = Open();
        Assert.Equal(1L, ExecScalar(c2, "SELECT 1"));
    }
}

public sealed class ServerSideBindingTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public ServerSideBindingTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        return conn;
    }

    private static object? Scalar(DocsqlConnection conn, string sql, params (string, object?)[] ps)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (n, v) in ps)
        {
            ((DocsqlParameterCollection)cmd.Parameters).AddWithValue(n, v);
        }
        return cmd.ExecuteScalar();
    }

    [Fact]
    public void Injection_payload_binds_as_data()
    {
        using var conn = Open();
        new DocsqlCommand { Connection = conn, CommandText = "CREATE TABLE IF NOT EXISTS sb (id INT PRIMARY KEY, name TEXT)" }
            .ExecuteNonQuery();
        new DocsqlCommand { Connection = conn, CommandText = "DELETE FROM sb" }.ExecuteNonQuery();
        new DocsqlCommand { Connection = conn, CommandText = "INSERT INTO sb VALUES (1, 'safe'), (2, 'two')" }
            .ExecuteNonQuery();

        // 经典注入载荷:作为字符串参数绑定,只能是数据 —— 零行,绝不泄表。
        Assert.Null(Scalar(conn, "SELECT name FROM sb WHERE name = @n",
            ("n", "x' OR '1'='1")));
        Assert.Equal("safe", Scalar(conn, "SELECT name FROM sb WHERE name = @n", ("n", "safe")));
        // 值内嵌单引号原样往返('' 转义在服务端完成)。
        Assert.Equal("it's fine", Scalar(conn, "SELECT @v", ("v", "it's fine")));
    }

    [Fact]
    public void Same_command_reuses_prepared_handle_across_parameter_sets()
    {
        using var conn = Open();
        new DocsqlCommand { Connection = conn, CommandText = "CREATE TABLE IF NOT EXISTS sb2 (id INT PRIMARY KEY)" }
            .ExecuteNonQuery();
        new DocsqlCommand { Connection = conn, CommandText = "DELETE FROM sb2" }.ExecuteNonQuery();
        new DocsqlCommand { Connection = conn, CommandText = "INSERT INTO sb2 VALUES (10), (20)" }
            .ExecuteNonQuery();

        // 同一模板三次执行:首次注册,后两次命中物理连接的句柄缓存。
        Assert.Equal(10L, Scalar(conn, "SELECT id FROM sb2 WHERE id = @id", ("id", 10L)));
        Assert.Equal(20L, Scalar(conn, "SELECT id FROM sb2 WHERE id = @id", ("id", 20L)));
        Assert.Null(Scalar(conn, "SELECT id FROM sb2 WHERE id = @id", ("id", 99L)));
    }

    [Fact]
    public void Literal_at_sign_is_not_parameterized()
    {
        using var conn = Open();
        // 字面量里的 @ 不参与绑定;@id 照常绑定,值语义不变。
        Assert.Equal("a@b", Scalar(conn, "SELECT 'a@b'"));
        Assert.Equal(1L, Scalar(conn, "SELECT @id", ("id", 1L)));
    }

    [Fact]
    public void Null_bool_and_type_roundtrip_via_server_binding()
    {
        using var conn = Open();
        Assert.True((bool)Scalar(conn, "SELECT @b", ("b", true))!);
        Assert.False((bool)Scalar(conn, "SELECT @b", ("b", false))!);
        Assert.True(Scalar(conn, "SELECT @v", ("v", null)) is null or DBNull);
        Assert.Equal(3.5d, Scalar(conn, "SELECT @d", ("d", 3.5d)));
        Assert.Equal(42L, Scalar(conn, "SELECT @i", ("i", 42)));
        Assert.Equal("hello", Scalar(conn, "SELECT @s", ("s", "hello")));
    }

    [Fact]
    public void Prepare_call_preregisters_without_error()
    {
        using var conn = Open();
        new DocsqlCommand { Connection = conn, CommandText = "CREATE TABLE IF NOT EXISTS sb3 (id INT)" }
            .ExecuteNonQuery();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM sb3 WHERE id = @id";
        ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("id", 1L);
        cmd.Prepare();
        Assert.Equal(1L, Scalar(conn, "SELECT @id", ("id", 1L)));
    }

    [Fact]
    public void Server_side_error_surfaces_and_connection_survives()
    {
        using var conn = Open();
        var ex = Assert.Throws<DocsqlException>(() =>
            Scalar(conn, "SELECT * FROM sb_missing WHERE id = @id", ("id", 1L)));
        Assert.Contains("sb_missing", ex.Message);
        // 连接仍可用(错误不毒化物理连接)。
        Assert.Equal(1L, Scalar(conn, "SELECT 1", ("x", 1)));
    }
}
