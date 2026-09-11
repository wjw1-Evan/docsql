// 客户端 ADO.NET 面契约测试:事务 Dispose 回滚、查询/非查询互斥、空结果
// 标量、参数 CLR 类型字面量化与往返、byte[] 拒绝、长语句不截断、
// SchemaTable、DbProviderFactory 与连接串解析。复用 AdoNetTests 的
// 无凭据 ServerFixture,每个用例独立表名。

using Docsql.Client;
using Xunit;

public sealed class ClientSurfaceTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public ClientSurfaceTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var c = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        c.Open();
        return c;
    }

    private object Scalar(string sql)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar()!;
    }

    [Fact]
    public void Dispose_without_commit_rolls_back_and_releases_the_server_transaction()
    {
        using (var conn = Open())
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE IF NOT EXISTS csurf_tx (v INT)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "DELETE FROM csurf_tx";
            cmd.ExecuteNonQuery();
        }

        // using 里只插入不提交:Dispose 应当回滚(ADO.NET 契约)。服务端是
        // 单全局事务,泄漏的打开事务会让后续 BEGIN 排队超时 —— 本用例结尾
        // 再开一个事务即证明服务端事务确已关闭。
        using (var conn = Open())
        {
            using (var tx = conn.BeginTransaction())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO csurf_tx VALUES (1)";
                Assert.Equal(1, cmd.ExecuteNonQuery());
                // 不提交,作用域结束触发 Dispose 回滚
            }
        }

        Assert.Equal(0L, Scalar("SELECT COUNT(v) FROM csurf_tx"));

        using (var conn = Open())
        using (conn.BeginTransaction())
        {
            // 能立刻再开事务 = 上一个事务已释放
        }
    }

    [Fact]
    public void ExecuteNonQuery_rejects_statements_that_return_rows()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var ex = Assert.Throws<DocsqlException>(() => cmd.ExecuteNonQuery());
        Assert.Contains("returned rows", ex.Message);
    }

    [Fact]
    public void ExecuteScalar_returns_null_when_no_rows()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS csurf_empty (v INT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM csurf_empty";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT v FROM csurf_empty";
        Assert.Null(cmd.ExecuteScalar());
    }

    [Fact]
    public void GetOrdinal_unknown_column_throws()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 AS known";
        using var r = cmd.ExecuteReader();
        Assert.Equal(0, r.GetOrdinal("known"));
        Assert.Throws<IndexOutOfRangeException>(() => r.GetOrdinal("missing"));
    }

    [Fact]
    public void Schema_table_and_GetValues_describe_columns()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS csurf_schema (id INT, name TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM csurf_schema";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO csurf_schema VALUES (1, NULL), (2, 'x')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT id, name FROM csurf_schema ORDER BY id";
        using var r = cmd.ExecuteReader();
        var schema = r.GetSchemaTable();
        var rows = schema!.Rows.Cast<System.Data.DataRow>().ToList();
        Assert.Equal(2, rows.Count);
        var id = rows[0];
        Assert.Equal("id", id["ColumnName"]);
        Assert.Equal(0, id["ColumnOrdinal"]);
        Assert.Equal(typeof(long), id["DataType"]);
        Assert.Equal(false, id["AllowDBNull"]);
        var name = rows[1];
        Assert.Equal("name", name["ColumnName"]);
        Assert.Equal(1, name["ColumnOrdinal"]);
        Assert.Equal(typeof(string), name["DataType"]);
        Assert.Equal(true, name["AllowDBNull"]);

        // GetValues 契约:NULL 列填 DBNull.Value,返回读取的列数
        Assert.True(r.Read());
        var values = new object[2];
        Assert.Equal(2, r.GetValues(values));
        Assert.Equal(1L, values[0]);
        Assert.Equal(DBNull.Value, values[1]);
    }

    [Fact]
    public void Parameter_literals_round_trip_by_clr_type()
    {
        using var conn = Open();

        // bool → TRUE/FALSE 字面量,读回 bool
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT @b";
            ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("b", true);
            Assert.Equal(true, cmd.ExecuteScalar());
            cmd.Parameters.Clear();
            ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("b", false);
            Assert.Equal(false, cmd.ExecuteScalar());
        }

        // 整数 / 浮点 / NULL(每次都要重设 CommandText 并清空参数)
        using (var cmd = conn.CreateCommand())
        {
            var p = (DocsqlParameterCollection)cmd.Parameters;
            cmd.CommandText = "SELECT @i";
            p.AddWithValue("i", 42L);
            Assert.Equal(42L, cmd.ExecuteScalar());
            cmd.Parameters.Clear();
            cmd.CommandText = "SELECT @d";
            p.AddWithValue("d", 3.5d);
            Assert.Equal(3.5d, cmd.ExecuteScalar());
            cmd.Parameters.Clear();
            cmd.CommandText = "SELECT @n";
            p.AddWithValue("n", null);
            Assert.Equal(DBNull.Value, cmd.ExecuteScalar());
        }

        // decimal:按不变文化内联,读回精度不丢
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT @m";
            ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("m", 1234.56m);
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(1234.56m, r.GetDecimal(0));
        }

        // DateTime:不变文化 "O" 文本往返,GetDateTime 可解析
        var dt = new DateTime(2026, 9, 10, 12, 34, 56, 789);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT @t";
            ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("t", dt);
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(dt, r.GetDateTime(0));
        }
    }

    [Fact]
    public void Byte_array_parameters_are_rejected_loudly()
    {
        // 引擎无 BLOB 存储:静默 ToString 会写坏数据,必须显式拒绝
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT @b";
        ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("b", new byte[] { 1, 2 });
        Assert.Throws<NotSupportedException>(() => cmd.ExecuteScalar());
    }

    [Fact]
    public void Long_statements_round_trip_without_truncation()
    {
        // 回归:服务端曾在执行路径截 512 字符,长文档 INSERT 被无声截断。
        // 文档本体有 ~4KB 存储上限,取 3KB ASCII:语句与值都远超 512。
        var payload = "{\"doc\":\"" + new string('x', 3000) + "\"}";
        Assert.True(payload.Length > 512);

        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS csurf_big (id INT, doc TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM csurf_big";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO csurf_big (id, doc) VALUES (1, @doc)";
        ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("doc", payload);
        Assert.Equal(1, cmd.ExecuteNonQuery());

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT doc FROM csurf_big WHERE id = 1";
        Assert.Equal(payload, cmd.ExecuteScalar());

        // 超过旧 4KB 单页上限的文档走溢出页链存储:无损往返而非报错
        // (上限为 16MB,超过才显式报错而不是静默截断)
        var bigOverflow = new string('y', 5000);
        cmd.CommandText = "INSERT INTO csurf_big (id, doc) VALUES (2, @doc)";
        ((DocsqlParameterCollection)cmd.Parameters).AddWithValue("doc", bigOverflow);
        Assert.Equal(1, cmd.ExecuteNonQuery());
        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT doc FROM csurf_big WHERE id = 2";
        Assert.Equal(bigOverflow, cmd.ExecuteScalar());
    }

    [Fact]
    public void Factory_creates_all_adonet_surfaces_and_builder_parses()
    {
        var f = DocsqlFactory.Instance;
        using var c = f.CreateConnection();
        Assert.IsType<DocsqlConnection>(c);
        using var cmd = f.CreateCommand();
        Assert.IsType<DocsqlCommand>(cmd);
        var p = f.CreateParameter();
        Assert.IsType<DocsqlParameter>(p);
        var b = Assert.IsType<DocsqlConnectionStringBuilder>(f.CreateConnectionStringBuilder());

        // 缺省值
        Assert.Equal("127.0.0.1", b.Host);
        Assert.Equal(7600, b.Port);
        Assert.Equal("", b.Token);
        Assert.Equal("", b.Key);

        // 解析往返
        var key = new string('a', 64);
        var full = new DocsqlConnectionStringBuilder
        {
            ConnectionString = $"host=example;port=1234;token=tok;key={key}",
        };
        Assert.Equal("example", full.Host);
        Assert.Equal(1234, full.Port);
        Assert.Equal("tok", full.Token);
        Assert.Equal(key, full.Key);
    }
}
