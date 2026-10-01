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
            ArgumentList = { tmp, $"127.0.0.1:{Port}" },
            CreateNoWindow = true,
            RedirectStandardError = false,
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
    public void Marker_shaped_user_objects_stay_raw_unless_exact()
    {
        // Symmetry with the server's decode_marker (and the Python hook): a
        // marker counts only when the object holds EXACTLY that one key and
        // the value parses. The reader used to probe single properties on
        // ANY object — multi-key documents were silently retyped (decimal
        // instead of the stored JSON), and an unparseable value threw out
        // of the reader constructor, failing the whole result set.
        using var conn = Open();
        using var cmd = conn.CreateCommand();

        // Exact single-key marker still decodes (the pinned positive path).
        cmd.CommandText = "SELECT JSON_EXTRACT('{\"$dec\":\"1.5\"}', '$')";
        using (var r = cmd.ExecuteReader())
        {
            Assert.True(r.Read());
            Assert.Equal(1.5m, r.GetValue(0));
        }

        // Multi-key: a user document that merely CONTAINS a $dec field.
        cmd.CommandText = "SELECT JSON_EXTRACT('{\"$dec\":\"1.5\",\"limit\":10}', '$')";
        using (var r = cmd.ExecuteReader())
        {
            Assert.True(r.Read());
            var raw = r.GetString(0);
            Assert.Contains("$dec", raw);
            Assert.Contains("limit", raw);
        }

        // Single key, unparseable value: raw text, never an exception.
        cmd.CommandText = "SELECT JSON_EXTRACT('{\"$dec\":\"nope\"}', '$')";
        using (var r = cmd.ExecuteReader())
        {
            Assert.True(r.Read());
            Assert.Contains("nope", r.GetString(0));
        }

        // Out-of-domain $ts stays raw (FromUnixTimeMilliseconds would throw).
        cmd.CommandText = "SELECT JSON_EXTRACT('{\"$ts\":999999999999999}', '$')";
        using (var r = cmd.ExecuteReader())
        {
            Assert.True(r.Read());
            Assert.Contains("$ts", r.GetString(0));
        }
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

    [Fact]
    public void Parameterized_update_and_delete_report_affected_rows()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS upd (id INT, name TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM upd";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO upd VALUES (1, 'a'), (2, 'b'), (3, 'c')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "UPDATE upd SET name = @n WHERE id = @id";
        var p = (DocsqlParameterCollection)cmd.Parameters;
        p.AddWithValue("n", "changed");
        p.AddWithValue("id", 2L);
        Assert.Equal(1, cmd.ExecuteNonQuery());
        cmd.Parameters.Clear();
        p.AddWithValue("id", 99L);
        p.AddWithValue("n", "x");
        Assert.Equal(0, cmd.ExecuteNonQuery());

        cmd.CommandText = "SELECT name FROM upd WHERE id = 2";
        Assert.Equal("changed", cmd.ExecuteScalar());

        cmd.CommandText = "DELETE FROM upd WHERE id = @id";
        cmd.Parameters.Clear();
        p.AddWithValue("id", 1L);
        Assert.Equal(1, cmd.ExecuteNonQuery());
        cmd.Parameters.Clear();
        p.AddWithValue("id", 1L);
        Assert.Equal(0, cmd.ExecuteNonQuery());
        cmd.CommandText = "SELECT COUNT(id) FROM upd";
        Assert.Equal(2L, cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Async_apis_roundtrip()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS async_t (id INT, v TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "DELETE FROM async_t";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO async_t VALUES (1, 'x'), (2, 'y')";
        Assert.Equal(2, await cmd.ExecuteNonQueryAsync());

        cmd.CommandText = "SELECT COUNT(id) FROM async_t";
        Assert.Equal(2L, await cmd.ExecuteScalarAsync());

        cmd.CommandText = "SELECT id, v FROM async_t WHERE id = 1";
        using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("x", reader.GetString(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public void Reader_handles_ordinals_and_dbnull()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS ord_t (id INT, v TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM ord_t";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ord_t VALUES (7, NULL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT id, v FROM ord_t";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0, reader.GetOrdinal("id"));
        Assert.Equal(1, reader.GetOrdinal("v"));
        Assert.True(reader.IsDBNull(1));
        Assert.False(reader.IsDBNull(0));
        Assert.Equal(System.DBNull.Value, reader.GetValue(1));
        Assert.False(reader.NextResult()); // single result set
    }

    [Fact]
    public void GetFieldValue_roundtrips_timespan_and_datetimeoffset()
    {
        // EF 映射把 TimeSpan 参数写成 "c" 文本、DateTimeOffset 走 $ts(UTC 毫秒);
        // 此前 GetFieldValue 缺这两个分支,落到 Convert.ChangeType 直接抛
        // "写得进、读不出"(string→TimeSpan / DateTime→DateTimeOffset 不支持)。
        using var conn = Open();
        using var cmd = (DocsqlCommand)conn.CreateCommand();
        var ts = new TimeSpan(1, 2, 3, 4, 567);
        var dto = new DateTimeOffset(2026, 9, 29, 8, 30, 5, TimeSpan.FromHours(8));
        cmd.CommandText = "SELECT @ts, @dto";
        cmd.Parameters.AddWithValue("ts", ts);
        cmd.Parameters.AddWithValue("dto", dto);
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(ts, reader.GetFieldValue<TimeSpan>(0));
        // $ts 归一化 UTC:读回偏移为零,比较 UtcDateTime(毫秒精度)。
        var back = reader.GetFieldValue<DateTimeOffset>(1);
        Assert.Equal(dto.UtcDateTime, back.UtcDateTime);
        Assert.Equal(TimeSpan.Zero, back.Offset);
    }

    [Fact]
    public void Typed_getters_throw_on_null_instead_of_silent_defaults()
    {
        // ADO.NET 契约:强类型 getter 对 NULL 抛 InvalidCastException;
        // 静默 0/false/null 会把漏判 IsDBNull 的调用方变成错数据。
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS null_t (n INT, s TEXT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM null_t";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO null_t VALUES (NULL, NULL)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT n, s FROM null_t";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Throws<InvalidCastException>(() => reader.GetInt64(0));
        Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
        Assert.Throws<InvalidCastException>(() => reader.GetDouble(0));
        Assert.Throws<InvalidCastException>(() => reader.GetBoolean(0));
        Assert.Throws<InvalidCastException>(() => reader.GetString(1));
        // GetValue/IsDBNull 契约不变:NULL 仍是 DBNull,预检路径可用。
        Assert.True(reader.IsDBNull(0));
        Assert.Equal(System.DBNull.Value, reader.GetValue(0));
    }

    [Fact]
    public void GetOrdinal_falls_back_to_case_insensitive_match()
    {
        // ADO.NET 惯例:精确匹配失败后大小写不敏感回退。
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 AS MixedCase";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0, reader.GetOrdinal("MixedCase"));
        Assert.Equal(0, reader.GetOrdinal("mixedcase"));
        Assert.Equal(0, reader.GetOrdinal("MIXEDCASE"));
        Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("nope"));
    }
    [Fact]
    public void Typed_getters_throw_on_null_decimal_datetime_and_narrow_types()
    {
        // GetDecimal/GetDateTime/GetInt16/GetByte/GetChar/GetFloat 曾静默返回
        // 0m/DateTime.MinValue/0(漏掉 NonNull 包装,漏判 IsDBNull 的调用方拿到
        // 错数据而非异常)。
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE IF NOT EXISTS null_wide (d DECIMAL, ts TIMESTAMP, i INT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DELETE FROM null_wide";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO null_wide VALUES (NULL, NULL, NULL)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "SELECT d, ts, i FROM null_wide";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Throws<InvalidCastException>(() => reader.GetDecimal(0));
        Assert.Throws<InvalidCastException>(() => reader.GetDateTime(1));
        Assert.Throws<InvalidCastException>(() => reader.GetInt16(2));
        Assert.Throws<InvalidCastException>(() => reader.GetByte(2));
        Assert.Throws<InvalidCastException>(() => reader.GetChar(2));
        Assert.Throws<InvalidCastException>(() => reader.GetFloat(2));
    }

    [Fact]
    public void Dec_marker_with_thousands_separator_stays_raw()
    {
        // {"$dec":"1,234"} 是合法的单键文档:rust_decimal 与 Python Decimal 都
        // 拒收千分位 —— .NET 若用 NumberStyles.Number(AllowThousands)解析成
        // 1234m 即三方不对称(同文档在 .NET 是 decimal、Python 是 dict)。
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT JSON_EXTRACT('{\"$dec\":\"1,234\"}', '$')";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.False(reader.GetValue(0) is decimal, $"must not re-type: {reader.GetValue(0)}");
        // 对照:无千分位仍解码为 decimal。
        cmd.CommandText = "SELECT JSON_EXTRACT('{\"$dec\":\"1234.5\"}', '$')";
        using var reader2 = cmd.ExecuteReader();
        Assert.True(reader2.Read());
        Assert.Equal(1234.5m, reader2.GetDecimal(0));
    }

    [Fact]
    public void Close_mid_transaction_does_not_leak_into_reopened_connection()
    {
        var conn = Open();
        var tx = conn.BeginTransaction();
        conn.Close();
        // 重开租到新物理连接:陈旧事务对象必须拒绝(它的连接已断,事务已在
        // 服务端回滚),且不得触碰新连接上的任何事务。
        conn.Open();
        Assert.Throws<InvalidOperationException>(() => tx.Commit());
        var tx2 = conn.BeginTransaction();
        tx2.Commit();
        // InTransaction 已随 Close 清零:再次 Close 走归还而不是物理丢弃。
        conn.Close();
        conn.Dispose();
    }

    [Fact]
    public void Pooled_connection_resets_tsql_session_state()
    {
        // A 借出者留下 @变量;B 借到同一条物理连接必须拿到干净会话 —— 否则
        // 同名 DECLARE 报 "already been declared"、@@IDENTITY 读到别人的值。
        var a = Open();
        using (a)
        {
            using var cmd = a.CreateCommand();
            cmd.CommandText = "DECLARE @marker INT = 42";
            cmd.ExecuteNonQuery();
        }
        var b = Open();
        using (b)
        {
            using var cmd = b.CreateCommand();
            cmd.CommandText = "DECLARE @marker INT = 7";
            cmd.ExecuteNonQuery(); // 同名再声明:会话已重置,必须成功
        }
    }
}
