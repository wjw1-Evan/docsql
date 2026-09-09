using Docsql.Client;
using Xunit;

// GUID 时序主键(auto-generated UUIDv7)走 ADO.NET 客户端栈:
// 建表声明 GUID 类型 + AUTOINCREMENT,INSERT 省略 id / 显式 NULL 时由
// 服务端生成;显式值原样保留;事务内插入提交后可见、回滚后丢弃。
public sealed class GuidPrimaryKeyTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public GuidPrimaryKeyTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var c = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        c.Open();
        return c;
    }

    private static bool IsUuidV7(string s) =>
        s.Length == 36
        && s[14] == '7'
        && (s[19] == '8' || s[19] == '9' || s[19] == 'a' || s[19] == 'b')
        && System.Guid.TryParse(s, out _);

    [Fact]
    public void Auto_generated_guid_primary_key()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE gpk (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)";
        cmd.ExecuteNonQuery();

        // 省略 id 列:自动生成
        cmd.CommandText = "INSERT INTO gpk (v) VALUES ('a'), ('b')";
        Assert.Equal(2, cmd.ExecuteNonQuery());

        var ids = new List<string>();
        cmd.CommandText = "SELECT id FROM gpk ORDER BY id";
        using (var r = cmd.ExecuteReader())
        {
            while (r.Read()) ids.Add(r.GetString(0));
        }
        Assert.Equal(2, ids.Count);
        Assert.All(ids, id => Assert.True(IsUuidV7(id), $"not a UUIDv7: {id}"));
        // 字符串序 == 时间序(时序有序 GUID)
        Assert.True(string.CompareOrdinal(ids[0], ids[1]) < 0, "ids must sort in generation order");

        // 显式 NULL 同样自动生成;显式值原样保留
        cmd.CommandText =
            "INSERT INTO gpk (id, v) VALUES " +
            "('00000000-0000-7000-8000-000000000001', 'explicit'), (NULL, 'nulled')";
        Assert.Equal(2, cmd.ExecuteNonQuery());
        cmd.CommandText = "SELECT id FROM gpk WHERE v = 'explicit'";
        Assert.Equal("00000000-0000-7000-8000-000000000001", cmd.ExecuteScalar());
        cmd.CommandText = "SELECT id FROM gpk WHERE v = 'nulled'";
        Assert.True(IsUuidV7(Assert.IsType<string>(cmd.ExecuteScalar())));

        // 全零显式 guid(时间戳前缀最小)排在所有生成值之前
        cmd.CommandText = "SELECT v FROM gpk ORDER BY id LIMIT 1";
        Assert.Equal("explicit", cmd.ExecuteScalar());
    }

    [Fact]
    public void Auto_guid_inside_transaction()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE gtx (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT)";
        cmd.ExecuteNonQuery();

        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "INSERT INTO gtx (v) VALUES ('commit-me')";
            Assert.Equal(1, cmd.ExecuteNonQuery());
            tx.Commit();
        }
        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "INSERT INTO gtx (v) VALUES ('rollback-me')";
            Assert.Equal(1, cmd.ExecuteNonQuery());
            tx.Rollback();
        }

        cmd.CommandText = "SELECT id, v FROM gtx";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        var id = r.GetString(0);
        Assert.True(IsUuidV7(id), $"committed row lost or not UUIDv7: {id}");
        Assert.Equal("commit-me", r.GetString(1));
        Assert.False(r.Read(), "rolled-back row must not be visible");
    }
}
