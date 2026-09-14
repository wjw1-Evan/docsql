// 实体集合属性(List<T>)与字典属性(Dictionary<string,object>)的映射与查询:
// - List<T> 走 EF primitive collection(JSON 数组文本列),Contains 由提供程序翻译为
//   引擎 JSON_ARRAY_CONTAINS,留在服务端执行(常量、跨列、取反、数值元素);
// - Dictionary<string,object> 由提供程序约定映射为 JSON 文本标量(读写、变更跟踪),
//   字典内部成员不参与 SQL 翻译。

using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public class CollTeam
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public List<string> RoleIds { get; set; } = new();
    public List<int> Numbers { get; set; } = new();
    public Dictionary<string, object> Payload { get; set; } = new();
}

public class CollPermission
{
    public string Id { get; set; } = "";
    public string Note { get; set; } = "";
}

public class CollDb : DbContext
{
    private readonly string _cs;
    public CollDb(string cs) => _cs = cs;
    public DbSet<CollTeam> Teams => Set<CollTeam>();
    public DbSet<CollPermission> Permissions => Set<CollPermission>();
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public sealed class CollectionMappingTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public CollectionMappingTests(EfServerFixture fx) => _fx = fx;
    private string Cs => $"host=127.0.0.1;port={_fx.Port}";

    private void Clean()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        foreach (var t in new[] { "Teams", "Permissions" })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {t}";
            cmd.ExecuteNonQuery();
        }
    }

    [Fact]
    public void Collection_and_dictionary_properties_roundtrip()
    {
        Clean();
        using (var db = new CollDb(Cs))
        {
            db.Teams.Add(new CollTeam
            {
                Name = "a",
                RoleIds = ["admin", "user"],
                Numbers = [1, 2, 3],
                Payload = new Dictionary<string, object>
                {
                    ["n"] = 42L,
                    ["s"] = "v",
                    ["b"] = true,
                    ["nested"] = new Dictionary<string, object> { ["k"] = "x" },
                    ["list"] = new List<object?> { 1L, "two", null },
                },
            });
            db.Teams.Add(new CollTeam { Name = "b", RoleIds = [], Numbers = [] });
            db.SaveChanges();
        }
        using (var db = new CollDb(Cs))
        {
            var a = db.Teams.Single(t => t.Name == "a");
            Assert.Equal(new[] { "admin", "user" }, a.RoleIds);
            Assert.Equal(new[] { 1, 2, 3 }, a.Numbers);
            Assert.Equal(42L, a.Payload["n"]);
            Assert.Equal("v", a.Payload["s"]);
            Assert.Equal(true, a.Payload["b"]);
            Assert.Equal("x", ((Dictionary<string, object>)a.Payload["nested"])["k"]);
            var list = Assert.IsType<List<object?>>(a.Payload["list"]);
            Assert.Equal(new object?[] { 1L, "two", null }, list);
            // 空集合与缺失区分:空 List 往返为空列表。
            Assert.Empty(db.Teams.Single(t => t.Name == "b").RoleIds);
        }
    }

    [Fact]
    public void Dictionary_mutation_is_tracked_and_persisted()
    {
        Clean();
        using (var db = new CollDb(Cs))
        {
            db.Teams.Add(new CollTeam
            {
                Name = "mut",
                Payload = new Dictionary<string, object> { ["k"] = 1L },
            });
            db.SaveChanges();
        }
        using (var db = new CollDb(Cs))
        {
            var t = db.Teams.Single(x => x.Name == "mut");
            t.Payload["k"] = 2L;          // 原地改写必须被 ValueComparer 快照捕获
            t.Payload["added"] = "yes";
            db.SaveChanges();
        }
        using (var db = new CollDb(Cs))
        {
            var t = db.Teams.Single(x => x.Name == "mut");
            Assert.Equal(2L, t.Payload["k"]);
            Assert.Equal("yes", t.Payload["added"]);
        }
    }

    [Fact]
    public void Collection_contains_translates_server_side()
    {
        Clean();
        using var db = new CollDb(Cs);
        db.Teams.Add(new CollTeam { Name = "a", RoleIds = ["admin", "user"], Numbers = [1, 2] });
        db.Teams.Add(new CollTeam { Name = "b", RoleIds = ["user"], Numbers = [2, 3] });
        db.Teams.Add(new CollTeam { Name = "c", RoleIds = [], Numbers = [] });
        db.Permissions.Add(new CollPermission { Id = "admin", Note = "p" });
        db.Permissions.Add(new CollPermission { Id = "other", Note = "p" });
        db.SaveChanges();

        // 常量元素。
        Assert.Equal(
            new[] { "a" },
            db.Teams.Where(t => t.RoleIds.Contains("admin")).Select(t => t.Name).ToList());
        // 取反 + 数值集合。
        Assert.Equal(
            new[] { "b", "c" },
            db.Teams.Where(t => !t.RoleIds.Contains("admin")).OrderBy(t => t.Name)
                .Select(t => t.Name).ToList());
        Assert.Equal(
            new[] { "a", "b" },
            db.Teams.Where(t => t.Numbers.Contains(2)).OrderBy(t => t.Name)
                .Select(t => t.Name).ToList());
        // 空集合不匹配任何元素。
        Assert.Empty(db.Teams.Where(t => t.RoleIds.Contains("missing")).ToList());

        // 权限核心查询形态:集合属性 Contains 另一个实体的列(翻译成列对列函数)。
        var hit = (from t in db.Teams
                   from p in db.Permissions
                   where t.RoleIds.Contains(p.Id) && p.Note == "p"
                   orderby t.Name
                   select t.Name).ToList();
        Assert.Equal(new[] { "a" }, hit); // admin 命中 a;other 不命中任何队
        var noHit = (from t in db.Teams
                     from p in db.Permissions
                     where t.RoleIds.Contains(p.Id) && p.Id == "none"
                     select t.Name).ToList();
        Assert.Empty(noHit);
    }
}
