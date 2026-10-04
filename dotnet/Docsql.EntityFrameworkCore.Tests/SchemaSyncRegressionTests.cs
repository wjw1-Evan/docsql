// 第四轮审查缺陷回归(EF 提供程序):
// 7) SyncIndexes 按「单实体」划定 IX_ 回收面 —— TPH/table-splitting 同表多
//    实体时,兄弟实体的索引被逐轮互 DROP(含 UNIQUE),且校验永远过不去、
//    每个上下文全量重同步。修复:SyncModel 先按表聚合全部映射实体的 wanted
//    索引集合,再逐表同步/回收。
// 9) SchemaVerify 只比列名子集,不核 PRIMARY KEY 存在性 —— 外部建的无 PK
//    同名表被判齐备,EF 的身份/唯一性保证静默失效。修复:校验补 PK 强制
//    形状判定(弱判定,与 SyncTable 同判据/门禁一致);同步侧走保数据重建
//    收敛(引擎无 ALTER ADD PRIMARY KEY)。
// EfServerFixture 在 EfTests.cs(全测试项目共用的进程启动辅助类);
// 本文件只引用它,不自行启动进程。

using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class SchemaSyncRegressionTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public SchemaSyncRegressionTests(EfServerFixture fx) => _fx = fx;
    private string Cs => $"host=127.0.0.1;port={_fx.Port}";

    private void Exec(string sql)
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private List<string> IndexNames(string table)
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '{table}'";
        using var reader = cmd.ExecuteReader();
        var names = new List<string>();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }
        return names;
    }

    private string IndexDdl(string index)
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = '{index}'";
        return (string)cmd.ExecuteScalar()!;
    }

    // ---- 7):TPH 同表兄弟实体的索引不被互 DROP ----

    [Fact]
    public void Tph_sibling_entity_indexes_survive_sync_and_verify_settles()
    {
        Exec("DROP TABLE IF EXISTS TphEvents");
        SchemaSyncAccounting.Reset(Cs);

        using (var db = new TphDb(Cs))
        {
            db.Clicks.Add(new TphClick { Alpha = "a" });
            db.Orders.Add(new TphOrder { Beta = "b" });
            db.SaveChanges();
        }
        var first = IndexNames("TphEvents").Where(n => n.StartsWith("IX_")).ToList();
        Assert.Equal(2, first.Count); // 两个兄弟实体的索引都建上了

        // 外部删一个索引制造漂移:下一上下文全量同步必须把它补回来,
        // 且不得把兄弟实体的另一个索引当「模型已移除」回收。
        var alpha = first.Single(n => n.EndsWith("Alpha", StringComparison.OrdinalIgnoreCase));
        Exec($"DROP INDEX \"{alpha}\"");
        using (var db = new TphDb(Cs))
        {
            Assert.Equal(2, db.Events.Count());
        }
        Assert.Equal(2, SchemaSyncAccounting.FullSyncCount(Cs));
        var after = IndexNames("TphEvents").Where(n => n.StartsWith("IX_")).ToList();
        Assert.Equal(2, after.Count); // 修复前:只剩最后同步的那个实体的索引

        // UNIQUE 索引也在聚合回收面里。
        var beta = after.Single(n => n.EndsWith("Beta", StringComparison.OrdinalIgnoreCase));
        Assert.StartsWith("CREATE UNIQUE INDEX", IndexDdl(beta).TrimStart());

        // 稳态:第三个上下文校验通过,不再全量同步。
        using (var db = new TphDb(Cs))
        {
            Assert.Equal(2, db.Events.Count());
        }
        Assert.Equal(2, SchemaSyncAccounting.FullSyncCount(Cs));
    }

    public abstract class TphEvent
    {
        public int Id { get; set; }
    }

    public class TphClick : TphEvent
    {
        public string? Alpha { get; set; }
    }

    public class TphOrder : TphEvent
    {
        public string? Beta { get; set; }
    }

    public class TphDb : DbContext
    {
        private readonly string _cs;
        public TphDb(string cs) => _cs = cs;
        public DbSet<TphClick> Clicks => Set<TphClick>();
        public DbSet<TphOrder> Orders => Set<TphOrder>();
        public IQueryable<TphEvent> Events => Set<TphEvent>();
        protected override void OnModelCreating(ModelBuilder b)
        {
            b.Entity<TphEvent>().ToTable("TphEvents");
            b.Entity<TphClick>().HasIndex(x => x.Alpha);
            b.Entity<TphOrder>().HasIndex(x => x.Beta).IsUnique();
        }
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    // ---- 9):外部无 PK 同名表不再被判齐备 ----

    [Fact]
    public void External_pkless_table_is_detected_rebuilt_and_unique_key_enforced()
    {
        Exec("DROP TABLE IF EXISTS PkItems");
        Exec("DROP TABLE IF EXISTS PkItems__efpksync");
        // 外部裸表:列名与模型一致,但完全没有 PRIMARY KEY。
        Exec("CREATE TABLE PkItems (\"Id\" INTEGER, \"Name\" TEXT)");
        Exec("INSERT INTO PkItems (\"Id\", \"Name\") VALUES (1, 'a'), (2, 'b')");
        // 手工索引:重建也不得丢(随 DROP TABLE 消失后须按原 DDL 重放)。
        Exec("CREATE INDEX pkitems_manual ON PkItems (\"Name\")");
        SchemaSyncAccounting.Reset(Cs);

        using (var db = new PkItemDb(Cs))
        {
            db.Items.Add(new PkItem { Name = "c" });
            db.SaveChanges();
        }
        // 校验看见「主键未强制」→ 全量同步 → 保数据重建收敛。
        Assert.Equal(1, SchemaSyncAccounting.FullSyncCount(Cs));

        // 主键列已按提供程序形状强制(NOT NULL PRIMARY KEY)。
        using (var conn = new DocsqlConnection(Cs))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT is_nullable FROM information_schema.columns " +
                "WHERE table_name = 'PkItems' AND column_name = 'Id'";
            Assert.Equal("NO", (string)cmd.ExecuteScalar()!);
        }

        using (var db = new PkItemDb(Cs))
        {
            var items = db.Items.OrderBy(i => i.Id).ToList();
            // 存量数据保留,自增键连续(重建拷贝显式 Id,新键 max+1)。
            Assert.Equal(new[] { 1, 2, 3 }, items.Select(i => i.Id).ToArray());
            Assert.Equal(new[] { "a", "b", "c" }, items.Select(i => i.Name).ToArray());
        }
        // 手工索引按原 DDL 重放。
        Assert.Contains(IndexNames("PkItems"), n => n == "pkitems_manual");

        // 稳态:校验通过,不再全量同步(门禁一致:校验失败 ⇒ 同步收敛)。
        using (var db = new PkItemDb(Cs))
        {
            Assert.Equal(3, db.Items.Count());
        }
        Assert.Equal(1, SchemaSyncAccounting.FullSyncCount(Cs));

        // 主键唯一性真的被引擎强制:重复 Id 响亮失败。
        using (var db = new PkItemDb(Cs))
        {
            db.Items.Add(new PkItem { Id = 1, Name = "dup" });
            Assert.Throws<DbUpdateException>(() => db.SaveChanges());
        }
    }

    public class PkItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class PkItemDb : DbContext
    {
        private readonly string _cs;
        public PkItemDb(string cs) => _cs = cs;
        public DbSet<PkItem> Items => Set<PkItem>();
        protected override void OnModelCreating(ModelBuilder b) =>
            b.Entity<PkItem>().ToTable("PkItems");
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }
}
