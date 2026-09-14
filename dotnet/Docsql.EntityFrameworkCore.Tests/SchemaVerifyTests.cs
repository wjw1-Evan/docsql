using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

// U1:schema 校验摊销 —— 新上下文先做两条一次性校验查询,只有缺表/缺列/
// 缺索引(含外部 DDL)才回落整场同步;重复上下文不再数百次往返。
public sealed class SchemaVerifyTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public SchemaVerifyTests(EfServerFixture fx) => _fx = fx;
    private string Cs => $"host=127.0.0.1;port={_fx.Port}";

    private void Exec(string sql)
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void Repeated_contexts_skip_full_sync_and_external_drop_triggers_resync()
    {
        Exec("DROP TABLE IF EXISTS Notes");
        SchemaSyncAccounting.Reset(Cs);

        using (var db = new VerifyNoteDb(Cs))
        {
            db.Notes.Add(new VerifyNote { Text = "a" });
            db.SaveChanges();
        }
        Assert.Equal(1, SchemaSyncAccounting.FullSyncCount(Cs));

        // 第二个上下文:校验通过 → 不再全量同步(仅两条校验查询)
        using (var db = new VerifyNoteDb(Cs))
        {
            Assert.Equal(1, db.Notes.Count());
        }
        Assert.Equal(1, SchemaSyncAccounting.FullSyncCount(Cs));

        // 外部删表(绕过 EF 拦截器):下一个上下文校验发现,全量重建
        Exec("DROP TABLE Notes");
        using (var db = new VerifyNoteDb(Cs))
        {
            Assert.Empty(db.Notes.ToList());
        }
        Assert.Equal(2, SchemaSyncAccounting.FullSyncCount(Cs));
    }

    [Fact]
    public void Missing_column_is_detected_and_backfilled()
    {
        Exec("DROP TABLE IF EXISTS Items");
        Exec("CREATE TABLE Items (\"Id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"Name\" TEXT)");
        SchemaSyncAccounting.Reset(Cs);

        using (var db = new VerifyItemDb(Cs))
        {
            db.Items.Add(new VerifyItem { Name = "n", Tag = "t" });
            db.SaveChanges();
        }
        // 旧表缺 Tag 列 → 校验失败 → 全量同步补列
        Assert.Equal(1, SchemaSyncAccounting.FullSyncCount(Cs));
        using (var db = new VerifyItemDb(Cs))
        {
            Assert.Equal("t", db.Items.Single().Tag);
        }
        Assert.Equal(1, SchemaSyncAccounting.FullSyncCount(Cs));
    }

    [Fact]
    public void Missing_index_is_detected_and_recreated()
    {
        Exec("DROP TABLE IF EXISTS Rows");
        SchemaSyncAccounting.Reset(Cs);

        using (var db = new VerifyIndexedDb(Cs))
        {
            db.Rows.Add(new VerifyIndexed { Email = "a@x" });
            db.SaveChanges();
        }
        Assert.Equal(1, SchemaSyncAccounting.FullSyncCount(Cs));

        string indexName;
        using (var conn = new DocsqlConnection(Cs))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'Rows'";
            indexName = (string)cmd.ExecuteScalar()!;
            Assert.StartsWith("IX_", indexName);
            using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP INDEX {indexName}";
            drop.ExecuteNonQuery();
        }

        // 缺索引 → 校验失败 → 全量同步补索引(数据保留)
        using (var db = new VerifyIndexedDb(Cs))
        {
            Assert.Equal(1, db.Rows.Count(r => r.Email == "a@x"));
        }
        Assert.Equal(2, SchemaSyncAccounting.FullSyncCount(Cs));
    }

    public class VerifyNote
    {
        public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    public class VerifyNoteDb : DbContext
    {
        private readonly string _cs;
        public VerifyNoteDb(string cs) => _cs = cs;
        public DbSet<VerifyNote> Notes => Set<VerifyNote>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    public class VerifyItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string? Tag { get; set; }
    }

    public class VerifyItemDb : DbContext
    {
        private readonly string _cs;
        public VerifyItemDb(string cs) => _cs = cs;
        public DbSet<VerifyItem> Items => Set<VerifyItem>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    [Index(nameof(Email))]
    public class VerifyIndexed
    {
        public int Id { get; set; }
        public string Email { get; set; } = "";
    }

    public class VerifyIndexedDb : DbContext
    {
        private readonly string _cs;
        public VerifyIndexedDb(string cs) => _cs = cs;
        public DbSet<VerifyIndexed> Rows => Set<VerifyIndexed>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }
}
