using System.Diagnostics;
using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Xunit;

[Index(nameof(Email), IsUnique = true)]
[Index(nameof(Label))]
public class Member
{
    public int Id { get; set; }
    public string Email { get; set; } = "";
    public string Label { get; set; } = "";
}

public class MemberDb : DbContext
{
    private readonly string _cs;
    public MemberDb(string cs) => _cs = cs;
    public DbSet<Member> Members => Set<Member>();
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

// v2 模型:去掉唯一 Email 索引(保留 Label 索引),验证双向同步会回收索引并解除约束。
public class MemberV2
{
    public int Id { get; set; }
    public string Email { get; set; } = "";
    public string Label { get; set; } = "";
}

public class MemberDbV2 : DbContext
{
    private readonly string _cs;
    public MemberDbV2(string cs) => _cs = cs;
    public DbSet<MemberV2> MembersV2 => Set<MemberV2>();
    protected override void OnModelCreating(ModelBuilder b) =>
        // 模型演进时同名索引跨版本对齐(默认名会随实体类型名变化)
        b.Entity<MemberV2>().ToTable("Members")
            .HasIndex(m => m.Label).HasDatabaseName("IX_Members_Label");
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public sealed class EfServerFixture : IDisposable
{
    public int Port { get; }
    private readonly System.Diagnostics.Process _proc;

    public EfServerFixture()
    {
        // 系统分配空闲端口,避免与其他并行测试类随机端口碰撞。
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        Port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var tmp = Path.Combine(Path.GetTempPath(), $"docsql-ef-{Port}.db");
        try { File.Delete(tmp); } catch { }
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        _proc = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
        {
            FileName = exe, ArgumentList = { tmp, $"127.0.0.1:{Port}" },
            CreateNoWindow = true, RedirectStandardError = false,
        })!;
        for (int i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", Port); return; }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException("server did not start");
    }

    public void Dispose()
    {
        try { _proc.Kill(); } catch { }
        _proc.Dispose();
    }
}

public class Blog
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public List<Post> Posts { get; } = new();
}

public class Post
{
    public int Id { get; set; }
    public string Content { get; set; } = "";
    public int BlogId { get; set; }
    public Blog Blog { get; set; } = null!;
}

public class AppDb : DbContext
{
    private readonly string _cs;
    public AppDb(string cs) => _cs = cs;
    public DbSet<Blog> Blogs => Set<Blog>();
    public DbSet<Post> Posts => Set<Post>();
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

// v2 模型:同一张 Blogs 表,模拟"模型加了字段"的演进场景。
public class BlogV2
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public int? Rating { get; set; }
}

public class AppDbV2 : DbContext
{
    private readonly string _cs;
    public AppDbV2(string cs) => _cs = cs;
    public DbSet<BlogV2> BlogsV2 => Set<BlogV2>();
    protected override void OnModelCreating(ModelBuilder b) =>
        b.Entity<BlogV2>().ToTable("Blogs");
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public sealed class EfExtraTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public EfExtraTests(EfServerFixture fx) => _fx = fx;
    private string Cs => $"host=127.0.0.1;port={_fx.Port}";

    [Fact]
    public void Database_Migrate_is_unsupported_and_fails_explicitly()
    {
        // DocSQL 的建表走 EnsureCreated/AutoCreate/SchemaSync,不提供迁移
        // 管线;Migrate() 必须显式报错并指向替代方案,而非生成方言外的 SQL。
        using var db = new TagDb(Cs);
        var ex = Assert.ThrowsAny<NotSupportedException>(() => db.Database.Migrate());
        Assert.Contains("EnsureCreated", ex.Message);
    }

    [Fact]
    public void String_primary_key_entity_roundtrips()
    {
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS Tags";
        cmd.ExecuteNonQuery();

        using (var db = new TagDb($"host=127.0.0.1;port={_fx.Port}"))
        {
            db.Tags.Add(new Tag { Id = "redis", Note = "缓存" });
            db.Tags.Add(new Tag { Id = "docsql", Note = "文档库" });
            db.SaveChanges();
        }
        using (var db = new TagDb($"host=127.0.0.1;port={_fx.Port}"))
        {
            Assert.Equal("文档库", db.Tags.Find("docsql")!.Note);
            Assert.Equal(2, db.Tags.Count(t => t.Id != ""));
        }
    }

    [Fact]
    public void Common_clr_types_roundtrip()
    {
        using (var db = new TypeDb($"host=127.0.0.1;port={_fx.Port}"))
        {
            db.Add(new TypedRow
            {
                Flag = true, Score = 3.5, Amount = 12.34m,
                At = new DateTime(2026, 9, 7, 12, 0, 0), Name = "typed",
            });
            db.SaveChanges();
        }
        using (var db = new TypeDb($"host=127.0.0.1;port={_fx.Port}"))
        {
            var row = db.TypedRows.Single(r => r.Name == "typed");
            Assert.True(row.Flag);
            Assert.Equal(3.5, row.Score);
            Assert.Equal(12.34m, row.Amount);
            Assert.Equal(new DateTime(2026, 9, 7, 12, 0, 0), row.At);
        }
    }

    [Fact]
    public void Table_name_requiring_quotes_works()
    {
        using (var db = new OrderDb($"host=127.0.0.1;port={_fx.Port}"))
        {
            db.OrderLines.Add(new OrderLine { Sku = "A-1", Qty = 3 });
            db.SaveChanges();
            Assert.Equal(3, db.OrderLines.Single(l => l.Sku == "A-1").Qty);
        }
    }

    [Fact]
    public void Concurrent_contexts_insert_and_lazy_create()
    {
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS Counters";
        cmd.ExecuteNonQuery();

        var cs = $"host=127.0.0.1;port={_fx.Port}";
        var threads = Enumerable.Range(0, 4).Select(i => new Thread(() =>
        {
            using var db = new CounterDb(cs);
            for (var n = 0; n < 10; n++)
                db.Counters.Add(new Counter { Label = $"t{i}-{n}" });
            db.SaveChanges();
        })).ToList();
        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        using var check = new CounterDb(cs);
        Assert.Equal(40, check.Counters.Count());
    }

    [Fact]
    public void Index_attribute_creates_unique_and_plain_indexes()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "DROP TABLE IF EXISTS Members";
            cmd.ExecuteNonQuery();
        }

        // [Index] 特性:首次访问即建索引,唯一索引由引擎强制
        using (var db = new MemberDb(Cs))
        {
            db.Members.Add(new Member { Email = "a@x", Label = "vip" });
            db.Members.Add(new Member { Email = "b@x", Label = "vip" });
            db.SaveChanges();
            // 同 Label 允许重复(非唯一索引),同 Email 不允许
            db.Members.Add(new Member { Email = "c@x", Label = "vip" });
            db.SaveChanges();
            Assert.Equal(3, db.Members.Count(m => m.Label == "vip"));

            db.Members.Add(new Member { Email = "a@x", Label = "dup" });
            Assert.ThrowsAny<Exception>(() => db.SaveChanges());
        }

        // 新连接重跑:IF NOT EXISTS 幂等,不炸
        using (var db = new MemberDb(Cs))
        {
            Assert.Equal(3, db.Members.Count());
        }
    }

    [Fact]
    public void Removing_index_from_model_drops_it_and_lifts_unique()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "DROP TABLE IF EXISTS Members";
            cmd.ExecuteNonQuery();
        }

        // v1:两个索引(Email 唯一 + Label 普通)
        using (var db = new MemberDb(Cs))
        {
            db.Members.Add(new Member { Email = "a@x", Label = "l1" });
            db.SaveChanges();
            var e = Assert.ThrowsAny<Exception>(() =>
            {
                db.Members.Add(new Member { Email = "a@x", Label = "l2" });
                db.SaveChanges();
            });
        }

        // v2:模型去掉唯一 Email 索引 → 索引被回收,重复 Email 不再报错
        using (var db = new MemberDbV2(Cs))
        {
            db.MembersV2.Add(new MemberV2 { Email = "a@x", Label = "l3" });
            db.SaveChanges();
            Assert.Equal(2, db.MembersV2.Count(m => m.Email == "a@x"));
        }

        // 服务器端确认:IX_Members_Email 已删,IX_Members_Label 保留
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText =
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'Members' ORDER BY name";
            using var reader = cmd.ExecuteReader();
            var names = new List<string>();
            while (reader.Read()) names.Add(reader.GetString(0));
            Assert.Contains("IX_Members_Label", names);
            Assert.DoesNotContain("IX_Members_Email", names);
        }
    }

    [Fact]
    public void Composite_index_is_skipped_without_breaking_the_context()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "DROP TABLE IF EXISTS Widgets";
            cmd.ExecuteNonQuery();
        }
        using (var db = new WidgetDb(Cs))
        {
            // 复合索引引擎暂不支持:同步按尽力而为跳过,读写不受影响
            db.Widgets.Add(new Widget { Sku = "A", Zone = 1 });
            db.Widgets.Add(new Widget { Sku = "B", Zone = 1 });
            db.SaveChanges();
            Assert.Equal(2, db.Widgets.Count(w => w.Zone == 1));
        }
    }

    [Fact]
    public void Manually_created_index_is_not_dropped_by_sync()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        Action<string> run = sql =>
        {
            using var c = conn.CreateCommand();
            c.CommandText = sql;
            c.ExecuteNonQuery();
        };
        run("DROP TABLE IF EXISTS Members");
        run("CREATE TABLE Members (Id INT PRIMARY KEY, Email TEXT, Label TEXT)");
        // 手工建的索引:不是 IX_ 前缀,同步不得回收
        run("CREATE INDEX manual_email ON Members (Email)");

        using (var db = new MemberDbV2(Cs))
        {
            db.MembersV2.Add(new MemberV2 { Email = "a@x", Label = "l" });
            db.SaveChanges();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText =
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'Members' ORDER BY name";
            using var reader = cmd.ExecuteReader();
            var names = new List<string>();
            while (reader.Read()) names.Add(reader.GetString(0));
            Assert.Contains("manual_email", names);
        }
    }

    public class Widget
    {
        public int Id { get; set; }
        public string Sku { get; set; } = "";
        public int Zone { get; set; }
    }

    public class WidgetDb : DbContext
    {
        private readonly string _cs;
        public WidgetDb(string cs) => _cs = cs;
        public DbSet<Widget> Widgets => Set<Widget>();
        protected override void OnModelCreating(ModelBuilder b) =>
            b.Entity<Widget>().HasIndex(w => new { w.Sku, w.Zone });
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    private void Clean()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        foreach (var t in new[] { "Posts", "Blogs" })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {t}";
            cmd.ExecuteNonQuery();
        }
    }

    [Fact]
    public void Dropped_table_is_recreated_by_a_new_context()
    {
        Clean();
        using (var db = new AppDb(Cs)) { db.Blogs.Add(new Blog { Title = "gen1" }); db.SaveChanges(); }
        Clean(); // 模拟外部删表
        using (var db = new AppDb(Cs))
        {
            db.Blogs.Add(new Blog { Title = "gen2" });
            db.SaveChanges();
            Assert.Equal("gen2", db.Blogs.Single().Title);
        }
    }

    [Fact]
    public void Column_sync_keeps_existing_rows()
    {
        Clean();
        using (var db = new AppDb(Cs))
        {
            db.Blogs.Add(new Blog { Title = "keep" });
            db.SaveChanges();
        }
        using (var db = new AppDbV2(Cs))
        {
            // 补列后旧行仍在,且可按新列过滤
            Assert.Equal(1, db.BlogsV2.Count());
            Assert.Equal(0, db.BlogsV2.Count(b => b.Rating > 0));
        }
    }

    // ---- 额外模型(独立表) ----

    public class Tag
    {
        public string Id { get; set; } = "";
        public string Note { get; set; } = "";
    }

    public class TagDb : DbContext
    {
        private readonly string _cs;
        public TagDb(string cs) => _cs = cs;
        public DbSet<Tag> Tags => Set<Tag>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    public class TypedRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool Flag { get; set; }
        public double Score { get; set; }
        public decimal Amount { get; set; }
        public DateTime At { get; set; }
    }

    public class TypeDb : DbContext
    {
        private readonly string _cs;
        public TypeDb(string cs) => _cs = cs;
        public DbSet<TypedRow> TypedRows => Set<TypedRow>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    public class OrderLine
    {
        public int Id { get; set; }
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
    }

    public class OrderDb : DbContext
    {
        private readonly string _cs;
        public OrderDb(string cs) => _cs = cs;
        protected override void OnModelCreating(ModelBuilder b) =>
            b.Entity<OrderLine>().ToTable("Order Lines");
        public DbSet<OrderLine> OrderLines => Set<OrderLine>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    public class Counter
    {
        public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    public class CounterDb : DbContext
    {
        private readonly string _cs;
        public CounterDb(string cs) => _cs = cs;
        public DbSet<Counter> Counters => Set<Counter>();
        protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
    }

    public sealed class EfTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public EfTests(EfServerFixture fx) => _fx = fx;

    private AppDb NewDb() => new($"host=127.0.0.1;port={_fx.Port}");

    private void Clean()
    {
        using var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        foreach (var t in new[] { "Posts", "Blogs" })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {t}";
            cmd.ExecuteNonQuery();
        }
    }

    [Fact]
    public void Fk_convention_index_follows_table_creation()
    {
        Clean();
        using (var db = NewDb())
        {
            // 一对多:EF 惯例在 Post.BlogId 上建索引,惰性建表时一并创建
            db.Blogs.Add(new Blog { Title = "ix", Posts = { new Post { Content = "c" } } });
            db.SaveChanges();
        }
        using (var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'Posts'";
            using var reader = cmd.ExecuteReader();
            var names = new List<string>();
            while (reader.Read()) names.Add(reader.GetString(0));
            Assert.Contains("IX_Posts_BlogId", names);
        }
    }

    [Fact]
    public void Ensure_created_path_also_creates_model_indexes()
    {
        Clean();
        using (var db = NewDb())
        {
            db.Database.EnsureCreated();
        }
        using (var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'Posts'";
            using var reader = cmd.ExecuteReader();
            var names = new List<string>();
            while (reader.Read()) names.Add(reader.GetString(0));
            Assert.Contains("IX_Posts_BlogId", names);
        }
    }

    [Fact]
    public void Tables_are_created_lazily_without_EnsureCreated()
    {
        Clean();
        using var db = NewDb();
        // 不调用 EnsureCreated:首个查询即自动建表(类似 EF MongoDB)。
        Assert.Empty(db.Blogs.ToList());

        db.Blogs.Add(new Blog { Title = "lazy" });
        db.SaveChanges();
        Assert.Equal("lazy", db.Blogs.Single().Title);
    }

    [Fact]
    public void Adding_a_model_property_requires_no_migration()
    {
        Clean();
        // v1 模型建表并写入一行
        using (var db = NewDb())
        {
            db.Blogs.Add(new Blog { Title = "v1" });
            db.SaveChanges();
        }
        // v2 模型:同一张表增加字段,不迁移,首次访问自动补列
        using (var db = new AppDbV2($"host=127.0.0.1;port={_fx.Port}"))
        {
            var old = db.BlogsV2.Single();
            Assert.Equal("v1", old.Title);
            Assert.Null(old.Rating);           // 旧行新列为 NULL

            old.Rating = 5;
            db.BlogsV2.Add(new BlogV2 { Title = "v2", Rating = 9 });
            db.SaveChanges();

            Assert.Equal(9, db.BlogsV2.Single(b => b.Title == "v2").Rating);
            Assert.Equal(5, db.BlogsV2.Single(b => b.Title == "v1").Rating);
        }
        // v1 模型回到旧 schema,仍能正常读写(多出的列被忽略)
        using (var db = NewDb())
        {
            Assert.Equal(2, db.Blogs.Count());
        }
    }

    [Fact]
    public void Linq_with_Include_loads_related_posts()
    {
        Clean();
        using var db = NewDb();
        db.Blogs.Add(new Blog
        {
            Title = "with posts",
            Posts = { new Post { Content = "p1" }, new Post { Content = "p2" } },
        });
        db.SaveChanges();

        var blog = db.Blogs.Include(b => b.Posts).First(b => b.Title == "with posts");
        Assert.Equal(2, blog.Posts.Count);
        Assert.Contains(blog.Posts, p => p.Content == "p2");
    }

    [Fact]
    public void EnsureCreate_and_crud_roundtrip()
    {
        Clean();
        using (var db = NewDb())
        {
            db.Blogs.Add(new Blog { Title = "first" });
            db.Blogs.Add(new Blog { Title = "second" });
            db.SaveChanges();
        }
        using (var db = NewDb())
        {
            var blogs = db.Blogs.OrderBy(b => b.Title).ToList();
            Assert.Equal(2, blogs.Count);
            Assert.Equal("first", blogs[0].Title);
            blogs[0].Title = "renamed";
            db.SaveChanges();
        }
        using (var db = NewDb())
        {
            Assert.Equal("renamed", db.Blogs.Single(b => b.Title == "renamed").Title);
            var victim = db.Blogs.First(b => b.Title == "second");
            db.Blogs.Remove(victim);
            db.SaveChanges();
            Assert.Single(db.Blogs.ToList());
        }
    }

    [Fact]
    public void Batch_update_and_delete_in_one_SaveChanges()
    {
        Clean();
        using (var db = NewDb())
        {
            db.Blogs.AddRange(new Blog { Title = "a" }, new Blog { Title = "b" }, new Blog { Title = "c" });
            db.SaveChanges();
        }
        using (var db = NewDb())
        {
            // 一次 SaveChanges 混合:2 个 UPDATE + 1 个 DELETE
            foreach (var b in db.Blogs.Where(x => x.Title != "b").ToList())
                b.Title = b.Title + "!";
            db.Blogs.Remove(db.Blogs.Single(x => x.Title == "b"));
            db.SaveChanges();
        }
        using (var db = NewDb())
        {
            var titles = db.Blogs.OrderBy(b => b.Title).Select(b => b.Title).ToList();
            Assert.Equal(new[] { "a!", "c!" }, titles);
        }
    }

    [Fact]
    public void Unchanged_entities_do_not_produce_updates()
    {
        Clean();
        using (var db = NewDb())
        {
            db.Blogs.Add(new Blog { Title = "stable" });
            db.SaveChanges();
        }
        // 二次上下文只读不 SaveChanges 不会写;真正 SaveChanges 空变更集也应是 0 条 SQL 写
        using (var db = NewDb())
        {
            var blog = db.Blogs.Single();
            Assert.Equal(EntityState.Unchanged, db.Entry(blog).State);
            db.SaveChanges();
            Assert.Equal(EntityState.Unchanged, db.Entry(blog).State);
        }
        using (var db = NewDb())
        {
            Assert.Equal("stable", db.Blogs.Single().Title);
        }
    }
}
}


// EF 层故障转移:主库写入 → 杀主进程 → PROMOTE 副本 → EF 在新主上读写。
public sealed class EfFailoverTests
{
    private static int FreePort()
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var p = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return p;
    }

    private sealed class Node : IDisposable
    {
        public readonly Process Proc;
        private readonly string _db;
        public Node(int port, string db, Process proc) => (_db, Proc) = (db, proc);
        public void Dispose()
        {
            try { if (!Proc.HasExited) Proc.Kill(); } catch { }
            Proc.Dispose();
            try { File.Delete(_db); } catch { }
        }
    }

    private static Node Start(int port, bool readOnly, string? replicateTo)
    {
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        var db = Path.Combine(Path.GetTempPath(), $"docsql-effo-{port}.db");
        try { File.Delete(db); } catch { }
        var psi = new System.Diagnostics.ProcessStartInfo
        {
            FileName = exe, ArgumentList = { db, $"127.0.0.1:{port}" },
            CreateNoWindow = true, RedirectStandardError = false,
        };
        if (replicateTo is not null) psi.Environment["DOCSQL_REPLICATE_TO"] = replicateTo;
        if (readOnly) psi.Environment["DOCSQL_READ_ONLY"] = "1";
        var proc = Process.Start(psi)!;
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return new(port, db, proc); }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException("node did not start");
    }

    [Fact]
    public void Ef_context_survives_primary_failure_after_promotion()
    {
        var replicaPort = FreePort();
        var primaryPort = FreePort();
        using var replica = Start(replicaPort, readOnly: true, replicateTo: null);
        using var primary = Start(primaryPort, readOnly: false, replicateTo: $"127.0.0.1:{replicaPort}");
        var primaryCs = $"host=127.0.0.1;port={primaryPort}";
        var replicaCs = $"host=127.0.0.1;port={replicaPort}";

        // 阶段 1:在主库上用 EF 写入业务数据
        using (var db = new AppDb(primaryCs))
        {
            db.Blogs.Add(new Blog { Title = "before-failover" });
            db.SaveChanges();
        }

        // 等复制收敛(异步转发)
        var seen = false;
        string? lastErr = null;
        for (var i = 0; i < 100 && !seen; i++)
        {
            try
            {
                using var db = new AppDb(replicaCs);
                seen = db.Blogs.Any(b => b.Title == "before-failover");
            }
            catch (Exception ex) { lastErr = ex.Message; }
            Thread.Sleep(30);
        }
        Assert.True(seen, $"副本未收到复制写入;最后一次错误: {lastErr ?? "(无,查询成功但无数据)"}");

        // 阶段 2:主库宕机,副本升主
        primary.Proc.Kill();
        primary.Proc.WaitForExit(5000);
        using (var conn = new DocsqlConnection(replicaCs))
        {
            conn.Open();
            conn.Promote();
        }

        // 阶段 3:EF 指向新主,复制来的数据可读,新写入可增删改
        using (var db = new AppDb(replicaCs))
        {
            Assert.Equal("before-failover", db.Blogs.Single().Title);

            db.Blogs.Add(new Blog { Title = "after-failover" });
            db.SaveChanges();
            Assert.Equal(2, db.Blogs.Count());

            var victim = db.Blogs.Single(b => b.Title == "after-failover");
            db.Blogs.Remove(victim);
            db.SaveChanges();
            Assert.Single(db.Blogs.ToList());
        }
    }
}

// EF 在对称集群上的体验:连接任意节点,写通过任意节点可见。
public sealed class EfSymmetricClusterTests
{
    private static (Process P, Process Q, int PortP, int PortQ) TwoPeers()
    {
        int Free()
        {
            using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            l.Start();
            var p = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return p;
        }
        var pp = Free();
        var qq = Free();
        Process Start(int port, string peers)
        {
            var exe = Path.GetFullPath(Path.Combine(
                AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
            Assert.True(File.Exists(exe), $"server binary not found at {exe}");
            var db = Path.Combine(Path.GetTempPath(), $"docsql-efsym-{port}.db");
            try { File.Delete(db); } catch { }
            var psi = new ProcessStartInfo
            {
                FileName = exe, ArgumentList = { db, $"127.0.0.1:{port}" },
                CreateNoWindow = true, RedirectStandardError = false,
            };
            psi.Environment["DOCSQL_PEERS"] = peers;
            var proc = Process.Start(psi)!;
            for (var i = 0; i < 100; i++)
            {
                try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return proc; }
                catch { Thread.Sleep(50); }
            }
            throw new InvalidOperationException("node did not start");
        }
        return (Start(pp, $"127.0.0.1:{qq}"), Start(qq, $"127.0.0.1:{pp}"), pp, qq);
    }

    [Fact]
    public void Ef_writes_via_one_node_are_visible_via_the_other()
    {
        var (p, q, portP, portQ) = TwoPeers();
        using var _p = p;
        using var _q = q;

        // 连节点 P:建表 + 写入(EF SaveChanges)
        using (var db = new AppDb($"host=127.0.0.1;port={portP}"))
        {
            db.Blogs.Add(new Blog { Title = "via-P" });
            db.SaveChanges();
        }

        // 连节点 Q:复制来的表和数据直接可读,还能继续写
        var seen = false;
        for (var i = 0; i < 100 && !seen; i++)
        {
            try
            {
                using var db = new AppDb($"host=127.0.0.1;port={portQ}");
                seen = db.Blogs.Any(b => b.Title == "via-P");
            }
            catch { }
            Thread.Sleep(30);
        }
        Assert.True(seen, "节点 Q 未见节点 P 的 EF 写入");

        using (var db = new AppDb($"host=127.0.0.1;port={portQ}"))
        {
            db.Blogs.Add(new Blog { Title = "via-Q" });
            db.SaveChanges();
        }

        // P 可能还在 bootstrap 窗口内,扇出落点有短暂延迟,轮询等待。
        var counted = false;
        for (var i = 0; i < 100 && !counted; i++)
        {
            try
            {
                using var db = new AppDb($"host=127.0.0.1;port={portP}");
                counted = db.Blogs.Count() == 2;
            }
            catch { }
            Thread.Sleep(30);
        }
        Assert.True(counted, "节点 P 未见节点 Q 的 EF 写入");
    }
}

// EF 显式事务 + SaveChanges(依赖引擎的 SAVEPOINT 支持)。
public sealed class EfTransactionTests
{
    private sealed class Node : IDisposable
    {
        public readonly Process Proc;
        private readonly string _db;
        public string Cs => $"host=127.0.0.1;port={Port}";
        public int Port { get; }
        public Node(int port, string db, Process proc) => (Port, _db, Proc) = (port, db, proc);
        public void Dispose()
        {
            try { if (!Proc.HasExited) Proc.Kill(); } catch { }
            Proc.Dispose();
            try { File.Delete(_db); } catch { }
        }
    }

    private static Node Start()
    {
        using var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var port = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        var db = Path.Combine(Path.GetTempPath(), $"docsql-eftx-{port}.db");
        try { File.Delete(db); } catch { }
        var proc = Process.Start(new ProcessStartInfo
        {
            FileName = exe, ArgumentList = { db, $"127.0.0.1:{port}" },
            CreateNoWindow = true, RedirectStandardError = false,
        })!;
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return new(port, db, proc); }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException("node did not start");
    }

    [Fact]
    public void Ef_transaction_commit_and_rollback_work_with_savepoints()
    {
        using var node = Start();

        // 提交路径:事务内 SaveChanges(隐式 SAVEPOINT)+ COMMIT
        using (var db = new AppDb(node.Cs))
        {
            using var tx = db.Database.BeginTransaction();
            db.Blogs.Add(new Blog { Title = "tx-commit" });
            db.SaveChanges();          // SAVEPOINT / RELEASE
            tx.Commit();
        }

        // 回滚路径:事务内 SaveChanges 后整体回滚,数据不落库
        using (var db = new AppDb(node.Cs))
        {
            Assert.Equal(1, db.Blogs.Count());
            using var tx = db.Database.BeginTransaction();
            db.Blogs.Add(new Blog { Title = "tx-rollback" });
            db.SaveChanges();
            tx.Rollback();
        }
        using (var db = new AppDb(node.Cs))
        {
            Assert.Equal(1, db.Blogs.Count());
            Assert.Equal("tx-commit", db.Blogs.Single().Title);
        }

        // 事务内多次 SaveChanges(保存点复用)
        using (var db = new AppDb(node.Cs))
        {
            using var tx = db.Database.BeginTransaction();
            db.Blogs.Add(new Blog { Title = "batch-1" });
            db.SaveChanges();
            db.Blogs.Add(new Blog { Title = "batch-2" });
            db.SaveChanges();
            tx.Commit();
        }
        using (var db = new AppDb(node.Cs))
        {
            Assert.Equal(3, db.Blogs.Count());
        }
    }
}
