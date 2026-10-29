// DocSQL 博客示例:一个用 EF Core + 嵌入式模式(Data Source=blog.db)写的
// 最小博客 —— 作者/文章/评论/标签,零 server、零 Docker、零迁移。
//
// 运行:
//   cd dotnet && dotnet run --project samples/BlogSample
//   连已有节点(远程模式,同时演示两种形态代码零差异):
//   dotnet run --project samples/BlogSample -- "host=127.0.0.1;port=7600"
//
// 演示点:
//   1) 懒建表 + 唯一索引/外键随模型自动落地(不调用 EnsureCreated);
//   2) 一对多 + 无 schema 的标签数组(EF primitive collection → 引擎 JSON 数组);
//   3) LINQ:JOIN 作者、时间倒序 + 分页、JSON_ARRAY_CONTAINS 标签筛选、LIKE 搜索;
//   4) 事务:发文章 + 首条评论原子提交,回滚演示;
//   5) 与原生 ADO.NET 混用:GROUP BY 统计作者产出(同一数据文件);
//   6) 重新打开上下文验证持久化,打印数据库文件位置与大小。

using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

// ---- 领域模型 ----

public class Author
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string Bio { get; set; } = "";
    public List<Post> Posts { get; } = new();
}

public class Post
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public string Body { get; set; } = "";
    public DateTime CreatedAt { get; set; }
    public int AuthorId { get; set; }
    public Author Author { get; set; } = null!;
    /// <summary>无 schema 标签列表:EF primitive collection 映射为 JSON 数组文本列。</summary>
    public List<string> Tags { get; set; } = new();
    public List<Comment> Comments { get; } = new();
}

public class Comment
{
    public int Id { get; set; }
    public string Text { get; set; } = "";
    public DateTime CreatedAt { get; set; }
    public int PostId { get; set; }
    public Post Post { get; set; } = null!;
}

public class BlogDb : DbContext
{
    private readonly string _cs;

    public BlogDb(string cs) => _cs = cs;

    public DbSet<Author> Authors => Set<Author>();
    public DbSet<Post> Posts => Set<Post>();
    public DbSet<Comment> Comments => Set<Comment>();

    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Author>().HasIndex(a => a.Name).IsUnique()
            .HasDatabaseName("IX_Authors_Name");
        b.Entity<Post>().HasIndex(p => p.AuthorId).HasDatabaseName("IX_Posts_AuthorId");
        b.Entity<Post>().HasIndex(p => p.CreatedAt).HasDatabaseName("IX_Posts_CreatedAt");
        b.Entity<Comment>().HasIndex(c => c.PostId).HasDatabaseName("IX_Comments_PostId");
    }
}

public static class Program
{
    public static int Main(string[] args)
    {
        // 默认嵌入式:数据库就是工作目录旁的一个文件。
        var embeddedPath = Path.Combine(AppContext.BaseDirectory, "blog.db");
        var cs = args.Length > 0 && args[0].Contains('=')
            ? args[0]
            : $"Data Source={embeddedPath}";
        var embedded = new DocsqlConnectionStringBuilder { ConnectionString = cs }.Embedded;

        Console.WriteLine($"DocSQL 博客示例 → {(embedded ? $"嵌入式 {embeddedPath}" : cs)}");
        Console.WriteLine(new string('─', 64));

        try
        {
            Reset(cs, embedded);
            Run(cs);
            Console.WriteLine("\n✅ 博客示例执行完毕");
            if (embedded)
            {
                Console.WriteLine($"   数据库文件:{embeddedPath}");
            }
            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ 失败:{ex.Message}");
            return 1;
        }
    }

    /// <summary>可重复运行:嵌入式删掉数据文件(连同 WAL),远程模式 DROP 表。</summary>
    private static void Reset(string cs, bool embedded)
    {
        if (embedded)
        {
            var path = new DocsqlConnectionStringBuilder { ConnectionString = cs }.DataSource;
            if (path != ":memory:" && File.Exists(path))
            {
                File.Delete(path);
                var wal = path + ".wal";
                if (File.Exists(wal))
                {
                    File.Delete(wal);
                }
            }
            return;
        }
        using var conn = new DocsqlConnection(cs);
        conn.Open();
        foreach (var t in new[] { "Comments", "Posts", "Authors" })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {t}";
            cmd.ExecuteNonQuery();
        }
    }

    private static void Run(string cs)
    {
        // 1) 懒建表:首个查询前,提供程序按模型建三张表与全部索引。
        using (var db = new BlogDb(cs))
        {
            Console.WriteLine($"1) 懒建表(未调用 EnsureCreated):作者 {db.Authors.Count()} / 文章 {db.Posts.Count()} 篇");
        }

        // 2) 两位作者、三篇文章(含标签与时间)。一对多 + primitive collection
        //    在一次 SaveChanges 内落盘。
        var now = new DateTime(2026, 10, 29, 9, 0, 0, DateTimeKind.Utc);
        using (var db = new BlogDb(cs))
        {
            var alice = new Author { Name = "alice", Bio = "写 .NET 与数据库" };
            var bob = new Author { Name = "bob", Bio = "写 Rust 与存储引擎" };
            alice.Posts.Add(new Post
            {
                Title = "DocSQL 入门:一个文件就是一个数据库",
                Body = "Data Source=blog.db,进程内引擎,零守护。",
                CreatedAt = now,
                Tags = { "dotnet", "embedded" },
            });
            alice.Posts.Add(new Post
            {
                Title = "EF Core 免迁移实践",
                Body = "模型变了就自动补表补列。",
                CreatedAt = now.AddDays(1),
                Tags = { "dotnet", "ef" },
            });
            bob.Posts.Add(new Post
            {
                Title = "WAL 与快照读",
                Body = "先写日志再落页;读走 MVCC 快照不阻塞写。",
                CreatedAt = now.AddDays(2),
                Tags = { "rust", "storage" },
            });
            db.Authors.AddRange(alice, bob);
            db.SaveChanges();
            Console.WriteLine("2) 写入 2 位作者 + 3 篇文章(含标签/时间)");
        }

        // 3) 首页:JOIN 作者、时间倒序、分页(第 1 页 2 条)。
        using (var db = new BlogDb(cs))
        {
            var page = db.Posts
                .OrderByDescending(p => p.CreatedAt)
                .Select(p => new { p.Title, Author = p.Author.Name, p.CreatedAt })
                .Take(2)
                .ToList();
            Console.WriteLine("3) 首页第 1 页(时间倒序,JOIN 作者):");
            foreach (var p in page)
            {
                Console.WriteLine($"   {p.CreatedAt:yyyy-MM-dd} [{p.Author}] {p.Title}");
            }
        }

        // 4) 标签筛选:List<string>.Contains 由提供程序翻译为引擎的
        //    JSON_ARRAY_CONTAINS,留在服务端执行。
        using (var db = new BlogDb(cs))
        {
            var dotnetPosts = db.Posts.Where(p => p.Tags.Contains("dotnet")).ToList();
            Console.WriteLine($"4) 标签筛选 dotnet → {dotnetPosts.Count} 篇");
        }

        // 5) 标题搜索(LIKE)。
        using (var db = new BlogDb(cs))
        {
            var hits = db.Posts.Where(p => p.Title.Contains("入门")).Select(p => p.Title).ToList();
            Console.WriteLine($"5) 标题搜索「入门」→ {string.Join(" / ", hits)}");
        }

        // 6) 评论:外键关联文章;Include 后统计。
        using (var db = new BlogDb(cs))
        {
            var post = db.Posts.Single(p => p.Title.StartsWith("DocSQL 入门"));
            post.Comments.Add(new Comment { Text = "这个模式很香", CreatedAt = now.AddHours(1) });
            post.Comments.Add(new Comment { Text = "和 SQLite 一样省心", CreatedAt = now.AddHours(2) });
            db.SaveChanges();

            var rows = db.Posts
                .OrderBy(p => p.Id)
                .Select(p => new { p.Title, Count = p.Comments.Count })
                .ToList();
            Console.WriteLine("6) 每篇文章的评论数:");
            foreach (var r in rows)
            {
                Console.WriteLine($"   {r.Title} → {r.Count}");
            }
        }

        // 7) 事务:发文章 + 首条评论要么一起成功,要么一起撤销。
        using (var db = new BlogDb(cs))
        {
            var before = db.Posts.Count();
            using (var tx = db.Database.BeginTransaction())
            {
                var alice = db.Authors.Single(a => a.Name == "alice");
                var draft = new Post
                {
                    Title = "事务里的草稿",
                    Body = "这条应该被回滚掉。",
                    CreatedAt = now.AddDays(3),
                    AuthorId = alice.Id,
                    Tags = { "draft" },
                };
                draft.Comments.Add(new Comment { Text = "评论也回滚", CreatedAt = now.AddDays(3) });
                db.Posts.Add(draft);
                db.SaveChanges();
                Console.WriteLine($"7) 事务内文章数 = {db.Posts.Count()}(含未提交)→ 回滚");
                tx.Rollback();
            }
            Console.WriteLine($"   回滚后文章数 = {db.Posts.Count()}(应等于 {before})");
        }

        // 8) 与原生 ADO.NET 混用同一数据文件:GROUP BY 统计作者产出与评论。
        using (var conn = new DocsqlConnection(cs))
        {
            conn.Open();
            using var cmd = (DocsqlCommand)conn.CreateCommand();
            cmd.CommandText =
                "SELECT a.Name AS author, COUNT(p.Id) AS posts " +
                "FROM Authors a LEFT JOIN Posts p ON p.AuthorId = a.Id " +
                "GROUP BY a.Name ORDER BY author";
            using var r = cmd.ExecuteReader();
            Console.WriteLine("8) 原生 SQL(GROUP BY + 关联子查询),作者产出:");
            while (r.Read())
            {
                Console.WriteLine($"   {r.GetString(0)}:文章 {r.GetInt64(1)} 篇");
            }
        }

        // 9) 重新打开:数据从文件里读回(嵌入式即"关掉进程再打开")。
        using (var db = new BlogDb(cs))
        {
            var total = db.Posts.Count();
            var latest = db.Posts.OrderByDescending(p => p.CreatedAt).Select(p => p.Title).First();
            Console.WriteLine($"9) 重新打开:文章 {total} 篇,最新《{latest}》");
        }
    }
}
