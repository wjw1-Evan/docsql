// EF Core 调用 docsql 的完整示例。
//
// 前置:target/debug/docsql-server 已编译。示例自动在临时端口拉起一个
// server(临时 db 文件),跑完即清理,直接 `dotnet run` 即可。
//
// 连接方式:o.UseDocsql("host=127.0.0.1;port=<port>"),底层是
// Docsql.Client 的 TCP 二进制协议(DSQ1),SQL 生成复用 EF 的 SQLite 管线。

using System.Diagnostics;
using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

// ---- 数据模型:Blog 1..* Post ----

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

public static class Program
{
    public static int Main(string[] args)
    {
        var port = args.Length > 0 && int.TryParse(args[0], out var p) ? p : 7600;
        using var server = ServerLauncher.Start(port);
        var cs = $"host=127.0.0.1;port={port}";
        try
        {
            Run(cs);
            Console.WriteLine("\n✅ 全部示例通过");
            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ 失败: {ex.Message}");
            return 1;
        }
    }

    private static void Run(string cs)
    {
        // 1) 无需建表:提供程序在每条连接首个命令前按模型自动 CREATE TABLE
        //    (类似 EF MongoDB 的惰性建集合)。这里先清掉旧数据保证可重复运行。
        using (var conn = new DocsqlConnection(cs))
        {
            conn.Open();
            foreach (var t in new[] { "Posts", "Blogs" })
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"DROP TABLE IF EXISTS {t}";
                cmd.ExecuteNonQuery();
            }
        }
        using (var db = new AppDb(cs))
        {
            var n = db.Blogs.Count();  // 首个查询触发自动建表
            Console.WriteLine($"1) 首次查询即自动建表(未调用 EnsureCreated),当前 Blog 数 = {n}");
        }

        // 2) 插入:一对多关系一次 SaveChanges 写入
        using (var db = new AppDb(cs))
        {
            db.Blogs.Add(new Blog
            {
                Title = "docsql 博客",
                Posts = { new Post { Content = "第一篇" }, new Post { Content = "第二篇" } },
            });
            db.Blogs.Add(new Blog { Title = "另一个博客" });
            db.SaveChanges();
            Console.WriteLine($"2) 插入 2 个 Blog(含 2 篇 Post)");
        }

        // 3) LINQ 查询:Where + OrderBy + Include 预加载关联
        using (var db = new AppDb(cs))
        {
            var blog = db.Blogs
                .Include(b => b.Posts)
                .Where(b => b.Title == "docsql 博客")
                .First();
            Console.WriteLine($"3) Include 查询:「{blog.Title}」有 {blog.Posts.Count} 篇文章");

            var count = db.Posts.Count(x => x.Content.EndsWith("篇"));
            Console.WriteLine($"   LINQ Count(Content LIKE '%篇') = {count}");
        }

        // 4) 更新与删除
        using (var db = new AppDb(cs))
        {
            var blog = db.Blogs.Single(b => b.Title == "另一个博客");
            blog.Title = "改名后的博客";
            db.SaveChanges();
            Console.WriteLine("4) 更新:「另一个博客」→「改名后的博客」");
        }
        using (var db = new AppDb(cs))
        {
            var victim = db.Blogs.Single(b => b.Title == "改名后的博客");
            var firstPost = db.Posts.First();
            db.Blogs.Remove(victim);
            db.Posts.Remove(firstPost);
            db.SaveChanges();
            Console.WriteLine($"4) 更新+删除后:剩 {db.Blogs.Count()} 个 Blog、{db.Posts.Count()} 篇 Post");
        }

        // 5) 原生 SQL(通过 ADO 桥接,参数客户端侧绑定 @p0)
        using (var db = new AppDb(cs))
        {
            var titles = db.Blogs.FromSql($"SELECT * FROM Blogs")
                .Where(b => b.Id > 0).ToList();
            Console.WriteLine($"5) FromSql 原生查询:{titles.Count} 行");

            using (var conn = new DocsqlConnection(cs))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM Posts";
                Console.WriteLine($"   ADO 原生 COUNT(*) = {cmd.ExecuteScalar()}");
            }
        }

        // 6) 事务:BEGIN 后插入再 ROLLBACK,数据不变。
        //    注意:引擎暂不支持 SAVEPOINT,EF 的 SaveChanges 在事务内会隐式
        //    创建保存点,因此这里用 ADO 原生事务演示(框架层支持有限)。
        using (var conn = new DocsqlConnection(cs))
        {
            conn.Open();
            using (var tx = conn.BeginTransaction())
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT INTO Blogs (Title) VALUES ('应被回滚')";
                cmd.ExecuteNonQuery();
                tx.Rollback();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT COUNT(*) FROM Blogs";
                Console.WriteLine($"6) 事务回滚后 Blog 数 = {cmd.ExecuteScalar()}(未新增)");
            }
        }

        // 7) 模型演进:加字段免迁移(类似 EF MongoDB)。v2 模型给 Blogs
        //    增加 Rating 字段,首次访问自动 ALTER TABLE ADD COLUMN,
        //    旧行新列读出为 NULL,新旧模型可共存。
        using (var db = new AppDbV2(cs))
        {
            var blog = db.BlogsV2.Single();
            Console.WriteLine($"7) v2 模型读旧数据:Rating = {blog.Rating?.ToString() ?? "NULL"}(未迁移自动补列)");
            blog.Rating = 5;
            db.SaveChanges();
            Console.WriteLine($"   写入后 Rating = {db.BlogsV2.Single().Rating}");
        }
    }
}

// v2 模型:同一张 Blogs 表,多一个 Rating 字段
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

/// <summary>拉起一个临时 docsql-server 子进程,Dispose 时回收。</summary>
internal sealed class ServerLauncher : IDisposable
{
    private readonly Process _proc;
    private readonly string _dbFile;

    private ServerLauncher(Process proc, string dbFile) { _proc = proc; _dbFile = dbFile; }

    public static ServerLauncher Start(int port)
    {
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        if (!File.Exists(exe))
            throw new FileNotFoundException(
                "找不到 docsql-server,请先 cargo build -p docsql-server:" + exe);
        var dbFile = Path.Combine(Path.GetTempPath(), $"docsql-efsample-{port}.db");
        try { File.Delete(dbFile); } catch { }
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            ArgumentList = { dbFile, $"127.0.0.1:{port}" },
            CreateNoWindow = true,
            RedirectStandardError = false,
        };
        var proc = Process.Start(psi) ?? throw new InvalidOperationException("无法启动 docsql-server");
        for (var i = 0; i < 100; i++)
        {
            try { using var _ = new System.Net.Sockets.TcpClient("127.0.0.1", port); return new(proc, dbFile); }
            catch { Thread.Sleep(50); }
        }
        throw new InvalidOperationException("docsql-server 启动超时");
    }

    public void Dispose()
    {
        try { _proc.Kill(); } catch { }
        _proc.Dispose();
        try { File.Delete(_dbFile); } catch { }
    }
}
