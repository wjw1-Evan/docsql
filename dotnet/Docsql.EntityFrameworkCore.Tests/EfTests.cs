using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class EfServerFixture : IDisposable
{
    public int Port { get; }
    private readonly System.Diagnostics.Process _proc;

    public EfServerFixture()
    {
        Port = 17800 + Random.Shared.Next(200);
        var tmp = Path.Combine(Path.GetTempPath(), $"docsql-ef-{Port}.db");
        try { File.Delete(tmp); } catch { }
        var exe = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "target", "debug", "docsql-server"));
        Assert.True(File.Exists(exe), $"server binary not found at {exe}");
        _proc = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
        {
            FileName = exe, Arguments = $"{tmp} 127.0.0.1:{Port}",
            CreateNoWindow = true, RedirectStandardError = true,
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
    public void Linq_with_Include_loads_related_posts()
    {
        Clean();
        using var db = NewDb();
        db.Database.EnsureCreated();
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
            db.Database.EnsureCreated();
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
}
