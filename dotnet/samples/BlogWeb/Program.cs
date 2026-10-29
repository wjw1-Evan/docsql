// DocSQL 博客前端示例:最小 API + 内置单页(wwwroot,零 npm)。
//
// 后端默认嵌入式:Data Source=blog.db(一个文件就是一个数据库,无 server、
// 无 Docker);也可以用 DOCSQL_CONN 指到已有节点(host=…;port=…)。
//
// 运行:
//   cd dotnet && dotnet run --project samples/BlogWeb
//   → http://127.0.0.1:17800(可用 BLOG_URLS 覆盖监听地址)
//
// 接口:
//   GET  /api/stats                  计数
//   GET  /api/posts?tag=&q=          文章列表(标签/标题过滤)
//   GET  /api/posts/{id}             文章详情 + 评论
//   POST /api/posts                  新建文章 {title, body, author, tags[]}
//   POST /api/posts/{id}/comments    新增评论 {text}
//   GET  /api/authors                作者列表

using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(Environment.GetEnvironmentVariable("BLOG_URLS") ?? "http://127.0.0.1:17800");

// ---- 连接:默认嵌入式文件;DOCSQL_CONN 可切到远程节点 ----
var cs = Environment.GetEnvironmentVariable("DOCSQL_CONN") is { Length: > 0 } remote
    ? remote
    : $"Data Source={Path.Combine(Environment.CurrentDirectory, "blog.db")}";
var embedded = new DocsqlConnectionStringBuilder { ConnectionString = cs }.Embedded;

builder.Services.AddDbContext<BlogDb>(o => o.UseDocsql(cs));

var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();

// ---- 启动:懒建表(首个查询/调用即同步模型)+ 空库灌入演示数据 ----
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<BlogDb>();
    await SeedAsync(db);
}

app.MapGet("/api/stats", async (BlogDb db) => new
{
    authors = await db.Authors.CountAsync(),
    posts = await db.Posts.CountAsync(),
    comments = await db.Comments.CountAsync(),
    embedded,
    database = embedded
        ? new DocsqlConnectionStringBuilder { ConnectionString = cs }.DataSource
        : cs,
});

app.MapGet("/api/authors", async (BlogDb db) =>
    await db.Authors.OrderBy(a => a.Name).Select(a => new { a.Id, a.Name, a.Bio }).ToListAsync());

app.MapGet("/api/posts", async (BlogDb db, string? tag, string? q) =>
{
    var query = db.Posts.AsQueryable();
    if (!string.IsNullOrWhiteSpace(q))
    {
        var needle = q.Trim();
        query = query.Where(p => p.Title.Contains(needle) || p.Body.Contains(needle));
    }
    if (!string.IsNullOrWhiteSpace(tag))
    {
        var name = tag.Trim();
        // List<string> primitive collection → 引擎 JSON_ARRAY_CONTAINS,
        // 在数据侧过滤(不是把全部文章拉到内存再筛)。
        query = query.Where(p => p.Tags.Contains(name));
    }
    return await query
        .OrderByDescending(p => p.CreatedAt)
        .Select(p => new
        {
            p.Id,
            p.Title,
            p.Body,
            p.CreatedAt,
            Author = p.Author.Name,
            p.Tags,
            Comments = p.Comments.Count,
        })
        .ToListAsync();
});

app.MapGet("/api/posts/{id:int}", async (BlogDb db, int id) =>
{
    var post = await db.Posts
        .Include(p => p.Author)
        .Include(p => p.Comments)
        .FirstOrDefaultAsync(p => p.Id == id);
    return post is null
        ? Results.NotFound(new { error = $"文章 {id} 不存在" })
        : Results.Ok(new
        {
            post.Id,
            post.Title,
            post.Body,
            post.CreatedAt,
            Author = post.Author.Name,
            post.Tags,
            Comments = post.Comments
                .OrderBy(c => c.CreatedAt)
                .Select(c => new { c.Id, c.Text, c.CreatedAt })
                .ToList(),
        });
});

app.MapPost("/api/posts", async (BlogDb db, PostInput input) =>
{
    var title = input.Title?.Trim() ?? "";
    if (title.Length is 0 or > 200)
    {
        return Results.BadRequest(new { error = "标题不能为空且不超过 200 字" });
    }
    var authorName = (input.Author ?? "").Trim();
    if (authorName.Length == 0)
    {
        authorName = "匿名";
    }
    var author = await db.Authors.FirstOrDefaultAsync(a => a.Name == authorName);
    if (author is null)
    {
        // 唯一索引保证同一名字只有一位作者;并发下由引擎拒绝重复。
        author = new Author { Name = authorName, Bio = "" };
        db.Authors.Add(author);
    }
    var post = new Post
    {
        Title = title,
        Body = input.Body?.Trim() ?? "",
        CreatedAt = DateTime.UtcNow,
        Author = author,
    };
    foreach (var t in (input.Tags ?? Array.Empty<string>())
                 .Select(x => x.Trim())
                 .Where(x => x.Length is > 0 and <= 40)
                 .Distinct()
                 .Take(8))
    {
        post.Tags.Add(t);
    }
    db.Posts.Add(post);
    await db.SaveChangesAsync();
    return Results.Created($"/api/posts/{post.Id}", new { post.Id });
});

app.MapPost("/api/posts/{id:int}/comments", async (BlogDb db, int id, CommentInput input) =>
{
    var text = input.Text?.Trim() ?? "";
    if (text.Length is 0 or > 2000)
    {
        return Results.BadRequest(new { error = "评论不能为空且不超过 2000 字" });
    }
    var exists = await db.Posts.AnyAsync(p => p.Id == id);
    if (!exists)
    {
        return Results.NotFound(new { error = $"文章 {id} 不存在" });
    }
    var comment = new Comment { Text = text, CreatedAt = DateTime.UtcNow, PostId = id };
    db.Comments.Add(comment);
    await db.SaveChangesAsync();
    return Results.Created($"/api/posts/{id}", new { comment.Id });
});

app.Run();

// ---- 领域模型(与 BlogSample 控制台示例一致) ----

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

public record PostInput(string? Title, string? Body, string? Author, string[]? Tags);

public record CommentInput(string? Text);

public class BlogDb(DbContextOptions<BlogDb> options) : DbContext(options)
{
    public DbSet<Author> Authors => Set<Author>();
    public DbSet<Post> Posts => Set<Post>();
    public DbSet<Comment> Comments => Set<Comment>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Author>().HasIndex(a => a.Name).IsUnique().HasDatabaseName("IX_Authors_Name");
        b.Entity<Post>().HasIndex(p => p.AuthorId).HasDatabaseName("IX_Posts_AuthorId");
        b.Entity<Post>().HasIndex(p => p.CreatedAt).HasDatabaseName("IX_Posts_CreatedAt");
        b.Entity<Comment>().HasIndex(c => c.PostId).HasDatabaseName("IX_Comments_PostId");
    }
}

static partial class Program
{
    /// <summary>空库才灌演示数据,避免每次启动重置用户内容。</summary>
    public static async Task SeedAsync(BlogDb db)
    {
        if (await db.Authors.AnyAsync())
        {
            return;
        }
        var now = DateTime.UtcNow;
        var alice = new Author { Name = "alice", Bio = "写 .NET 与数据库" };
        var bob = new Author { Name = "bob", Bio = "写 Rust 与存储引擎" };
        alice.Posts.Add(new Post
        {
            Title = "DocSQL 入门:一个文件就是一个数据库",
            Body = "Data Source=blog.db,进程内引擎,零守护。没有 server、没有端口、没有 Docker;"
                 + "第一次查询时表与索引会按模型自动建好,不需要迁移脚本。",
            CreatedAt = now.AddDays(-2),
            Tags = { "dotnet", "embedded" },
        });
        alice.Posts.Add(new Post
        {
            Title = "EF Core 免迁移实践",
            Body = "模型变了就自动补表补列。索引随模型同步,DDL 由提供程序在首条命令前完成。",
            CreatedAt = now.AddDays(-1),
            Tags = { "dotnet", "ef" },
        });
        bob.Posts.Add(new Post
        {
            Title = "WAL 与快照读",
            Body = "先写日志再落页;读走 MVCC 快照,长查询不阻塞写入。",
            CreatedAt = now,
            Tags = { "rust", "storage" },
        });
        db.Authors.AddRange(alice, bob);
        await db.SaveChangesAsync();

        var intro = await db.Posts.OrderBy(p => p.CreatedAt).FirstAsync();
        db.Comments.AddRange(
            new Comment { PostId = intro.Id, Text = "这个模式很香", CreatedAt = now.AddHours(1) },
            new Comment { PostId = intro.Id, Text = "和 SQLite 一样省心", CreatedAt = now.AddHours(2) });
        await db.SaveChangesAsync();
    }
}
