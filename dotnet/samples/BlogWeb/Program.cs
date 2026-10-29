// DocSQL 博客前端示例:最小 API + 内置单页(wwwroot,零 npm)。
//
// 后端默认嵌入式:Data Source=blog.db(一个文件就是一个数据库,无 server、
// 无 Docker);也可以用 DOCSQL_CONN 指到已有节点(host=…;port=…)。
//
// 这个示例刻意当"功能导览"用:领域之外,/api/status 展示数据库状态
// (对象清单/行数/文件大小),/api/lab 逐项跑一遍引擎的 SQL 能力面
// (窗口/CTE/分组扩展/T-SQL/Oracle 兼容/JSON/DECIMAL/BLOB/计划),写路径
// 演示(upsert、事务+SAVEPOINT、物化视图 REFRESH、MERGE、触发器、auto-GUID)
// 各有对应端点,全部走真实引擎执行。
//
// 运行:
//   cd dotnet && dotnet run --project samples/BlogWeb
//   → http://127.0.0.1:17800(可用 BLOG_URLS 覆盖监听地址)
//
// 接口:
//   GET  /api/stats                  计数
//   GET  /api/status                 数据库状态(模式/对象/行数/文件/引擎)
//   GET  /api/lab                    功能面板(只读演示批,每项附 SQL 与结果)
//   POST /api/lab/action/{action}    写路径演示: refresh-matview | merge-stats |
//                                    tx-savepoint | trigger-fire
//   GET  /api/posts?tag=&q=          文章列表(标签/标题过滤)
//   GET  /api/posts/{id}             文章详情 + 评论(浏览数走 upsert +1)
//   GET  /api/posts/{id}/cover       BLOB 封面
//   POST /api/posts                  新建文章 {title, body, author, tags[]}
//   POST /api/posts/{id}/comments    新增评论 {text}(长评论触发审计触发器)
//   POST /api/posts/{id}/like        点赞(ON CONFLICT upsert 原子 +1)
//   GET  /api/subscribers            订阅列表(auto-GUID 主键,引擎填 UUIDv7)
//   POST /api/subscribers            新增订阅 {email}
//   GET  /api/authors                作者列表

using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Data.Common;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(Environment.GetEnvironmentVariable("BLOG_URLS") ?? "http://127.0.0.1:17800");
var startedAt = DateTime.UtcNow;

// ---- 连接:默认嵌入式文件;DOCSQL_CONN 可切到远程节点 ----
var cs = Environment.GetEnvironmentVariable("DOCSQL_CONN") is { Length: > 0 } remote
    ? remote
    : $"Data Source={Path.Combine(Environment.CurrentDirectory, "blog.db")}";
var embedded = new DocsqlConnectionStringBuilder { ConnectionString = cs }.Embedded;

builder.Services.AddDbContext<BlogDb>(o => o.UseDocsql(cs));

var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();

// ---- 启动:懒建表(首个 EF 命令即同步模型)→ 派生对象(视图/触发器等,
//      先于种子,长评论才能触发审计)→ 空库灌入演示数据 ----
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<BlogDb>();
    _ = await db.Authors.AnyAsync();
    await EnsureDerivedObjectsAsync(db);
    await SeedAsync(db);
}

// ============================================================
// 数据库状态:/api/status —— 模式、对象清单(表/视图/索引)、行数、
// 文件大小(嵌入式)、引擎信息。全部走系统视图(sqlite_master /
// information_schema / ALL_INDEXES),不读私有状态。
// ============================================================
app.MapGet("/api/status", async (BlogDb db) =>
{
    // 对象清单:sqlite_master 里视图与表同为 type='table',视图无存储 DDL
    // (sql 列为空),据此区分;物化视图有真实 CREATE TABLE,以表形态出现
    // (名字 mv_ 前缀在 UI 上注明)。内部保留名(docsql_*)也如实列出。
    var objs = await db.QueryAsync(
        "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite%' ORDER BY name");
    var tableNames = objs.Rows
        .Where(r => r[1] is string s && s.Length > 0)
        .Select(r => (string)r[0]!).ToList();
    var viewNames = objs.Rows
        .Where(r => r[1] is not string s || s.Length == 0)
        .Select(r => (string)r[0]!).ToList();
    var tables = new List<object>();
    foreach (var name in tableNames)
    {
        var rows = await db.QueryAsync($"SELECT COUNT(*) FROM {RawSql.Qi(name)}");
        tables.Add(new { name, rowCount = Convert.ToInt64(rows.Rows[0][0]) });
    }
    var indexes = await db.QueryAsync(
        "SELECT index_name, table_name, uniqueness FROM ALL_INDEXES ORDER BY index_name");

    // 存储面(仅嵌入式:远程节点的文件在节点侧,这里不猜)。
    object? storage = null;
    if (embedded)
    {
        var path = new DocsqlConnectionStringBuilder { ConnectionString = cs }.DataSource;
        var data = new FileInfo(path);
        var wal = new FileInfo(path + ".wal");
        storage = new
        {
            dataFileBytes = data.Exists ? data.Length : 0L,
            walBytes = wal.Exists ? wal.Length : 0L,
        };
    }

    var dbName = (string)(await db.QueryAsync("SELECT DB_NAME()")).Rows[0][0]!;
    var subscribers = Convert.ToInt64((await db.QueryAsync("SELECT COUNT(*) FROM subscriptions")).Rows[0][0]);

    return new
    {
        embedded,
        database = embedded
            ? new DocsqlConnectionStringBuilder { ConnectionString = cs }.DataSource
            : cs,
        engine = new
        {
            name = "DocSQL",
            dbName,
            clientVersion = typeof(DocsqlConnection).Assembly.GetName().Version?.ToString(),
            uptimeSeconds = (long)(DateTime.UtcNow - startedAt).TotalSeconds,
        },
        objects = new
        {
            tables,
            views = viewNames,
            indexes = indexes.Rows
                .Select(r => new { name = r[0]?.ToString(), table = r[1]?.ToString(), uniqueness = r[2]?.ToString() })
                .ToList(),
        },
        storage,
        subscribers,
    };
});

// ============================================================
// 功能面板:/api/lab —— 只读演示批。每项 = 特性名 + 原文 SQL + 结果。
// 单项失败不影响整批(错误项带 error 字段,面板照常展示)。
// ============================================================
app.MapGet("/api/lab", async (BlogDb db) =>
{
    var items = new List<object>();
    foreach (var (id, group, feature, sql) in LabQueries.All)
    {
        try
        {
            var r = await db.QueryAsync(sql);
            items.Add(new { id, group, feature, sql, columns = r.Columns, rows = r.Rows });
        }
        catch (Exception ex)
        {
            items.Add(new { id, group, feature, sql, error = ex.Message });
        }
    }
    return items;
});

// ---- 写路径演示(每个动作 = 一项引擎能力的真实执行) ----
app.MapPost("/api/lab/action/{action}", async (BlogDb db, string action) =>
{
    switch (action)
    {
        // 物化视图:真实表存储,基表变化不自动传播,REFRESH 单语句原子重算。
        case "refresh-matview":
        {
            await db.ExecAsync("REFRESH MATERIALIZED VIEW mv_author_stats");
            var r = await db.QueryAsync("SELECT * FROM mv_author_stats");
            return Results.Ok(new { action, note = "REFRESH MATERIALIZED VIEW 完成", result = r.ToRaw() });
        }

        // MERGE:按源聚合 upsert 作者统计(存在更新、缺失插入)。
        case "merge-stats":
        {
            await db.ExecAsync(
                "MERGE INTO author_stats t USING (SELECT author_id, COUNT(*) AS c FROM posts GROUP BY author_id) s " +
                "ON t.author_id = s.author_id " +
                "WHEN MATCHED THEN UPDATE SET posts = s.c " +
                "WHEN NOT MATCHED THEN INSERT (author_id, posts) VALUES (s.author_id, s.c)");
            var r = await db.QueryAsync("SELECT * FROM author_stats ORDER BY author_id");
            return Results.Ok(new { action, note = "MERGE INTO author_stats 完成", result = r.ToRaw() });
        }

        // 事务 + SAVEPOINT:+10 提交、清零段 ROLLBACK TO 丢弃 —— 引擎事务语义。
        case "tx-savepoint":
        {
            var pidRow = await db.QueryAsync("SELECT MIN(id) FROM posts");
            if (pidRow.Rows.Length == 0 || pidRow.Rows[0][0] is null || Convert.ToInt64(pidRow.Rows[0][0]) == 0)
            {
                return Results.BadRequest(new { error = "库里还没有文章" });
            }
            var pid = Convert.ToInt64(pidRow.Rows[0][0]);
            await db.ExecAsync(
                "INSERT INTO post_stats (post_id, views, likes) VALUES (@id, 0, 0) ON CONFLICT (post_id) DO NOTHING",
                ("@id", pid));
            var before = Convert.ToInt64(
                (await db.QueryAsync("SELECT likes FROM post_stats WHERE post_id = @id", ("@id", pid))).Rows[0][0]);

            var conn = await db.OpenAsync();
            await using var tx = await conn.BeginTransactionAsync();
            var demo = await TxSavepointDemoAsync(conn, tx, pid, before);
            return Results.Ok(demo);
        }

        // 触发器:插入一条长评论,行级触发器把它记进 audit_log。
        case "trigger-fire":
        {
            var pidRow = await db.QueryAsync("SELECT MIN(id) FROM posts");
            if (pidRow.Rows.Length == 0 || pidRow.Rows[0][0] is null || Convert.ToInt64(pidRow.Rows[0][0]) == 0)
            {
                return Results.BadRequest(new { error = "库里还没有文章" });
            }
            var pid = Convert.ToInt64(pidRow.Rows[0][0]);
            var longText = "这是一条超过五十个字符的长评论,用于触发行级触发器:任何超过五十字的评论都会被自动记入审计表 audit_log,语句级原子性保证评论与审计同生共死。";
            await db.ExecAsync(
                "INSERT INTO comments (text, created_at, post_id) VALUES (@text, NOW(), @id)",
                ("@text", longText), ("@id", pid));
            var audit = await db.QueryAsync("SELECT * FROM audit_log ORDER BY id DESC LIMIT 3");
            return Results.Ok(new { action, note = "插入长评论,触发器自动写入 audit_log", result = audit.ToRaw() });
        }

        default:
            return Results.BadRequest(new { error = $"未知演示动作 {action}" });
    }
});

// ============================================================
// 点赞 = ON CONFLICT upsert(冲突行原位 +1,一次往返,无读改写竞态)
// ============================================================
app.MapPost("/api/posts/{id:int}/like", async (BlogDb db, int id) =>
{
    // 两列都显式给值:EF 建的统计表 NOT NULL 无默认,缺列插入会被拒。
    await db.ExecAsync(
        "INSERT INTO post_stats (post_id, views, likes) VALUES (@id, 0, 1) " +
        "ON CONFLICT (post_id) DO UPDATE SET likes = likes + 1",
        ("@id", id));
    var likes = Convert.ToInt64(
        (await db.QueryAsync("SELECT likes FROM post_stats WHERE post_id = @id", ("@id", id))).Rows[0][0]);
    return Results.Ok(new { id, likes });
});

// BLOB 封面:引擎原生 Value::Bytes,经 $bytes 标记往返。
app.MapGet("/api/posts/{id:int}/cover", async (BlogDb db, int id) =>
{
    var r = await db.QueryAsync("SELECT cover FROM posts WHERE id = @id", ("@id", id));
    return r.Rows.Length > 0 && r.Rows[0][0] is byte[] bytes && bytes.Length > 0
        ? Results.Bytes(bytes, "image/svg+xml")
        : Results.NotFound();
});

// ============================================================
// 订阅:表由原始 DDL 创建(id UNIQUEIDENTIFIER AUTOINCREMENT),
// INSERT 省略主键 → 引擎自动生成 UUIDv7(时序有序)。
// ============================================================
app.MapGet("/api/subscribers", async (BlogDb db) =>
    Results.Ok((await db.QueryAsync(
        "SELECT id, email, created_at FROM subscriptions ORDER BY created_at")).ToRaw()));

app.MapPost("/api/subscribers", async (BlogDb db, SubscriberInput input) =>
{
    var email = (input.Email ?? "").Trim();
    if (email.Length == 0 || !email.Contains('@') || email.Length > 200)
    {
        return Results.BadRequest(new { error = "邮箱格式不对" });
    }
    try
    {
        // created_at 显式 NOW():写路径折叠为字面量,复制/回放确定值。
        await db.ExecAsync(
            "INSERT INTO subscriptions (email, created_at) VALUES (@email, NOW())",
            ("@email", email));
    }
    catch (Exception ex) when (ex.Message.Contains("UNIQUE"))
    {
        return Results.Conflict(new { error = "这个邮箱已经订阅过了" });
    }
    var id = (string)(await db.QueryAsync("SELECT id FROM subscriptions WHERE email = @email",
        ("@email", email))).Rows[0][0]!;
    return Results.Created("/api/subscribers", new { id, email, generatedBy = "引擎 auto-GUID(UUIDv7)" });
});

// ---- 博客本体(与之前一致) ----

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
    // 浏览计数走 upsert(无行则建),DO UPDATE 普通自增形式。
    await db.ExecAsync(
        "INSERT INTO post_stats (post_id, views, likes) VALUES (@id, 1, 0) " +
        "ON CONFLICT (post_id) DO UPDATE SET views = views + 1",
        ("@id", id));
    var stats = await db.QueryAsync(
        "SELECT views, likes FROM post_stats WHERE post_id = @id", ("@id", id));
    var views = stats.Rows.Length > 0 ? Convert.ToInt64(stats.Rows[0][0]) : 0;
    var likes = stats.Rows.Length > 0 ? Convert.ToInt64(stats.Rows[0][1]) : 0;

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
            post.Payout,
            post.Meta,
            Author = post.Author.Name,
            post.Tags,
            Views = views,
            Likes = likes,
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

// ---- 领域模型(Post 扩展三个类型演示列:Payout=DECIMAL、Meta=JSON 文本、Cover=BLOB) ----

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

    // 类型演示:DECIMAL 精确标量($dec 标记,十进制语义)、JSON 文本
    // (JSON_EXTRACT 演示数据)、BLOB($bytes 标记,封面 SVG)。
    public decimal? Payout { get; set; }
    public string? Meta { get; set; }
    public byte[]? Cover { get; set; }
}

public class Comment
{
    public int Id { get; set; }
    public string Text { get; set; } = "";
    public DateTime CreatedAt { get; set; }
    public int PostId { get; set; }
    public Post Post { get; set; } = null!;
}

// upsert/浏览计数/点赞的目标表(与 posts 的关系用裸 SQL 维护,不进导航)。
public class PostStat
{
    public int PostId { get; set; }
    public int Views { get; set; }
    public int Likes { get; set; }
}

public record PostInput(string? Title, string? Body, string? Author, string[]? Tags);

public record CommentInput(string? Text);

public record SubscriberInput(string? Email);

public class BlogDb(DbContextOptions<BlogDb> options) : DbContext(options)
{
    public DbSet<Author> Authors => Set<Author>();
    public DbSet<Post> Posts => Set<Post>();
    public DbSet<Comment> Comments => Set<Comment>();
    public DbSet<PostStat> PostStats => Set<PostStat>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        // 表/列名显式 snake_case:功能面板展示的原文 SQL 与库内名字一致可读
        // (EF 默认 PascalCase 引号名会让裸 SQL 对不上)。
        b.Entity<Author>(e =>
        {
            e.ToTable("authors");
            e.HasKey(a => a.Id);
            e.Property(a => a.Id).HasColumnName("id");
            e.Property(a => a.Name).HasColumnName("name");
            e.Property(a => a.Bio).HasColumnName("bio");
            e.HasIndex(a => a.Name).IsUnique().HasDatabaseName("IX_Authors_Name");
        });
        b.Entity<Post>(e =>
        {
            e.ToTable("posts");
            e.HasKey(p => p.Id);
            e.Property(p => p.Id).HasColumnName("id");
            e.Property(p => p.Title).HasColumnName("title");
            e.Property(p => p.Body).HasColumnName("body");
            e.Property(p => p.CreatedAt).HasColumnName("created_at");
            e.Property(p => p.AuthorId).HasColumnName("author_id");
            e.Property(p => p.Tags).HasColumnName("tags");
            e.Property(p => p.Payout).HasColumnName("payout");
            e.Property(p => p.Meta).HasColumnName("meta");
            e.Property(p => p.Cover).HasColumnName("cover");
            e.HasIndex(p => p.AuthorId).HasDatabaseName("IX_Posts_AuthorId");
            e.HasIndex(p => p.CreatedAt).HasDatabaseName("IX_Posts_CreatedAt");
            e.HasOne(p => p.Author).WithMany(a => a.Posts).HasForeignKey(p => p.AuthorId);
        });
        b.Entity<Comment>(e =>
        {
            e.ToTable("comments");
            e.HasKey(c => c.Id);
            e.Property(c => c.Id).HasColumnName("id");
            e.Property(c => c.Text).HasColumnName("text");
            e.Property(c => c.CreatedAt).HasColumnName("created_at");
            e.Property(c => c.PostId).HasColumnName("post_id");
            e.HasIndex(c => c.PostId).HasDatabaseName("IX_Comments_PostId");
            e.HasOne(c => c.Post).WithMany(p => p.Comments).HasForeignKey(c => c.PostId);
        });
        b.Entity<PostStat>(e =>
        {
            e.ToTable("post_stats");
            e.HasKey(s => s.PostId); // upsert 目标表:文章一比一统计
            e.Property(s => s.PostId).HasColumnName("post_id");
            e.Property(s => s.Views).HasColumnName("views");
            e.Property(s => s.Likes).HasColumnName("likes");
        });
    }
}

// ---- 原始 SQL 助手(状态/功能面板/写路径演示共用) ----

public record RawResult(string[] Columns, object?[][] Rows);

public static class RawSql
{
    /// <summary>命名参数(@name)→ 引擎 qmark 服务端绑定;decimal/byte[]/DateTime
    /// 走 $dec/$bytes/$ts 标记,精确类型不降级。</summary>
    public static async Task<RawResult> QueryAsync(this BlogDb db, string sql, params (string Name, object? Value)[] args)
    {
        await using var cmd = await CommandAsync(db, sql, args);
        await using var r = await cmd.ExecuteReaderAsync();
        var cols = Enumerable.Range(0, r.FieldCount).Select(r.GetName).ToArray();
        var rows = new List<object?[]>();
        while (await r.ReadAsync())
        {
            var row = new object?[r.FieldCount];
            r.GetValues(row);
            rows.Add(row);
        }
        return new RawResult(cols, rows.ToArray());
    }

    public static async Task<int> ExecAsync(this BlogDb db, string sql, params (string Name, object? Value)[] args)
    {
        await using var cmd = await CommandAsync(db, sql, args);
        return await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>取底层连接(事务演示需要显式 BeginTransaction)。</summary>
    public static async Task<DocsqlConnection> OpenAsync(this BlogDb db)
    {
        var conn = (DocsqlConnection)db.Database.GetDbConnection();
        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync();
        }
        return conn;
    }

    /// <summary>标识符加引号(系统视图读出的表名回查 COUNT 用)。</summary>
    public static string Qi(string ident) => "\"" + ident.Replace("\"", "\"\"") + "\"";

    private static async Task<DocsqlCommand> CommandAsync(
        BlogDb db, string sql, (string Name, object? Value)[] args)
    {
        var conn = await db.OpenAsync();
        var cmd = (DocsqlCommand)conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in args)
        {
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        }
        return cmd;
    }
}

file static class RawExtensions
{
    public static object ToRaw(this RawResult r) => new { columns = r.Columns, rows = r.Rows };
}

// ---- 功能面板查询批(全部只读;SQL 原文展示在 UI 上) ----

public static class LabQueries
{
    public static readonly (string Id, string Group, string Feature, string Sql)[] All =
    [
        // ---- 窗口与排名 ----
        ("window-qualify", "窗口与排名", "每位作者最新一篇(ROW_NUMBER + QUALIFY)",
            "SELECT p.title, p.author_id, ROW_NUMBER() OVER (PARTITION BY p.author_id ORDER BY p.created_at DESC) AS rn FROM posts p QUALIFY ROW_NUMBER() OVER (PARTITION BY p.author_id ORDER BY p.created_at DESC) <= 1"),
        ("window-rank", "窗口与排名", "作者发文榜(DENSE_RANK,聚合包派生表)",
            "SELECT name, posts, DENSE_RANK() OVER (ORDER BY posts DESC) AS rank FROM (SELECT a.name AS name, COUNT(p.id) AS posts FROM authors a LEFT JOIN posts p ON p.author_id = a.id GROUP BY a.name) t"),
        ("window-frame", "窗口与排名", "自定义帧:滑动邻域(ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
            "SELECT title, COUNT(*) OVER (ORDER BY created_at ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS neighborhood FROM posts"),
        ("window-lag", "窗口与排名", "LAG(上一篇标题)",
            "SELECT title, LAG(title) OVER (ORDER BY created_at) AS prev_title FROM posts"),
        ("window-ntile", "窗口与排名", "NTILE 分桶 + FIRST_VALUE",
            "SELECT title, NTILE(2) OVER (ORDER BY created_at) AS bucket, FIRST_VALUE(title) OVER (ORDER BY created_at) AS earliest FROM posts"),
        ("window-interval", "窗口与排名", "RANGE 帧按 INTERVAL(近 1 天发文累计)",
            "SELECT title, COUNT(*) OVER (ORDER BY created_at RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS recent FROM posts"),

        // ---- CTE 与分组 ----
        ("cte", "CTE 与分组", "非递归 CTE(按年月统计,DATEPART 原生表达式分组)",
            "WITH m AS (SELECT DATEPART(year, created_at) AS y, DATEPART(month, created_at) AS mo, COUNT(*) AS c FROM posts GROUP BY DATEPART(year, created_at), DATEPART(month, created_at)) SELECT * FROM m"),
        ("cte-recursive", "CTE 与分组", "递归 CTE(半朴素迭代,数字序列 1..8)",
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 8) SELECT n FROM seq"),
        ("groupby-rollup", "CTE 与分组", "GROUP BY ROLLUP + GROUPING()(小计/总计行)",
            "SELECT a.name, DATEPART(year, p.created_at) AS y, COUNT(*) AS c, GROUPING(a.name) AS g_name FROM authors a JOIN posts p ON p.author_id = a.id GROUP BY ROLLUP(a.name, DATEPART(year, p.created_at))"),
        ("grouping-sets", "CTE 与分组", "GROUPING SETS(仅总计行)",
            "SELECT a.name, COUNT(*) AS c, GROUPING(a.name) AS g FROM authors a JOIN posts p ON p.author_id = a.id GROUP BY GROUPING SETS ((a.name), ())"),
        ("agg-filter", "CTE 与分组", "聚合 FILTER + DECIMAL 求和",
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE payout IS NOT NULL) AS paid, SUM(payout) AS payout_total FROM posts"),
        ("string-agg", "CTE 与分组", "STRING_AGG(作者全部标题串联)",
            "SELECT a.name, STRING_AGG(p.title, ' | ') AS titles FROM authors a JOIN posts p ON p.author_id = a.id GROUP BY a.name"),

        // ---- 子查询与谓词 ----
        ("subquery-correlated", "子查询与谓词", "相关子查询(每篇文章评论数,逐行求值)",
            "SELECT p.title, (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) AS comments FROM posts p ORDER BY comments DESC LIMIT 3"),
        ("subquery-exists", "子查询与谓词", "EXISTS(发过文章的作者)",
            "SELECT a.name FROM authors a WHERE EXISTS (SELECT 1 FROM posts p WHERE p.author_id = a.id)"),
        ("quantified-any", "子查询与谓词", "量化比较 > ANY(标题比最短者长)",
            "SELECT title FROM posts WHERE LENGTH(title) > ANY (SELECT LENGTH(title) FROM posts WHERE author_id = (SELECT MIN(id) FROM authors))"),
        ("distinct-from", "子查询与谓词", "IS DISTINCT FROM(NULL 安全比较)",
            "SELECT title, payout FROM posts WHERE payout IS DISTINCT FROM NULL"),
        ("set-except", "子查询与谓词", "集合运算 EXCEPT(没发过文章的作者)",
            "SELECT name FROM authors EXCEPT SELECT a.name FROM authors a JOIN posts p ON p.author_id = a.id"),
        ("ilike", "子查询与谓词", "ILIKE 大小写不敏感匹配",
            "SELECT title FROM posts WHERE title ILIKE 'docsql%'"),

        // ---- JSON 与精确标量 ----
        ("json-extract", "JSON 与精确标量", "JSON_EXTRACT / JSON_TYPE / JSON_ARRAY_CONTAINS",
            "SELECT title, JSON_EXTRACT(meta, '$.reading_time') AS reading_time, JSON_TYPE(tags) AS tags_type FROM posts WHERE JSON_ARRAY_CONTAINS(tags, 'dotnet')"),
        ("json-openjson", "JSON 与精确标量", "CROSS APPLY OPENJSON(标签数组展开)",
            "SELECT p.title, j.[key] AS tag_index, j.value AS tag FROM posts p CROSS APPLY OPENJSON(p.tags) j"),
        ("decimal", "JSON 与精确标量", "DECIMAL 精确聚合(rust_decimal,不掉精度)",
            "SELECT SUM(payout) AS total, AVG(payout) AS average, MAX(payout) AS max_payout FROM posts"),
        ("blob", "JSON 与精确标量", "BLOB 封面($bytes 往返,LENGTH 字节数)",
            "SELECT title, LENGTH(cover) AS cover_bytes FROM posts WHERE cover IS NOT NULL"),

        // ---- T-SQL 兼容面 ----
        ("tsql-top", "T-SQL 兼容面", "SELECT TOP(服务端限量)",
            "SELECT TOP 3 title, created_at FROM posts ORDER BY created_at DESC"),
        ("tsql-convert", "T-SQL 兼容面", "CONVERT 日期 style + IIF",
            "SELECT CONVERT(VARCHAR(10), created_at, 23) AS day, IIF(payout IS NULL, '未设置', '已设置') AS payout_state FROM posts"),
        ("tsql-series", "T-SQL 兼容面", "表值函数 GENERATE_SERIES",
            "SELECT g.value AS n FROM GENERATE_SERIES(1, 5) g"),
        ("tsql-pivot", "T-SQL 兼容面", "PIVOT(按月透视发文数)",
            "SELECT name, [9] AS sep, [10] AS oct FROM (SELECT a.name AS name, DATEPART(month, p.created_at) AS m, p.id AS pid FROM authors a JOIN posts p ON p.author_id = a.id) src PIVOT (COUNT(pid) FOR m IN (9, 10)) pv"),
        ("tsql-hashbytes", "T-SQL 兼容面", "HASHBYTES(SHA2_256 → BLOB)",
            "SELECT title, HASHBYTES('SHA2_256', title) AS sha256 FROM posts LIMIT 2"),

        // ---- Oracle 兼容 ----
        ("oracle", "Oracle 兼容", "NVL / DECODE / GREATEST / ROWNUM",
            "SELECT NVL(payout, 0) AS payout, DECODE(author_id, 1, 'first', 2, 'second', 'other') AS grp, GREATEST(1, 5, 3) AS greatest, ROWNUM FROM posts WHERE ROWNUM <= 3"),

        // ---- 自省与计划 ----
        ("explain", "自省与计划", "EXPLAIN SELECT(与执行器共享的计划前缀)",
            "EXPLAIN SELECT id, title FROM posts WHERE author_id = 1 ORDER BY created_at DESC LIMIT 3"),
        ("typeof", "自省与计划", "TYPEOF 动态类型自省 + DB_NAME()",
            "SELECT TYPEOF(payout) AS payout_type, TYPEOF(cover) AS cover_type, TYPEOF(tags) AS tags_type, DB_NAME() AS db_name FROM posts LIMIT 1"),
        ("view", "自省与计划", "视图查询(v_paid_posts,SELECT 穿透)",
            "SELECT * FROM v_paid_posts"),
        ("matview", "自省与计划", "物化视图查询(mv_author_stats,真实表存储)",
            "SELECT * FROM mv_author_stats"),
        ("unpivot", "自省与计划", "UNPIVOT(统计表列转行)",
            "SELECT post_id, stat, val FROM (SELECT post_id, CAST(views AS TEXT) AS v, CAST(likes AS TEXT) AS l FROM post_stats) s UNPIVOT (val FOR stat IN (v, l)) u"),
        ("ts-arith", "自省与计划", "TIMESTAMP ±毫秒算术",
            "SELECT title, created_at + 86400000 AS tomorrow_utc FROM posts LIMIT 2"),
        ("info-schema", "自省与计划", "ALL_INDEXES 索引自省",
            "SELECT i.index_name, i.table_name, i.uniqueness FROM ALL_INDEXES i ORDER BY i.index_name"),
    ];
}

// ---- 种子数据与派生对象 ----

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
        var carol = new Author { Name = "carol", Bio = "还没发文(集合运算 EXCEPT 的演示数据)" };
        alice.Posts.Add(new Post
        {
            Title = "DocSQL 入门:一个文件就是一个数据库",
            Body = "Data Source=blog.db,进程内引擎,零守护。没有 server、没有端口、没有 Docker;"
                 + "第一次查询时表与索引会按模型自动建好,不需要迁移脚本。",
            CreatedAt = now.AddDays(-35), // 跨月:PIVOT 按月透视有两列非零
            Tags = { "dotnet", "embedded" },
            Payout = 12.50m,
            Meta = """{"reading_time":5,"lang":"zh","featured":true}""",
            Cover = CoverSvg("DocSQL", "#2563eb"),
        });
        alice.Posts.Add(new Post
        {
            Title = "EF Core 免迁移实践",
            Body = "模型变了就自动补表补列。索引随模型同步,DDL 由提供程序在首条命令前完成。",
            CreatedAt = now.AddDays(-1),
            Tags = { "dotnet", "ef" },
            Payout = 3.75m,
            Meta = """{"reading_time":2,"lang":"zh"}""",
            Cover = CoverSvg("EF Core", "#7c3aed"),
        });
        bob.Posts.Add(new Post
        {
            Title = "WAL 与快照读",
            Body = "先写日志再落页;读走 MVCC 快照,长查询不阻塞写入。",
            CreatedAt = now,
            Tags = { "rust", "storage" },
            Payout = null, // IS DISTINCT FROM / NVL / CONVERT 演示的 NULL 数据
            Meta = """{"reading_time":8,"lang":"zh"}""",
            Cover = CoverSvg("WAL", "#059669"),
        });
        db.Authors.AddRange(alice, bob, carol);
        await db.SaveChangesAsync();

        var intro = await db.Posts.OrderBy(p => p.CreatedAt).FirstAsync();
        db.Comments.AddRange(
            new Comment { PostId = intro.Id, Text = "这个模式很香", CreatedAt = now.AddHours(1) },
            new Comment { PostId = intro.Id, Text = "和 SQLite 一样省心", CreatedAt = now.AddHours(2) },
            // 超过 50 字:种下时就触发一次长评论审计触发器(audit_log 有数据)。
            new Comment
            {
                PostId = intro.Id,
                Text = "长评论样本:嵌入式形态下数据就落在当前目录的一个文件里,拷走这个文件就是完整的备份,"
                     + "没有额外的导出步骤,这一点和 SQLite 的心智模型完全一致。",
                CreatedAt = now.AddHours(3),
            });
        await db.SaveChangesAsync();

        // 首篇文章的统计行(upsert 的落点)。
        await db.ExecAsync(
            "INSERT INTO post_stats (post_id, views, likes) VALUES (@id, 1, 0) ON CONFLICT (post_id) DO NOTHING",
            ("@id", intro.Id));
    }

    /// <summary>派生对象:视图/物化视图/触发器/部分索引/auto-GUID 表。
    /// 每次启动幂等执行(旧库升级也能补齐);这些 DDL 同时演示引擎的 DDL 面。
    /// 部分索引不带 IX_ 前缀 —— EF SchemaSync 只回收自己模型的 IX_ 索引。</summary>
    public static async Task EnsureDerivedObjectsAsync(BlogDb db)
    {
        await db.ExecAsync(
            "CREATE TABLE IF NOT EXISTS subscriptions (" +
            "id UNIQUEIDENTIFIER AUTOINCREMENT PRIMARY KEY, email TEXT NOT NULL UNIQUE, created_at TIMESTAMP)");
        await db.ExecAsync(
            "CREATE TABLE IF NOT EXISTS audit_log (" +
            "id INT AUTOINCREMENT PRIMARY KEY, kind TEXT NOT NULL, detail TEXT)");
        await db.ExecAsync(
            "CREATE TABLE IF NOT EXISTS author_stats (" +
            "author_id INT PRIMARY KEY, posts INT NOT NULL DEFAULT 0)");

        await db.ExecAsync("DROP TRIGGER IF EXISTS trg_long_comment");
        await db.ExecAsync(
            "CREATE TRIGGER trg_long_comment AFTER INSERT ON comments FOR EACH ROW " +
            "WHEN (LENGTH(NEW.text) > 50) " +
            "BEGIN INSERT INTO audit_log (kind, detail) VALUES ('long_comment', CAST(NEW.id AS TEXT)); END");

        await db.ExecAsync(
            "CREATE OR REPLACE VIEW v_paid_posts AS " +
            "SELECT id, title, payout FROM posts WHERE payout IS NOT NULL");

        var hasMatview = (await db.QueryAsync(
                "SELECT name FROM sqlite_master WHERE name = 'mv_author_stats'")).Rows.Length > 0;
        if (!hasMatview)
        {
            // 物化视图查询要求确定性(禁 NEWID/RAND/墙钟)——REFRESH 每节点重放。
            await db.ExecAsync(
                "CREATE MATERIALIZED VIEW mv_author_stats AS " +
                "SELECT a.name, COUNT(p.id) AS posts FROM authors a LEFT JOIN posts p ON p.author_id = a.id " +
                "GROUP BY a.name");
        }

        await db.ExecAsync(
            "CREATE INDEX IF NOT EXISTS idx_comments_long ON comments(post_id) WHERE LENGTH(text) > 50");

        // 演示订阅者一行(唯一冲突静默跳过,幂等)。
        await db.ExecAsync(
            "INSERT INTO subscriptions (email, created_at) VALUES ('reader@docsql.dev', NOW()) " +
            "ON CONFLICT DO NOTHING");
    }

    private static byte[] CoverSvg(string label, string color) =>
        Encoding.UTF8.GetBytes(
            "<svg xmlns='http://www.w3.org/2000/svg' width='320' height='96'>" +
            $"<rect width='100%' height='100%' fill='{color}' rx='8'/>" +
            $"<text x='50%' y='54%' fill='white' font-size='26' font-family='sans-serif' " +
            $"text-anchor='middle' dominant-baseline='middle'>{label}</text></svg>");

    /// <summary>事务+SAVEPOINT 演示:+10 提交、清零段 ROLLBACK TO 丢弃,
    /// 全程在一个引擎事务里,最后 COMMIT。返回每一步的 likes 值。</summary>
    private static async Task<object> TxSavepointDemoAsync(
        DocsqlConnection conn, DbTransaction tx, long postId, long before)
    {
        async Task<long> LikesInTx()
        {
            await using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = $"SELECT likes FROM post_stats WHERE post_id = {postId}";
            return Convert.ToInt64(await cmd.ExecuteScalarAsync() ?? 0L);
        }

        async Task Exec(string sql)
        {
            await using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }

        await Exec($"UPDATE post_stats SET likes = likes + 10 WHERE post_id = {postId}");
        var afterPlus10 = await LikesInTx();
        await Exec("SAVEPOINT demo_sp");
        await Exec($"UPDATE post_stats SET likes = 0 WHERE post_id = {postId}");
        var insideSavepoint = await LikesInTx();
        await Exec("ROLLBACK TO demo_sp");
        var afterRollback = await LikesInTx();
        await tx.CommitAsync();
        return new
        {
            action = "tx-savepoint",
            note = "BEGIN → +10 → SAVEPOINT → 清零 → ROLLBACK TO → COMMIT",
            before,
            afterPlus10,
            insideSavepoint,
            afterRollback,
            committedFinal = await LikesCommitted(conn, postId),
        };
    }

    private static async Task<long> LikesCommitted(DocsqlConnection conn, long postId)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT likes FROM post_stats WHERE post_id = {postId}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync() ?? 0L);
    }
}
