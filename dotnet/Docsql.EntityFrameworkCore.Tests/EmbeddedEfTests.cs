// EF Core 提供程序在嵌入式模式(Data Source=app.db)下的行为:与远程
// ServerFixture 的套件相互独立 —— 这里不起进程、不占端口,全部语句走
// crates/docsql-ffi 的进程内引擎(原生库由 cargo build -p docsql-ffi 产出)。
//
// 覆盖:懒建表/索引同步(提供程序的拦截器)、LINQ 读写、两个上下文共享
// 同一文件、事务回滚、连接串透传。

using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Docsql.EntityFrameworkCore.Tests;

public class EmbeddedTodo
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public bool Done { get; set; }
}

public class EmbeddedTodoDb : DbContext
{
    private readonly string _cs;

    public EmbeddedTodoDb(string cs) => _cs = cs;

    public DbSet<EmbeddedTodo> Todos => Set<EmbeddedTodo>();

    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);

    protected override void OnModelCreating(ModelBuilder b) =>
        b.Entity<EmbeddedTodo>().HasIndex(t => t.Title).HasDatabaseName("IX_EmbeddedTodos_Title");
}

public sealed class EmbeddedEfTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("docsql-ef-embed-").FullName;

    private string NewDb([System.Runtime.CompilerServices.CallerMemberName] string name = "") =>
        Path.Combine(_dir, name + ".db");

    private static string Cs(string path) => $"Data Source={path}";

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public async Task LazySchema_And_LinqCrud()
    {
        var cs = Cs(NewDb());
        await using (var db = new EmbeddedTodoDb(cs))
        {
            // No EnsureCreated / no Migrations: the provider's interceptor
            // creates the table + index before the first command.
            db.Todos.Add(new EmbeddedTodo { Title = "写方案", Done = false });
            db.Todos.Add(new EmbeddedTodo { Title = "快速上手", Done = true });
            Assert.Equal(2, await db.SaveChangesAsync());
        }
        await using (var db = new EmbeddedTodoDb(cs))
        {
            var pending = await db.Todos.Where(t => !t.Done).OrderBy(t => t.Id).ToListAsync();
            Assert.Single(pending);
            Assert.Equal("写方案", pending[0].Title);

            var first = await db.Todos.SingleAsync(t => t.Title == "快速上手");
            first.Done = false;
            await db.SaveChangesAsync();
        }
        await using (var db = new EmbeddedTodoDb(cs))
        {
            Assert.Equal(0, await db.Todos.CountAsync(t => t.Done));
        }
    }

    [Fact]
    public async Task TwoContexts_OnOneFile_SeeEachOthersData()
    {
        var cs = Cs(NewDb());
        await using (var a = new EmbeddedTodoDb(cs))
        {
            a.Todos.Add(new EmbeddedTodo { Title = "shared" });
            await a.SaveChangesAsync();
        }
        await using var b = new EmbeddedTodoDb(cs);
        Assert.Equal("shared", await b.Todos.Select(t => t.Title).SingleAsync());
    }

    [Fact]
    public async Task Transaction_RollbackDiscardsSaveChanges()
    {
        var cs = Cs(NewDb());
        await using var db = new EmbeddedTodoDb(cs);
        db.Todos.Add(new EmbeddedTodo { Title = "keep" });
        await db.SaveChangesAsync();

        await using (var tx = await db.Database.BeginTransactionAsync())
        {
            db.Todos.Add(new EmbeddedTodo { Title = "drop" });
            await db.SaveChangesAsync();
            await tx.RollbackAsync();
        }
        await using var fresh = new EmbeddedTodoDb(cs);
        Assert.Equal(1, await fresh.Todos.CountAsync());
    }

    [Fact]
    public async Task ConnectionString_FlowsThrough_Unchanged()
    {
        var cs = Cs(NewDb());
        await using var db = new EmbeddedTodoDb(cs);
        Assert.Equal(cs, db.Database.GetConnectionString());
        Assert.Equal("Docsql.EntityFrameworkCore", db.Database.ProviderName);
        Assert.Equal(0, await db.Todos.CountAsync());
    }

    [Fact]
    public async Task AsyncEnumeration_And_Projection_WorkOverTheEngine()
    {
        var cs = Cs(NewDb());
        await using var db = new EmbeddedTodoDb(cs);
        db.Todos.AddRange(
            new EmbeddedTodo { Title = "a", Done = true },
            new EmbeddedTodo { Title = "b", Done = true },
            new EmbeddedTodo { Title = "c", Done = false });
        await db.SaveChangesAsync();

        var titles = new List<string>();
        await foreach (var t in db.Todos
                           .Where(t => t.Done)
                           .OrderBy(t => t.Title)
                           .Select(t => t.Title)
                           .AsAsyncEnumerable())
        {
            titles.Add(t);
        }
        Assert.Equal(new[] { "a", "b" }, titles);
    }

    [Fact]
    public async Task UniqueIndex_Violation_SurfacesAsSaveChangesFailure()
    {
        var cs = Cs(NewDb());
        await using (var db = new EmbeddedUserDb(cs))
        {
            db.Users.Add(new EmbeddedUser { Email = "dup@example.com" });
            await db.SaveChangesAsync();
        }
        await using (var db = new EmbeddedUserDb(cs))
        {
            db.Users.Add(new EmbeddedUser { Email = "dup@example.com" });
            var ex = await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync());
            Assert.True(
                ex.InnerException is Docsql.Client.DocsqlException { IsUniqueViolation: true },
                ex.ToString());
        }
    }

    [Fact]
    public async Task ModelEvolution_AddsNewTablesWithoutMigrations()
    {
        var cs = Cs(NewDb());
        await using (var db = new EmbeddedTodoDb(cs))
        {
            db.Todos.Add(new EmbeddedTodo { Title = "kept" });
            await db.SaveChangesAsync();
        }
        // A second model over the same file: the provider's lazy sync creates
        // the new table and leaves the existing one untouched.
        await using (var db = new EmbeddedNoteDb(cs))
        {
            db.Notes.Add(new EmbeddedNote { Text = "brand new" });
            await db.SaveChangesAsync();
            Assert.Equal("brand new", await db.Notes.Select(n => n.Text).SingleAsync());
        }
        await using (var db = new EmbeddedTodoDb(cs))
        {
            Assert.Equal("kept", await db.Todos.Select(t => t.Title).SingleAsync());
        }
    }
}

public class EmbeddedUser
{
    public int Id { get; set; }
    public string Email { get; set; } = "";
}

public class EmbeddedUserDb : DbContext
{
    private readonly string _cs;

    public EmbeddedUserDb(string cs) => _cs = cs;

    public DbSet<EmbeddedUser> Users => Set<EmbeddedUser>();

    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);

    protected override void OnModelCreating(ModelBuilder b) =>
        b.Entity<EmbeddedUser>().HasIndex(u => u.Email).IsUnique()
            .HasDatabaseName("IX_EmbeddedUsers_Email");
}

public class EmbeddedNote
{
    public int Id { get; set; }
    public string Text { get; set; } = "";
}

public class EmbeddedNoteDb : DbContext
{
    private readonly string _cs;

    public EmbeddedNoteDb(string cs) => _cs = cs;

    public DbSet<EmbeddedNote> Notes => Set<EmbeddedNote>();

    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}
