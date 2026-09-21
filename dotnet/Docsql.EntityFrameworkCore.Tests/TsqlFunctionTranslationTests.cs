// T-SQL 函数翻译集成测试:验证成员/方法翻译器把 LINQ 下推到引擎第四批
// T-SQL 函数族(DATEADD/DATEDIFF/DATEPART/YEAR/LENGTH/UPPER/NEWID/…),
// 且查询在真实 server 上执行并返回正确结果(不回落客户端求值)。

using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Docsql.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore;

public class FuncItem
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public DateTime Created { get; set; }
    public double Amount { get; set; }
}

public class FuncItemDb : DbContext
{
    private readonly string _cs;
    public FuncItemDb(string cs) => _cs = cs;
    public DbSet<FuncItem> Items => Set<FuncItem>();
    protected override void OnModelCreating(ModelBuilder b) =>
        b.Entity<FuncItem>().ToTable("FuncItems");
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public class NoteRow
{
    public int Id { get; set; }
    public string? Note { get; set; }
}

public class NoteDb : DbContext
{
    private readonly string _cs;
    public NoteDb(string cs) => _cs = cs;
    public DbSet<NoteRow> Notes => Set<NoteRow>();
    protected override void OnModelCreating(ModelBuilder b) =>
        b.Entity<NoteRow>().ToTable("NoteRows");
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public sealed class TsqlFunctionTranslationTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public TsqlFunctionTranslationTests(EfServerFixture fx) => _fx = fx;
    private string Cs => $"host=127.0.0.1;port={_fx.Port}";

    private void Seed()
    {
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS FuncItems";
        cmd.ExecuteNonQuery();
    }

    private FuncItemDb NewDb() => new(Cs);

    [Fact]
    public void DateTime_member_extraction_pushes_down_year_month_day()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem
        {
            Id = 1,
            Name = "one",
            Created = new DateTime(2024, 3, 15, 10, 30, 0, DateTimeKind.Utc),
        });
        db.Items.Add(new FuncItem
        {
            Id = 2,
            Name = "two",
            Created = new DateTime(2025, 7, 4, 8, 0, 0, DateTimeKind.Utc),
        });
        db.SaveChanges();

        using var q = NewDb();
        // WHERE 用成员抽取 → 引擎 YEAR/MONTH;整体下推(不抛翻译异常即未回落)。
        var hit = q.Items.Where(i => i.Created.Year == 2024 && i.Created.Month == 3).ToList();
        var row = Assert.Single(hit);
        Assert.Equal(1, row.Id);
        Assert.Equal("one", row.Name);

        // 投影:DayOfYear / Hour / Minute / Second / Millisecond → DATEPART。
        var proj = q.Items.OrderBy(i => i.Id)
            .Select(i => new
            {
                i.Id,
                i.Created.Year,
                Doy = i.Created.DayOfYear,
                H = i.Created.Hour,
                S = i.Created.Second,
            })
            .ToList();
        Assert.Equal(2, proj.Count);
        Assert.Equal(75, proj[0].Doy); // 2024-03-15 是闰年第 75 天
        Assert.Equal(10, proj[0].H);
        Assert.Equal(185, proj[1].Doy); // 2025-07-04(非闰年)
    }

    [Fact]
    public void EF_DateDiffDay_translates_to_datediff()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem
        {
            Id = 1,
            Name = "span",
            Created = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        });
        db.SaveChanges();
        var cutoff = new DateTime(2026, 1, 11, 0, 0, 0, DateTimeKind.Utc);

        using var q = NewDb();
        // DATEDIFF 计跨越边界:1-01 → 1-11 = 10 天。
        var hit = q.Items
            .Where(i => EF.Functions.DateDiffDay(i.Created, cutoff) == 10)
            .Select(i => i.Name)
            .ToList();
        Assert.Equal(["span"], hit);

        var years = q.Items.Select(
            i => EF.Functions.DateDiffYear(i.Created, cutoff)).ToList();
        Assert.Equal(0, years[0]);
        // 月边界:1-01 → 1-11 未跨月,月差为 0(T-SQL 边界语义)。
        var months = q.Items.Select(
            i => EF.Functions.DateDiffMonth(i.Created, cutoff)).ToList();
        Assert.Equal(0, months[0]);
    }

    [Fact]
    public void DateTime_AddMethods_translate_to_dateadd()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem
        {
            Id = 1,
            Name = "add",
            Created = new DateTime(2026, 1, 31, 0, 0, 0, DateTimeKind.Utc),
        });
        db.SaveChanges();

        using var q = NewDb();
        // AddDays(1.5):毫秒换算(引擎毫秒分辨率)。
        var plus = q.Items.Select(
            i => new { V = i.Created.AddDays(1.5) }).ToList();
        Assert.Equal(
            new DateTime(2026, 2, 1, 12, 0, 0, DateTimeKind.Utc), plus[0].V);
        // AddMonths 月末钳制:1-31 + 1 月 → 2-28(T-SQL 语义)。
        var month = q.Items.Select(
            i => new { V = i.Created.AddMonths(1) }).ToList();
        Assert.Equal(
            new DateTime(2026, 2, 28, 0, 0, 0, DateTimeKind.Utc), month[0].V);
        // WHERE 内使用 AddYears(下推到过滤条件)。
        var hit = q.Items
            .Where(i => i.Created.AddYears(1) > new DateTime(2027, 1, 1, 0, 0, 0, DateTimeKind.Utc))
            .ToList();
        Assert.Single(hit);
    }

    [Fact]
    public void Math_functions_translate()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem { Id = 1, Name = "m", Amount = -2.5 });
        db.SaveChanges();

        using var q = NewDb();
        var row = q.Items.Select(i => new
        {
            Abs = Math.Abs(i.Amount),
            Floor = Math.Floor(i.Amount),
            Ceil = Math.Ceiling(i.Amount),
            Pow = Math.Pow(2, 10),
            Sqrt = Math.Sqrt(16.0),
            Round = Math.Round(i.Amount, 0),
            Sign = Math.Sign(i.Amount),
        }).Single();
        Assert.Equal(2.5, row.Abs);
        Assert.Equal(-3.0, row.Floor);
        Assert.Equal(-2.0, row.Ceil);
        Assert.Equal(1024.0, row.Pow);
        Assert.Equal(4.0, row.Sqrt);
        Assert.Equal(-3.0, row.Round); // 引擎 ROUND 半离零(T-SQL 语义)
        Assert.Equal(-1, row.Sign);
        // WHERE 内使用 Math 函数。
        var neg = q.Items.Where(i => Math.Abs(i.Amount) > 2).ToList();
        Assert.Single(neg);
    }

    [Fact]
    public void String_functions_translate()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem { Id = 1, Name = "  Hello World  " });
        db.SaveChanges();

        using var q = NewDb();
        // LENGTH / TRIM / UPPER / LOWER。
        var row = q.Items.Select(i => new
        {
            Raw = i.Name.Length,
            Trimmed = i.Name.Trim(),
            Upper = i.Name.Trim().ToUpper(),
        }).Single();
        Assert.Equal(15, row.Raw);
        Assert.Equal("Hello World", row.Trimmed);
        Assert.Equal("HELLO WORLD", row.Upper);
        // IsNullOrEmpty → IS NULL OR LENGTH(x)=0。
        var notEmpty = q.Items.Where(i => !string.IsNullOrEmpty(i.Name)).ToList();
        Assert.Single(notEmpty);
        // Replace / Substring。
        var rep = q.Items.Select(
            i => i.Name.Trim().Replace("World", "DocSQL")).Single();
        Assert.Equal("Hello DocSQL", rep);
        var sub = q.Items.Select(
            i => i.Name.Trim().Substring(6, 3)).Single();
        Assert.Equal("Wor", sub);
        // WHERE 内使用 REPLACE。
        var hit = q.Items.Where(i => i.Name.Replace("World", "X").Contains("X")).ToList();
        Assert.Single(hit);
    }

    /// 全 datepart 覆盖:表驱动断言 DATEDIFF 的"跨越边界"语义
    /// (2026-01-01T00:00 → 2026-03-15T12:30:45.123,非闰年)。
    [Fact]
    public void DateDiff_covers_all_dateparts()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem
        {
            Id = 1,
            Name = "dd",
            Created = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        });
        db.SaveChanges();
        var end = new DateTime(2026, 3, 15, 12, 30, 45, 123, DateTimeKind.Utc);

        using var q = NewDb();
        var row = q.Items.Select(i => new
        {
            Year = EF.Functions.DateDiffYear(i.Created, end),
            Quarter = EF.Functions.DateDiffQuarter(i.Created, end),
            Month = EF.Functions.DateDiffMonth(i.Created, end),
            DayOfYear = EF.Functions.DateDiffDayOfYear(i.Created, end),
            Day = EF.Functions.DateDiffDay(i.Created, end),
            Week = EF.Functions.DateDiffWeek(i.Created, end),
            Hour = EF.Functions.DateDiffHour(i.Created, end),
            Minute = EF.Functions.DateDiffMinute(i.Created, end),
            Second = EF.Functions.DateDiffSecond(i.Created, end),
        }).Single();
        // 毫秒差 63.5 亿超出 int(T-SQL DATEDIFF 同为 int),见下方小窗口断言。

        Assert.Equal(0, row.Year);           // 同一自然年,未跨边界
        Assert.Equal(0, row.Quarter);        // 同为 Q1
        Assert.Equal(2, row.Month);          // 跨 1 月、2 月两个边界
        Assert.Equal(73, row.DayOfYear);     // doy 1 → 74
        Assert.Equal(73, row.Day);
        Assert.Equal(11, row.Week);          // 周日边界:12-28 → 03-15(恰为周日)
        Assert.Equal(73 * 24 + 12, row.Hour);
        Assert.Equal((73 * 24 + 12) * 60 + 30, row.Minute);
        Assert.Equal(((73L * 24 + 12) * 60 + 30) * 60 + 45, row.Second);
        // 毫秒差 63.5 亿超出 int(T-SQL DATEDIFF 同样是 int):用小窗口断言。
        var ms = q.Items.Select(i => EF.Functions.DateDiffMillisecond(
            i.Created, i.Created.AddMilliseconds(1500))).Single();
        Assert.Equal(1500, ms);

        // DateTimeOffset 重载(常量形态,走 TIMESTAMP 字面量路径)。
        var dto = q.Items.Select(i => EF.Functions.DateDiffHour(
            (DateTimeOffset)i.Created,
            new DateTimeOffset(2026, 1, 2, 12, 0, 0, TimeSpan.Zero))).Single();
        Assert.Equal(36, dto);
    }

    /// IsNullOrEmpty 真分支:空串与 NULL 行都命中;非空行被过滤。
    [Fact]
    public void IsNullOrEmpty_matches_null_and_empty()
    {
        Seed();
        using var conn = new DocsqlConnection(Cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS NoteRows";
        cmd.ExecuteNonQuery();

        using var db = new NoteDb(Cs);
        db.Notes.AddRange(
            new NoteRow { Id = 1, Note = "" },
            new NoteRow { Id = 2, Note = null },
            new NoteRow { Id = 3, Note = "x" });
        db.SaveChanges();

        using var q = new NoteDb(Cs);
        var empty = q.Notes
            .Where(n => string.IsNullOrEmpty(n.Note))
            .OrderBy(n => n.Id)
            .Select(n => n.Id)
            .ToList();
        Assert.Equal([1, 2], empty);
        var filled = q.Notes
            .Where(n => !string.IsNullOrEmpty(n.Note))
            .Select(n => n.Id)
            .ToList();
        Assert.Equal([3], filled);
    }

    /// Math.Log/Log10/Exp 与无长度 Substring;ToUpper/ToLower 参与 WHERE。
    [Fact]
    public void log_exp_and_string_edges()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem { Id = 1, Name = "  Hello World  " });
        db.SaveChanges();

        using var q = NewDb();
        var row = q.Items.Select(i => new
        {
            Ln = Math.Log(Math.E),
            Lg = Math.Log10(100.0),
            Ex = Math.Exp(0.0),
            Tail = i.Name.Trim().Substring(6),
        }).Single();
        Assert.Equal(1.0, row.Ln);
        Assert.Equal(2.0, row.Lg);
        Assert.Equal(1.0, row.Ex);
        Assert.Equal("World", row.Tail); // Trim 后无尾随空格

        // 大小写函数参与 WHERE(列扫描内执行)。
        var hit = q.Items
            .Where(i => i.Name.Trim().ToUpper() == "HELLO WORLD"
                     && i.Name.Trim().ToLower() == "hello world")
            .ToList();
        Assert.Single(hit);
    }

    /// 翻译边界:引擎/提供程序不支持的方法(Math.Truncate)在 WHERE 中
    /// 显式抛翻译异常,而不是静默降级(与文档"翻译期显式失败"一致)。
    [Fact]
    public void unsupported_translation_fails_loudly_in_where()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem { Id = 1, Name = "x", Amount = 1.5 });
        db.SaveChanges();
        using var q = NewDb();
        Assert.Throws<InvalidOperationException>(() =>
            q.Items.Where(i => Math.Truncate(i.Amount) == 1.0).ToList());
    }

    [Fact]
    public void Guid_NewId_translates_to_newid()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem { Id = 1, Name = "g" });
        db.SaveChanges();
        using var q = NewDb();
        var g1 = q.Items.Select(_ => Guid.NewGuid()).First();
        var g2 = q.Items.Select(_ => Guid.NewGuid()).First();
        Assert.NotEqual(g1, g2);
        Assert.NotEqual(Guid.Empty, g1);
    }

    [Fact]
    public void Static_date_functions_translate()
    {
        Seed();
        using var db = NewDb();
        db.Items.Add(new FuncItem { Id = 1, Name = "now" });
        db.SaveChanges();
        using var q = NewDb();
        // UtcNow → GETUTCDATE():与服务端时钟一致(客户端容差 ±1 天,消除
        // 跨日竞态)。
        var before = DateTime.UtcNow;
        var now = q.Items.Select(_ => DateTime.UtcNow).First();
        var after = DateTime.UtcNow;
        var nowOffsets = new[] { now - before, now - after };
        Assert.All(nowOffsets, d => Assert.True(
            Math.Abs(d.TotalMinutes) < 1, $"{now:O} vs [{before:O} .. {after:O}]"));
        var local = q.Items.Select(_ => DateTime.Now).First();
        Assert.True(Math.Abs((local - before).TotalMinutes) < 1, $"{local:O}");
    }
}
