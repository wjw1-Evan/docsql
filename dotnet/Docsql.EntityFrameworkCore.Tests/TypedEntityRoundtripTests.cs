// 精确类型实体的端到端往返与变量(参数化)字符串方法翻译回归。
// 覆盖两处"写得进、读不出"修复:IsoTimeSpanMapping/IsoDateTimeOffsetMapping
// 此前物化落到 Convert.ChangeType(string→TimeSpan / DateTime→DateTimeOffset
// 均不支持)直接抛 InvalidCastException;变量 StartsWith/EndsWith/Contains
// 此前裸塞 SqlParameterExpression 未应用类型映射,EF 校验阶段即抛
// "does not have a type mapping assigned" —— 前缀搜索整类查询失效。
// EfServerFixture 在 EfTests.cs(全测试项目共用的进程启动辅助类)。

using Docsql.Client;
using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

public class Slot
{
    public int Id { get; set; }
    public TimeSpan Duration { get; set; }
    public DateTimeOffset At { get; set; }
    public string Name { get; set; } = "";
}

public class SlotDb : DbContext
{
    private readonly string _cs;
    public SlotDb(string cs) => _cs = cs;
    public DbSet<Slot> Slots => Set<Slot>();
    protected override void OnConfiguring(DbContextOptionsBuilder o) => o.UseDocsql(_cs);
}

public sealed class TypedEntityRoundtripTests : IClassFixture<EfServerFixture>
{
    private readonly EfServerFixture _fx;
    public TypedEntityRoundtripTests(EfServerFixture fx) => _fx = fx;

    private string Cs() => $"host=127.0.0.1;port={_fx.Port}";

    [Fact]
    public void TimeSpan_and_DateTimeOffset_entities_round_trip()
    {
        using var db = new SlotDb(Cs());
        db.Database.EnsureCreated();
        var ts = new TimeSpan(1, 2, 3, 4);
        var at = new DateTimeOffset(2026, 9, 29, 8, 30, 5, TimeSpan.FromHours(8));
        db.Slots.Add(new Slot { Id = 1, Duration = ts, At = at, Name = "s1" });
        db.SaveChanges();

        var back = db.Slots.Single(s => s.Id == 1);
        Assert.Equal(ts, back.Duration);
        // $ts 只存 UTC 毫秒:时刻精确往返(偏移口径随物化层,不做断言)。
        Assert.Equal(at.ToUnixTimeMilliseconds(), back.At.ToUnixTimeMilliseconds());
    }

    [Fact]
    public void Variable_patterns_translate_to_LIKE()
    {
        using var db = new SlotDb(Cs());
        db.Database.EnsureCreated();
        db.Slots.AddRange(
            new Slot { Id = 10, Name = "alpha", Duration = TimeSpan.MinValue, At = default },
            new Slot { Id = 11, Name = "beta", Duration = TimeSpan.MinValue, At = default },
            new Slot { Id = 12, Name = "alphabet", Duration = TimeSpan.MinValue, At = default });
        db.SaveChanges();

        var prefix = "alph";
        Assert.Equal(2, db.Slots.Count(s => s.Name!.StartsWith(prefix)));
        Assert.Equal(2, db.Slots.Count(s => s.Name!.Contains("pha")));
        Assert.Equal(2, db.Slots.Count(s => s.Name!.EndsWith("a")));
        var names = db.Slots.Where(s => s.Name!.StartsWith(prefix))
            .OrderBy(s => s.Id).Select(s => s.Name).ToList();
        Assert.Equal(new[] { "alpha", "alphabet" }, names);
    }
}
