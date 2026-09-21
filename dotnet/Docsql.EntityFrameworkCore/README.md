# Docsql.EntityFrameworkCore

Entity Framework Core provider for [DocSQL](https://github.com/wjw1-Evan/docsql) — 原生提供程序,基于 [Docsql.Client](https://github.com/wjw1-Evan/docsql/pkgs/nuget/Docsql.Client),不依赖 SQLite。

## 安装

包发布在 GitHub Packages(`net10.0`);先把源 `https://nuget.pkg.github.com/wjw1-Evan/index.json`
配到 `nuget.config`(读取需 GitHub PAT,权限 `read:packages`;配置见
[docs/drivers.md](https://github.com/wjw1-Evan/docsql/blob/main/docs/drivers.md)),然后:

```bash
dotnet add package Docsql.EntityFrameworkCore
```

```csharp
services.AddDbContext<AppDb>(o =>
    o.UseDocsql("host=127.0.0.1;port=7600;token=YOUR-TOKEN"));

class AppDb(DbContextOptions<AppDb> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
}
```

- `EnsureCreated` / 惰性自动建表:模型缺表缺列启动即补(免迁移)
- 模型与索引自动同步:`[Index]` 特性索引(含唯一索引)与复合索引随模型增删自动创建/回收
- 类型映射:`decimal` → 精确 `DECIMAL`(`HasPrecision` 生效;服务端 `Sum`/比较不丢精度)、`DateOnly`/`TimeOnly`、`byte[]` → `BLOB`(单值 ≤16MiB);`List.Contains` 翻译为 `IN (…)`,字符串 `StartsWith/EndsWith/Contains` 翻译为 `LIKE`
- 实体集合属性 `List<T>`(JSON 数组列)的 `Contains` 翻译为服务端 `JSON_ARRAY_CONTAINS`(常量/跨列/取反/数值元素);`Dictionary<string, object>` 映射为 JSON 文本(读写与变更跟踪)
- **T-SQL 函数下推**(引擎原生支持,查询不再回落客户端求值):
  - 日期时间:`DateTime.Now/UtcNow` → `GETDATE()/GETUTCDATE()`(引擎仅 UTC)、
    `d.Year/Month/Day/DayOfYear/Hour/Minute/Second/Millisecond` →
    `YEAR/MONTH/DAY/DAYOFYEAR/DATEPART(…)`;
    `d.AddYears/AddMonths/AddDays/AddHours/AddMinutes/AddSeconds/AddMilliseconds` →
    `DATEADD(…)`(月末钳制与毫秒分辨率与 T-SQL 一致);
  - `EF.Functions.DateDiffYear/Quarter/Month/Day/Week/Hour/Minute/Second/Millisecond`
    → `DATEDIFF`(计跨越边界次数,SQL Server 语义);
  - 数学:`Math.Abs/Ceiling/Floor/Pow/Round/Sqrt/Sign/Exp/Log/Log10`;
  - 字符串:`Length/ToUpper/ToLower/Trim/Replace/Substring/IsNullOrEmpty/Concat`;
  - 标识:`Guid.NewGuid()` → `NEWID()`。
- 异常分类:`DocsqlException.IsUniqueViolation` / `IsSyntaxError`,幂等写入按类型分支
- CRUD / LINQ / `Include` / 原生 `FromSql` 全支持
- `Database.Migrate()` 显式报错:DocSQL 不支持 EF Migrations,请使用 EnsureCreated(显式报错并指引,绝不静默)

License: MIT OR Apache-2.0.
