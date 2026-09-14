# Docsql.EntityFrameworkCore

Entity Framework Core provider for [DocSQL](https://github.com/wjw1-Evan/docsql) — 原生提供程序,基于 [Docsql.Client](https://github.com/wjw1-Evan/docsql/pkgs/nuget/Docsql.Client),不依赖 SQLite。

## 安装

包发布在 GitHub Packages(`net10.0`);先把源 `https://nuget.pkg.github.com/wjw1-Evan/index.json`
配到 `nuget.config`(读取需 GitHub PAT,权限 `read:packages`;配置见
[仓库 README](https://github.com/wjw1-Evan/docsql#net-与-aspireadonet--ef-core--apphost-编排)),然后:

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
- 模型与索引自动同步:`[Index]` 特性索引(含唯一索引)随模型增删自动创建/回收
- CRUD / LINQ / `Include` / 原生 `FromSql` 全支持
- `Database.Migrate()` 显式报错:DocSQL 不支持 EF Migrations,请使用 EnsureCreated(显式报错并指引,绝不静默)

License: MIT OR Apache-2.0.
