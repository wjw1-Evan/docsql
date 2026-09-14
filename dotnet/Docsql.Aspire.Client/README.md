# Docsql.Aspire.Client

DocSQL 的 Aspire client 集成:从 `ConnectionStrings:{name}`(由
[Docsql.Aspire.Hosting](https://github.com/wjw1-Evan/docsql) 经 `WithReference` 注入)注册
`DocsqlConnection`,并按需注册连接健康检查。

## 安装

包发布在 GitHub Packages(`net10.0`),源 `https://nuget.pkg.github.com/wjw1-Evan/index.json`,
读取需要 GitHub PAT(权限 `read:packages`;`nuget.config` 源配置见
[仓库 README](https://github.com/wjw1-Evan/docsql#net-与-aspireadonet--ef-core--apphost-编排)):

```bash
# 消费项目(Worker/ASP.NET Core):
dotnet add package Docsql.Aspire.Client
# EF 项目可加容器级注册:
dotnet add package Docsql.EntityFrameworkCore
```

## 快速开始

```csharp
var builder = Host.CreateApplicationBuilder(args);
builder.AddDocsqlConnection("docsql");

using var host = builder.Build();
var connection = host.Services.GetRequiredService<DocsqlConnection>();
```

- 注册 transient `DocsqlConnection`(连接池在 [Docsql.Client](https://github.com/wjw1-Evan/docsql) 内部)。
- 默认注册健康检查 `Docsql_{name}`(打开连接并执行 `SELECT 1`),可通过
  `settings => settings.DisableHealthChecks = true` 关闭或 `HealthCheckName` 改名。

连接串格式:`host=..;port=..;token=..;user=..;password=..;key=..`(详见 Docsql.Client)。

## 许可

MIT OR Apache-2.0
