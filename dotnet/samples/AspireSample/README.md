# AspireSample(DocSQL × .NET Aspire 示例)

端到端示例:AppHost 用 **GitHub Packages 发布的官方 NuGet 包**编排 DocSQL,Worker 通过注入
的连接串做一轮参数化写读。与真实用户的使用方式完全一致(不引用仓库源码)。

| 项目 | 内容 |
|---|---|
| `AspireSample.AppHost` | `AddDocsql` 单节点 + `WithDataVolume()` + `WithWebConsole()`;演示 `AddDockerComposeEnvironment` 发布出口 |
| `AspireSample.Worker` | `AddDocsqlConnection("docsql")` 注册连接,`INSERT ... RETURNING` + 参数化 `SELECT` 验证往返 |

包版本在 [`Directory.Build.props`](Directory.Build.props) 的 `DocsqlPackageVersion`(当前
`0.4.0`)统一钉住;升级包版本时改这一处即可。

## 1. 配置包源(GitHub Packages)

四个 DocSQL 包发布在 `https://nuget.pkg.github.com/wjw1-Evan/index.json`,读取需要 GitHub
PAT(权限 `read:packages`)。本目录的 [`nuget.config`](nuget.config) 已声明源,把凭据配到
**用户级**配置一次即可(推荐):

```bash
# 用户级同名源 + 凭据(对本机所有仓库生效):
dotnet nuget update source github \
  --username <你的 GitHub 用户名> \
  --password <PAT,read:packages> \
  --store-password-in-clear-text

# 或者,只在用户级追加源与凭据(写入 ~/.nuget/NuGet/NuGet.Config):
dotnet nuget add source https://nuget.pkg.github.com/wjw1-Evan/index.json \
  --name github --username <用户名> --password <PAT> --store-password-in-clear-text
```

> 也可按 [仓库 README](../../../README.md#net-与-aspireadonet--ef-core--apphost-编排)
> 的 `packageSourceCredentials` 形式配置;CI 用 `GITHUB_TOKEN` 注入凭据。

## 2. 运行

```bash
cd dotnet/samples/AspireSample

# 方式一:Aspire CLI(推荐;dashboard 可视化)
aspire start

# 方式二:直接运行 AppHost
dotnet run --project AspireSample.AppHost
```

启动后会发生:

1. 拉取 `ghcr.io/wjw1-evan/docsql:latest` 容器并起单节点(`/data` 挂命名卷持久化);
2. 追加伴生 `docsql-console` Web 控制台容器(账号门默认开启,首次访问设置用户名/密码);
3. Worker 注入 `ConnectionStrings__docsql` 后建表、写一行、读回并打印:
   `[aspire-worker] docsql round-trip ok: ...`;
4. 默认随机客户端 token 保存在 AppHost 的 user secrets(重启不换锁)。

停止:`aspire stop`(数据卷保留)。

## 3. 换成对称集群

把 AppHost 的单节点换成集群(其余代码不变):

```csharp
var cluster = builder.AddDocsqlCluster("docsql", nodeCount: 3);

builder.AddProject<Projects.AspireSample_Worker>("worker")
    .WithReference(cluster.Primary)
    .WaitFor(cluster.Primary);
```

三个节点互配 `DOCSQL_PEERS`,任意节点可写、写入自动复制;`cluster.Primary` 为连接入口。

## 4. 发布部署产物

AppHost 已调 `AddDockerComposeEnvironment("deployment")`:

```bash
aspire publish        # 输出 docker-compose 部署产物
```

## 参考

- [Aspire 集成指南](../../../docs/aspire.md):完整 API、集群/控制台/凭据/排障;
- [Docsql.Aspire.Hosting](../../Docsql.Aspire.Hosting/README.md) /
  [Docsql.Aspire.Client](../../Docsql.Aspire.Client/README.md) 包说明;
- [功能总览](../../../docs/features.md):数据库能力清单。
