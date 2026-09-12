# Docsql.Aspire.Hosting

DocSQL 的 Aspire hosting 集成:在 AppHost 中以容器方式编排 DocSQL 数据库节点(单节点或对称集群),
连接串自动注入消费项目,并可选附带 Web 管理控制台容器。

镜像为官方多架构 `ghcr.io/wjw1-evan/docsql`(amd64/arm64);容器内协议端口固定 7600,
控制台固定 7700,数据目录挂载点为 `/data`。

## 快速开始

AppHost 项目(`<Project Sdk="Aspire.AppHost.Sdk/13.5.3">`):

```csharp
var builder = DistributedApplication.CreateBuilder(args);

var docsql = builder.AddDocsql("docsql")
    .WithDataVolume()      // 命名卷持久化 /data(默认生成稳定卷名)
    .WithWebConsole();     // 伴生 docsql-web 控制台容器(命名 {节点名}-console)

builder.AddProject<Projects.MyApi>("myapi")
    .WithReference(docsql) // 注入 ConnectionStrings__docsql
    .WaitFor(docsql);      // 等待 TCP 健康检查通过

builder.Build().Run();
```

消费项目(配合 [Docsql.Aspire.Client](https://github.com/wjw1-Evan/docsql)):

```csharp
builder.AddDocsqlConnection("docsql");
// 之后在任意位置注入 DocsqlConnection(连接池在 Docsql.Client 内部)
```

## 集群(对称复制)

```csharp
var cluster = builder.AddDocsqlCluster("docsql", nodeCount: 3);
// cluster.Primary 为连接入口;节点名 docsql-a/b/c,DOCSQL_PEERS 自动互配,
// 客户端 token 与 DOCSQL_CLUSTER_TOKEN 各自生成并全节点共享。
```

## API 速览

| API | 说明 |
|---|---|
| `AddDocsql(name, port?)` | 单节点容器;默认生成随机客户端 token(secret 参数) |
| `WithDataVolume(volumeName?)` | 命名卷挂载 `/data` |
| `WithToken(param)` | 替换客户端 token 参数 |
| `WithClusterToken(param)` | 注入 `DOCSQL_CLUSTER_TOKEN` |
| `WithPeers(...)` | 注入 `DOCSQL_PEERS` 对等节点表 |
| `AddDocsqlCluster(name, nodeCount=3, port?)` | 三件套集群(token/cluster-token/peers/数据卷一次到位) |
| `WithWebConsole(port?, name?)` | 伴生控制台容器,账号门默认开启(凭据卷 `/auth`) |

镜像版本默认 `latest`,可用 Aspire 通用 `WithImageTag(...)` 钉住;其余运行时变量
(如 `DOCSQL_ASYNC_COMMIT`、`DOCSQL_CATCHUP_WINDOW`)用通用 `WithEnvironment(...)` 注入。

## 许可

MIT OR Apache-2.0
