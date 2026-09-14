# Aspire 集成指南

DocSQL 通过两个 NuGet 包接入 .NET Aspire:

| 包 | 用在 | 作用 |
|---|---|---|
| `Docsql.Aspire.Hosting` | AppHost 项目 | 以容器编排 DocSQL 节点(单节点/对称集群)、可选 Web 控制台、连接串注入、健康检查 |
| `Docsql.Aspire.Client` | 消费项目(Worker/ASP.NET Core) | 从 `ConnectionStrings:{name}` 注册 `DocsqlConnection` + 连接健康检查 |

本文是完整使用手册;包级 API 速览见 [Hosting 包 README](../dotnet/Docsql.Aspire.Hosting/README.md) /
[Client 包 README](../dotnet/Docsql.Aspire.Client/README.md),数据库能力见[功能总览](features.md)。

## 0. 前置要求与约定

- **.NET 10 + Aspire 13.5+**:AppHost 项目 SDK 形如 `<Project Sdk="Aspire.AppHost.Sdk/13.5.3">`;
- **Docker 可用**(Aspire 以容器方式拉起节点;本地镜像缺失时会拉取 GHCR);
- **包源**:两个包发布在 GitHub Packages(`https://nuget.pkg.github.com/wjw1-Evan/index.json`,
  读取需 GitHub PAT `read:packages`),`nuget.config` 配置见
  [README](../README.md#net-与-aspireadonet--ef-core--apphost-编排);
- **镜像与端口约定**(`ghcr.io/wjw1-evan/docsql`,多架构 amd64/arm64):

  | 约定 | 值 |
  |---|---|
  | 协议端口(容器内固定) | `7600` |
  | Web 控制台端口(容器内固定) | `7700` |
  | 数据库文件 | `/data/docsql.db`(server 第一个位置参数) |
  | 数据卷挂载点 | `/data`(`WithDataVolume`) |
  | 控制台凭据文件 | `/auth/console-auth.json`(`WithWebConsole` 的凭据卷) |

## 1. 安装

```bash
# AppHost 项目:
dotnet add package Docsql.Aspire.Hosting

# 消费项目(Worker / ASP.NET Core):
dotnet add package Docsql.Aspire.Client
dotnet add package Docsql.Client                # ADO.NET(被 Client 集成引用)

# EF 项目额外加:
dotnet add package Docsql.EntityFrameworkCore   # 含 AddDocsqlDbContext<T>
```

> 示例工程 [`dotnet/samples/AspireSample/`](../dotnet/samples/AspireSample/) 与真实用户一致,
> 直接引用 **GitHub Packages 已发布包**(版本由示例的 `Directory.Build.props` 的
> `DocsqlPackageVersion` 统一钉住),并刻意不在 `Docsql.sln` 内——主解决方案的自测不依赖私有包。
> 本地构建示例前先按[示例 README](../dotnet/samples/AspireSample/README.md#1-配置包源github-packages)
> 配置包源凭据;CI 用 `GITHUB_TOKEN` 认证后单独构建它。

## 2. 五分钟起步(单节点 + 控制台)

**AppHost(`Program.cs`)**:

```csharp
using Aspire.Hosting.Docker;
using Docsql.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

// aspire publish 输出 docker-compose 产物;本地 aspire start 不受影响。
builder.AddDockerComposeEnvironment("deployment");

var docsql = builder.AddDocsql("docsql")   // 资源名 docsql;协议端口由 Aspire 分配
    .WithDataVolume()                      // 命名卷挂载 /data,容器重建数据保留
    .WithWebConsole();                     // 伴生 docsql-console 管理控制台

builder.AddProject<Projects.MyApi>("myapi")
    .WithReference(docsql)                 // 注入 ConnectionStrings__docsql
    .WaitFor(docsql);                      // 等 TCP 健康检查通过再启动

builder.Build().Run();
```

**消费项目(`Program.cs`)**:

```csharp
using Docsql.Aspire.Client;
using Docsql.Client;

var builder = Host.CreateApplicationBuilder(args);
builder.AddDocsqlConnection("docsql");     // 注册 transient DocsqlConnection + 健康检查

using var host = builder.Build();
var conn = host.Services.GetRequiredService<DocsqlConnection>();
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT 1";
Console.WriteLine(await cmd.ExecuteScalarAsync());
```

**运行**:

```bash
aspire start                    # 或 dotnet run --project <AppHost 目录>
# dashboard 里可见 docsql 容器、TCP 健康态、控制台端点;日志在资源页查看
aspire stop
```

起步时自动发生的事:

1. `AddDocsql` 生成随机客户端 token(secret 参数,运行期持久化到 **user secrets**),
   注入节点 `DOCSQL_TOKEN`,并写入连接串 `host=...;port=...;token=...`;
2. `WithDataVolume()` 生成稳定卷名并挂到 `/data`——容器重建、镜像升级数据不丢;
3. `WithWebConsole()` 以**同镜像**追加 `docsql-web` entrypoint 的伴生容器(命名 `docsql-console`),
   上游指向本节点;控制台**账号门默认开启**(`DOCSQL_WEB_AUTH_FILE=/auth/console-auth.json`,
   凭据卷持久化),首次打开页面强制设置用户名/密码;
4. `WithReference` 注入 `ConnectionStrings__docsql`,消费侧 `AddDocsqlConnection` 才能按名取到。

## 3. 对称集群(AddDocsqlCluster)

```csharp
var cluster = builder.AddDocsqlCluster("docsql", nodeCount: 3);

builder.AddProject<Projects.MyApi>("myapi")
    .WithReference(cluster.Primary)   // 以主节点为连接入口
    .WaitFor(cluster.Primary);
```

- 节点命名 `docsql-a` / `docsql-b` / `docsql-c`,**相互配置 `DOCSQL_PEERS` 互扇出**——
  任意节点可读写,SQL 写入自动复制到其余节点;
- 客户端 token 与 `DOCSQL_CLUSTER_TOKEN` 各自生成一次并**全节点共享**(集群内互认);
- 每个节点独立数据卷(自动 `WithDataVolume`);
- `port:` 参数**只作用于主节点**(其余节点走宿主随机端口;集群内部按 Aspire endpoint 展开的
  `host:port` 互通,不依赖固定端口);
- 返回的 `DocsqlCluster` 句柄:`Primary`(入口)、`Nodes`(全部节点,可用于逐节点
  `WithWebConsole()`)、`Token`、`ClusterToken`。

```csharp
// 每个节点挂一个控制台(便于分别观察):
cluster.Nodes[0].WithWebConsole();
cluster.Nodes[1].WithWebConsole();
```

节点数量 1~8。集群的写入复制、离线补齐、扩容加入、故障恢复语义见
[README「Docker 部署」](../README.md#docker-部署单节点--多节点本地开发与生产两个-compose-文件)与
[功能总览 · 复制与集群](features.md#8-复制与集群)。

## 4. Web 控制台(WithWebConsole)

```csharp
var docsql = builder.AddDocsql("docsql")
    .WithDataVolume()
    .WithWebConsole(port: 8080);           // 宿主端口;名字默认 {节点名}-console
```

行为:

| 项 | 说明 |
|---|---|
| 伴生容器 | 同镜像、`docsql-web` entrypoint,命名 `{节点名}-console`(`consoleName` 可改) |
| 上游 | 自动指向本节点 endpoint,数据操作全部以客户端身份转发到节点 |
| 账号门 | 默认开启(凭据卷 `/auth`);首次访问必须设置用户名/密码(≥8 位,非单字符重复) |
| 凭据卷 | Aspire 自动生成的命名卷挂 `/auth`——容器重建后账号与登录会话保留 |
| 节点鉴权 | 资源存在 token 时自动注入控制台 `DOCSQL_TOKEN`(程序化旁路同凭据) |
| 存储 | 控制台自身**零存储**,不存在独立的控制台数据库 |

> **重要**:一旦节点上创建了数据库用户,匿名连接会被节点拒绝,控制台必须持有
> `DOCSQL_TOKEN`。`WithWebConsole` 已自动注入;若用 `WithToken` 替换了节点 token,
> 控制台同步使用替换后的值,无需额外配置。生产部署建议给控制台配 TLS 反代并设
> `DOCSQL_WEB_COOKIE_SECURE=1`,详见[安全指南](security.md#传输与静态数据)。

## 5. Token 与凭据

- `AddDocsql` / `AddDocsqlCluster` 默认用 `GenerateParameterDefault` 生成随机 token
  (`secret: true`),首次运行保存到 AppHost 的 **user secrets**——重启/重建应用不会换锁;
- 生产或需要固定值时,传入显式参数替换:

  ```csharp
  var token = builder.AddParameter("docsql-token", secret: true);         // 部署时经环境变量/配置提供
  var clusterToken = builder.AddParameter("docsql-cluster-token", secret: true);

  var docsql = builder.AddDocsql("docsql").WithToken(token).WithDataVolume();
  docsql.WithClusterToken(clusterToken);   // 自定义集群(非 AddDocsqlCluster)时使用
  ```

- **自锁风险**:从控制台创建首个数据库用户前,务必确认控制台持有所在节点的 token
  (默认路径已满足)。节点一旦存在用户,未带 token 的匿名连接(含控制台自身)会被拒绝;
  恢复方式是给节点与控制台配置一致的 token 后重建容器(数据保留),或清卷重建。

## 6. 消费侧:连接注册与健康检查

```csharp
// Worker / 控制台应用
builder.AddDocsqlConnection("docsql");

// 可选配置:关闭健康检查或改名(默认 Docsql_{name}:打开连接并执行 SELECT 1)
builder.AddDocsqlConnection("docsql", s =>
{
    s.DisableHealthChecks = true;
    // s.HealthCheckName = "docsql-ready";
});
```

- 注册的是 **transient** `DocsqlConnection`;物理连接池在 `Docsql.Client` 内部
  (池键含 host/port/token/user,不同身份不共享物理连接);
- 注入示例:**构造函数注入 `DocsqlConnection`** 即可;每次 Open/Close 借还池化连接;
- 数据库用户登录:连接串加 `user=...;password=...`(与 token 二选一,同时给出时用户优先)。

**EF Core**:

```csharp
builder.Services.AddDocsqlDbContext<AppDb>("docsql");
// 或按需追加选项:
builder.Services.AddDocsqlDbContext<AppDb>("docsql", (sp, o) => o.EnableSensitiveDataLogging(false));
```

`AddDocsqlDbContext<T>` 的连接串同样取自 `ConnectionStrings:{name}`(AppHost 经
`WithReference` 注入),建表/索引同步走 EnsureCreated + 惰性拦截器(免迁移;
`Database.Migrate()` 显式报错)。类型映射与查询翻译见[客户端与驱动](drivers.md#net-ef-coredocsqlentityframeworkcore)。

## 7. 运行时配置与镜像版本

镜像默认 tag `latest`;钉版用 Aspire 通用 API:

```csharp
var docsql = builder.AddDocsql("docsql")
    .WithImageTag("v0.2.0")
    .WithEnvironment("DOCSQL_ASYNC_COMMIT", "1")          // 组提交(~2ms 丢失窗口)
    .WithEnvironment("DOCSQL_CATCHUP_WINDOW", "200000")   // 追赶日志保留
    .WithEnvironment("DOCSQL_BACKUP_INTERVAL_SECS", "3600")
    .WithEnvironment("DOCSQL_MAX_CONN", "200")
    .WithEnvironment("DOCSQL_KEY", keyParameter)          // AES-256-GCM 帧加密(64 hex)
    .WithDataVolume();
```

- `WithEnvironment` 可注入服务器全部运行时变量,清单与含义见
  [运维手册 · 环境变量](operations.md#环境变量运维相关);
- 数值型变量非法值会导致节点**拒绝启动**(exit 2),dashboard 上表现为资源不健康——这是
  有意的快速失败,别配错;
- 集群各节点必须使用**同一个镜像 tag 与同一 `DOCSQL_CLUSTER_TOKEN`**。

## 8. 本地开发工作流

```bash
aspire start                  # 拉起应用并打开 dashboard(资源状态/日志/端点)
aspire ps                     # 查看运行中的资源
aspire logs docsql            # 查看节点日志
aspire stop                   # 停止应用(数据卷保留)
```

- 应用停止后命名数据卷仍在;彻底清理用容器运行时(卷名可在 dashboard 或
  `docker volume ls` 查看);
- 修改 AppHost 后重新 `aspire start` 即可;节点容器会按新配置重建,**数据卷不重建**;
- 镜像升级:`WithImageTag` 换版本后重启;或 `docker pull ghcr.io/wjw1-evan/docsql:<tag>` 后重启。

## 9. 发布与部署

```csharp
builder.AddDockerComposeEnvironment("deployment");
```

`aspire publish` 生成部署产物(该配置下为 docker-compose),包含节点/控制台容器、环境变量
与卷定义。两个部署路径按场景选择:

| 路径 | 产物 | 适用 |
|---|---|---|
| `aspire publish` | Aspire 生成的 compose(由 Aspire 模型推导) | 应用与数据库一起以 Aspire 为准编排 |
| `deploy/docker-compose.prod.yml` | 仓库手工维护的生产 compose(GHCR 镜像,固定 tag) | 数据库独立运维、需要 CLI/备份页/部署测试语义时 |

两者都拉取同一个多架构镜像,数据卷命名不同、互不共享。生产要点:固定镜像 tag、配置
`DOCSQL_TOKEN`(集群再加 `DOCSQL_CLUSTER_TOKEN`,与客户端 token 不同值)、GHCR 私有包先
`docker login ghcr.io`。数据库侧的备份/恢复/监控配置见[运维手册](operations.md)。

## 10. 故障排查

| 现象 | 可能原因 | 处理 |
|---|---|---|
| 镜像拉取失败(401/denied) | GHCR 私有包未登录 | `docker login ghcr.io`;确认有包读取权限 |
| 资源一直 Waiting / 不健康 | 节点启动即退出(配置非法、端口冲突) | dashboard → 资源 → 日志;数值型 env 非法值必须修正 |
| 消费项目抛 `Connection string 'docsql' was not found` | AppHost 未 `WithReference` 或名字不一致 | 两端连接名保持一致;确认注入为 `ConnectionStrings__{name}` |
| 控制台提示认证被拒 / 匿名不可用 | 节点已存在数据库用户,控制台没有 token | 确认 `WithWebConsole`(自动注入)或给控制台显式配 `DOCSQL_TOKEN` |
| 控制台忘记账号密码 | 凭据在 `/auth` 卷 | 删除该卷(或卷内 `console-auth.json`)后重建控制台,回到首次 setup |
| `Database.Migrate()` 抛 NotSupported | EF Migrations 不支持 | 改用 EnsureCreated/惰性建表;模型变更自动补列/建索引 |
| 集群写入偶发超时(30s) | 单写者引擎 + 并发 BEGIN 排队上限 | 降低并发写事务粒度;或把写入导向单一节点;详见[已知边界](limitations.md) |

## 11. API 速览(hosting)

| API | 说明 |
|---|---|
| `AddDocsql(name, port?)` | 单节点容器;默认生成随机客户端 token(secret 参数) |
| `WithDataVolume(volumeName?)` | 命名卷挂载 `/data`;不指定卷名时按应用路径生成稳定名 |
| `WithToken(param)` | 替换客户端 token(连接串与 `DOCSQL_TOKEN` 同步更新) |
| `WithClusterToken(param)` | 注入 `DOCSQL_CLUSTER_TOKEN`(集群各节点同值) |
| `WithPeers(...)` | 注入 `DOCSQL_PEERS`(endpoint 表达式展开,逗号分隔) |
| `AddDocsqlCluster(name, nodeCount=3, port?)` | 集群三件套:token / cluster token / peers / 数据卷一次到位;返回 `DocsqlCluster` |
| `WithWebConsole(port?, consoleName?)` | 伴生控制台(账号门默认开启,凭据卷 `/auth`) |
| `WithImageTag(...)` | 钉镜像版本(Aspire 通用) |
| `WithEnvironment(name, value)` | 任意服务器运行时变量(Aspire 通用) |
| `DocsqlServerResource.ProtocolPort` 等常量 | `7600` / `7700` / `/data/docsql.db` |

## 12. 参考

- 示例工程:[`dotnet/samples/AspireSample/`](../dotnet/samples/AspireSample/)(AppHost + Worker 端到端);
- 包说明:[Docsql.Aspire.Hosting](../dotnet/Docsql.Aspire.Hosting/README.md) /
  [Docsql.Aspire.Client](../dotnet/Docsql.Aspire.Client/README.md);
- 连接串与 ADO.NET/EF 细节:[客户端与驱动](drivers.md);
- 数据库全部能力:[功能总览](features.md);边界:[已知边界](limitations.md)。
