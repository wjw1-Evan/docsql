# 客户端与驱动

## 安装(四个 NuGet 包,GitHub Packages)

包发布在 GitHub Packages 源 `https://nuget.pkg.github.com/wjw1-Evan/index.json`(读取需 GitHub
PAT,权限 `read:packages`;`nuget.config` 源配置见 [README](../README.md#net-与-aspireadonet--ef-core--apphost-编排)):

```bash
dotnet add package Docsql.Client                # .NET ADO.NET
dotnet add package Docsql.EntityFrameworkCore   # EF Core 提供程序
dotnet add package Docsql.Aspire.Hosting        # Aspire AppHost 编排
dotnet add package Docsql.Aspire.Client         # Aspire 消费侧
```

## .NET ADO.NET(Docsql.Client)

```bash
dotnet add package Docsql.Client
```

```csharp
await using var conn = new DocsqlConnection("host=127.0.0.1;port=7600;token=YOUR-TOKEN");
// 数据库用户: "host=...;port=...;user=analyst;password=..."(与 token 二选一,用户优先)
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT id, note FROM orders WHERE id > @min";
cmd.Parameters.AddWithValue("min", 42);
await using var reader = await cmd.ExecuteReaderAsync();
```

- 事务 `BeginDbTransaction` → BEGIN/COMMIT/ROLLBACK + SAVEPOINT(EF 事务内 SaveChanges 依赖它);
- `RETURNING` 可直接 `ExecuteScalar/ExecuteReader`;
- 持久化 pub/sub:`conn.Publish(channel, payload)` 返回 `(id, receivers)`;
  `DocsqlSubscriber` 专用连接 + 专职读线程(订阅必须独占连接),断线按最后 id 续传;
- 服务端配置 `DOCSQL_KEY` 后自动启用 AES-256-GCM。

### 参数绑定:服务端 prepared statements(默认路径)

带参数的命令**不再在客户端拼接字面量**:`@name` 改写为 `?` 占位符,模板经 REQ_PREPARE
注册(物理连接内按句柄缓存,同一模板重复执行零注册开销),参数数组经 REQ_EXECUTE 执行。
值在**服务端**渲染为类型化字面量(引号感知、字符串翻倍转义)——任何取值都无法逃逸
字面量,注入载荷只能是数据。授权/语句超时/审计与普通语句同路径。`cmd.Prepare()`
可预注册句柄。

### 连接池(默认开启)

- `Close()` 归还物理连接而非断开;`Open()` 借出前 PING 验活,死连接自动丢弃重建
  (服务器重启后的客户端韧性由此免费获得);
- 池键 = host/port/user/password/token/key/max pool size:不同身份绝不共享物理连接;
- **池容量封顶"借出+空闲"总物理连接数**(与 SqlClient 同语义):池满时 `Open` 等待
  `connect timeout` 秒(默认 15,亦即 TCP 建连超时)后抛超时错,绝不悄悄超限新建;
- **事务安全**:事务未了结就 `Close()` 的连接被物理丢弃(服务器对断连自动 ROLLBACK),
  残留事务不可能泄漏给下一个借出者;
- 池化连接的服务端 prepared 句柄缓存随物理连接有效,复用零成本;
- 开关:`pooling=false`(直连模式);`max pool size=N`(池上限,默认 100);
- `ClearPool()` / `ClearAllPools()` 物理清空空闲连接;
- **真异步**:`OpenAsync`/`ExecuteReaderAsync`/`ExecuteNonQueryAsync`/`ExecuteScalarAsync`/
  `CommitAsync`/`RollbackAsync`/保存点 `SaveAsync`/`RollbackAsync(name)`/`ReleaseAsync(name)`
  全链路异步 IO(帧收发走 `NetworkStream` 异步,不占线程池线程);语句级取消由服务端
  语句超时承担(帧中途取消会错位帧流,客户端不假装可取消);
- **保存点 API**:`DocsqlTransaction.Save`/`Rollback(name)`/`Release(name)` 映射引擎
  SAVEPOINT/ROLLBACK TO/RELEASE(`SupportsSavepoints=true`);注意引擎语义:ROLLBACK TO
  会把命名保存点自身也丢弃(异于 SQLite/PG),回滚后勿再 Release 同名保存点。

## .NET EF Core(Docsql.EntityFrameworkCore)

```bash
dotnet add package Docsql.EntityFrameworkCore
```

```csharp
services.AddDbContext<AppDb>(o => o.UseDocsql(connectionString));
// 容器级(Aspire/宿主集成):services.AddDocsqlDbContext<AppDb>("docsql") 按名取注入的连接串
```

- 原生提供程序(不依赖 SQLite);`EnsureCreated` + 惰性建表 + 模型/索引自动同步(免迁移);
- 类型映射:`decimal` → `DECIMAL`(参数经 `$dec` 文本标记精确绑定,服务端十进制聚合/比较;`HasPrecision` 进入列类型与 CAST 字面量)、`byte[]` → `BLOB`(参数经 `$bytes` 标记,响应按 `{"$bytes":[…]}` 解码;单值 ≤16MiB 文档上限)、`DateOnly`/`TimeOnly` → 可排序 ISO 文本、`DateTime`/`DateTimeOffset`/`TimeSpan`/`Guid` 沿用文本;未映射的 CLR 类型在模型构建期显式报错;
- 模型复合索引(`HasIndex(e => new { … })`)按列序创建;`List.Contains` 翻译为 `IN (…)`;字符串 `StartsWith/EndsWith/Contains` 翻译为 `LIKE`;引擎不支持的语法在翻译期显式失败;
- `Database.Migrate()` 显式报错(Migrations 不支持)。

## Aspire 集成(Docsql.Aspire.Hosting / Docsql.Aspire.Client)

在 AppHost 中以容器方式编排 DocSQL 节点(单节点或对称集群),连接串自动注入消费项目。

```bash
dotnet add package Docsql.Aspire.Hosting   # AppHost 项目(需 Aspire AppHost SDK)
dotnet add package Docsql.Aspire.Client    # 消费项目(Worker/ASP.NET Core)
```

AppHost:

```csharp
using Docsql.Aspire.Hosting;

var docsql = builder.AddDocsql("docsql")
    .WithDataVolume()      // 命名卷持久化 /data
    .WithWebConsole();     // 伴生 docsql-web 控制台容器(账号门默认开启)

builder.AddProject<Projects.MyApi>("myapi")
    .WithReference(docsql) // 注入 ConnectionStrings__docsql
    .WaitFor(docsql);      // 等待 TCP 健康检查通过
```

消费侧:

```csharp
builder.AddDocsqlConnection("docsql");       // 注册 transient DocsqlConnection + 健康检查
// EF 项目:builder.Services.AddDocsqlDbContext<AppDb>("docsql") 按名取注入的连接串
```

- 对称集群:`builder.AddDocsqlCluster("docsql", nodeCount: 3)`(token / cluster token / peers /
  数据卷一次到位,`cluster.Primary` 为连接入口);
- 镜像默认 `latest`,`WithImageTag(...)` 钉版;`WithToken` / `WithClusterToken` / `WithPeers` /
  `WithEnvironment(...)` 可定制;完整 API 速览见包 README
  ([Hosting](../dotnet/Docsql.Aspire.Hosting/README.md)、[Client](../dotnet/Docsql.Aspire.Client/README.md));
- 本地:`aspire start`(或 `dotnet run --project <AppHost>`)拉起节点 + dashboard;部署出口:
  `aspire publish`(示例配 `AddDockerComposeEnvironment` 输出 docker-compose)。完整示例见
  `dotnet/samples/AspireSample/`(直接引用 GitHub Packages 发布包,含消费侧 Worker);
  使用手册见 [Aspire 集成指南](aspire.md)。

## CLI(docsql-cli)

```
docsql connect 127.0.0.1:7600 --user analyst   # 密码走 DOCSQL_PASSWORD 或交互提示
  --csv / --json        行导出(RFC 4180 CSV / JSON 对象数组)
  -f script.sql         脚本批执行(错误即退出 1)
  help;                 内联命令帮助(含 pub/sub 命令面)
```

订阅是专用连接 + 专职读线程(CLI 已内置):普通一问一答连接会把推送帧错当成命令响应。

## 驱动作者:线协议参数化

v1 二进制协议(`core/proto.rs`),一句一帧。服务端 prepared statements 三帧:

| 帧 | 载荷 | 响应 |
|---|---|---|
| `REQ_PREPARE 0x0003` | SQL 文本,`?` 为占位符 | `RESP_PREPARED 0x010E` `{"handle":n}` |
| `REQ_EXECUTE 0x0004` | `{"handle":n,"params":[...]}` | 与 REQ_SQL 相同(RESP_ROWS/AFFECTED/ERROR) |
| `REQ_CLOSE_STMT 0x0005` | `{"handle":n}` | RESP_AFFECTED / RESP_ERROR |

- 参数按位置绑定,服务端渲染类型化字面量(引号感知;字符串值翻倍转义)——驱动无需自行转义;
- 句柄按连接隔离,连接断开即失效;
- 参数类型:JSON `null/bool/number/string`(嵌套对象/数组按 JSON 文本绑定);精确标量用标记对象:`$dec`(十进制文本 → `CAST(… AS DECIMAL)`)、`$bytes`(整数数组 → `x'…'` hex 字面量);响应中 DECIMAL 为 `{"$dec":"…"}`、BLOB 为 `{"$bytes":[…]}`;
- 授权、`DOCSQL_STATEMENT_TIMEOUT_MS`、审计与 REQ_SQL 完全同路径;
- 订阅帧、REQ_STATUS(含 `metrics` 运行时计数器)等其余帧见 `core/proto.rs` 模块头注释(权威)。
