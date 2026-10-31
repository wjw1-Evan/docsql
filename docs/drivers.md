# 客户端与驱动

> **引用包 ≠ 启动数据库**:`Docsql.Client` / `Docsql.EntityFrameworkCore` /
> `Docsql.Aspire.*` 在 `host=…;port=…` 模式下都是纯网络客户端,装包不会在进程里启动
> 数据库 —— server 由部署形态负责(本地裸二进制一行 / compose / Aspire AppHost 拉容器;
> 演示程序 `EfSample` 是特例:它自己拉起本地 server 供示例跑通,不属包的职责)。
> 唯一例外是 **.NET 嵌入式模式(`Data Source=app.db`)**:应用进程内直接跑引擎
> (原生库 + C ABI,见下方「嵌入式模式」),不需要任何 server。
> 连接级自动化:EF 首命令前的懒建表/索引同步、连接池、服务端 prepared statements。
> 真·零守护的边界与设计见[设计 006](design/006-dotnet-embedded-mode.md)。

## 安装(四个 NuGet 包,GitHub Packages)

包发布在 GitHub Packages 源 `https://nuget.pkg.github.com/wjw1-Evan/index.json`(读取需 GitHub
PAT,权限 `read:packages`)。先把源与凭据配进 `nuget.config`(仓库级或用户级皆可):

```xml
<configuration>
  <packageSources>
    <add key="github" value="https://nuget.pkg.github.com/wjw1-Evan/index.json" />
  </packageSources>
  <packageSourceCredentials>
    <github>
      <add key="Username" value="<GitHub 用户名>" />
      <add key="ClearTextPassword" value="<PAT,read:packages>" />
    </github>
  </packageSourceCredentials>
</configuration>
```

> 也可走用户级配置(对本机所有仓库生效):`dotnet nuget add source
> https://nuget.pkg.github.com/wjw1-Evan/index.json --name github --username <用户名>
> --password <PAT> --store-password-in-clear-text`。CI 用 `GITHUB_TOKEN` 注入凭据。
> 示例见 [`dotnet/samples/AspireSample/`](../dotnet/samples/AspireSample/)。

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
- 服务端配置 `DOCSQL_KEY` 后自动启用 AES-256-GCM。**版本配套**:启用了传输加密的部署要求客户端与服务端同版本——每条加密连接以服务端随机挑战(RESP_HELLO)开头并绑定进 GCM AAD(防跨连接重放),跨版本连接会显式失败;升级时先升到同版本再滚动重启。

### 多主机池 `hosts=`(客户端负载均衡 + 故障转移 + 实例自动发现)

连接串给 `hosts=h1:p1,h2:p2[,…]`(逗号分隔;`;` 是连接串的配对分隔符,列表内只能用
`,`),连接建立即自动轮换 + 失败切换;写**一个种子**即可,其余实例自动发现:

```csharp
await using var conn = new DocsqlConnection("hosts=db1:7600;token=...");
await conn.OpenAsync();
conn.DataSource        // 打开后 = 实际连上的节点 host:port(轮换/切换可观测)
conn.DiscoveredPeers   // 从该节点学到的其余实例(host:port),无需手工列表
```

- **自动发现**:每次建连成功后客户端向该节点要一次集群状态(`REQ_STATUS` 的
  `peers` = 活扇出成员表,含 `join` 动态注册的节点;主从形态取 `replicate_to`),
  学到的实例与连接串候选合并后一起参与轮换 —— `hosts=seed:7600` 与手工写全列表
  等价,新节点加入/退出无需改连接串;
- **出错即刷新**:一次 `Open` 中任一候选失败、切换成功后,立即从幸存节点重建实例表
  (下一次 Open 就按新表轮换);正常运行期缓存约 5s 刷新一次,连接池空闲命中不额外
  打请求;
- **轮换**:合并表(配置 + 发现)按进程级游标轮转,三节点集群的新连接均匀散布
  (负载分摊);近期失败过的端点自动沉到候选尾部、60s 后回到轮换(不重启进程),
  Open 不会每次都先吃一次失败拨号;池化默认开启,空闲连接命中不拨号也就不轮换;
- **故障转移**:候选离线、拨号超时、认证拒绝、池验活失败、池满等待超时,一律自动
  换下一个节点重试,应用代码无感知;全部候选失败 → 一条合并错误,每个端点一行原因;
- **预算**:`connect timeout`(默认 15s,单位秒)按候选均分到每次尝试(下限 1s),
  长列表不会把首个死节点拨号拖到趋零;
- **降级安全**:只读令牌 / 非 admin 数据库用户读不到集群状态时发现自动退回连接串
  候选表(轮换与故障转移照常);连接串里配置的节点永远保留,发现只增不删;
- **部署形态提醒**:发现到的是节点 `DOCSQL_PEERS` 里配置的地址(容器部署如
  `node-b:7600`,只在容器网络内可解析)。宿主机上的客户端连接 compose 发布的
  端口时,请在 `hosts=` 里写全需要访问的发布端点(如
  `hosts=127.0.0.1:17601,127.0.0.1:17602,127.0.0.1:17603`),或让客户端进入
  同一容器网络、或走 LB 入口;不可解析的发现项失败一次后即被降温排到尾部,
  不影响正常轮换。
- 与 `host=`/`port=` 同时给出显式报错(避免歧义);裸条目(无端口)用默认 7600;
  嵌入式 `Data Source=` 形态仍优先;`DocsqlSubscriber` 不支持 hosts(订阅专用连接
  与节点一一绑定,换节点续传需应用侧重建订阅);
- 池按端点分槽(与单 host 连接串同认 `pooling`/`max pool size`;
  `ClearPool()` 清空列表内每个端点的池)。

对称集群下推荐 `hosts=<任一种子>` 直连各节点;若更想要**单地址入口**(对外只暴露一个
端口、死节点自动摘除),部署侧可用 compose 方案的 stream-LB(见
`docs/operations.md#负载均衡lb-profile`),两者可叠加:LB 背后仍然轮换三个节点。


### 原生 TLS(连接串开关)

节点启用 `DOCSQL_TLS_CERT`/`DOCSQL_TLS_KEY` 后,连接串加 `tls=true` 即走原生 TLS
(SslStream/rustls,协议帧不变):

```
host=node-a;port=7600;tls=true
host=10.0.0.5;port=7600;tls=true;tls_ca=/certs/ca.pem;tls_host=db.internal
```

- `tls_ca=<PEM 路径>`:对该 CA 做完整链 + 名称验证;**未设 = 只加密不验证证书**
  (自签部署形态,与服务端 `DOCSQL_TLS_CA` 未设时一致);
- `tls_host=<名称>`:按 IP 连接而证书签给域名时,指定 SNI/校验名;
- 明文客户端连 TLS 节点(或反之)握手即败,无降级;
- `DocsqlSubscriber` 与连接池复用同一路径(池内重开不重复握手)。

CLI 远程模式:`DOCSQL_TLS_CONNECT=1`(可选 `DOCSQL_TLS_CA`)后 `docsql connect <addr>`
即走 TLS —— compose 健康检查在容器 env 里设置后自动生效。

### Python 驱动(DB-API 2.0)

`python/` 目录自带纯标准库的 DB-API 2.0 驱动(第二语言驱动,直接对话 v1
二进制协议;详见 `python/README.md`):

```python
import docsql
from decimal import Decimal
from datetime import datetime, timezone

with docsql.connect(host="127.0.0.1", port=7600, token="...",
                    tls=True, tls_ca="/certs/ca.pem") as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO t VALUES (?, ?, ?)",
                    (1, "alice", Decimal("9.99")))
        cur.execute("SELECT * FROM t WHERE ts > ?",
                    (datetime(2026, 1, 1, tzinfo=timezone.utc),))
        rows = cur.fetchall()
```

- `paramstyle="qmark"`:模板 REQ_PREPARE 注册一次、REQ_EXECUTE 服务端绑定
  (与 .NET 同一注入关闭机制);prepared 模板按连接缓存(上限 96,超限
  REQ_CLOSE_STMT 逐个注销);
- 精确标量:`Decimal`↔DECIMAL(`$dec`)、UTC `datetime`↔TIMESTAMP(`$ts`)、
  `bytes`↔BLOB(`$bytes`);超 int64 的整数自动走 `$dec`,超出 DECIMAL 值域
  (2^96-1)的整数/`Decimal` 绑定时客户端即抛 `DataError`(不再静默降级为文本);
- `autocommit` 默认 **True**(单写者网络库,隐式事务会停住全局写路径;
  DB-API 偏离已在文档明示),`autocommit=False` 为 psycopg2 风格隐式块;
- 读/写超时会**毒化连接**(服务端仍会送出迟到应答,复用会让下一条语句拿到
  上一条的结果——静默错位一帧);捕获超时后重连,不要复用旧连接对象执行;
- 原生 TLS 用 stdlib `ssl`(`tls_ca` 缺省 = 只加密不验证,自签形态);
  `DOCSQL_KEY` 帧加密需 `pip install docsql[crypto]`(cryptography 选装);
- `docsql.Subscriber`:持久 pub/sub 专用连接 + 专职读线程,空闲自动 PING 保活
  (应对 `DOCSQL_IDLE_TIMEOUT`),断线自动重连并从每频道最后收到的 id 之后
  续传(`id > from`,不丢不重;从未投递过消息的频道断连窗口内的消息仅显式
  数字 `from_` 可恢复);`ping(reconnect=True)` 重连后未决事务响亮报错
  (服务端已在断连时回滚),连接级 prepared 缓存自动失效重编(句柄随物理连接存活,短命游标共享同一缓存);
- 错误层级:DB-API 标准族,映射基于服务端错误文本启发式;协议级
  `ProtocolError` 属 `InterfaceError` 子类(传输失效,连接必须弃用)。

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
- **借出即重置 T-SQL 会话**:@变量/`@@IDENTITY`/`@@ROWCOUNT` 挂在物理连接上,借出
  验活时以 `REQ_SESSION_RESET` 一并清零(下一位借出者不继承上一位的会话状态);
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
// 运行期连接串工厂(每次创建物理连接时调用;连接信息不进模型缓存键):
// services.AddDbContext<AppDb>(o => o.UseDocsql(() => Route.CurrentConnectionString));
```

- 原生提供程序(不依赖 SQLite);`EnsureCreated` + 惰性建表 + 模型/索引自动同步(免迁移);
- `UseDocsql` 三种入参:`string`(静态连接串)、`DocsqlConnection`(固定连接实例)、
  `Func<string>`(**运行期连接串工厂**——测试夹具把每个用例路由到独立节点、读写入口切换
  等场景;同一进程模型只建一次,连接按上下文解析);
- 类型映射:`decimal` → `DECIMAL`(参数经 `$dec` 文本标记精确绑定,服务端十进制聚合/比较;`HasPrecision` 进入列类型与 CAST 字面量)、`byte[]` → `BLOB`(参数经 `$bytes` 标记,响应按 `{"$bytes":[…]}` 解码;单值 ≤16MiB 文档上限)、`DateTime`/`DateTimeOffset` → `TIMESTAMP`(参数经 `$ts` UTC 毫秒标记,精度无损;`timestampformat=iso` 为旧服务器兼容开关,发 ISO-8601 文本)、`DateOnly`/`TimeOnly`/`TimeSpan`/`Guid` → 可排序 ISO 文本;未映射的 CLR 类型在模型构建期显式报错;
- 模型复合索引(`HasIndex(e => new { … })`)按列序创建;实体集合属性 `List<T>.Contains` 翻译为 `JSON_ARRAY_CONTAINS`(常量/参数/取反/数值元素);字符串 `StartsWith/EndsWith/Contains` 翻译为 `LIKE`(常量转义 `%`/`_`/`[`,保持 .NET 字面子串语义;变量路径与 SQL Server 提供程序同形);引擎不支持的语法在翻译期显式失败;
- **T-SQL 函数下推**(引擎第四批函数族,查询不回落客户端求值):
  日期成员 `Year/Month/Day/DayOfYear/Hour/Minute/Second/Millisecond` → `YEAR/MONTH/DAY/DAYOFYEAR/DATEPART`;
  `AddYears/…/AddMilliseconds` → `DATEADD`(月末钳制、毫秒分辨率);
  `DateTime.Now/UtcNow` → `GETDATE()/GETUTCDATE()`(引擎仅 UTC);
  `EF.Functions.DateDiffYear/Quarter/Month/Day/Week/Hour/Minute/Second/Millisecond` → `DATEDIFF`(边界语义);
  `Math.Abs/Ceiling/Floor/Pow/Round/Sqrt/Sign/Exp/Log/Log10`;字符串 `Length/ToUpper/ToLower/Trim/Replace/Substring/IsNullOrEmpty/Concat`;`Guid.NewGuid()` → `NEWID()`;
- **实体集合属性**(`List<string>`/`List<int>` 等,JSON 数组列)的 `Contains` 翻译为服务端
  `JSON_ARRAY_CONTAINS(list, item)`——常量元素、跨实体列(`t.RoleIds.Contains(p.Id)` 权限形态)、
  取反与数值集合均可;成员查询不回落客户端求值;
- `Dictionary<string, object>` 属性约定映射为 JSON 文本标量(读写、整体变更跟踪);字典内部
  成员不参与 SQL 翻译(在 WHERE 里按键过滤仍在客户端侧不可用,请提取为独立列);
- 异常分类:`DocsqlException.IsUniqueViolation`(唯一约束冲突,幂等写入/重复键语义)与
  `IsSyntaxError`(解析错误),不需要匹配错误文本;
- 导航 `Any`/关系层的相关 EXISTS 子查询(引用外层行的子查询)会被引擎显式拒绝:迁移应用时
  对导航条件、集合成员查询逐条审计,必要时改写为显式 JOIN + 引擎支持的形态;
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
docsql-cli todos.db -f init.sql                    # 嵌入式模式:直开数据文件(SQLite 使用姿势,:memory: 内存模式)
docsql-cli connect 127.0.0.1:7600 --user analyst   # 远程模式:连接 server;密码走 DOCSQL_PASSWORD 或交互提示;--user 缺值是用法错误(不静默降级匿名)
  --csv / --json        行导出(RFC 4180 CSV / JSON 对象数组;启动横幅走 stderr,批处理 stdout 恒为纯格式文档)
  --table               对齐表格输出(默认;按终端显示宽度对齐,CJK 双宽字符不再错位)
  -f script.sql         脚本批执行(错误即退出 1)
  -V, --version         版本号
  help;                 内联命令帮助(含 pub/sub 命令面)
```

嵌入式模式把数据库文件直接交给进程内引擎(与 server 同源),无守护进程、无 Docker;第一个
参数即数据库文件路径。**该文件同一时刻只归属本进程**(见[已知边界](limitations.md))。

Rust 库嵌入与 .NET 嵌入路线评估:[设计 006 · .NET 嵌入式模式](design/006-dotnet-embedded-mode.md)
(.NET 短期以本地裸 server 达成;FFI 零守护为此设计路线图,评估稿未实现)。

交互细节:`exit`/`quit`/`help` 大小写不敏感、可带可不带分号(嵌入与远程模式行为一致,
脚本末尾的 `exit;` 行在两种模式下都正常结束);sqlite3/psql 风格元命令(`.exit`、`\q`)不
支持,会给出提示而非 SQL 解析错误;交互终端(stdin 与 stdout 均为 tty)提供行编辑与
↑/↓ 历史(历史存 `~/.docsql_history` 跨会话保留)并显示 `docsql> ` / 多行续行 `...> `
提示符;Ctrl+C 丢弃当前语句缓冲回到提示符(空提示上连续两次才退出,退出码 130;正在执行
的语句在引擎/服务器侧跑完后会话继续,不再整个进程被杀),Ctrl+D 在语句缓冲非空时**丢弃**
该残缺语句(半截 DELETE 不因缺分号而执行),脚本/管道输入仍冲刷无尾分号的末条语句;表格
输出含 CJK 数据按显示宽度对齐,空结果保留表头行并显示 `(0 rows)`;脚本错误附出错语句的
单行回显(`in: …`)。

退出码约定:0 = 成功(交互会话即使中途 SQL 出错也以 0 结束,与 psql 一致);1 = 脚本
(`-f`/管道 stdin)中的 SQL 错误、`publish`/`subscribe`/`trim` 被拒、传输失败、坏帧;2 = 用法
错误或登录失败;130 = SIGINT 终止。CSV 公式注入防护只拦 `=`/`+`/`@` 与非常规 `-` 形态,负数
保持数值单元格。注:CLI 暂不支持 `DOCSQL_KEY` 帧加密节点(用 .NET/Python 驱动或 TLS)。

订阅是专用连接 + 专职读线程(CLI 已内置):普通一问一答连接会把推送帧错当成命令响应。

## 嵌入式模式(Data Source,进程内引擎)

.NET 的 SQLite 式形态:**连接串写 `Data Source=<文件>` 就在本进程内跑引擎** ——
无 server、无端口、零守护(设计 [006](design/006-dotnet-embedded-mode.md),实现为
`crates/docsql-ffi` 的 C ABI + `Docsql.Client` 的 `EmbeddedSession`)。

```csharp
// EF Core(懒建表/索引同步与远程模式完全一致)
services.AddDbContext<TodoDb>(o => o.UseDocsql("Data Source=todos.db"));
// ADO.NET
using var conn = new DocsqlConnection("Data Source=todos.db");
// 进程内内存库:每次 Open 一个独立引擎(不共享)
using var mem = new DocsqlConnection("Data Source=:memory:");
```

- **能力**:全部 SQL(DDL/DML/JOIN/聚合/窗口/约束/触发器)、事务(SAVEPOINT)、
  prepared 路径与参数化 —— 参数渲染复用 `docsql_core::bind`(与 server 同一份实现,
  注入面不复制);DECIMAL/BLOB/TIMESTAMP 走与线协议相同的 `$dec`/`$bytes`/`$ts` 标记;
- **原生库获取**:GitHub Releases 的 `docsql-native-<版本>-<平台>.tar.gz/.zip`;
  加载顺序:`DOCSQL_EMBEDDED_NATIVE`(完整路径)→ 应用目录 `runtimes/<rid>/native/`
  → 应用目录 → 仓库 `target/{debug,release}`(开发/测试用)。缺库抛
  `DocsqlException`,消息里带放置指引与 ABI 版本约定;
- **进程内并发**:同一文件的所有连接共享一个引擎实例(进程级注册表);读走 MVCC
  快照、不阻塞写;**任一连接开启事务后,其他连接的语句按 `CommandTimeout`(缺省 30s)
  排队**,超时报 "another connection holds the open transaction";事务 owner 的
  SELECT 看见自己未提交的写(与 server 的 owner 语义一致);
- **单进程归属**:数据文件同一时刻只允许一个进程打开(引擎无跨进程锁)——
  跨进程/跨机并发用 server 形态;
- **不支持的项(响亮报错)**:pub/sub(`Publish`/订阅)、`Promote`、备份 REST/协议、
  集群复制、T-SQL 批会话变量 —— 这些能力属于 server,连接 `host=…;port=…` 使用;
- 安全模型与 SQLite 一致:**能加载库的进程 = 全库权限**(本地嵌入没有 token/用户概念;
  文件权限 0600 由引擎创建时保证)。

## 从其他数据库切换

**EF Core 应用**:把 `UseSqlServer` / `UseMySql` / `UseSqlite` 换成 `UseDocsql`,连接串换成
DocSQL 的 `host=…;port=…`(远程)或 `Data Source=app.db`(进程内嵌入)——模型、LINQ、事务
与 `EnsureCreated` 语义不变(免迁移,模型漂移自动同步)。

**ADO.NET 应用**:`SqlConnection` / `MySqlConnection` / `SqliteConnection` 换成
`DocsqlConnection`;SQL 文本按下方方言矩阵核对(常见语句——`TOP`/`LIMIT`、`OFFSET`、
窗口函数、`ON CONFLICT`/`ON DUPLICATE KEY UPDATE`、`RETURNING`、`NVL`/`IFNULL`/
`ISNULL`、`::` 转换——已兼容)。

**方言矩阵(测试守着)**:SQL Server(`[方括号]`、`N'…'`、`IDENTITY(1,1)`、
`SET IDENTITY_INSERT`)、MySQL(backticks、`AUTO_INCREMENT`、`TINYINT(1)`/`UNSIGNED`、
`ON DUPLICATE KEY UPDATE`、`IF`/`LOCATE`/`CHAR_LENGTH`/`MOD`/`POW`/`TRUNCATE`)、
PostgreSQL(`SERIAL`/`BIGSERIAL`、`GENERATED … AS IDENTITY`、
`::`、`ILIKE`、`ON CONFLICT … EXCLUDED`、`POSITION`/`OCTET_LENGTH`)、
SQLite(`INSERT OR REPLACE`、`sqlite_master`)、
Oracle(`NVL`/`DUAL`/`ROWNUM`/`SYSDATE`)均有用例;拒绝面(表选项、`ON UPDATE`、
`SET`/`SEQUENCE`、`datetime('now')`、Oracle 序列)一律**响亮报错**。完整差异清单见
[已知边界 · 从其他数据库切换/迁移](limitations.md#从其他数据库切换--迁移)。

> **线协议现实**:DocSQL 使用自有 v1 二进制协议。使用 `Microsoft.Data.SqlClient` /
> `MySqlConnector` / `Npgsql` / `Microsoft.Data.Sqlite` 等**第三方驱动**的程序无法仅靠
> 连接串切换——需要换成 `Docsql.Client`(或 Python DB-API / CLI / REST),这一步是驱动级
> 改动,不是 SQL 级改动。DocSQL 不模拟 Postgres/MySQL 线协议。

## 驱动作者:线协议参数化

v1 二进制协议(`crates/docsql-core/src/proto.rs`),一句一帧。服务端 prepared statements 三帧:

| 帧 | 载荷 | 响应 |
|---|---|---|
| `REQ_PREPARE 0x0003` | SQL 文本,`?` 为占位符 | `RESP_PREPARED 0x010E` `{"handle":n}` |
| `REQ_EXECUTE 0x0004` | `{"handle":n,"params":[...]}` | 与 REQ_SQL 相同(RESP_ROWS/AFFECTED/ERROR) |
| `REQ_CLOSE_STMT 0x0005` | `{"handle":n}` | RESP_AFFECTED / RESP_ERROR |

- 参数按位置绑定,服务端渲染类型化字面量(引号感知;字符串值翻倍转义)——驱动无需自行转义;
- 句柄按连接隔离,连接断开即失效;
- 参数类型:JSON `null/bool/number/string`(嵌套对象/数组按 JSON 文本绑定);精确标量用标记对象:`$dec`(十进制文本 → `CAST(… AS DECIMAL)`)、`$ts`(UTC 毫秒整数 → TIMESTAMP 值)、`$bytes`(整数数组 → `x'…'` hex 字面量);响应中 DECIMAL 为 `{"$dec":"…"}`、非有限浮点为 `{"$float":"NaN"|"inf"|"-inf"}`、TIMESTAMP 为 `{"$ts":毫秒}`、BLOB 为 `{"$bytes":[…]}`;
- 授权、`DOCSQL_STATEMENT_TIMEOUT_MS`、审计与 REQ_SQL 完全同路径;
- 订阅帧、REQ_STATUS(含 `metrics` 运行时计数器)等其余帧见 `crates/docsql-core/src/proto.rs` 模块头注释(权威)。
