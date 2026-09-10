# AGENTS.md

面向 AI 编码代理与贡献者的开发指南(原 DEVELOPMENT.md 已并入本文)。用户侧功能见 [README.md](README.md)。

## 项目是什么

DocSQL:Rust 实现的原生文档数据库 + .NET 客户端栈。约 1.3 万行 Rust + 约 4 千行 C#:

- **文档式存储**:JSON 文档整体存储,无强制 schema
- **完整 SQL**:DDL/DML/JOIN/聚合/事务/约束/系统视图
- **Web 管理控制台**:DocSQL Studio(SSMS 风格;纯管理工具,自身零存储,所有数据操作连接指定节点执行)
- **.NET 生态**:ADO.NET 提供程序 + EF Core 提供程序
- **集群**:对称集群复制(任意节点可写)、主从写转发、PROMOTE 故障转移
- **发布订阅**:持久化 pub/sub(消息先落盘再推送,at-least-once,glob 模式订阅,跨节点扇出)

**运行环境仅 Docker**:镜像由 GitHub Actions 自动构建发布到 `ghcr.io/wjw1-evan/docsql`;文档与部署不再提供本地二进制/cargo run 运行方式(cargo 仅用于开发测试)。

## 常用命令

```bash
# 构建与测试(开发门禁,非运行方式)
cargo build --workspace
cargo test --workspace              # Rust 全量(约 290 用例)

# .NET 测试(需先 cargo build 出 server 二进制)
cd dotnet && dotnet test

# 运行(仅 Docker;镜像来自 GHCR,由 CI 自动发布)
# 单节点部署(生产:compose single profile:独立节点 :18600 + web :18710)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600    # SQL shell

# 多节点部署(compose cluster profile:3 节点对等集群 + web)
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d   # 生产(GHCR 镜像 + .env,端口 :18601-18603 + web :18700)
cd deploy && docker compose --profile cluster up -d --build   # 本地开发集群(源码构建镜像 :local,端口 :17601-17603 + web :17700)
# 本地开发与生产完全分离(项目名 docsql-dev/docsql-prod、端口 dev 1760x+1770x / prod 1860x+1870x、卷 docsql-dev-data-* / docsql-data-*),两套可同时运行;
# 停止带相同 profile 参数:--profile single --profile cluster down
# 数据卷是 external 卷(dev docsql-dev-data-*,prod docsql-data-* + join 用 docsql-prod-data-d):down/-v 都不清数据(发布不丢数据);
# 彻底清数据唯一入口 ./deploy/reset-data.sh;首次部署先 docker volume create(见 README)

# 部署测试(改复制/部署逻辑后必跑;用本地开发 compose 文件):
# 默认构建 :local 镜像(内置 cargo test 门禁);传 DOCSQL_DEV_IMAGE_TAG 复用已有镜像
./deploy/run-tests.sh            # 同时拉起 single + cluster 两个 profile:多节点 76 项 + 单节点 26 项
```

## 提交门禁(全部通过才能提交)

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

改动 dotnet 或协议相关时,加跑 `cd dotnet && dotnet test`。

### Mimosa 安全门禁(ZCode 插件,只作用于 AI 会话内的 commit/push)

ZCode 的 Mimosa 插件在 `git commit`/`git push` 前做 L3 静态扫描,high 拦截、medium 询问、low 提示。**已知现状:native 引擎对任何进程启动无差别报「命令注入」high(全字面量也报;`mimosa-ignore` 注释与 `validate` Oracle 均不适用),且不止 C#——Rust 测试里的 `std::process::Command` 同样命中,并且 Edit/Write 的增量扫描会直接拦截写入(high=拦,不受 git warn 档影响;warn 只放宽 commit/push)**。集成测试必须在测试进程内启动 `docsql-server`,故 dotnet 测试基建的 12 条命中(File: EfTests.cs ×4、TransportEncryptionTests.cs ×2、AdoNetTests/AsyncCommitTests/FailoverTests/QueryLogTests/SymmetricClusterTests/EfSample Program.cs 各 1)是已裁定误报,代码层面无法消除。本机配置 `MIMOSA_GIT_GATE_MODE=warn`(见 `~/.zshrc`):git 门禁保持扫描与记录,high 只告警不拦截。面向该标准的代码约定:

- **生产代码**(core/server/web/cli)不启动子进程,天然无此类 finding;今后任何生产代码里的进程执行都必须参数列表传递、禁止拼接 shell 字符串,且不得有用户可控输入流入。
- **测试基建**启动 server 的代码集中在各测试文件的既有辅助方法(C#:`FindServer`/`StartServer` 形态,`ProcessStartInfo` + `UseShellExecute=false` + `ArgumentList`;Rust:进程内 `tokio::spawn`,见 e2e.rs 的 `spawn_node`/`spawn_node_handle`),新测试复用,不在测试体内新增散落的进程启动;Rust 侧需要「重启节点」用 `JoinHandle::abort()` + 等端口关闭 + 同数据文件重启(`spawn_node_handle`),**不要起子进程**——会被 Edit 增量扫描拦截;不要为绕过扫描改写 API 形状(反射/P-Invoke 等是掩盖不是修复)。
- 改动上述 spawn 辅助方法的提交,在 `warn` 模式下会看到对应告警,属预期,忽略即可;若换新机器/重装插件后提交被拦,检查该环境变量是否生效。注意 `~/.zshrc` 的 export 只对终端直启 ZCode 生效,GUI(Dock/Finder/`open`)启动继承 launchd 环境——已用 `launchctl setenv MIMOSA_GIT_GATE_MODE warn` 注入,并由 `~/Library/LaunchAgents/com.user.mimosa-gate-mode.plist`(登录时自动 setenv)持久化;新机器需重建这两处。验证:`launchctl getenv MIMOSA_GIT_GATE_MODE`,然后重启 ZCode。

CI(`.github/workflows/docker-image.yml`,push/PR 触发)执行同样三门禁 + dotnet 测试,通过后构建多架构镜像(amd64/arm64,`RUN_TESTS=false`)发布到 `ghcr.io/wjw1-evan/docsql`;main 分支另跑 compose 部署测试(多节点 76 项 + 单节点 26 项)。

测试层次(约 290 个 Rust 用例):

- **单元/内核**:core 的 pager/WAL/B+树/engine 各模块内测试;server 的 pubsub 注册表/存储辅助
- **SQL 集成 / 协议**:各 crate tests
- **端到端**:`crates/docsql-server/tests/e2e.rs`(含 pub/sub 实时/回放/续传/trim/跨节点、peer 离线→再上线补齐(反熵修复)、无分歧重启不动数据)与 `crates/docsql-web/tests/e2e.rs`(随机端口起真实 web 服务 + 手写 HTTP/1.1 客户端,覆盖控制台页、`/api/sql` 单语句/批量、`/api/parse`、token 门禁全端点、meta/stats、cluster 对活/死节点探测)
- **多节点部署**:Docker compose 内 76 项测试(CI 对 main 分支在镜像发布后执行;本地 `./deploy/run-tests.sh` 会先构建带测试门禁的本地镜像),改动部署/复制相关逻辑后必跑;测试的干净态由 run-tests.sh 自己 rm+重建 external 卷保证
- **.NET**:`dotnet test`(ADO.NET Client 43 项 + EF Core 22 项)

## 仓库结构与模块地图

```
Cargo.toml              # workspace
crates/
  docsql-core/          # 存储引擎 + SQL 内核(无网络依赖,可嵌入式)
  docsql-server/        # TCP 服务器、鉴权、复制
  docsql-cli/           # 嵌入式 shell 与远程客户端
  docsql-web/           # Web 控制台(REST API + 内嵌单页 UI)
dotnet/                 # Docsql.Client(ADO.NET)、Docsql.EntityFrameworkCore、测试与示例
deploy/                 # docker-compose.yml(本地开发,源码构建)、docker-compose.prod.yml(生产,GHCR 镜像 + .env);均含 single/cluster/join 三个 profile(join = 第四数据节点,node-d,验证新节点自动同步);测试脚本 single-test.sh(26 项)与 multinode-test.sh(76 项,含节点离线再上线自动补齐、网络分区与重启收敛、第 12 章新节点加入自动同步)
.github/workflows/      # CI:cargo/dotnet 测试 → 构建多架构镜像发布 GHCR → 部署测试
target/                 # 构建产物(git 忽略)
```

**docsql-core**(存储 + SQL 内核,自底向上):

| 文件 | 职责 |
|---|---|
| `value.rs` / `encode.rs` | 值类型与磁盘编码(可比较的有序编码,索引键序依赖它) |
| `pager.rs` | 手写分页器,页面读写与文件布局 |
| `wal.rs` | WAL 日志与崩溃恢复 |
| `heap.rs` | 堆文件:表的文档存取 |
| `btree.rs` | 页式 B+ 树索引(索引接入执行器、单事务提交、原地更新/删除);分裂按**字节**驱动(序列化超页即分裂,分裂点优先取插入位置以保持等键 run 按插入序),单键编码超过半页(~2KB)显式报 KeyTooLarge;等键可横跨分裂点,查找/删除经 `candidate_children` 对名义区间覆盖该键的子树兜底 |
| `json.rs` | JSON 文档模型与编解码 |
| `engine.rs` | SQL 执行器:AST → 计划 → 执行;约束、事务、SAVEPOINT、RETURNING、DISTINCT、外键检查、批量执行 |
| `guid.rs` | 时序有序 GUID(UUIDv7)生成:48bit 毫秒时间戳 + 毫秒内 12bit 计数器(新毫秒随机重播种,节点内严格单调、时钟回退不回退)+ 62bit 随机位(std `RandomState` 做随机源,无 rand 依赖);GUID 主键自动生成用 |
| `proto.rs` | 自定义二进制协议 v1 帧编解码(预留拓扑版本/重定向字段) |
| `meta.rs` | 对象浏览器元数据组装(server/storage/totals/tables):server 的 REQ_META 帧用该实现组装(控制台对象树的后端数据源由此与 UI 渲染形状稳定) |
| `stmt.rs` | 语句拆分 `split_statements`:批量 SQL → 逐句文本(单句原文直通,多句按 AST 重渲染);wire 协议一句一帧,控制台到节点的发送侧用 |
| `lib.rs` | AST/解析器入口 |

**docsql-server**:

| 文件 | 职责 |
|---|---|
| `lib.rs` / `main.rs` | tokio TCP 服务器:SQL 会话、REQ_AUTH 鉴权(客户端 token / 集群 token 双身份 `ConnRole`,复制帧仅 peer)、REQ_PROMOTE 故障转移提升、REQ_STATUS 节点状态报告(JSON)、REQ_LOGS 日志报告、REQ_DIGEST/REQ_SQL_SEQ/REQ_CATCHUP(catch-up 复制:摘要探测、带序号复制写、增量拉取)、pub/sub 帧路由、副本协议 |
| `pubsub.rs` | 持久化发布订阅:活订阅注册表(精确频道 + glob 模式)、Redis 式 `*`/`?`/`[...]` 匹配、推送帧构造、`_pubsub_messages` 存储辅助(建表/插入/水位/区间回放/trim)、`docsql_pubsub` 视图重写 |
| `crypto.rs` / `querylog.rs` | 鉴权辅助与双日志:**查询日志**(`docsql_log` 环形缓冲,语句审计)+ **同步日志**(`SyncLog`:写扇出/发布扇出/PROMOTE/join 逐目标成败);`logs_payload` 组装 REQ_LOGS 载荷(web 控制台日志页数据源,两环各取最新 N 条倒序) |

**docsql-web**:

| 文件 | 职责 |
|---|---|
| `lib.rs` / `main.rs` | REST API(`/api/sql` `/api/parse` `/api/meta` `/api/stats` `/api/cluster` `/api/logs` `/api/auth/*`)、`DOCSQL_TOKEN` 认证。**纯管理工具、自身零存储**(不打开任何数据文件):启动参数/`DOCSQL_UPSTREAM`/首个 peer 指定**默认管理节点**,数据端点不带 `node` 时即连接它执行;`DOCSQL_PEERS` 节点探测(PING + REQ_STATUS,集群状态页数据源)兼作节点切换的允许列表,数据端点可带 `node` 参数切换管理目标(白名单仅放行 `DOCSQL_PEERS`,SSRF 防护;连接经 wire 协议 REQ_AUTH——用服务端自身 token,非浏览器提交值;批量 SQL 由 `stmt::split_statements` 逐句发 REQ_SQL,单连接保序;meta/stats 走 REQ_META/REQ_STATUS),未配置管理节点时数据端点 in-band 报配置缺失;**`/api/parse` 恒为本地静态检查**(纯解析,无存储);**`/api/logs`** 返回控制台自身提交语句的审计环(`record_console_sql`,peer=实际执行该语句的节点)+ 并发拉取各节点 REQ_LOGS(读缓冲上限 LOGS_RECV_CAP 4MB,limit 1..=1000 夹紧),不感知 `node`(日志页按来源聚合是它的本意);**`/api/auth/*`** 控制台账号门(见 `auth.rs`),setup 一次性 / login / logout / status,数据端点凭 HttpOnly 会话 Cookie 或 `DOCSQL_TOKEN` 旁路通过,Setup 态且未配 token 时保持匿名可达(兼容窗口,setup 完成即关) |
| `auth.rs` | 控制台账号:自带 SHA-256/HMAC/PBKDF2(FIPS 180-4 / RFC 2104 / RFC 8018,有已知答案测试,勿引加密 crate 除非删除本实现)、凭据文件存储(盐化 PBKDF2-HMAC-SHA256,0600,损坏文件拒绝启动而非重置)、内存会话(滑动 12h)与登录锁定(10 次/60s → 锁 60s,按来源 IP)。`DOCSQL_WEB_AUTH_FILE` 未设或为空 = 整个门禁关闭(legacy;run-tests.sh 依赖此点),已设 = 首次使用强制 setup |
| `console.html` | DocSQL Studio 单页 UI(通过 `include_str!` 内嵌进二进制,改完必须重新 `cargo build`);含集群状态页(5s 轮询 `/api/cluster`)、日志页(`openLogsTab`:数据/同步/仅错误类别 + 来源 + 关键词过滤,5s 激活时轮询,同步事件中文标签 SYNC_EVENTS)与工具栏节点切换器(`nodesel`:首项为「默认节点」即后端配置的管理目标,api 层统一携带 `node`,选择持久化 localStorage,节点从 peers 消失自动回落默认节点,节点不可达时资源管理器显示 in-band error) |

**dotnet/**:

| 项目 | 职责 |
|---|---|
| `Docsql.Client` | ADO.NET 提供程序(`DocsqlConnection/Command/DataReader` 等),token 认证(REQ_AUTH 原始 token);pub/sub:`Publish(channel, payload)` 返回 (id, receivers)、`PubsubTrim`,订阅走 `DocsqlSubscriber`(专用连接 + 后台读线程分发回调,`DocsqlSubscriber.cs`) |
| `Docsql.EntityFrameworkCore` | 复用 SQLite 管线 + Docsql ADO.NET;`UseDocsql(connectionString)`;惰性建表/索引同步默认开启(`AutoCreate`/`SchemaSync`/`Infrastructure/`) |
| `Docsql.Client.Tests` / `Docsql.EntityFrameworkCore.Tests` | xUnit 套件 |
| `Docsql.EfSample` | 一键端到端示例 |

## 关键内部机制

### 存储:页面 + WAL + B+ 树

- 一切落盘经由 `pager`;WAL 先写日志再应用页面,启动时恢复;事务内的页像延迟到 WAL fsync 后才落数据文件(pager `pending_writes`),动 pager 提交路径时保持该顺序
- 索引键序依赖 `encode.rs` 的可比较有序编码——**新增值类型必须同步扩展编码,否则索引序被破坏**
- 索引接入执行器(点查走索引探针)、UPDATE/DELETE 走快速路径原地更新、单事务一次提交
- **主键/表声明 UNIQUE 的自动索引是派生展示,不落 catalog**:建表即建 B+ 树(`index_roots`),`catalog()` 按 `primary_key`+`constraint_unique` 派生 `sqlite_autoindex_<表>_<n>`(旧数据卷零迁移,各节点重算一致);`DROP INDEX`/`CREATE INDEX` 对该前缀显式报错;`sqlite_master` 不列自动索引(SQLite 语义——EF SchemaSync 只看 sqlite_master 且只回收 `IX_` 前缀,互不影响)

### 会话事务与复制缓冲

- SQL 事务 `BEGIN/COMMIT/ROLLBACK` + `SAVEPOINT`(`SAVEPOINT/ROLLBACK TO/RELEASE`);引擎是单全局事务(单写者),并发连接的 BEGIN 在服务端排队(`BEGIN_QUEUE_WAIT` 30s 上限)
- **全局事务有连接所有权**(`ServerState::tx_owner`):BEGIN 的连接成为 owner,其写语句缓冲在 `tx_pending`,提交时按执行顺序转发给上游/对等节点,回滚即丢弃;**非 owner 连接的写与复制写不并入打开的事务**——像 BEGIN 一样有界排队等待事务关闭(超时报错),否则 owner 的 ROLLBACK 会丢掉别人已确认的写造成静默分叉;COMMIT/ROLLBACK/SAVEPOINT 来自非 owner 直接报错;owner 连接断开时服务端自动 ROLLBACK 其未提交事务并清 `tx_pending`(控制台的短连接因此不会在节点上悬挂事务;cluster join 的 drain 路径持 write_order 直用,复用 `execute_sql` 的 `order_held` 参数防自锁)
- 写执行与扇出经 `write_order` 互斥串行化,保证对等节点按本节点执行顺序应用

### 网络与复制

- 自定义二进制协议 v1(`core/proto.rs`),帧头预留拓扑版本/重定向字段;请求帧:REQ_SQL、REQ_AUTH(token 认证)、REQ_PREPARE/REQ_EXECUTE/REQ_CLOSE_STMT(参数化语句)、REQ_PING、REQ_PROMOTE(故障转移提升)、REQ_STATUS(节点状态 JSON 报告,需认证;web 控制台集群页的探测数据源)、REQ_LOGS(日志报告,需认证;载荷可选 `{"limit":n}` 默认 200 上限 1000,返回查询日志+同步日志两环最新条目,web 日志页数据源)、REQ_META(对象浏览器元数据,需认证;控制台对象树的数据源,`core/meta.rs` 组装)、REQ_DIGEST(表复制摘要,需认证 + FLAG_REPLICATION;重启反熵修复的探测帧)、REQ_SQL_SEQ(带原点日志序号的复制写,载荷 [seq][node_id][sql];对端应用后记位点,旧版本对端回错则回退纯 REQ_SQL 重发)、REQ_CATCHUP(按位点增量拉取原点日志,RESP_CATCHUP 分块 + RESP_AFFECTED(head) 终止;重启反熵修复用,见下)、REQ_SUBSCRIBE/REQ_PSUBSCRIBE/REQ_UNSUBSCRIBE/REQ_PUNSUBSCRIBE、REQ_PUBLISH、REQ_PUBSUB(内省/trim)、REQ_SYNC/REQ_HOLD/REQ_RELEASE(新节点加入与重启反熵修复的快照通道,见下),推送帧 RESP_PUSH(仅发给订阅过的连接)。**REQ_SQL 执行全文不截断**:查询日志自行截断显示;曾在执行路径截 512 字符,超过 512 字符的语句被无声截断(控制台发长文档 INSERT 即触发),已修并有回归测试
- 复制两种形态:**对称集群**(`DOCSQL_PEERS` 互相扇出,任意节点可写)与**主从写转发**(`replicate_to` 指向主,副本只读);`PROMOTE` 提升副本为主
- **新节点自动同步(cluster join)**:全新节点(无用户表)配置了 `DOCSQL_PEERS` 即在启动后自动 bootstrap——先经 REQ_STATUS 探测(带 FLAG_REPLICATION),从首个有数据的 peer 发 REQ_SYNC(可带 `DOCSQL_ADVERTISE` 通告自身地址);服务端持自身 `write_order` → 向每个 peer 发 REQ_HOLD(对端 5s 内排空在途写后冻结并注册 joiner,抢不到锁回 busy,60s 看门狗防泄漏)→ **任一 hold 失败整体中止**(部分冻结的网格会漏写,joiner 下轮重试)→ 全网静止时 `Database::dump_script()` 取快照(DDL 全部在前、INSERT 在后,FK 延迟到插入期检查所以建表顺序无关;跳过 `_pubsub_messages`)→ 注册 joiner → REQ_RELEASE 解冻 → 以 ≤4MB 的 RESP_SYNC 块流式回传。joiner 在单事务内整体重放(失败即回滚保持全新)。**加入期间的复制写入队即确认**(`sync_queue`):扇出在源节点写路径内执行,若让扇出在门上等待会占死源节点 write_order、与 REQ_SYNC 互锁(实测过的活锁);快照 → 队列按到达序 → 闭队后直用,构成收敛所需全序,入队写按构造晚于快照不会重复;bootstrap 的每个终态(Applied/LocalData/放弃/空集群)都必须 drain 队列。加入前若本地已有客户端写入则放弃 bootstrap 保留本地数据(注册仍生效,后续写靠扇出收敛)。空集群(所有 peer 都 0 表)不互相 sync,静态 peers 配置即覆盖
- **重启反熵修复(rejoin repair,两阶段)**:持有数据的节点配置了 `DOCSQL_PEERS` 时,启动即并发探测各 peer 的表摘要(REQ_DIGEST,载荷为 `Database::digests()` 的 JSON:每表 行数 + 行哈希(逐行编码字节的序无关求和,堆布局无关)+ schema 哈希;系统表全跳过,schema 哈希刻意排除 pages/index_roots 等物理字段)与期刊窗口(REQ_STATUS 的 cluster_id/journal_head/journal_oldest)。与任一可达 peer 摘要一致 → 不修,直接 drain 关 sync 门。**阶段一:增量补齐(缺多少补多少)**——每个节点把本地提交的写按序记入 `_cluster_log`(节点内单调 seq,`journal_append` 在数据提交后、扇出前执行),扇出用 REQ_SQL_SEQ 携带 (seq, node_id),接收方应用后在 `_cluster_pos` 记位点;重入节点对每个原点按位点拉缺口(REQ_CATCHUP,只读不冻结),应用后重排空 sync 门再复验摘要,一致即收敛、**全程无快照无冻结**。边界:位点缺失(新加入原点/从未采纳过)、位点在窗口外(`DOCSQL_CATCHUP_WINDOW` 裁剪,默认 10 万条,每 512 条运行时裁剪)→ 增量不可行。**阶段二:快照兜底**——增量不可行或复验仍分歧时,按选举出的参考方快照整体重建(规则见下):复用 join 的 hold 冻结 → dump → 释放 → 流式回传,`apply_repair_sync` 在单事务内 `wipe_user_tables()`(绕过 DROP 的 FK 引用检查——快照里没有的本地表也要清掉)+ 回放,失败回滚保持原状;采纳成功后用探测到的各原点 head 播种位点,之后的重入走增量。参考方选择(全网格确定性,各节点对同一摘要集合算出同一结论):peer 按相同摘要分组,最大组胜出——组内成员互见一致走「不修」,其余节点拉取(从组内任一成员拉结果相同);无多数方(各方可达状态互不相同,如两节点各持独有写)时按「行数多者优先,并列按摘要序列化序」选出唯一参考方,参考方照常服务、其余拉取,**与启动顺序无关**;**空组永不采纳**(本节点是唯一有数据的副本时不得因 peer 全空而清库);所有 peer 不可达则重试数轮再按本地服务。**代价:少数方/数据较少一方的独有写被参考方快照覆盖**——没有向量时钟/行级合并,分区分歧中少数侧的写不保留(部署测试第 10.5 章断言该策略)。修复只由重启触发:仅分区未重启不收敛,任一 divergent 节点重启即全网格收敛。安全性要点:位点只能落后于数据不能超前(应用在前、位点在后,崩溃窗口只会造成重放冲突→快照兜底,绝不漏);期刊条目在数据提交后写入(崩溃窗口丢条目→摘要复验兜底);日志/位点/身份表全部系统表化(隐藏、排除摘要/快照/对象树)。sync 门对一切配置了 peers 的节点在启动期开放(join 与 repair 共用「快照 < 队列 < 直用」全序);修复轮数(20)刻意多于 fresh bootstrap(10),因为对端自身启动同步期间会拒绝服务快照,拉取方必须熬过对方的开门窗口。
- 动态注册只存在于内存:原节点重启后会丢失 joiner 注册,要长期保留把 joiner 写进各节点 `DOCSQL_PEERS` 并重建(不丢数据、不会重复同步——非空节点不 bootstrap)
- 鉴权:token(常数时间比较);集群节点间扇出自动先认证。**节点身份与客户端凭据分离**:`DOCSQL_CLUSTER_TOKEN` 配置后,REQ_AUTH 命中它则连接标记为 peer(`ConnRole::Peer`),只有 peer 连接可发 `FLAG_REPLICATION` 帧(节点间流量),peer 连接也只能发复制帧(AUTH/PING 除外);扇出认证优先用 cluster token(`fanout_auth`),未配置时回退 `DOCSQL_TOKEN`(历史行为:任意已认证连接可发复制帧)。改连接循环的复制帧门禁时保持"未配置 cluster token = 完全向后兼容"
- **auto-GUID 主键的复制回写**:GUID/UUID/UNIQUEIDENTIFIER/UUIDV7 类型列 + AUTOINCREMENT 声明为自动生成主键(`TableMeta.autoguid`,随 catalog 持久化);INSERT 省略/NULL 时引擎填 UUIDv7(`core/guid.rs`),并把该语句回写为显式值的规范化 INSERT(保留 OR REPLACE/OR IGNORE 冲突策略,`Database::take_resolved_insert` 取走)——server 的扇出与 `tx_pending` 缓冲一律用回写文本,因为随机值不能像 INT AUTOINCREMENT 的 max+1 那样在对端确定性重算;auto-GUID 表的 `INSERT ... SELECT` 显式报错(同理,对端重放 SELECT 无法收敛)

### 持久化发布订阅(pub/sub)

- 消息存于引擎系统表 `_pubsub_messages`(常量 `core::engine::PUBSUB_TABLE`;server 启动时 `CREATE TABLE IF NOT EXISTS`),白得 WAL 持久化;对 SQL 客户端隐藏(`execute_sql` 防护拦截直引用,目录查询 information_schema/sqlite_master 放行),只读视图 `docsql_pubsub`(仿 docsql_log,重写标识符后走正常 SQL 路径);status/web 的表计数与对象树均过滤它
- **顺序契约**:PUBLISH 必须先落盘(autocommit 引擎写路径,`lock_engine_for_write` 与其它写同样排队单写者事务)再 `notify` 本地订阅者、再向 peers 扇出(REQ_PUBLISH + FLAG_REPLICATION,对端只落盘+推送本地订阅者、不再转发);SUBSCRIBE 在注册表锁内完成 注册→快照水位→回放→`arm_filter`,水位(`skip_through`)去重保证回放与实时之间不丢不重——动 pubsub.rs 的锁顺序前先读该文件模块注释
- id 为**节点内**单调递增 AUTOINCREMENT(重启后按 max(existing)+1 续推,前提:表非空,故 TRIM 强制 keep ≥ 1);投递 at-least-once,慢连接丢实时帧但可凭 id 重订阅补回;回放帧与确认帧经同一 per-connection writer mpsc,顺序天然保证
- CLI 远程 shell 有专职读线程(`try_clone` + std mpsc)分流 RESP_PUSH,推送帧不会错位当成命令响应;dotnet 侧 `DocsqlSubscriber` 用专用连接 + 后台读线程同理——**不要在普通一问一答连接上订阅**

### EF Core 提供程序

- 策略是**复用 SQLite 管线** + Docsql ADO.NET,不重写关系生成
- `EnsureCreated`/惰性建表默认开启,模型增删表、索引(含唯一索引)自动同步,无需 Migrations;`Database.Migrate()` 显式报错并指引改用 EnsureCreated;重复 EnsureCreated 前需先 DROP 旧表
- 已知边界:SAVEPOINT 语义有限,EF 事务内 SaveChanges 依赖它——相关改动需跑两个 dotnet 测试套件验证

## 红线与已知坑

1. **索引排序不依赖编码字节序**——`core/encode.rs` 只保证往返一致;排序统一走 `Value::cmp_values`(B+树/ORDER BY/DISTINCT)。新增值类型必须同时扩展编码与 `cmp_values`,否则索引序被破坏。注意 Int(3) 与 Float(3.0) 比较相等但编码不同,DISTINCT/UNION 去重按编码字节判重,两者不会互相去重。
2. **`docsql-web/src/console.html` 通过 `include_str!` 内嵌**——改 UI 后必须重新 `cargo build` 才生效;开发循环用 Playwright 验证(对象树单击选中、双击打开数据网格)。
3. **deploy 有兼容性钉子**——`deploy/multinode-test.sh` 断言固定的 REST/协议接口;改协议或 REST 字段前先同步该脚本与 dotnet 客户端。
4. **PK ≠ NOT NULL**:主键当前不隐含 NOT NULL,与主流数据库不同;动约束逻辑需全量回归约束测试。
5. **不支持的 SQL 必须显式报错**——窗口函数(OVER)/DISTINCT ON/ON CONFLICT DO UPDATE/ON DUPLICATE KEY UPDATE/自定义 TRIM 字符集/FK 的 ON DELETE|UPDATE 动作/相关子查询(限定引用外层表报 "correlated subqueries are not supported")/无 GROUP BY 的 HAVING 均已显式报错;`PRAGMA` 是有意兼容垫片(接受并忽略)。新增不支持语法时在解析/执行层报错,不要静默吞掉。WITH(非递归)/CTAS/ON CONFLICT DO NOTHING|REPLACE/`SELECT *, expr` 已支持。
6. **会话事务是单全局事务**(单写者引擎):并发连接的 BEGIN 在服务端排队等待(`BEGIN_QUEUE_WAIT` 30s 上限)而非立即报错,dotnet 端事务错误如实上抛——并发 EF SaveChanges 依赖该排队,别改成直接报错或吞错。事务归 BEGIN 它的连接所有:非 owner 的写/复制写同样排队(见"会话事务与复制缓冲"),别改回"直接并入全局事务"的老行为。
7. **dotnet 的 bin/obj 不入库**(已在 .gitignore);新建 dotnet 项目注意沿用。
8. **数据卷是 external 卷且 dev/prod 已分离(2026-09-09:dev `docsql-dev-data-*`,prod 沿用 `docsql-data-*` + join 节点 `docsql-prod-data-d`;项目名 `docsql-dev`/`docsql-prod`,端口 dev 1760x+1770x / prod 1860x+1870x,两套可同时运行)**——`down -v` 不再清数据:发布/重部署只重建容器,数据保留(用户明确要求发布不丢数据);测试的干净态由 `run-tests.sh` 显式 rm+`docker volume create`(只动 dev 卷)保证;手动清数据唯一入口 `./deploy/reset-data.sh`。所有服务都在 profile 内,不带 profile 参数的 pull/up/down 是空集静默空操作(exit 0),必须带 profile 参数。
9. **pub/sub 先落盘后推送**:PUBLISH 必须在引擎写路径提交(WAL)之后才能 notify 订阅者/扇出 peers,游标(id)续传依赖"已返回的 id 必可回放";改 handle_publish/store_insert 顺序时保持该约束。另外 catalog 持久化曾丢失 AUTOINCREMENT 标志(重启后插入 id 变 NULL,已修并有回归测试 `autoinc_column_survives_reopen`)——动 `save_catalog_into` 时注意 TableMeta 的每个字段都要写全。
10. **分区测试重连必须带 `--alias`**——`docker network disconnect` 后裸 `docker network connect` 重连,`node-c` 别名不再注册进嵌入 DNS(a→node-c 与容器自名解析双双失效,healthcheck 持续红,`docker restart` 也不恢复),集群扇出到该节点从此静默失败;必须 `docker network connect --alias node-c <net> docsql-c` 显式重注册。multinode-test.sh 第 10 章已按此实现,改动分区逻辑时保持;测试中该节点侧查询一律走 127.0.0.1 回环。
11. **随机生成值必须由写入节点定值下发**——auto-GUID 插入若把原文(缺 id 的 INSERT)直接扇出,对端会各自生成不同 GUID,数据静默分叉且无报错;engine 侧 `resolved_insert` 回写与 server 侧 `take_resolved_insert` 接线(execute_sql 的 db 锁内捕获、转发/tx_pending 用回写文本)缺一不可,动 execute_sql/事务缓冲/扇出路径时保持。今后新增任何非确定性默认值(随机、时间戳精度截断等)同理:要么显式定值回写,要么确定性可重算。

## 工作流约定

- 小步提交直接在 `main`;提交信息风格见 git log(如 `M16: ...`、`Engine milestones: ...`),里程碑式概括。
- 完整流程:过提交门禁 → 相关专项测试 → **最后一步提交并推送源码**;push 即触发 CI(门禁 + dotnet 测试 → 多架构镜像发布 GHCR → main 分支部署测试);CI 失败等同门禁失败。
- 改动跨复制:本地 e2e 之外必须跑 `./deploy/run-tests.sh`。
- 改动 EF 相关(SAVEPOINT 语义敏感):跑两个 dotnet 测试套件验证。
- 文档分工:用户可见行为 → README;开发向内容与代理工作所需的命令、红线 → 本文件。
- 本机到 github.com:443 间歇阻断,推送失败用 `git -c http.version=HTTP/1.1 push` 重试。
- 环境:macOS/arm64;Docker 构建基于 mcr.microsoft.com/azurelinux(docker.io 不可达)。

## 运行时环境变量

- `DOCSQL_TOKEN`:server(协议端口,REQ_AUTH)与 web 控制台共用的认证 token;集群节点复制扇出会自动先认证。
- `DOCSQL_READ_TOKEN`:只读客户端凭据。REQ_AUTH 命中它 → `ConnRole::ReadOnly`:可查询/可订阅,SQL 写、PUBLISH、PUBSUB TRIM、PROMOTE 一律协议层拒绝(错误信息含 "read-only");扇出永不使用该凭据。与 `DOCSQL_TOKEN` 同值时按客户端 token 处理(匹配顺序:cluster → client → read)。
- `DOCSQL_MAX_CONN`:并发连接上限(0=不限,默认不限);超限连接立即回 RESP_ERROR "too many connections" 并关闭,不排队。测试注意:每连接占一个信号量槽,起服务器的端口探测连接会短暂占槽。
- `DOCSQL_IDLE_TIMEOUT`:空闲会话超时秒数(0=不限,默认不限);连接静默超时后服务端发 "idle session timed out" 并断开——**订阅型客户端(DocsqlSubscriber/CLI subscribe)依赖长连接,启用后必须定期 PING 保活**。
- 认证失败锁定:同源 IP 60s 窗口内失败 ≥ `AUTH_LOCK_THRESHOLD`(常量 10)锁定 60s(`AUTH_LOCKOUT`),期间正确 token 也被拒("account locked");认证成功/失败/锁定均写 sync_log("auth" 事件,web 日志页可见)。ServerConfig 字段 `auth_lock_threshold` 供测试调小;0 关闭锁定。
- 凭据强度:server 二进制启动时校验所有 token(长度 ≥ `MIN_TOKEN_LEN` 8、非单字符重复),不合规拒绝启动(exit 2)——e2e 直构 `ServerConfig` 不受影响;非回环绑定且无 `DOCSQL_KEY` 时启动告警。
- `DOCSQL_CLUSTER_TOKEN`:节点间集群凭据。配置后:扇出用它认证、`FLAG_REPLICATION` 复制帧仅接受用它认证过的连接(peer 身份)、peer 连接不能跑普通客户端语句;集群所有节点必须同值,且应与 `DOCSQL_TOKEN` 不同(相同会启动告警)。不配置则复制沿用 `DOCSQL_TOKEN` 的历史行为。dev compose 集群固定为 `dev-cluster-secret`,生产经 `.env` 的 `DOCSQL_CLUSTER_TOKEN` 传入。
- `DOCSQL_PEERS`:对称集群节点表。全新节点(无用户表)启动时自动从有数据的 peer 同步全量状态并注册自己(见"网络与复制"的 cluster join);指向自身的条目会在启动时被忽略(防双写)。PROMOTE(故障转移提升)走 REQ_PROMOTE 帧。docsql-web 也读取该变量:集群状态页探测 + 节点切换允许列表(SSRF 白名单)。
- `DOCSQL_UPSTREAM`:docsql-web 的默认管理节点(`host:port`)。优先级:启动参数 > `DOCSQL_UPSTREAM` > `DOCSQL_PEERS` 首条。控制台自身零存储,所有数据操作连接该节点执行;compose 两个 profile 已分别配置(node-web → node-a,node-web-single → node-single)。未配置时控制台仍可起服务(状态页/日志页可用),数据端点 in-band 报「未配置管理目标节点」。
- `DOCSQL_WEB_AUTH_FILE`:web 控制台账号凭据文件路径。已设(非空)= 账号门激活:首次打开控制台强制设置用户名/密码(盐化 PBKDF2-HMAC-SHA256 存该文件,数据仍在节点;会话 HttpOnly Cookie 12h 滑动;登录失败锁定同 server 策略;`DOCSQL_TOKEN` 仍可作 API 旁路),之后进入需登录。未设或空 = 关闭账号门(legacy 仅 token 头;**run-tests.sh 显式置空依赖此语义**)。compose 两个文件均为 web 服务挂命名卷(`web-auth`/`web-auth-single`,项目前缀区分)并默认指向 `/auth/console-auth.json`;reset-data.sh 一并清除(恢复首次设置态)。
- `DOCSQL_ADVERTISE`:加入集群时通告给其它节点的自身地址(`host:port`,须从 peer 侧可达)。未设置时 join 仍同步数据但不注册(仅静态 peers 配置的部署需要)。注意动态注册不落盘:原节点重启后丢失,长期成员请同步更新各节点的 `DOCSQL_PEERS`。
- `DOCSQL_ASYNC_COMMIT=1`:组提交模式——语句提交跳过 WAL fsync,后台 flusher 每 ~2ms 批量刷盘(数据文件回写与 checkpoint 也由 flusher 驱动);断电丢最后 ~2ms 已确认写。PUBLISH 不受影响(推送前强制 fsync,先落盘后推送契约不变)。默认关闭(每写 fsync,全持久)。
- `DOCSQL_CATCHUP_WINDOW`:catch-up 期刊 `_cluster_log` 的保留条数(默认 100000;0=不限,每 512 条运行时裁剪一次)。重入节点位点在窗口内 → 增量补齐;落后于窗口起点 → 快照兜底。调大可延长"允许离线多久还能增量补"。
- `DOCSQL_DEV_IMAGE_TAG`:本地开发 compose(`docker-compose.yml`)的镜像标签(默认 `local`;部署测试用 `ci` 复用 CI 缓存镜像)。与生产的 `DOCSQL_IMAGE_TAG` 刻意分开——`.env` 里配 `DOCSQL_IMAGE_TAG` 只影响 `docker-compose.prod.yml`,绝不会改到开发栈(2026-09-09 修的泄漏坑:`.env` 曾用 `DOCSQL_IMAGE_TAG=latest` 让 dev compose 跑起生产镜像)。
- `DOCSQL_IMAGE_TAG`:生产 compose(`docker-compose.prod.yml`)使用的镜像标签(默认 `latest`)。
- compose 发布端口默认绑定 `127.0.0.1`(无认证部署不暴露到网络);对外服务需改端口映射并设置 `DOCSQL_TOKEN`。
