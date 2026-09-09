# DocSQL

原生文档数据库:文档式存储 + 完整 SQL + Web 管理控制台 + EF Core 兼容 + 对等集群复制。

## 快速开始

Docker 是唯一的运行方式。镜像由 GitHub Actions 自动构建并发布到 [ghcr.io/wjw1-evan/docsql](https://github.com/wjw1-Evan/docsql/pkgs/container/docsql)(push 到 `main` 或打 `v*` 标签触发;`latest`、`v1.2.3`、`sha-*` 等标签可用)。

```bash
# 单节点部署(compose,标准方式:独立节点 + web 控制台,数据落在命名卷)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
#   db 127.0.0.1:17600,web 控制台 http://127.0.0.1:17710

# SQL 远程 shell(镜像自带 CLI,容器内执行)
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600

# 多节点部署(生产,3 节点对等集群:node-a 17601 + node-b 17602 + node-c 17603 + web 17700)
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d
```

> 私有仓库的 GHCR 镜像包默认不可匿名拉取,先 `docker login ghcr.io`。镜像 tag 可用环境变量 `DOCSQL_IMAGE_TAG` 覆盖。

## 发布订阅(pub/sub,持久化)

参考 Redis 命令面,但消息**先落盘再推送**(WAL 持久化,重启不丢;即使 `DOCSQL_ASYNC_COMMIT=1`,PUBLISH 也在推送前强制 fsync,契约不受组提交影响)。CLI 内联命令:

```
docsql-cli connect 127.0.0.1:7600
> subscribe news;                        # 仅新消息(from=latest,默认)
> subscribe news earliest;               # 全量回放历史后再收新消息
> subscribe news 42;                     # 从 id 42 之后续传(断线重连补齐)
> psubscribe news.*;                     # glob 模式订阅(* ? [...])
> publish news "hello world";            # 返回 [id, receivers] 表格
> unsubscribe news;                      # punsubscribe 退订模式;不带参数退订全部
> pubsub channels;                       # numsub <ch> / numpat / trim <ch> <n>
[pubsub] message news #7 hello world     # 推送消息异步打印(无需发命令)
```

语义:每条消息有**节点内单调递增 id**;投递 at-least-once,断线重连用最后收到的 id 重新订阅即补齐缺口;实时推送尽力而为(慢连接丢帧不阻塞发布者),历史回放不丢。集群内任一节点发布,消息扇出到全部节点(各节点本地落盘 + 推送本地订阅者)。消息历史可经 SQL 查询:`SELECT * FROM docsql_pubsub WHERE channel = 'news' ORDER BY id DESC LIMIT 10`;`pubsub trim <ch> <n>` 保留每频道最新 n 条(keep ≥ 1)。.NET 客户端:`DocsqlConnection.Publish(channel, payload)` 返回 `(id, receivers)`,`DocsqlSubscriber`(专用连接)提供 `Subscribe/Psubscribe/Unsubscribe` 回调式订阅。

## GUID 主键(时序有序,自动生成)

主键列声明 GUID 类型(别名 `UUID` / `UNIQUEIDENTIFIER` / `UUIDV7`)并带 `AUTOINCREMENT` 标记后,INSERT 省略该列或显式给 NULL 时自动生成 **UUIDv7**(RFC 9562 时序有序 GUID):

```sql
CREATE TABLE orders (
    id   GUID PRIMARY KEY AUTOINCREMENT,   -- 或 UUID / UNIQUEIDENTIFIER / UUIDV7
    note TEXT
);
INSERT INTO orders (note) VALUES ('first');           -- id 自动生成
INSERT INTO orders (id, note) VALUES (NULL, 'two');   -- NULL 同样自动生成
INSERT INTO orders (id, note)
  VALUES ('018f6b2e-4a10-7abc-9def-123456789abc', 'explicit');  -- 显式值原样保留
```

- **有序**:id 在节点内严格单调递增,字符串序即时间序——B+ 树索引插入保持追加型局部性,`ORDER BY id` 即按创建顺序
- **集群收敛**:自动生成的 id 由写入节点定值,随复制语句以显式值下发给对等节点(对端不再各自生成),任意节点写入结果一致
- 存储为规范小写 UUID 文本(36 字符);`information_schema.columns.data_type` 对该列显示 `GUID`
- 边界:auto-GUID 表不支持 `INSERT ... SELECT`(显式报错;请用 VALUES 并按需显式给 id)

## 开发(构建与测试,非运行方式)

```bash
cargo build --workspace
cargo test --workspace          # Rust 全量测试(开发门禁;本地 Docker 构建亦内置)
cd dotnet && dotnet test        # .NET 测试(需先 cargo build 出 server 二进制)
./deploy/run-tests.sh           # 本地构建镜像 + 部署测试(多节点 69 项 + 单节点 24 项)
```

## DocSQL Studio(Web 管理控制台)

参考 SQL Server Management Studio 的交互重新设计:

- **对象资源管理器**(左栏树):服务器 → 表(列含 PK/UQ/NN/AI 徽章、索引、键)→ 每表可双击打开数据网格;系统视图(`information_schema.*`、`sqlite_master`)。列节点区分**声明列**(建表/ALTER 定义,约束面)与**实测列**(数据中顶层字段并集):schemaless 写入带出表结构之外的字段时,列文件夹计数显示「声明 N / 实测 M」,仅见于数据的字段带「数据」徽章——声明列不随数据自动改写,`SELECT *` 按实测字段投影
- **查询工作台**(多标签文档):SQL 语法高亮 + 行号编辑器,F5 执行 / Ctrl+F5 仅语法分析 / 执行所选;多语句批次依次执行并逐结果集呈现(网格 + "(N 行受影响)" 消息页 + 总耗时);表头点击排序
- **右键任务**:新建查询、选择前 1000 行、查看数据、编辑表、插入文档、编写 CREATE/DROP 脚本、删除表
- **新建表 / 编辑表 / 插入文档**(参考 mongo-express 的写入面):对象资源管理器「＋表」按钮 / 右键 / 「文件 → 新建表…」打开列编辑网格(列名/类型/PK/NOT NULL/自增,PK 自动联动 NOT NULL;类型含 GUID——勾自增即建时序有序 UUIDv7 主键,INSERT 省略该列即可)一键 CREATE TABLE;已有表右键「编辑表…」(数据页工具栏同入口)复用同一网格——已有列可改名/删除(主键列除外,类型与约束锁定为只读,SQL 层不支持在线改约束),新增列选类型即加,保存时按「重命名 → 删除 → 新增」生成 ALTER 批次执行,失败语句与已生效前缀明确提示;数据网格「插入文档…」或表右键打开 JSON 编辑域——对象插入一行、数组批量插入,允许表结构之外的字段(文档式存储),嵌套对象/数组以 JSON 文本存储
- **仪表盘**:表/行数/页与文件占用/运行时长一览
- **集群状态**:按 `DOCSQL_PEERS` 只读探测各节点(PING 延迟 + REQ_STATUS 状态报告),展示在线/离线/只读、表与行数收敛、存储占用、LSN 收敛指标;5 秒自动刷新;未配置 peers 时显示单机模式
- **日志**(视图 → 日志,或服务器右键):全部日志一览——**数据日志**为各处执行的 SQL 语句审计(控制台提交的语句 + 各集群节点,含耗时/影响行数/错误;来自对等节点扇出的语句带「复制」徽章),**同步日志**为集群同步事件(写扇出 publish/trim 扇出/PROMOTE/节点加入,逐目标记录成功与失败原因);条目按时间倒序合并,支持类别(全部/数据/同步/仅错误)、来源(控制台/各节点)、关键词过滤与 5 秒自动刷新;节点离线时显示离线清单
- **节点切换**(工具栏「节点」下拉框):控制台是纯管理工具,**自身不存任何数据**——默认连接并管理启动时指定的节点(如 `node-a:7600`),配置了 `DOCSQL_PEERS` 时可一键切换到其它集群节点——查询、对象资源管理器、数据网格、建表/插入文档、仪表盘全部以数据库客户端身份连接所选节点执行(二进制协议直连,认证用服务端配置的 `DOCSQL_TOKEN`);在该节点上的写入按其集群配置正常扇出;切换仅允许 `DOCSQL_PEERS` 中配置的地址,节点离线时操作返回明确错误

## 能力总览

| 领域 | 支持 |
|---|---|
| 存储 | JSON 文档整体存储(无强制 schema)、WAL 崩溃恢复、手写分页器与 B+ 树 |
| SQL | CREATE/ALTER/DROP TABLE+INDEX、INSERT(多行/RETURNING)、UPDATE/DELETE(RETURNING)、SELECT(WHERE/ORDER/LIMIT/OFFSET/GROUP BY+HAVING/COUNT/SUM/AVG/MIN/MAX/JOIN:INNER/LEFT/CROSS/USING/子查询派生表/UNION(ALL)/IN)、事务 BEGIN/COMMIT/ROLLBACK、PRIMARY KEY/UNIQUE/NOT NULL/AUTOINCREMENT、GUID 主键(UUIDv7 时序有序自动生成)、information_schema、sqlite_master 兼容视图、PRAGMA 兼容 |
| 网络 | 自定义二进制协议 v1(预留拓扑版本/重定向字段)、REQ_AUTH token 认证、节点间集群认证(DOCSQL_CLUSTER_TOKEN:复制帧仅接受集群身份,客户端凭据无法伪造节点流量)、REQ_PROMOTE 故障转移提升、REQ_STATUS 节点状态报告 |
| 发布订阅 | 持久化 pub/sub(参考 Redis 命令面):PUBLISH/SUBSCRIBE/PSUBSCRIBE(glob `*` `?` `[...]`)/UNSUBSCRIBE/PUBSUB CHANNELS·NUMSUB·NUMPAT·TRIM;消息先经 WAL 落盘再推送,重启不丢;订阅可指定起点(`earliest` 全量回放 / `latest` 仅新消息 / 指定 id 续传),断线用最后收到的 id 重新订阅即补齐(at-least-once);集群内发布自动扇出到全部节点,各节点本地落盘并推送本地订阅者;`docsql_pubsub` 系统视图可查消息历史 |
| Web | **DocSQL Studio**(SSMS 风格管理控制台,纯管理工具、自身不存数据,默认连接并管理指定节点):对象资源管理器(表/列/索引/键 + 系统视图)、多标签查询编辑器(SQL 高亮/F5 执行/Ctrl+F5 分析/批量多结果集)、数据网格(排序/分页/删行)、新建表 / 插入文档(参考 mongo-express:列编辑网格建表——类型含 GUID 时序主键、JSON 文档插入,支持批量与表外字段)、服务器仪表盘、集群状态页、日志页(数据/同步/错误,含各节点来源)、节点切换(默认管理节点 ↔ 任意 `DOCSQL_PEERS` 节点);REST API(/api/sql /api/parse /api/meta /api/stats /api/cluster /api/logs,其中数据端点 /api/sql /api/meta /api/stats 可带 `node` 参数指定目标节点) |
| EF Core | `UseDocsql(connectionString)`(复用 SQLite 管线 + Docsql ADO.NET):EnsureCreated/CRUD/LINQ/Include/`[Index]` 特性索引(含唯一索引;模型增删索引均自动同步,免迁移) |
| 集群 | 对等集群(`DOCSQL_PEERS`:任意节点可写,SQL 写入扇出至全部对等节点;节点之间用独立集群凭据互相认证)、主从复制(写转发)、只读副本、PROMOTE 故障转移 |

## 结构

| crate | 职责 |
|---|---|
| docsql-core | 存储引擎(pager/WAL/B+树/heap)+ SQL 解析执行 + 协议帧 + JSON |
| docsql-server | TCP 服务器、认证、复制、持久化发布订阅(pub/sub) |
| docsql-cli | 嵌入式 + 远程 shell |
| docsql-web | Web 管理控制台(SSMS 风格 UI + REST API;纯管理工具,自身不存数据,所有数据操作连接指定节点执行) |
| dotnet/ | Docsql.Client(ADO.NET)与 Docsql.EntityFrameworkCore |

## 测试

- Rust:单元 + SQL 集成 + 协议 + 端到端 + 复制故障转移 + 发布订阅(pub/sub 实时/回放/续传/trim/跨节点)+ 批处理/目录元数据(亦在本地 Docker 构建内作为门禁执行)
- Docker:compose 双 profile 部署测试全绿——多节点 69 项(3 节点对等集群:任意节点写入/多向 SQL 复制/事务回滚/一致性收敛/GUID 主键跨节点收敛/Web 控制台 + 集群状态探测/跨节点 pub/sub 与重启回放/节点离线再上线/网络分区/新节点加入自动同步)+ 单节点 24 项(SQL 读写/事务回滚/容器重启持久性/GUID 主键生成与重启续用/与集群隔离/Web 控制台/pub/sub 实时与重启回放)
- .NET:xUnit(ADO.NET Client 43 项 + EF Core 22 项:CRUD/LINQ/Include/Savepoint/集群/加密传输/pub/sub)
- CI(GitHub Actions,push/PR 触发):`cargo fmt` + `cargo clippy -D warnings` + `cargo test` + `dotnet test` 全过 → 构建镜像 → main 分支另跑同一套部署测试(69 + 24 项)

## Docker 部署(单节点 / 多节点;本地开发与生产两个 compose 文件)

两种部署拓扑,用 compose profile 切换,**同一套文件支持单节点与多节点**:

| profile | 节点 | 端口 |
|---|---|---|
| `single` | node-single(独立单节点,无复制)+ 独立 web 控制台 | 17600 / 17710 |
| `cluster` | node-a + node-b + node-c 对等集群(任意节点可读写,SQL 写入自动扇出至 `DOCSQL_PEERS`)+ web 控制台(集群状态页监控三节点) | 17601-17603 / 17700 |
| `join` | node-d(向运行中的集群加入第四数据节点:全新节点启动即自动拉取全量历史数据并注册进扇出网格;见下文"扩容") | 17604 |

两个 profile 端口不冲突,可同时运行(便于对比验证);命令均需带 profile 参数:

> **架构说明(控制台无本地存储)**:DocSQL Studio 是纯管理工具——web 容器不挂数据卷、不内嵌数据库,启动参数即**默认管理节点**(`docsql-web node-a:7600 …`,或 `DOCSQL_UPSTREAM` / 首个 `DOCSQL_PEERS` 条目),所有数据操作都以客户端身份连接该节点执行,写入落在集群数据卷并正常扇出。界面里的「节点」下拉框只切换管理目标,不存在独立的控制台数据库。从旧版本升级:web 容器会自动重建(不再挂 `docsql-data-web*` 卷),旧内嵌引擎卷成为遗留数据,可用 `./deploy/reset-data.sh` 一并清除。

```bash
cd deploy
docker compose --profile single up -d     # 单节点部署;发布新版本加 --build(数据保留)
docker compose --profile cluster up -d    # 三节点对等集群部署
docker compose --profile single --profile cluster down    # 全部停止(数据保留)
```

**扩容(新数据节点自动同步)**:集群已有数据时,起一个指向现有节点的全新节点即可——它会自动拉取全量历史(schema、约束、索引、数据、GUID 值),注册进各节点的扇出列表,随后与其它节点互相同步写入。compose 用 `join` profile:

```bash
cd deploy && docker volume create docsql-data-d    # 一次性建卷
docker compose --profile cluster --profile join up -d node-d
```

新节点需要两个环境变量:`DOCSQL_PEERS`(现有节点地址表)与 `DOCSQL_ADVERTISE`(其它节点回连自己的地址)。注意:动态注册保存在原节点内存中,原节点重启后会丢失——要把 node-d 变成长期成员,请把它写进各节点的 `DOCSQL_PEERS` 并重建(数据保留,且不会重复同步)。另外,节点**离线/分区期间**其它节点的写仍不会补发(无反熵追赶,同步只发生在加入时)。

**数据持久化**:每个节点的数据放在 external 卷(`docsql-data-a/b/c/d/web/single/web-single`),发布换镜像、重建容器乃至 `down -v` 都**不会**删数据;彻底清数据唯一入口是 `./deploy/reset-data.sh`。首次部署前先建卷(一次性):

```bash
cd deploy && for v in a b c d web single web-single; do docker volume create docsql-data-$v; done
```

**本地开发**(`deploy/docker-compose.yml`):从源码构建镜像(构建期内置全量 cargo test 门禁),tag `:local`。上面的命令加 `--build` 即触发构建。

**生产**(`deploy/docker-compose.prod.yml`):拉取 CI 发布的 GHCR 镜像(不本地构建),断线自动重启,日志轮转,Web 控制台可配 token:

```bash
cd deploy
cp .env.example .env    # 固定 DOCSQL_IMAGE_TAG(建议固定版本 tag)、设置 DOCSQL_TOKEN;集群拓扑再设 DOCSQL_CLUSTER_TOKEN(节点间认证,用与 DOCSQL_TOKEN 不同的值)
docker compose -f docker-compose.prod.yml --profile single up -d    # 生产单节点
docker compose -f docker-compose.prod.yml --profile cluster up -d   # 生产三节点集群
```

> 本地开发与生产共享主机端口和数据卷(docsql-data-*),同一拓扑同一时间只能运行一套;切换前 `down` 停掉另一套即可,切换后数据仍在。

部署测试(`./deploy/run-tests.sh`,同时拉起两个 profile):多节点 69 项(任意节点写入/多向 SQL 复制/事务回滚/一致性收敛/GUID 主键跨节点收敛/Web 控制台 + 集群状态探测 + 节点切换/跨节点 pub/sub/节点离线再上线/网络分区/新节点加入自动同步)+ 单节点 24 项(SQL 读写、事务回滚、容器重启后数据持久、GUID 主键生成与重启续用、与集群的数据隔离、Web 控制台、pub/sub)。

> 注:镜像基于 mcr.microsoft.com/azurelinux(本环境 docker.io 不可达)。

## 安全(对照等保 2.0 / GB/T 20273 数据库管理系统安全技术要求)

面向国内数据库安全检测(等级保护第三级)的能力对照:

| 控制项 | DocSQL 实现 |
|---|---|
| 身份标识与鉴别 | 协议层 token 认证(REQ_AUTH,常数时间比较防时序侧信道);三种凭据:`DOCSQL_TOKEN`(客户端)、`DOCSQL_READ_TOKEN`(只读客户端)、`DOCSQL_CLUSTER_TOKEN`(节点间,`FLAG_REPLICATION` 复制帧仅接受节点身份) |
| 登录失败处理 | 同一来源 IP 在 60 秒窗口内认证失败达 10 次(阈值可按部署调严)即锁定 60 秒,期间任何 token(含正确值)均被拒绝;锁定事件写入审计日志 |
| 口令/凭据复杂度 | 服务器启动时校验所有已配置凭据:长度不足 8 或单一字符重复即拒绝启动(进程退出码 2) |
| 访问控制(最小权限) | `DOCSQL_READ_TOKEN` 只读身份:可查询、可订阅,一切持久化写(SQL 写/`PUBLISH`/`PUBSUB TRIM`/`PROMOTE`)在协议层拒绝;副本模式 `DOCSQL_READ_ONLY=1` 整节点只读;Web 控制台独立 token 门禁 |
| 安全审计 | 语句审计(`docsql_log` 环形缓冲,含语句文本/耗时/影响行数/是否复制/错误)、认证事件审计(成功与失败均记录,来源 IP + 授予身份/失败原因,web 控制台日志页可见)、`DOCSQL_LOG_FILE` 可同步落 JSONL 文件留存 |
| 资源控制 | `DOCSQL_MAX_CONN` 并发连接数上限(超限立即拒绝不排队);`DOCSQL_IDLE_TIMEOUT` 空闲会话超时(服务端主动断开,订阅客户端需定期 PING 保活);单帧 64MB 上限;对端 IO 预算(连接 3s/读写 10s) |
| 传输保密性 | `DOCSQL_KEY` AES-256-GCM 帧加密(含认证 token 与数据);绑定非回环地址且未配置 `DOCSQL_KEY` 时启动显式告警;默认端口映射仅绑定 `127.0.0.1` |
| SQL 注入防护 | 参数化语句面(REQ_PREPARE/REQ_EXECUTE,ADO.NET/EF Core 参数绑定);服务端不拼接外部输入;系统表 `_pubsub_messages` 对 SQL 客户端隐藏 |
| 数据完整性 | WAL 先写日志后落数据、崩溃恢复;节点间复制依赖独立集群凭据防伪造 |

已知边界:审计环形缓冲在内存(重启丢失,需要长期留存请启用 `DOCSQL_LOG_FILE` 外发);静态数据加密(TDE)暂未内置(可部署在加密卷之上)。

## 性能(索引引擎)

主键/UNIQUE 列与 `CREATE INDEX` 列均有 B+ 树支撑:判重与点查/范围查询走索引(O(log n)),不再全表扫描;INSERT 单语句单事务提交(一次 WAL fsync);UPDATE/DELETE 命中索引条件时页内原地改写并增量维护索引树;WAL 超过 8MB 自动检查点。

主键与表声明 UNIQUE 约束的索引**随建表自动创建**,以保留名 `sqlite_autoindex_<表>_<n>` 出现在对象资源管理器与 `/api/meta`(带 PK/UQ 徽标,只读):它们随表定义维护,`DROP INDEX`/`CREATE INDEX` 对该前缀显式报错;`sqlite_master` 按 SQLite 语义不列出自动索引。

本机基准(单线程、进程内直调、autocommit,含每次提交的 fsync;对照 SQLite 3.54 WAL/FULL 同条件):

| 场景 | DocSQL(索引引擎) | 优化前 | SQLite |
|---|---|---|---|
| INSERT(主键表)×5k | ~1,300 行/秒(线性) | 二次方劣化 | ~6,900 行/秒 |
| 点查 WHERE id=?(1k 行) | ~38,000 次/秒 | ~5,600 | ~241,000 |

写入吞吐受每语句一次 fsync 支配(与 SQLite FULL 同语义);点查为解析+索引开销。

## 已知边界(v1)

- 事务为单连接快照隔离;多连接并发由服务器互斥串行化(单写者引擎)
- auto-GUID 主键列不支持 `INSERT ... SELECT`(对等节点重放 SELECT 时无法收敛随机生成值;请用 VALUES 并按需显式给 id)
- EF Core 通过 SQLite 管线桥接(非独立提供程序);`Database.Migrate()` 不支持(显式报错并指引改用 `EnsureCreated`),模型/索引同步由 EnsureCreated 自动完成
