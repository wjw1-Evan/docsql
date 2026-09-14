# DocSQL

原生文档数据库:文档式存储 + 完整 SQL + Web 管理控制台 + EF Core 兼容 + 对等集群复制。

## 快速开始

Docker 是唯一的运行方式。镜像由 GitHub Actions 自动构建并发布到 [ghcr.io/wjw1-evan/docsql](https://github.com/wjw1-Evan/docsql/pkgs/container/docsql)(push 到 `main` 或打 `v*` 标签触发;`latest`、`v1.2.3`、`sha-*` 等标签可用)。

```bash
# 单节点部署(compose,标准方式:独立节点 + web 控制台,数据落在命名卷)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
#   db 127.0.0.1:18600,web 控制台 http://127.0.0.1:18710

# SQL 远程 shell(镜像自带 CLI,容器内执行)
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600
#   常用参数:--csv / --json(行导出格式)、-f script.sql(脚本批执行,快速失败)、help;(内联帮助)

# 多节点部署(生产,3 节点对等集群:node-a 18601 + node-b 18602 + node-c 18603 + web 18700)
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d
```

> 私有仓库的 GHCR 镜像包默认不可匿名拉取,先 `docker login ghcr.io`。镜像 tag 可用环境变量覆盖:生产用 `DOCSQL_IMAGE_TAG`,本地开发用 `DOCSQL_DEV_IMAGE_TAG`(两者互不影响)。
>
> .NET / Aspire 项目无需手写 compose:`dotnet add package Docsql.Aspire.Hosting` 后在 AppHost 里三行编排 DocSQL 容器节点(见下文「.NET 与 Aspire」)。

## 文档导航

| 我想… | 看这里 |
|---|---|
| 了解数据库全部能力 | [功能总览](docs/features.md) |
| 查 SQL 语法与函数 | [SQL 参考](docs/sql-reference.md) |
| 用 Aspire 编排(单节点/集群/控制台) | [Aspire 集成指南](docs/aspire.md) |
| 用 .NET ADO.NET / EF Core / CLI / 写驱动 | [客户端与驱动](docs/drivers.md) |
| 部署、扩容、备份恢复、环境变量、监控 | [运维手册](docs/operations.md) |
| 配用户/角色/审计/加密(等保对照) | [安全指南](docs/security.md) |
| 确认架构边界与不支持项 | [已知边界](docs/limitations.md) |
| 参与开发(构建/测试/门禁) | [贡献指南](CONTRIBUTING.md) · [AGENTS](AGENTS.md) |

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

## DocSQL Studio(Web 管理控制台)

- **对象资源管理器**:表/列(声明列与实测列区分)/索引/键/系统视图/系统表,双击打开数据网格;
- **查询工作台**:多标签、SQL 高亮、F5 执行 / Ctrl+F5 分析 / 执行所选、批量多结果集、表头排序;
- **可调整布局**:对象资源管理器宽度、编辑器与结果区高度用分隔条拖动调整(双击恢复默认,
  尺寸按浏览器记忆),窗口缩放时编辑器/结果区按设定比例重新分配;
- **数据网格与写入面**:行内编辑/删除(按主键定位,可置 NULL)、JSON 批量插入(允许表外字段)、
  列编辑网格建表/改表(含 GUID 时序主键;失败语句明确提示);
- **监控与管理页**:仪表盘、集群状态(在线/只读/行数/LSN 收敛,5s 刷新)、日志页(数据审计 +
  同步事件,多条件过滤)、备份管理(立即备份/一键恢复)、用户与角色(建号授权/表级权限);
- **控制台账号门**:首次强制设置用户名/密码,HttpOnly 会话,改密踢出其它会话;浏览器不再需要
  填写令牌 —— 控制台连节点使用服务端环境变量 `DOCSQL_TOKEN`(与节点同值),该 token 同时可作
  程序化 API 旁路(详见[安全指南](docs/security.md));
- **零存储**:控制台不落任何数据,默认管理启动指定的节点,可在 `DOCSQL_PEERS` 白名单内切换节点。

完整功能清单与 REST API 表见[功能总览 · Web 控制台](docs/features.md#9-web-控制台docsql-studio)。

## 能力总览

> 每项能力的完整说明、SQL 示例与细节链接见 [文档 · 功能总览](docs/features.md);用户手册索引见 [docs/](docs/README.md)。

| 领域 | 支持 |
|---|---|
| 存储 | JSON 文档整体存储(无强制 schema)、WAL 崩溃恢复、手写分页器与 B+ 树 |
| SQL | CREATE/ALTER/DROP TABLE+INDEX、CREATE TABLE AS SELECT(CTAS)、TRUNCATE、INSERT(多行/RETURNING/INSERT…SELECT/`OR REPLACE`·`OR IGNORE`/`ON CONFLICT DO NOTHING`——目标列或 `ON CONSTRAINT` 限定只跳过该唯一约束,命中其他唯一约束仍报错)、MERGE、UPDATE/DELETE(RETURNING)、SELECT(WHERE/ORDER/LIMIT/OFFSET/GROUP BY+HAVING/`ROLLUP`·`CUBE`·`GROUPING SETS`+`GROUPING()`/聚合 `FILTER`/COUNT/SUM/AVG/MIN/MAX/JOIN:INNER/LEFT/RIGHT/FULL/CROSS/USING/子查询派生表/UNION·INTERSECT·EXCEPT(ALL)/IN/EXISTS/`IS [NOT] DISTINCT FROM`/量化比较 `> ANY`·`<> ALL`(非相关子查询)/`FETCH … WITH TIES`/非递归 `WITH`/`FROM (VALUES …) AS t(cols)`)、事务 BEGIN/COMMIT/ROLLBACK、PRIMARY KEY/UNIQUE/NOT NULL/AUTOINCREMENT、GUID 主键(UUIDv7 时序有序自动生成)、**精确 DECIMAL**(`CAST('123.45' AS DECIMAL)`,算术/聚合/比较按十进制语义且 28~29 位有效数字)、**BLOB**(`x'hex'` 字面量)、JSON 函数(JSON_EXTRACT/JSON_TYPE/JSON_VALID:文档点读路径 `$.a.b[0]`)、**多列(复合)索引**(`CREATE INDEX … ON t (a, b)`,复合 UNIQUE 判重;前导列等值探测)、**Oracle 兼容**(DUAL 哑表、ROWNUM 伪列、FETCH FIRST n ROWS ONLY、NVL/NVL2/DECODE/INSTR/LPAD/RPAD/GREATEST/LEAST/TO_NUMBER/TO_CHAR/SYSDATE()、ALL_/USER_ 数据字典视图)、information_schema、sqlite_master 兼容视图、PRAGMA 兼容;未实现语法一律显式报错(清单见[已知边界](docs/limitations.md)) |
| 网络 | 自定义二进制协议 v1(预留拓扑版本/重定向字段)、REQ_AUTH token 认证、节点间集群认证(DOCSQL_CLUSTER_TOKEN:复制帧仅接受集群身份,客户端凭据无法伪造节点流量)、REQ_PROMOTE 故障转移提升、REQ_STATUS 节点状态报告(含运行时计数器:连接/语句/字节/认证失败,可直接喂 `/metrics`)、REQ_BACKUP 备份管理与手动触发、REQ_PREPARE/REQ_EXECUTE/REQ_CLOSE_STMT 服务端参数化(占位符在服务端引号感知绑定,注入载荷无法逃逸字面量) |
| 发布订阅 | 持久化 pub/sub(参考 Redis 命令面):PUBLISH/SUBSCRIBE/PSUBSCRIBE(glob `*` `?` `[...]`)/UNSUBSCRIBE/PUBSUB CHANNELS·NUMSUB·NUMPAT·TRIM;消息先经 WAL 落盘再推送,重启不丢;订阅可指定起点(`earliest` 全量回放 / `latest` 仅新消息 / 指定 id 续传),断线用最后收到的 id 重新订阅即补齐(at-least-once);集群内发布自动扇出到全部节点,各节点本地落盘并推送本地订阅者;`docsql_pubsub` 系统视图可查消息历史 |
| Web | **DocSQL Studio**(SSMS 风格管理控制台,纯管理工具、自身不存数据,默认连接并管理指定节点):对象资源管理器(表/列/索引/键 + 系统视图)、多标签查询编辑器(SQL 高亮/F5 执行/Ctrl+F5 分析/批量多结果集)、数据网格(排序/分页/行内编辑与删除——按主键定位生成 UPDATE/DELETE,自增列只读、可勾选置 NULL;主键列缺失的表不提供行编辑)、新建表 / 插入文档(参考 mongo-express:列编辑网格建表——类型含 GUID 时序主键、JSON 文档插入,支持批量与表外字段)、服务器仪表盘、集群状态页、日志页(数据/同步/错误,含各节点来源)、备份管理页(备份状态/文件列表/立即备份/一键恢复——恢复需输入完整文件名确认)、节点切换(默认管理节点 ↔ 任意 `DOCSQL_PEERS` 节点);REST API(/api/sql /api/parse /api/meta /api/stats /api/cluster /api/logs /api/backup,其中数据端点 /api/sql /api/meta /api/stats /api/backup 可带 `node` 参数指定目标节点) |
| EF Core | `UseDocsql(connectionString)`(独立原生提供程序,基于 Docsql ADO.NET,不依赖 SQLite):EnsureCreated/CRUD/LINQ/Include/`[Index]` 特性索引与模型复合索引(含唯一索引;模型增删索引均自动同步,免迁移);`decimal`→精确 `DECIMAL`(服务端 Sum/比较不丢精度)、`DateOnly`/`TimeOnly`、`byte[]`→`BLOB`、`List.Contains`→`IN`;实体集合属性 `List<T>` 的 `Contains`→`JSON_ARRAY_CONTAINS`(权限/受众形态的服务端查询)、`Dictionary<string,object>`→JSON 文本 |
| 集群 | 对等集群(`DOCSQL_PEERS`:任意节点可写,SQL 写入扇出至全部对等节点;节点之间用独立集群凭据互相认证)、主从复制(写转发)、只读副本、PROMOTE 故障转移 |

## 结构

| crate | 职责 |
|---|---|
| docsql-core | 存储引擎(pager/WAL/B+树/heap)+ SQL 解析执行 + 协议帧 + JSON |
| docsql-server | TCP 服务器、认证、复制、持久化发布订阅(pub/sub) |
| docsql-cli | 嵌入式 + 远程 shell |
| docsql-web | Web 管理控制台(SSMS 风格 UI + REST API;纯管理工具,自身不存数据,所有数据操作连接指定节点执行) |
| dotnet/ | Docsql.Client(ADO.NET)、Docsql.EntityFrameworkCore、Docsql.Aspire.Hosting / Docsql.Aspire.Client(AppHost 编排与消费侧);示例:Docsql.Sample(ADO.NET 数据操作实例,连已运行节点)、Docsql.EfSample(EF Core 端到端)、samples/AspireSample(AppHost + Worker 编排示例) |

全部用户文档索引见 [docs/](docs/README.md)。

## 测试

- Rust:单元 + SQL 集成 + 协议 + 端到端 + 复制故障转移 + 发布订阅(pub/sub 实时/回放/续传/trim/跨节点)+ 批处理/目录元数据(604 项,亦在本地 Docker 构建内作为门禁执行)
- Docker:compose 双 profile 部署测试全绿——多节点 81 项(3 节点对等集群:任意节点写入/多向 SQL 复制/事务回滚/一致性收敛/GUID 主键跨节点收敛/Web 控制台 + 集群状态探测/跨节点 pub/sub 与重启回放/节点离线再上线自动补齐/网络分区与重启收敛/新节点加入自动同步)+ 单节点 34 项(SQL 读写/事务回滚/容器重启持久性/GUID 主键生成与重启续用/与集群隔离/Web 控制台/pub/sub 实时与重启回放/自动备份与恢复演练)
- .NET:xUnit(ADO.NET Client 96 项 + EF Core 45 项 + Aspire 集成 12 项:CRUD/LINQ/Include/Savepoint/集群/加密传输/pub/sub/认证契约/事务回滚/参数类型(含 DECIMAL/BLOB/DateOnly/TimeOnly)与长语句契约/复合索引/资源模型快照与 DI 注册)
- CI(GitHub Actions,push/PR 触发):`cargo fmt` + `cargo clippy -D warnings` + `cargo test` + `dotnet test`(含按 GitHub Packages 发布包构建 Aspire 示例)全过 → 构建镜像 → main 分支另跑同一套部署测试(81 + 34 项)

本地开发(构建、门禁、本地集群、覆盖率)命令见[贡献指南](CONTRIBUTING.md),机制约束与红线见 [AGENTS.md](AGENTS.md)。

## Docker 部署(单节点 / 多节点;本地开发与生产两个 compose 文件)

两种部署拓扑,用 compose profile 切换,**同一套文件支持单节点与多节点**。本地开发(`docker-compose.yml`)与生产(`docker-compose.prod.yml`)完全分离——项目名、端口、数据卷、镜像 tag 变量互不相同,两套可同时运行:

| profile | 节点 | 开发端口(`docsql-dev`) | 生产端口(`docsql-prod`) |
|---|---|---|---|
| `single` | node-single(独立单节点,无复制)+ 独立 web 控制台 | 17600 / 17710 | 18600 / 18710 |
| `cluster` | node-a + node-b + node-c 对等集群(任意节点可读写,SQL 写入自动扇出至 `DOCSQL_PEERS`)+ web 控制台(集群状态页监控三节点) | 17601-17603 / 17700 | 18601-18603 / 18700 |
| `join` | node-d(向运行中的集群加入第四数据节点:全新节点启动即自动拉取全量历史数据并注册进扇出网格;步骤见[运维手册](docs/operations.md#扩容新数据节点自动同步)) | 17604 | 18604 |

两个 profile 端口不冲突,可同时运行(便于对比验证);命令均需带 profile 参数:

> **架构说明(控制台无本地存储)**:控制台是纯管理工具——web 容器不挂数据卷、不内嵌数据库,启动参数即**默认管理节点**(`DOCSQL_UPSTREAM` / 首个 `DOCSQL_PEERS` 条目),数据操作全部转发到该节点执行;界面「节点」下拉框只切换管理目标。从旧版本升级:web 容器自动重建(不再挂旧内嵌引擎卷),遗留卷可用 `./deploy/reset-data.sh` 清除。

```bash
cd deploy
docker compose --profile single up -d     # 单节点部署;发布新版本加 --build(数据保留)
docker compose --profile cluster up -d    # 三节点对等集群部署
docker compose --profile single --profile cluster down    # 全部停止(数据保留)
```

**扩容与修复**:集群扩容只需起一个指向现有节点的全新节点(compose `join` profile,先建卷 `docsql-dev-data-d` / `docsql-prod-data-d`),自动拉取全量历史并注册扇出;节点离线错过的写在**重启时自动增量补齐**(超出日志窗口或仍有分歧时转多数派快照采纳)。步骤、动态注册注意事项与边界见[运维手册 · 扩容](docs/operations.md#扩容新数据节点自动同步)与[离线补齐](docs/operations.md#离线补齐与重启反熵)。

**数据持久化**:每个节点的数据放在 external 卷(生产 `docsql-data-a/b/c/single` + `docsql-prod-data-d`;开发 `docsql-dev-data-*`),发布换镜像、重建容器乃至 `down -v` 都**不会**删数据;部署测试跑在一次性卷(`docsql-dev-testdata-*`)上,同样不动开发数据;彻底清数据唯一入口是 `./deploy/reset-data.sh`。首次部署前先建卷(一次性):

```bash
cd deploy && for v in a b c single; do docker volume create docsql-data-$v; done && docker volume create docsql-prod-data-d  # 生产
cd deploy && for v in a b c d single; do docker volume create docsql-dev-data-$v; done                                       # 开发
```

**备份与恢复**:每节点默认每日自动备份(保留 7 份,备份文件带 sha256 校验和,恢复前强校验),写数据卷 `backups/`;控制台备份页 / `POST /api/backup` 可手动触发与一键恢复。恢复为整库脚本经正常写路径重放并扇出全网,完成后自动比对摘要验证收敛。完整语义(覆盖范围、恢复期间写排队、跨节点互斥、AUTOINCREMENT、归档)见[运维手册 · 备份与恢复](docs/operations.md#备份与恢复)。

**本地开发**(`deploy/docker-compose.yml`,项目名 `docsql-dev`):从源码构建镜像(构建期内置全量 cargo test 门禁),tag `:local`,数据卷 `docsql-dev-data-*`。上面的命令加 `--build` 即触发构建;镜像 tag 用 `DOCSQL_DEV_IMAGE_TAG` 覆盖。客户端 token 用 `DOCSQL_DEV_TOKEN` 配置(同时下发给所有节点与 Web 控制台,控制台以它认证节点,浏览器端无需填写令牌;**从控制台创建数据库用户之前必须配置**——节点一旦存在用户,匿名的控制台连接会被拒绝,部署测试脚本会自动将其置空)。

**生产**(`deploy/docker-compose.prod.yml`,项目名 `docsql-prod`):拉取 CI 发布的 GHCR 镜像(不本地构建),端口 +1000(18600-18604/18700/18710,与开发栈互不冲突),数据卷沿用历史命名 `docsql-data-*`(真实数据在此),断线自动重启,日志轮转,Web 控制台可配 token:

```bash
cd deploy
cp .env.example .env    # 固定 DOCSQL_IMAGE_TAG(建议固定版本 tag)、设置 DOCSQL_TOKEN;集群拓扑再设 DOCSQL_CLUSTER_TOKEN(节点间认证,用与 DOCSQL_TOKEN 不同的值)
docker compose -f docker-compose.prod.yml --profile single up -d    # 生产单节点
docker compose -f docker-compose.prod.yml --profile cluster up -d   # 生产三节点集群
```

> 本地开发与生产完全分离:项目名(`docsql-dev` / `docsql-prod`)、端口(1760x+1770x / 1860x+1870x)、数据卷(`docsql-dev-data-*` / `docsql-data-*`)、镜像 tag 变量(`DOCSQL_DEV_IMAGE_TAG` / `DOCSQL_IMAGE_TAG`)与客户端 token 变量(`DOCSQL_DEV_TOKEN` / `DOCSQL_TOKEN`)互不相同,两套拓扑可同时运行、互不共享数据。环境变量完整清单见[运维手册](docs/operations.md#环境变量运维相关)。

> 注:镜像基于 mcr.microsoft.com/azurelinux(本环境 docker.io 不可达)。

## 数据库用户与角色(SQL 级访问控制)

除 token 认证外,DocSQL 支持在数据库内管理用户、角色与表级权限——用户定义随集群复制(在任一节点创建,全网格生效),并随备份/快照一致传播:

```sql
-- 管理员(持有 DOCSQL_TOKEN 的连接,或尚未创建任何用户时的开放连接)建号授权:
CREATE USER analyst PASSWORD '至少8位密码';   -- 用户名已存在时报错(改密码走 ALTER USER)
ALTER USER analyst PASSWORD '新密码';
GRANT readonly   TO analyst;            -- 内置角色:只读(可 SELECT 全部业务表)
GRANT readwrite  TO app_service;        -- 内置角色:读写(DML + PUBLISH/TRIM,无 DDL)
GRANT admin      TO ops_backup;         -- 内置角色:完全权限(DDL、用户管理、备份/恢复、PROMOTE)

-- 自定义角色 + 表级权限:
CREATE ROLE reporting;
GRANT SELECT, UPDATE ON orders TO reporting;   -- SELECT/INSERT/UPDATE/DELETE/ALL
GRANT reporting TO analyst;
REVOKE UPDATE ON orders FROM reporting;
REVOKE reporting FROM analyst;                 -- 撤销立即生效
DROP USER analyst;                             -- 级联清理其授权与角色成员关系
```

**权限矩阵**:admin=全部;readwrite=全部表 DML + `PUBLISH`/`PUBSUB TRIM`;readonly=全部表 `SELECT`;自定义角色=被授予的表级 DML 位。DDL(`CREATE/DROP/ALTER TABLE`、`CREATE INDEX`)与用户管理、备份触发/恢复、`PROMOTE` 仅 admin。子查询同样受读权限约束,无法分类的语句形状按拒绝处理(_fail-closed_)。GRANT/REVOKE/DROP USER 对**既有连接**即时生效:授权在每帧处理前按纪元刷新,被撤销或被删除用户的连接从下一帧起被拒(涵盖 SQL、PUBLISH/TRIM、备份、PROMOTE 全部权限面)。

**登录方式**:

- 协议帧 `REQ_AUTH_USER`(JSON `{"user","password"}`);密码以盐化 PBKDF2-HMAC-SHA256(60000 轮)存储,校验常数时间;未知用户与错误密码返回同一错误并执行等价计算(防用户名枚举);失败同样按来源 IP 锁定(10 次/60 秒)。
- ADO.NET 连接串:`host=...;port=...;user=analyst;password=...`(与 `token=` 二选一,同时给出时用户登录优先);EF Core `UseDocsql("...")` 同一连接串。
- CLI:`docsql-cli connect 127.0.0.1:7600 --user analyst`(密码从 `DOCSQL_PASSWORD` 或交互提示读取,不走命令行参数)。

**Web 控制台管理**:「视图 → 用户与角色」页可完成上述全部操作(用户列表/角色管理/按表勾选表级权限)。**节点启用用户后,务必为控制台与节点配置一致的 `DOCSQL_TOKEN`(开发栈为 `DOCSQL_DEV_TOKEN`)**,否则控制台的匿名连接会被拒绝;详见[安全指南 · 数据库用户与角色](docs/security.md#数据库用户与角色)与[功能总览 · 控制台](docs/features.md#9-web-控制台docsql-studio)。

**兼容与过渡**:`DOCSQL_TOKEN` 恒为管理员身份(存量部署零变化);未配置 token 且从未创建用户的节点维持开放访问(开发模式);**一旦存在任一用户,新建的匿名连接即被拒绝**(判定取连接建立时刻——正在建号授权的会话不会被自己锁死)。用户/角色数据存于保留名内部表(复制但不可直接读写),明文密码只在执行节点出现,日志、复制流、备份里均为 PBKDF2 哈希形式。

## .NET 与 Aspire(ADO.NET / EF Core / AppHost 编排)

四个 NuGet 包发布在 GitHub Packages(`net10.0`,版本随 `v*` tag 发布,当前 `0.4.0`):

| 包 | 用途 | 装到哪 |
|---|---|---|
| `Docsql.Client` | ADO.NET 提供程序(连接池/事务/pub-sub/传输加密) | 数据访问项目 |
| `Docsql.EntityFrameworkCore` | 原生 EF Core 提供程序(EnsureCreated/索引自动同步) | EF 项目 |
| `Docsql.Aspire.Hosting` | Aspire AppHost 编排:容器节点/对称集群/连接串注入/健康检查/伴生控制台 | AppHost 项目 |
| `Docsql.Aspire.Client` | Aspire 消费侧:`AddDocsqlConnection` + 连接健康检查 | 消费项目(Worker/ASP.NET Core) |

**安装**:先把 GitHub Packages 源配到 `nuget.config`(读取需要 GitHub PAT,权限 `read:packages`):

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

```bash
dotnet add package Docsql.Client                # ADO.NET(连接池/事务/pub-sub)
dotnet add package Docsql.EntityFrameworkCore   # EF Core 提供程序
dotnet add package Docsql.Aspire.Hosting        # AppHost 项目
dotnet add package Docsql.Aspire.Client         # 消费项目(Worker/API)
```

**AppHost 三步起步**(完整示例见 `dotnet/samples/AspireSample/`):

```csharp
var docsql = builder.AddDocsql("docsql").WithDataVolume().WithWebConsole();
builder.AddProject<Projects.MyApi>("myapi").WithReference(docsql).WaitFor(docsql);
// 对称集群:builder.AddDocsqlCluster("docsql", nodeCount: 3)
```

`aspire start`(或 `dotnet run --project <AppHost>`)即按编排拉起容器节点并打开 dashboard;
示例 AppHost 配 `AddDockerComposeEnvironment`,`aspire publish` 输出 docker-compose 部署产物。
消费侧 `builder.AddDocsqlConnection("docsql")` 注册连接与健康检查;EF 侧
`services.AddDocsqlDbContext<TodoDb>("docsql")` 按名取注入的连接串。默认生成随机客户端
token(运行期持久化到 user secrets),`WithToken`/`WithClusterToken`/`WithEnvironment`
可完整定制;镜像版本默认 `latest`,`WithImageTag` 钉版。API 速览见
[Docsql.Aspire.Hosting](dotnet/Docsql.Aspire.Hosting/README.md) /
[Docsql.Aspire.Client](dotnet/Docsql.Aspire.Client/README.md) 包 README。

> 完整使用手册(集群/控制台/凭据管理/消费侧/发布与排障)见 [Aspire 集成指南](docs/aspire.md)。

## 安全(对照等保 2.0 / GB/T 20273 数据库管理系统安全技术要求)

面向国内数据库安全检测(等级保护第三级)的能力对照:

| 控制项 | DocSQL 实现 |
|---|---|
| 身份标识与鉴别 | 协议层 token 认证(REQ_AUTH,常数时间比较防时序侧信道);三种凭据:`DOCSQL_TOKEN`(客户端)、`DOCSQL_READ_TOKEN`(只读客户端)、`DOCSQL_CLUSTER_TOKEN`(节点间,`FLAG_REPLICATION` 复制帧仅接受节点身份);**数据库用户**(REQ_AUTH_USER,盐化 PBKDF2-HMAC-SHA256 存储,常数时间校验,未知用户等价计算防枚举),角色与表级权限随集群复制;Web 控制台账号门:首次使用强制设置用户名/密码,HttpOnly 会话 Cookie,`DOCSQL_TOKEN` 可作为程序化旁路 |
| 登录失败处理 | 同一来源 IP 在 60 秒窗口内认证失败达 10 次(阈值可按部署调严)即锁定 60 秒,期间任何 token(含正确值)均被拒绝;锁定事件写入审计日志;Web 控制台登录门同策略(10 次/60 秒窗口,锁定 60 秒,按来源 IP) |
| 口令/凭据复杂度 | 服务器启动时校验所有已配置凭据:长度不足 8 或单一字符重复即拒绝启动(进程退出码 2) |
| 访问控制(最小权限) | `DOCSQL_READ_TOKEN` 只读身份:可查询、可订阅,一切持久化写在协议层拒绝;**数据库角色**:内置 `admin`/`readwrite`/`readonly` + 自定义角色表级 DML 授权(GRANT/REVOKE 即时生效,DDL 与管理操作仅 admin,读目标含子查询 fail-closed);存在任一用户后匿名连接关闭;副本模式 `DOCSQL_READ_ONLY=1` 整节点只读;Web 控制台独立 token 门禁 |
| 安全审计 | 语句审计(`docsql_log` 环形缓冲,含语句文本/耗时/影响行数/是否复制/错误)、认证事件审计(成功与失败均记录,来源 IP + 授予身份/失败原因,web 控制台日志页可见)、`DOCSQL_LOG_FILE` 可同步落 JSONL 文件留存 |
| 资源控制 | `DOCSQL_MAX_CONN` 并发连接数上限(超限立即拒绝不排队);`DOCSQL_IDLE_TIMEOUT` 空闲会话超时(服务端主动断开,订阅客户端需定期 PING 保活);`DOCSQL_STATEMENT_TIMEOUT_MS` 客户端语句墙钟预算(超时即报错回滚;复制 apply 与恢复重放不受限,慢节点不偏离已确认写入);单帧 64MB 上限;对端 IO 预算(连接 3s/读写 10s);TCP keepalive + NODELAY(NAT/防火墙后的长会话不被静默掐断) |
| 传输保密性 | `DOCSQL_KEY` AES-256-GCM 帧加密(含认证 token 与数据);绑定非回环地址且未配置 `DOCSQL_KEY` 时启动显式告警;默认端口映射仅绑定 `127.0.0.1`;**Web 控制台原生 TLS**(`DOCSQL_WEB_TLS_CERT`/`DOCSQL_WEB_TLS_KEY`,rustls),或置于 TLS 反代之后(`DOCSQL_WEB_COOKIE_SECURE=1`) |
| SQL 注入防护 | **ADO.NET/EF Core 默认服务端参数绑定**(REQ_PREPARE/REQ_EXECUTE:占位符在服务端引号感知绑定,字符串值翻倍转义,任何取值都无法逃逸字面量);服务端不拼接外部输入;系统表 `_pubsub_messages` 对 SQL 客户端隐藏 |
| 数据完整性 | WAL 先写日志后落数据、崩溃恢复;节点间复制依赖独立集群凭据防伪造 |
| 数据备份 | 自动定时备份(默认每日,`DOCSQL_BACKUP_INTERVAL_SECS`/`DOCSQL_BACKUP_KEEP` 可调):整库一致点逻辑快照,随数据卷持久;**每份备份带 sha256 校验和 sidecar,恢复前强校验**(损坏/被篡改的转储在重放前被拒,旧备份无 sidecar 仍可恢复);恢复为整库重放,控制台备份页可手动触发;备份成败计入审计日志 |

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

## 运维与监控

监控、环境变量、备份恢复、扩容与部署运维的完整手册见[运维手册](docs/operations.md)。要点:

- **存活探针**:`GET /healthz` 无门禁回答控制台进程自身状态(不触碰数据库节点,setup 前同样可用);
- **Prometheus 指标**:`GET /metrics`(账号门激活时抓取需会话 Cookie 或 `X-Docsql-Token` 旁路,
  未启用账号门时开放)——逐节点并行抓取
  REQ_STATUS,输出 `docsql_node_up`/`docsql_sql_statements_total`/`docsql_connections_active`/
  `docsql_network_bytes_total`/`docsql_auth_failures_total`/存储与期刊收敛等指标族(节点标签 `node`),
  另有控制台自身 `docsql_web_http_requests_total`;
- **优雅停机**:节点与控制台处理 SIGTERM/SIGINT——停止接受新连接,存量连接限时排空(节点侧最长 10s),
  未及收尾的事务由断连回滚 + WAL 恢复兜底,`docker stop`/滚动发布安全;
- **原生 TLS**:控制台设 `DOCSQL_WEB_TLS_CERT` + `DOCSQL_WEB_TLS_KEY`(PEM)即以 HTTPS 服务全 API 面
  (rustls 实现;两者只设其一会拒绝启动);TLS 节点建议同时开 `DOCSQL_WEB_COOKIE_SECURE=1`;
  未配置时为明文 HTTP,生产置于 TLS 反代之后;
- **配置快速失败**:数值型环境变量非法值拒绝启动(exit 2),不再静默回退默认值。

## 已知边界(v1)

- 事务为单连接快照隔离;多连接并发由服务器互斥串行化(单写者引擎)
- auto-GUID 主键列不支持 `INSERT ... SELECT`(对等节点重放 SELECT 时无法收敛随机生成值;请用 VALUES 并按需显式给 id)
- EF Core 为独立原生提供程序(不依赖 SQLite);`Database.Migrate()` 不支持(显式报错并指引改用 `EnsureCreated`),模型/索引同步由 EnsureCreated 自动完成

完整边界清单与不在支持面的 SQL 语法见[已知边界](docs/limitations.md)与 [SQL 参考 · 不支持](docs/sql-reference.md#不支持)。
