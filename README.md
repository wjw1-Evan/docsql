# DocSQL

Rust 原生文档数据库:JSON 文档存储 + 完整 SQL + Web 管理控制台 + ADO.NET / EF Core 客户端 + 对等集群复制。**运行仅 Docker**(镜像由 CI 发布至 GHCR)。

## 特性一览

- **文档存储** — JSON 文档整体存储,无强制 schema;手写分页器 + B+ 树索引 + WAL 崩溃恢复
- **完整 SQL** — DDL/DML/JOIN/聚合/**窗口函数**/事务(SAVEPOINT)/视图/CTAS;精确 `DECIMAL`、`BLOB`、JSON 函数、多列索引、GUID 主键(自动生成 UUIDv7,时序有序)、Oracle 兼容层(DUAL/ROWNUM/NVL 等);未实现的语法一律显式报错,不静默降级
- **集群** — `DOCSQL_PEERS` 对等复制(任意节点可写)、新节点自动 join 同步、重启反熵修复、主从写转发 + PROMOTE 故障转移
- **持久化 pub/sub** — 消息先落盘(WAL)再推送,重启不丢;订阅可从 `earliest` / 指定 id 续传(at-least-once)
- **DocSQL Studio** — SSMS 风格 Web 管理控制台(对象浏览器/查询工作台/集群监控/备份/用户管理),自身零存储
- **.NET 栈** — `Docsql.Client`(ADO.NET)、`Docsql.EntityFrameworkCore`(原生 EF Core 提供程序,免迁移自动同步模型)、Aspire 编排三件套
- **安全** — token / 数据库用户与角色 / 表级权限、AES-256-GCM 传输加密、登录锁定、审计日志、自动备份(带 sha256 校验)

各项能力的完整说明与示例见[功能总览](docs/features.md)。

## 快速开始

```bash
# 1. 首次部署先建数据卷(一次性;external 卷,重建容器不丢数据)
cd deploy && for v in a b c single; do docker volume create docsql-data-$v; done

# 2. 配置生产环境(固定镜像 tag 与 token;集群再加 DOCSQL_CLUSTER_TOKEN)
cp .env.example .env

# 3. 启动:单节点 或 三节点对等集群
docker compose -f docker-compose.prod.yml --profile single up -d    # db 127.0.0.1:18600,控制台 http://127.0.0.1:18710
docker compose -f docker-compose.prod.yml --profile cluster up -d   # node-a/b/c 18601-18603,控制台 18700

# 4. SQL shell(镜像自带 CLI;--csv / --json 导出,-f script.sql 批执行)
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600
```

> GHCR 镜像包默认不可匿名拉取,先 `docker login ghcr.io`。本地开发(源码构建)与扩展节点(`join` profile 自动拉取全量历史)见[运维手册](docs/operations.md)。彻底清数据唯一入口 `./deploy/reset-data.sh`。

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

## 集群与数据安全

- **写入扇出**:任一节点的 SQL 写入按序扇出至全部对等节点;节点间用独立集群凭据(`DOCSQL_CLUSTER_TOKEN`)互相认证
- **扩容**:起一个指向现有节点的全新容器(`join` profile)即自动拉取全量历史并注册进扇出网格
- **自愈**:节点离线错过的写在重启时自动增量补齐,超出日志窗口转多数派快照采纳
- **持久化**:数据在 external 卷,换镜像、重建容器乃至 `down -v` 都不丢
- **备份**:默认每日自动备份(保留 7 份、带 sha256 校验),控制台或 `POST /api/backup` 可手动触发与一键恢复;支持 PITR 增量链

## .NET 与 Aspire

四个 NuGet 包发布在 GitHub Packages(`net10.0`,版本随 `v*` tag):`Docsql.Client`(ADO.NET)、`Docsql.EntityFrameworkCore`、`Docsql.Aspire.Hosting` / `Docsql.Aspire.Client`。安装需在 `nuget.config` 配置 GitHub Packages 源(`read:packages` PAT),见[客户端与驱动](docs/drivers.md)。

```csharp
// EF Core
services.AddDbContext<TodoDb>(o => o.UseDocsql("host=...;port=...;user=...;password=..."));

// Aspire AppHost 三行编排(单节点;AddDocsqlCluster("docsql", 3) 为集群)
var docsql = builder.AddDocsql("docsql").WithDataVolume().WithWebConsole();
builder.AddProject<Projects.MyApi>("myapi").WithReference(docsql).WaitFor(docsql);
```

完整手册(凭据管理/集群/消费侧/发布排障)见 [Aspire 集成指南](docs/aspire.md)。

## 数据库用户与角色

token 之外支持 SQL 级访问控制,用户定义随集群复制、随备份传播;`DOCSQL_TOKEN` 恒为管理员身份:

```sql
CREATE USER analyst PASSWORD '至少8位密码';
GRANT readonly TO analyst;                 -- 内置角色:admin / readwrite / readonly
CREATE ROLE reporting;
GRANT SELECT, UPDATE ON orders TO reporting;   -- 自定义角色,表级权限
GRANT reporting TO analyst;
```

登录:ADO.NET 连接串 `user=...;password=...`;CLI `connect <addr> --user <name>`(密码走 `DOCSQL_PASSWORD` 或交互提示)。Web 控制台「用户与角色」页可完成全部操作。密码以 PBKDF2 哈希存储与传播,明文不落盘;存在任一用户后匿名连接关闭。详见[安全指南](docs/security.md)。

## 部署拓扑与端口

开发(`docker-compose.yml`,源码构建)与生产(`docker-compose.prod.yml`,GHCR 镜像)两套完全分离,可同时运行;compose 必须带 profile:

| profile | 拓扑 | 开发端口 | 生产端口 |
|---|---|---|---|
| `single` | 独立单节点 + 控制台 | 17600 / 17710 | 18600 / 18710 |
| `cluster` | 三节点对等集群 + 控制台 | 17601-17603 / 17700 | 18601-18603 / 18700 |
| `join` | 向运行中集群加入第四节点 | 17604 | 18604 |

## 性能

主键/UNIQUE/`CREATE INDEX` 列均有 B+ 树支撑(点查/范围 O(log n));`ORDER BY <索引键> + LIMIT` 走索引序窗口,无索引排序走 top-K 堆,`COUNT(*)` 免解码。写入吞吐受每语句一次 fsync 支配(可选 `DOCSQL_ASYNC_COMMIT=1` 组提交)。基准脚本见 `crates/docsql-core/examples/bench*.rs`。

## 已知边界

- 单写者引擎:写路径全库互斥(读已 MVCC 快照化,读不阻塞写);横向扩展靠集群分摊写入点
- auto-GUID 主键表不支持 `INSERT ... SELECT`(集群收敛约束,显式报错)
- EF Core 不支持 `Database.Migrate()`(显式报错,模型/索引同步由 EnsureCreated 自动完成)

完整清单见[已知边界](docs/limitations.md)。

## 开发与测试

```bash
cargo fmt && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
cd dotnet && dotnet test          # 改 dotnet/协议时另需先 cargo build -p docsql-server
./deploy/run-tests.sh             # 改复制/部署逻辑后必跑(single 34 + cluster 81)
```

CI 执行同样门禁 + 覆盖率回归 + .NET 套件,然后发布多架构镜像并在 main 分支跑部署测试。构建、门禁与覆盖率细节见[贡献指南](CONTRIBUTING.md);架构约束与红线见 [AGENTS.md](AGENTS.md)。

| 模块 | 职责 |
|---|---|
| `crates/docsql-core` | 存储引擎(pager/WAL/B+树/heap)+ SQL 内核 + 协议帧 + JSON |
| `crates/docsql-server` | TCP 服务器、鉴权、复制、pub/sub、备份恢复 |
| `crates/docsql-cli` | 嵌入式/远程 SQL shell |
| `crates/docsql-web` | REST API + 内嵌控制台 |
| `dotnet/` | ADO.NET / EF Core / Aspire 包与示例 |

## 许可证

双许可:Apache-2.0 或 MIT,任选其一。见 [LICENSE-APACHE](LICENSE-APACHE) / [LICENSE-MIT](LICENSE-MIT)。
