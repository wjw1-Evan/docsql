# docsql

原生多模数据库:文档式存储 + 完整 SQL + KV 命令与发布订阅 + Web 管理控制台 + EF Core 兼容 + 对等集群复制与分片路由。

## 快速开始

Docker 是唯一的运行方式。镜像由 GitHub Actions 自动构建并发布到 [ghcr.io/wjw1-evan/docsql](https://github.com/wjw1-Evan/docsql/pkgs/container/docsql)(push 到 `main` 或打 `v*` 标签触发;`latest`、`v1.2.3`、`sha-*` 等标签可用)。

```bash
# 单节点部署(compose,标准方式:独立节点 + web 控制台,数据落在命名卷)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
#   db 127.0.0.1:17600,web 控制台 http://127.0.0.1:17710

# SQL/KV 远程 shell(镜像自带 CLI,容器内执行)
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600

# 多节点部署(生产,3 节点对等集群:node-a 17601 + node-b 17602 + node-c 17603 + web 17700)
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d
```

> 私有仓库的 GHCR 镜像包默认不可匿名拉取,先 `docker login ghcr.io`。镜像 tag 可用环境变量 `DOCSQL_IMAGE_TAG` 覆盖。

## 开发(构建与测试,非运行方式)

```bash
cargo build --workspace
cargo test --workspace          # Rust 全量测试(开发门禁;本地 Docker 构建亦内置)
cd dotnet && dotnet test        # .NET 测试(需先 cargo build 出 server 二进制)
./deploy/run-tests.sh           # 本地构建镜像 + 部署测试(多节点 24 项 + 单节点 18 项)
```

## docsql Studio(Web 管理控制台)

参考 SQL Server Management Studio 的交互重新设计:

- **对象资源管理器**(左栏树):服务器 → 表(列含 PK/UQ/NN/AI 徽章、索引、键)→ 每表可双击打开数据网格;KV 存储按 字符串/列表/哈希/集合/有序集合 分组浏览(TTL 标记);发布订阅监视器入口;系统视图(`_kv`、`information_schema.*`、`sqlite_master`)
- **查询工作台**(多标签文档):SQL 语法高亮 + 行号编辑器,F5 执行 / Ctrl+F5 仅语法分析 / 执行所选;多语句批次依次执行并逐结果集呈现(网格 + "(N 行受影响)" 消息页 + 总耗时);表头点击排序
- **右键任务**:新建查询、选择前 1000 行、查看数据、编写 CREATE/DROP 脚本、删除表;KV 键的 打开/设 TTL/删除/复制
- **KV 类型化编辑器**:字符串编辑保存、列表推入/弹出、哈希字段、集合成员、有序集合分数,统一 TTL 管理
- **仪表盘**:表/行数/KV 分布/页与文件占用/运行时长一览

## 能力总览

| 领域 | 支持 |
|---|---|
| 存储 | JSON 文档整体存储(无强制 schema)、WAL 崩溃恢复、手写分页器与 B+ 树 |
| SQL | CREATE/ALTER/DROP TABLE+INDEX、INSERT(多行/RETURNING)、UPDATE/DELETE(RETURNING)、SELECT(WHERE/ORDER/LIMIT/OFFSET/GROUP BY+HAVING/COUNT/SUM/AVG/MIN/MAX/JOIN:INNER/LEFT/CROSS/USING/子查询派生表/UNION(ALL)/IN)、事务 BEGIN/COMMIT/ROLLBACK、PRIMARY KEY/UNIQUE/NOT NULL/AUTOINCREMENT、information_schema、sqlite_master 兼容视图、PRAGMA 兼容 |
| KV | GET/SET(NX/XX/EX/PX)/DEL/EXPIRE/TTL/PERSIST/INCR 系、LIST、HASH、SET、ZSET、MULTI/EXEC/DISCARD(与 SQL 同一事务系统)、TTL 惰性+定期清理、`_kv` 系统表与 SQL 双向互通 |
| 发布订阅 | SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUBLISH,服务器主动推送(RESP_PUSH 帧) |
| 网络 | 自定义二进制协议 v1(预留拓扑版本/重定向字段)、token 认证 |
| Web | **docsql Studio**(SSMS 风格管理控制台):对象资源管理器(表/列/索引/键 + KV 按类型分组 + 系统视图)、多标签查询编辑器(SQL 高亮/F5 执行/Ctrl+F5 分析/批量多结果集)、数据网格(排序/分页/删行)、KV 类型化编辑器(字符串/列表/哈希/集合/有序集合 + TTL 管理)、发布订阅监视器、服务器仪表盘;REST API(/api/sql /api/parse /api/meta /api/keys /api/kvkey /api/kv /api/stats /api/publish /api/events) |
| EF Core | `UseDocsql(connectionString)`(复用 SQLite 管线 + Docsql ADO.NET):EnsureCreated/CRUD/LINQ/Include/`[Index]` 特性索引(含唯一索引;模型增删索引均自动同步,免迁移) |
| 集群 | 对等集群(`DOCSQL_PEERS`:任意节点可写,SQL/KV 写入扇出至全部对等节点)、主从复制(写转发)、只读副本、PROMOTE 故障转移、16384 哈希槽分片路由 |

## 结构

| crate | 职责 |
|---|---|
| docsql-core | 存储引擎(pager/WAL/B+树/heap)+ SQL 解析执行 + 协议帧 + JSON |
| docsql-kv | KV/集合命令、发布订阅总线 |
| docsql-server | TCP 服务器、复制、分片路由 |
| docsql-cli | 嵌入式 + 远程 shell |
| docsql-web | Web 管理控制台(SSMS 风格 UI + REST API) |
| dotnet/ | Docsql.Client(ADO.NET)与 Docsql.EntityFrameworkCore |

## 测试

- Rust:11 个测试目标、120 个用例全绿(单元 + SQL 集成 + KV 语义 + 协议 + 端到端 + 复制故障转移 + 多分片 + 批处理/目录元数据;亦在本地 Docker 构建内作为门禁执行)
- Docker:compose 双 profile 部署测试全绿——多节点 24 项(3 节点对等集群:任意节点写入/多向 SQL+KV 复制/事务回滚/一致性收敛/Web 控制台)+ 单节点 18 项(SQL/KV 读写/事务回滚/容器重启持久性/与集群隔离/Web 控制台)
- .NET:xUnit(ADO.NET 合规 5 项 + EF Core CRUD/LINQ/Include 2 项)
- CI(GitHub Actions,push/PR 触发):`cargo fmt` + `cargo clippy -D warnings` + `cargo test` + `dotnet test` 全过 → 构建镜像 → main 分支另跑同一套部署测试(24 + 18 项)

## Docker 部署(单节点 / 多节点;本地开发与生产两个 compose 文件)

两种部署拓扑,用 compose profile 切换,**同一套文件支持单节点与多节点**:

| profile | 节点 | 端口 |
|---|---|---|
| `single` | node-single(独立单节点,无复制)+ 独立 web 控制台 | 17600 / 17710 |
| `cluster` | node-a + node-b + node-c 对等集群(任意节点可读写,SQL/KV 写入自动扇出至 `DOCSQL_PEERS`)+ web 控制台 | 17601-17603 / 17700 |

两个 profile 端口不冲突,可同时运行(便于对比验证);命令均需带 profile 参数,停止时用相同参数 `down -v`:

```bash
cd deploy
docker compose --profile single up -d     # 单节点部署
docker compose --profile cluster up -d    # 三节点对等集群部署
docker compose --profile single --profile cluster down -v   # 全部停止并清数据
```

**本地开发**(`deploy/docker-compose.yml`):从源码构建镜像(构建期内置全量 cargo test 门禁),tag `:local`。上面的命令加 `--build` 即触发构建。

**生产**(`deploy/docker-compose.prod.yml`):拉取 CI 发布的 GHCR 镜像(不本地构建),断线自动重启,日志轮转,Web 控制台可配 token:

```bash
cd deploy
cp .env.example .env    # 固定 DOCSQL_IMAGE_TAG(建议固定版本 tag)、设置 DOCSQL_TOKEN
docker compose -f docker-compose.prod.yml --profile single up -d    # 生产单节点
docker compose -f docker-compose.prod.yml --profile cluster up -d   # 生产三节点集群
```

> 本地开发与生产共享主机端口,同一拓扑同一时间只能运行一套,切换前先 `down -v`。

部署测试(`./deploy/run-tests.sh`,同时拉起两个 profile):多节点 24 项(任意节点写入/多向 SQL+KV 复制/事务回滚/一致性收敛/Web 控制台)+ 单节点 18 项(SQL/KV 读写、事务回滚、容器重启后数据持久、与集群的数据隔离、Web 控制台)。

> 注:镜像基于 mcr.microsoft.com/azurelinux(本环境 docker.io 不可达)。

## 性能(索引引擎)

主键/UNIQUE 列与 `CREATE INDEX` 列均有 B+ 树支撑:判重与点查/范围查询走索引(O(log n)),不再全表扫描;INSERT 单语句单事务提交(一次 WAL fsync);UPDATE/DELETE 命中索引条件时页内原地改写并增量维护索引树;WAL 超过 8MB 自动检查点;`_kv` 的 key 列为主键,KV 读写全部走索引。

本机基准(单线程、进程内直调、autocommit,含每次提交的 fsync;对照 SQLite 3.54 WAL/FULL 同条件):

| 场景 | docsql(索引引擎) | 优化前 | SQLite |
|---|---|---|---|
| INSERT(主键表)×5k | ~1,300 行/秒(线性) | 二次方劣化 | ~6,900 行/秒 |
| 点查 WHERE id=?(1k 行) | ~38,000 次/秒 | ~5,600 | ~241,000 |
| KV GET | ~15,000 ops/秒 | ~1,100 | — |
| KV SET/INCR/DEL | 540–1,350 ops/秒 | ~460 | — |

写入吞吐受每语句一次 fsync 支配(与 SQLite FULL 同语义);点查与 KV 读为解析+索引开销。

## 已知边界(v1)

- 跨分片 JOIN 不支持;在线槽迁移(平滑扩缩容)未实现,扩容需重导数据
- 事务为单连接快照隔离;多连接并发由服务器互斥串行化
- EF Core 通过 SQLite 管线桥接(非独立提供程序),复杂迁移 SQL 可能超出引擎方言
