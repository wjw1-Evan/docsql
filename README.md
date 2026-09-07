# docsql

原生多模数据库:文档式存储 + 完整 SQL + KV 命令与发布订阅 + Web 管理控制台 + EF Core 兼容 + 主从复制与分片路由。

## 快速开始

```bash
cargo build --workspace
cargo test --workspace          # Rust 全量测试
cd dotnet && dotnet test        # .NET 测试(需先 cargo build 出 server 二进制)
```

```bash
# 嵌入式 shell
cargo run -p docsql-cli -- :memory:

# 服务器 + 远程 shell
./target/debug/docsql-server my.db 127.0.0.1:7600 &
./target/debug/docsql-cli connect 127.0.0.1:7600

# Web 管理控制台 docsql Studio(SSMS 风格,REST + 单页 UI)
./target/debug/docsql-web my.db 127.0.0.1:7700
# 开启认证:DOCSQL_TOKEN=secret ./target/debug/docsql-web my.db 127.0.0.1:7700
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
| 集群 | 主从复制(写转发)、只读副本、PROMOTE 故障转移、16384 哈希槽分片路由 |

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

- Rust:11 个测试目标、120 个用例全绿(单元 + SQL 集成 + KV 语义 + 协议 + 端到端 + 复制故障转移 + 多分片 + 批处理/目录元数据;亦在 Docker 构建内作为门禁执行)
- Docker:3 节点 compose 集群,16 项多节点部署测试全绿(SQL+KV 复制/只读/故障转移/分片隔离/事务)
- .NET:xUnit(ADO.NET 合规 5 项 + EF Core CRUD/LINQ/Include 2 项)
- 门禁:`cargo fmt` + `cargo clippy -D warnings` + `cargo test` 全过

## Docker 多节点部署

```bash
# 构建镜像(构建期内置全量 cargo test 门禁)并启动 3 节点集群:
#   node-primary(17601, 主)+ node-replica-a(17602, 只读副本)+ node-shard-b(17603, 独立分片)
./deploy/run-tests.sh     # 一键:重建集群 + 16 项多节点部署测试

# 单独操作
cd deploy && docker compose up -d
docker compose logs -f node-primary
```

部署测试覆盖:主节点 SQL/KV 写入 → 副本复制可见、只读强制、PROMOTE 故障转移后恢复写入、独立分片数据隔离、跨网络事务回滚。

> 注:本环境的 Docker 构建基于 mcr.microsoft.com/azurelinux(docker.io 不可达)。

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
