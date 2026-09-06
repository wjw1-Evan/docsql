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

# Web 控制台(REST + 单页 UI)
DOCSQL_WEB=1 ./target/debug/docsql-web my.db 127.0.0.1:7700
```

## 能力总览

| 领域 | 支持 |
|---|---|
| 存储 | JSON 文档整体存储(无强制 schema)、WAL 崩溃恢复、手写分页器与 B+ 树 |
| SQL | CREATE/ALTER/DROP TABLE+INDEX、INSERT(多行/RETURNING)、UPDATE/DELETE(RETURNING)、SELECT(WHERE/ORDER/LIMIT/OFFSET/GROUP BY+HAVING/COUNT/SUM/AVG/MIN/MAX/JOIN:INNER/LEFT/CROSS/USING/子查询派生表/UNION(ALL)/IN)、事务 BEGIN/COMMIT/ROLLBACK、PRIMARY KEY/UNIQUE/NOT NULL/AUTOINCREMENT、information_schema、sqlite_master 兼容视图、PRAGMA 兼容 |
| KV | GET/SET(NX/XX/EX/PX)/DEL/EXPIRE/TTL/PERSIST/INCR 系、LIST、HASH、SET、ZSET、MULTI/EXEC/DISCARD(与 SQL 同一事务系统)、TTL 惰性+定期清理、`_kv` 系统表与 SQL 双向互通 |
| 发布订阅 | SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUBLISH,服务器主动推送(RESP_PUSH 帧) |
| 网络 | 自定义二进制协议 v1(预留拓扑版本/重定向字段)、token 认证 |
| Web | REST API(/api/sql /api/kv /api/keys /api/stats)+ 内置单页控制台 |
| EF Core | `UseDocsql(connectionString)`(复用 SQLite 管线 + Docsql ADO.NET):EnsureCreated/CRUD/LINQ/Include |
| 集群 | 主从复制(写转发)、只读副本、PROMOTE 故障转移、16384 哈希槽分片路由 |

## 结构

| crate | 职责 |
|---|---|
| docsql-core | 存储引擎(pager/WAL/B+树/heap)+ SQL 解析执行 + 协议帧 + JSON |
| docsql-kv | KV/集合命令、发布订阅总线 |
| docsql-server | TCP 服务器、复制、分片路由 |
| docsql-cli | 嵌入式 + 远程 shell |
| docsql-web | Web 控制台 |
| dotnet/ | Docsql.Client(ADO.NET)与 Docsql.EntityFrameworkCore |

## 测试

- Rust:11 个测试目标、109 个用例全绿(单元 + SQL 集成 + KV 语义 + 协议 + 端到端 + 复制故障转移 + 多分片;亦在 Docker 构建内作为门禁执行)
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

## 已知边界(v1)

- B+ 树为独立索引结构,执行器尚未走索引扫描(全表扫描路径)
- 跨分片 JOIN 不支持;在线槽迁移(平滑扩缩容)未实现,扩容需重导数据
- 事务为单连接快照隔离;多连接并发由服务器互斥串行化
- EF Core 通过 SQLite 管线桥接(非独立提供程序),复杂迁移 SQL 可能超出引擎方言
