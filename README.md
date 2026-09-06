# docsql

原生多模数据库:文档式存储 + 完整 SQL + KV 命令与发布订阅 + 分布式部署。

- **文档存储**:数据以 JSON 风格文档整体存储,无强制 schema
- **SQL**:DDL/DML、JOIN、聚合、子查询、事务、JSON 路径
- **KV**:GET/SET/EXPIRE/TTL/INCR、LIST/HASH/SET/ZSET,与 SQL 同一存储引擎、双向互通
- **发布订阅**:SUBSCRIBE/PUBLISH/PSUBSCRIBE
- **嵌入式 + 服务器**:单机嵌入库、CLI、网络服务器、Web 管理控制台
- **分布式**:Raft 复制、分片、故障转移、负载均衡(后期里程碑)
- **EF Core**:自写 ADO.NET / EF Core 提供程序(后期里程碑)

## 构建

```bash
cargo build --workspace
cargo test --workspace
```

## 结构

| crate | 职责 |
|---|---|
| docsql-core | 存储引擎(pager/WAL/B+树)+ SQL 解析/优化/执行 |
| docsql-kv | KV 与发布订阅命令层 |
| docsql-cluster | Raft 复制 / 分片 / 故障转移 |
| docsql-server | 网络服务器(SQL/KV 二进制协议 + Web 控制台) |
| docsql-cli | 交互式 shell(嵌入与远程模式) |
| dotnet/ | ADO.NET 与 EF Core 提供程序 |
