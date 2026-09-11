# Docsql.Client

ADO.NET data provider for [DocSQL](https://github.com/wjw1-Evan/docsql) — a Rust-native JSON document database with full SQL.

```csharp
await using var conn = new DocsqlConnection("host=127.0.0.1;port=7600;token=YOUR-TOKEN");
// 或数据库用户登录: "host=...;port=...;user=analyst;password=..."
await conn.OpenAsync();

await using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT id, note FROM orders WHERE id > @min";
cmd.Parameters.AddWithValue("min", 42);
await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync()) { /* ... */ }
```

- 命令面:完整 SQL(DDL/DML/JOIN/聚合/事务/SAVEPOINT/RETURNING)、`RETURNING` 子句、GUID/UUIDv7 自动主键
- 事务:`BeginDbTransaction` 映射 BEGIN/COMMIT/ROLLBACK,支持 SAVEPOINT
- 认证:协议 token(`token=`)或数据库用户(`user=`/`password=`)二选一
- 持久化 pub/sub:`DocsqlConnection.Publish(...)` + `DocsqlSubscriber`(专用连接、断线按 id 续传)
- 传输加密:服务端配置 `DOCSQL_KEY` 后自动启用 AES-256-GCM 帧加密

参数在客户端转义为类型化字面量(字符串单引号强转义);服务端同时提供 REQ_PREPARE/REQ_EXECUTE 服务端绑定帧供自研驱动使用。

License: MIT OR Apache-2.0.
