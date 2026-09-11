# 客户端与驱动

## .NET ADO.NET(Docsql.Client)

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
- 服务端配置 `DOCSQL_KEY` 后自动启用 AES-256-GCM;
- 参数在客户端转义为类型化字面量(见[安全指南](security.md#sql-注入防护双层))。

## .NET EF Core(Docsql.EntityFrameworkCore)

```csharp
services.AddDbContext<AppDb>(o => o.UseDocsql(connectionString));
```

- 原生提供程序(不依赖 SQLite);`EnsureCreated` + 惰性建表 + 模型/索引自动同步(免迁移);
- `Database.Migrate()` 显式报错(Migrations 不支持);
- 两包均带 NuGet 元数据,`dotnet pack` 可出包(`Docsql.Client` / `Docsql.EntityFrameworkCore`)。

## CLI(docsql-cli)

```
docsql connect 127.0.0.1:7600 --user analyst   # 密码走 DOCSQL_PASSWORD 或交互提示
  --csv / --json        行导出(RFC 4180 CSV / JSON 对象数组)
  -f script.sql         脚本批执行(错误即退出 1)
  help;                 内联命令帮助(含 pub/sub 命令面)
```

订阅是专用连接 + 专职读线程(CLI 已内置):普通一问一答连接会把推送帧错当成命令响应。

## 驱动作者:线协议参数化

v1 二进制协议(`core/proto.rs`),一句一帧。服务端 prepared statements 三帧:

| 帧 | 载荷 | 响应 |
|---|---|---|
| `REQ_PREPARE 0x0003` | SQL 文本,`?` 为占位符 | `RESP_PREPARED 0x010E` `{"handle":n}` |
| `REQ_EXECUTE 0x0004` | `{"handle":n,"params":[...]}` | 与 REQ_SQL 相同(RESP_ROWS/AFFECTED/ERROR) |
| `REQ_CLOSE_STMT 0x0005` | `{"handle":n}` | RESP_AFFECTED / RESP_ERROR |

- 参数按位置绑定,服务端渲染类型化字面量(引号感知;字符串值翻倍转义)——驱动无需自行转义;
- 句柄按连接隔离,连接断开即失效;
- 参数类型:JSON `null/bool/number/string`(嵌套对象/数组按 JSON 文本绑定,二进制走 hex 字面量);
- 授权、`DOCSQL_STATEMENT_TIMEOUT_MS`、审计与 REQ_SQL 完全同路径;
- 订阅帧、REQ_STATUS(含 `metrics` 运行时计数器)等其余帧见 `core/proto.rs` 模块头注释(权威)。
