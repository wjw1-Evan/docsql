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
- 服务端配置 `DOCSQL_KEY` 后自动启用 AES-256-GCM。

### 参数绑定:服务端 prepared statements(默认路径)

带参数的命令**不再在客户端拼接字面量**:`@name` 改写为 `?` 占位符,模板经 REQ_PREPARE
注册(物理连接内按句柄缓存,同一模板重复执行零注册开销),参数数组经 REQ_EXECUTE 执行。
值在**服务端**渲染为类型化字面量(引号感知、字符串翻倍转义)——任何取值都无法逃逸
字面量,注入载荷只能是数据。授权/语句超时/审计与普通语句同路径。`cmd.Prepare()`
可预注册句柄。

### 连接池(默认开启)

- `Close()` 归还物理连接而非断开;`Open()` 借出前 PING 验活,死连接自动丢弃重建
  (服务器重启后的客户端韧性由此免费获得);
- 池键 = host/port/user/password/token/key/max pool size:不同身份绝不共享物理连接;
- **事务安全**:事务未了结就 `Close()` 的连接被物理丢弃(服务器对断连自动 ROLLBACK),
  残留事务不可能泄漏给下一个借出者;
- 池化连接的服务端 prepared 句柄缓存随物理连接有效,复用零成本;
- 开关:`pooling=false`(直连模式);`max pool size=N`(池上限,默认 100,超限归还即关闭);
- `ClearPool()` / `ClearAllPools()` 物理清空空闲连接。

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
