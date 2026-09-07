# EF Core 调用示例 + 测试

## 背景
仓库已有 `dotnet/Docsql.EntityFrameworkCore`(UseDocsql 扩展,桥接 SQLite 管线到 Docsql.Client TCP 二进制协议)和 `dotnet/Docsql.EntityFrameworkCore.Tests`(仅 2 个用例)。用户要一个可运行的 EF Core 调用示例并测试。

## 计划

1. **新建可运行示例项目** `dotnet/Docsql.EfSample`(控制台,引用 Docsql.EntityFrameworkCore):
   - 启动/连接本地 docsql-server(127.0.0.1:7600,或由参数指定端口)
   - 演示:`UseDocsql` 连接串配置 → `EnsureCreated` 建表 → 插入(含一对多关系)→ LINQ 查询(Where/OrderBy/Include)→ 更新 → 删除 → 原生 SQL 查询 → 事务
   - 每步打印结果,中文输出注释(符合用户偏好)

2. **测试验证**:
   - 确认 `target/debug/docsql-server` 存在(缺失则 `cargo build -p docsql-server`,注意 PATH 前缀 `~/.cargo/bin`)
   - `dotnet run --project dotnet/Docsql.EfSample` 端到端跑通示例(临时起 server + 临时 db 文件)
   - `dotnet test dotnet/Docsql.EntityFrameworkCore.Tests` 确认现有测试仍绿

3. 视运行情况在 EfTests 中补 1-2 个覆盖示例中特性的用例(如原生 SQL / 事务),如全部已覆盖则不强行加。

不做:修改 EF 提供程序本身、协议改动、迁移(Migrations)支持(README 已注明边界)。