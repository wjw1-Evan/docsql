# DocSQL 文档

面向用户与运维者的手册。快速上手见仓库根目录 [README](../README.md);面向 AI 编码代理的开发约束见 [AGENTS](../AGENTS.md)。

| 文档 | 内容 |
|---|---|
| [功能总览](features.md) | DocSQL 全部能力的完整清单与索引入口(存储/类型/SQL/索引/事务/发布订阅/复制/备份/安全/控制台/驱动/可观测性) |
| [SQL 参考](sql-reference.md) | 支持的 DDL/DML/查询语法、函数清单(含 JSON 函数族)、事务与约束、不支持面 |
| [Aspire 集成指南](aspire.md) | AppHost 编排(Docsql.Aspire.Hosting)、消费侧注册(Docsql.Aspire.Client)、集群/控制台/凭据/发布与排障 |
| [运维手册](operations.md) | 部署拓扑、环境变量参考、备份/恢复与校验和、监控(/metrics /healthz)、优雅停机、语句超时 |
| [安全指南](security.md) | 认证模型(三凭据 + 数据库用户/角色)、登录锁定、传输加密、SQL 注入防护、等级保护对照 |
| [客户端与驱动](drivers.md) | .NET ADO.NET / EF Core / Aspire 集成(GitHub Packages 安装)、CLI、线协议 prepared statements(驱动作者) |
| [已知边界与定位](limitations.md) | 当前架构边界(单写者、单文档 16MiB、无 TIMESTAMP 精确类型等)、商用化现状与路线 |

## 五分钟速览

```bash
# 生产单节点(独立节点 + Web 控制台)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
# 控制台 http://127.0.0.1:18710,节点 127.0.0.1:18600

# SQL shell
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600
```

```sql
CREATE TABLE orders (
    id      GUID PRIMARY KEY AUTOINCREMENT,   -- UUIDv7 时序主键,INSERT 省略即自动生成
    user_id INT NOT NULL,
    doc     TEXT,                             -- JSON 文档按文本存储
    created INT
);
INSERT INTO orders (user_id, doc) VALUES (42, '{"sku":"A1","qty":2,"tags":["x","y"]}');
SELECT JSON_EXTRACT(doc, '$.sku') AS sku FROM orders WHERE user_id = 42;
```

## Aspire 快速开始

.NET / Aspire 项目不必手写 compose——AppHost 里编排 DocSQL 容器节点:

```bash
dotnet add package Docsql.Aspire.Hosting   # AppHost 项目
dotnet add package Docsql.Aspire.Client    # 消费项目
```

```csharp
var docsql = builder.AddDocsql("docsql").WithDataVolume().WithWebConsole();
builder.AddProject<Projects.MyApi>("myapi").WithReference(docsql).WaitFor(docsql);
// 对称集群:builder.AddDocsqlCluster("docsql", nodeCount: 3)
```

`aspire start` 本地拉起(dashboard 可视),`aspire publish` 出 docker-compose 部署产物;
安装细节与完整 API 见[客户端与驱动](drivers.md#aspire-集成docsqlaspirehosting--docsqlaspireclient)。
