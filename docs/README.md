# DocSQL 文档

面向用户与运维者的手册。快速上手见仓库根目录 [README](../README.md);面向 AI 编码代理的
开发约束见 [AGENTS](../AGENTS.md);贡献流程见 [CONTRIBUTING](../CONTRIBUTING.md)。

## 按任务找文档

| 我想… | 看这里 |
|---|---|
| 认识 DocSQL、跑起来 | [README · 快速开始](../README.md#快速开始) |
| 了解数据库全部能力 | [功能总览](features.md) |
| 查 SQL 语法、函数、不支持项 | [SQL 参考](sql-reference.md) |
| 用 Aspire 编排(单节点/集群/控制台) | [Aspire 集成指南](aspire.md) |
| 用 .NET ADO.NET / EF Core / CLI / 自研驱动 | [客户端与驱动](drivers.md) |
| 部署、扩容、备份恢复、环境变量、监控 | [运维手册](operations.md) |
| 配用户/角色、审计、加密(等保对照) | [安全指南](security.md) |
| 确认架构边界与路线 | [已知边界与定位](limitations.md) |
| 查版本历史 / 报安全漏洞 | [CHANGELOG](../CHANGELOG.md) · [SECURITY](../SECURITY.md) |
| 参与开发(门禁、红线、测试) | [CONTRIBUTING](../CONTRIBUTING.md) · [AGENTS](../AGENTS.md) |

## 全部文档

**上手**

| 文档 | 内容 |
|---|---|
| [功能总览](features.md) | 全部能力的完整清单与索引入口(运行形态/类型/SQL/索引/发布订阅/复制/备份/安全/控制台/驱动/可观测性) |
| [Aspire 集成指南](aspire.md) | AppHost 编排、消费侧注册、集群/控制台/凭据、发布与排障 |

**参考**

| 文档 | 内容 |
|---|---|
| [SQL 参考](sql-reference.md) | DDL/DML/查询语法、函数清单(JSON 函数族)、事务与约束、不支持面 |
| [客户端与驱动](drivers.md) | ADO.NET / EF Core / Aspire 包安装与用法、CLI、线协议(驱动作者) |

**运维与安全**

| 文档 | 内容 |
|---|---|
| [运维手册](operations.md) | 部署拓扑、数据持久化、扩容与修复、备份恢复、环境变量、监控、CLI |
| [安全指南](security.md) | 三凭据 + 数据库用户/角色、登录锁定、注入防护、传输加密、等保对照 |

**边界**

| 文档 | 内容 |
|---|---|
| [已知边界与定位](limitations.md) | 架构硬边界(单写者、16MiB、无递归 CTE/行级合并等)、商用化现状与路线 |

**内部设计(贡献者)**

| 文档 | 内容 |
|---|---|
| [复合索引设计](design/001-composite-indexes.md) | 复合键 = `Value::Array`、root_key 泛化与判重语义(已实施) |
| [溢出页链设计](design/002-overflow-page-chains.md) | >4KB 文档的堆层分帧布局、链页回收(已实施) |
| [MVCC 读并发设计](design/003-mvcc-read-concurrency.md) | 快照读分阶段路线、锁兼容矩阵(阶段 A/B 已实施) |

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
    amount  DECIMAL(18,2),                    -- 精确十进制
    created INT
);
INSERT INTO orders (user_id, doc, amount)
  VALUES (42, '{"sku":"A1","qty":2,"tags":["x","y"]}', CAST('99.90' AS DECIMAL));
SELECT JSON_EXTRACT(doc, '$.sku') AS sku, amount FROM orders WHERE user_id = 42;
```

## Aspire 快速开始

```bash
dotnet add package Docsql.Aspire.Hosting   # AppHost 项目
dotnet add package Docsql.Aspire.Client    # 消费项目
```

```csharp
var docsql = builder.AddDocsql("docsql").WithDataVolume().WithWebConsole();
builder.AddProject<Projects.MyApi>("myapi").WithReference(docsql).WaitFor(docsql);
// 对称集群:builder.AddDocsqlCluster("docsql", nodeCount: 3)
```

`aspire start` 本地拉起(dashboard 可视),`aspire publish` 出部署产物;完整用法见
[Aspire 集成指南](aspire.md),示例工程见
[`dotnet/samples/AspireSample/`](../dotnet/samples/AspireSample/)(引用 GitHub Packages
发布包)。
