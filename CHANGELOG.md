# 更新日志

本文件记录用户可见的功能、修复与行为变更。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.1.0] - 2026-09-11

首个公开基线版本。

### 新增

- 存储引擎:B+ 树 + heap + WAL(先写日志后落数据页、崩溃恢复、8MB 自动检查点),JSON 文档整体存储,单页 4KB;
- SQL:完整 DDL/DML、INNER/LEFT/RIGHT/FULL/CROSS JOIN、聚合(COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/STRING_AGG)、
  非递归 CTE、派生表、标量/IN/EXISTS 子查询、事务 + SAVEPOINT、UNIQUE/NOT NULL/CHECK/DEFAULT/外键(RESTRICT)、
  ON CONFLICT DO NOTHING|REPLACE、LIKE/ILIKE、GUID/UUIDv7 自动主键、RETURNING;
- 索引:单列 B+ 树索引(点查/范围/判重),PK 与 UNIQUE 随建表自动建树;
- 集群:对称集群(任意节点可写、互扇出)+ 主从写转发 + PROMOTE、新节点自动 join(快照引导)、
  重启反熵修复(增量追赶 + 快照兜底、多数派裁决)、持久化 pub/sub(先落盘后推送、断线按 id 续传);
- 认证与授权:协议 token 三凭据(客户端/只读/节点间)、数据库用户/角色(PBKDF2 存储、表级 GRANT/REVOKE 即时生效、
  匿名关闭、按 IP 登录锁定)、明文密码绝不离开执行节点;
- 备份恢复:整库一致点逻辑快照、定时自动备份 keep-N、整库重放恢复 + 跨节点摘要收敛验证;
- DocSQL Studio Web 控制台:零存储管理面(查询/对象树/表设计/数据网格/仪表盘/集群状态/用户与角色/备份/日志),
  控制台账号门(首次强制 setup、改账号踢其它会话)、节点切换白名单;
- 客户端:.NET ADO.NET 提供程序 + EF Core 10 原生提供程序(EnsureCreated/惰性建表/模型与索引自动同步)、
  Rust CLI(嵌入式/远程、pub/sub 专用连接);
- 传输加密:DOCSQL_KEY AES-256-GCM 帧加密;异步组提交、审计 JSONL、慢查询日志;
- 部署:Docker 镜像(多架构,GHCR)、dev/prod 双 compose(single/cluster/join profile)、等级保护第三级能力对照。
