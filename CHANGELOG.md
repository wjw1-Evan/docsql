# 更新日志

本文件记录用户可见的功能、修复与行为变更。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 商用交付加固批次(2026-09-11)

#### 新增

- **许可与治理**:双许可文本(LICENSE-MIT / LICENSE-APACHE,对应 Cargo.toml 声明的 `MIT OR Apache-2.0`)、
  SECURITY.md(私密漏洞报告渠道与响应目标)、CONTRIBUTING.md、CHANGELOG.md;
- **可观测性**:服务端运行时计数器(连接总数/活跃/拒绝、语句总数/错误、发布数、认证失败、网络字节)
  随 `REQ_STATUS` 的 `metrics` 对象暴露;Web 控制台 `GET /metrics` 输出 Prometheus 文本
  (逐节点并行抓取,`docsql_*` 指标族 + 控制台自身 `docsql_web_http_requests_total`),
  `GET /healthz` 无门禁存活探针;抓取认证与其他 API 一致(`X-Docsql-Token`);
- **语句超时**:`DOCSQL_STATEMENT_TIMEOUT_MS`(默认 0=不限)为客户端语句设置墙钟预算,
  引擎在嵌套循环 JOIN/SELECT WHERE/UPDATE·DELETE 行循环协作式采样(每 1024 行)超时报错回滚;
  复制 apply 与恢复重放**不受限**(慢节点不得偏离主节点已确认的写入);
- **语句级参数绑定(服务端)**:预留帧 REQ_PREPARE/REQ_EXECUTE/REQ_CLOSE_STMT 落地为真实服务端
  prepared statements——`?` 占位符在服务端按位置绑定类型化字面量(引号感知:字符串字面量内的
  `?` 是数据;字符串值单引号翻倍转义,注入载荷无法逃逸字面量),授权/超时/审计与 REQ_SQL 全同路径;
- **备份完整性**:每份备份写入 sha256sum 格式校验和 sidecar(`backup-*.sql.sha256`),
  恢复前强校验(损坏/被篡改的转储在触碰集群前被拒),缺失 sidecar 容忍旧备份,
  保留策略随主文件一并清理,备份列表带 `checksum` 在位标记;
- **JSON 函数族**:`JSON_EXTRACT(doc, path)`(`$.`/`.成员`/`[索引]` 点读文法,对象/数组保持结构)、
  `JSON_TYPE(doc[, path])`(SQLite 风格类型名)、`JSON_VALID(text)`;坏文本/缺路径返回 NULL 不中断扫描;
- **多列(复合)索引**:`CREATE [UNIQUE] INDEX … ON t (a, b)`。复合键为按列序的
  `Value::Array`(排序走 `cmp_values` 逐元素字典序,零新增编码);复合 UNIQUE 按**完整键
  组合**判重(树级强制,任一列 NULL 跳过整键);`WHERE a = 1 AND b = 2` 全列等值走精确
  探测,前导列等值走前缀探测(非前导列条件回退全表扫描,正确性不变);catalog 每定义
  记录全列清单(向后兼容旧卷三元格式,无需 MAGIC 升版);`schema_hash`/摘要纳入全列,
  跨节点收敛一致;dump/sqlite_master/`/api/meta` 带全列;设计说明见
  `docs/design/001-composite-indexes.md`;
- **文档 >4KB 溢出页链**:旧单页 4KB 文档上限解除——超页文档主页槽存溢出头+内联
  前缀(`0xFF` 标记),其余沿 `0xFE` 溢出链页存储(encode/B+ 树/复制位点零改动);
  硬上限 16MiB(对齐主流文档库默认;超限显式报错);删除/替换回收链页进表内
  free 清单(catalog 持久化)并优先复用;旧卷字节级兼容(旧槽首字节必非 `0xFF`);
  设计说明见 `docs/design/002-overflow-page-chains.md`;
- **CLI**:`-f/--file <script.sql>` 脚本批执行(快速失败,容忍缺失末尾分号)、
  `--csv`(RFC 4180)/`--json` 行导出、`help;` 内联命令(嵌入式与远程模式通用);
- **优雅停机**:server/web 处理 SIGTERM/SIGINT——停止接受新连接,存量连接限时(10s)排空,
  引擎断连回滚 + WAL 恢复兜底;axum 挂接 graceful shutdown;
- **Web 控制台原生 TLS**:`DOCSQL_WEB_TLS_CERT`/`DOCSQL_WEB_TLS_KEY`(PEM)以 rustls 服务 HTTPS
  (ring 后端,无 cmake 工具链依赖;只设其一拒绝启动);明文请求落在 TLS 端口得不到 HTTP 应答;
  未配置仍为明文 HTTP(反代场景),`DOCSQL_WEB_COOKIE_SECURE=1` 配套;
- **TCP keepalive(60s)+ NODELAY**:长会话不再死于 NAT/防火墙静默超时;
- **配置校验**:数值型环境变量(`DOCSQL_MAX_CONN`/`DOCSQL_IDLE_TIMEOUT`/`DOCSQL_CATCHUP_WINDOW`/
  `DOCSQL_BACKUP_*`/`DOCSQL_SLOW_MS`/`DOCSQL_STATEMENT_TIMEOUT_MS`)非法值拒绝启动(exit 2),
  不再静默回退默认值;
- **NuGet 打包**:Docsql.Client 与 Docsql.EntityFrameworkCore 具备完整包元数据
  (LicenseExpression `MIT OR Apache-2.0`、包 README),`dotnet pack` 可出包;
- **ADO.NET 连接池**:默认开启(`pooling=false` 关闭;`max pool size=N` 池上限,默认 100)。
  Close 归还、Open 借出前 PING 验活(死连接自动丢弃重建,服务器重启后客户端自愈);
  池键含全部凭据(不同身份不共享物理连接);**事务未了结的 Close 物理丢弃连接**
  (断连自动回滚,残留事务不可能泄漏给下一个借出者);`ClearPool()`/`ClearAllPools()`;
- **ADO.NET 参数绑定切换服务端**:带参数命令改走 REQ_PREPARE/REQ_EXECUTE ——
  `@name` 改写 `?` 占位符,模板按物理连接缓存句柄(重复执行零注册开销),
  值在服务端渲染为类型化字面量(引号感知、字符串翻倍转义,注入载荷只能是数据),
  授权/超时/审计同路径;`cmd.Prepare()` 预注册句柄。原有 62 项行为测试在切换下直接通过。

#### 修正

- README 两处失真:EF Core 描述更新为独立原生提供程序(不再"借壳 SQLite 管线");
  安全对照表不再宣称未实现的参数化帧(随本批实现已成立)。

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
