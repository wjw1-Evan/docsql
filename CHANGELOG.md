# 更新日志

本文件记录用户可见的功能、修复与行为变更。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 等值 JOIN hash join(2026-09-13)

- **等值 JOIN 从嵌套循环改为哈希连接**:ON 中可跨连接边界切分的等值条件
  (`l.x = r.y`,含 USING 合成与左右反转)为右表建立规范键索引、左行探针,
  每对「整行合并 + 表达式求值」的 O(N×M) 成本降为 O(N+M)——3k×3k 等值 JOIN
  从 8.36 秒降至 6 毫秒(约 1400×),复合等值(两列同时相等)约 2700×;
- 键归一化与引擎比较语义一致:`=` 基于 `cmp_values`(NULL = NULL 相等、
  Int/Float 跨类型按数值比较),索引是候选超集、完整 ON 逐候选终裁,
  不改变任何连接语义;无法索引的值(文档列)只降级自身所在行为全探针;
- 非等值谓词、无限定列名(`lookup_col` 后缀解析有歧义)、两侧同表前缀、
  OR/子查询等一律回退原嵌套循环;LEFT/RIGHT/FULL OUTER 补 NULL 与
  行输出顺序与嵌套循环逐行一致;
- 新增 `bench_join` 基准(等值/LEFT/复合等值/非等值四项)与 hash 路径
  边界测试(NULL 相等、混合 residual、Int-Float 键、FULL OUTER 与
  无限定回退);工作区 503 测试 + dotnet 83/24 全绿。

### 存储引擎性能批次(2026-09-13)

#### 优化

- **checkpoint 后台化**:WAL 越过软阈值(8MB)时由后台线程对数据文件做 fsync,写入持续服务、
  不再在写路径上同步整库 `sync_all`;仅当越过硬阈值(64MB,写速快于后台消化时的流控兜底)写路径
  停等一次覆盖全日志的 fsync 并内联截断。WAL 截断仍严格发生在覆盖性 fsync 之后,先写日志顺序不变;
- **WAL CRC-32 查表化**:逐位循环改为 256 项查表(校验值逐字节不变,旧日志兼容),4KB 页帧校验的
  CPU 开销约降至 1/8;WAL 长度改为内存计数,逐条 commit 不再 `fstat`;
- **页 IO 位置化**:pager 页读写全部改 `pread`/`pwrite`(`read_exact_at`/`write_all_at`),
  每页操作省一次 `lseek`。

#### 修复

- 文件页数增长此前仅靠 WAL 回放在重启时恢复页计数,页头不持久化;WAL 被截断后重启会丢失
  页计数(页在盘上却越界不可读)。现在页头随页数增长即时持久化,WAL 截断与重启解耦。

### MVCC 阶段 B 第一步:pager 层快照读机制(2026-09-13)

- pager 全方法 `&self` 化(WAL、延迟回写队列、页计数、页 LSN 均改内部锁;写串行仍由
  引擎写锁保证),为「读不阻塞写」的共享读路径铺路;
- 新增快照读原语:`begin_snapshot` / `read_page_as_of` / `end_snapshot`。快照 =
  WAL epoch + commit-head LSN(checkpoint 会把 LSN 归零,故引入单调 epoch);快速路径
  (页的最新提交版本早于快照)直接走当前读面,慢速路径一次 WAL 扫描物化全部页的 as-of
  镜像并缓存在快照上;
- 截断语义:软阈值截断在有活跃快照时让位(快照的 as-of 页历史只存在于 WAL);硬阈值
  流控优先、照常截断,失去历史的快照在其后的读取中响亮报「snapshot too old」,绝不
  静默回退到更新的页版本。提交路径在 WAL 锁内完成「append+commit+读面应用」,快照
  取 head 与页面可见性保证原子。引擎与服务器行为不变,本批为后续「SELECT 锁外执行」
  的机制层落地。

### MVCC 阶段 B 第二步:读不阻塞写(2026-09-13)

- **只读 SELECT 不再阻塞写入**:server 读级改为「微秒级短锁建视图、语句全程锁外执行」——
  拿锁仅用于克隆 catalog(`Arc` 引用计数)并取 pager 快照,长查询/大报表不再卡住同节点的
  写入与复制应用;视图语义为可重复读(语句期内看不到并发提交,新语句立即可见);
- SELECT 执行链整体迁移到 `ReadCx` 上下文(pager 引用 + catalog 快照 + 语句超时 + 可选快照),
  页读取统一走 `PageReader`(当前版本 / 快照 as-of 双模);索引探针、溢出链、JOIN、聚合、
  子查询全部支持快照读;
- 快照过旧的兜底:读语句存活期间若写入流量把 WAL 推到硬阈值(64MB)触发截断,该读的后续
  as-of 页访问响亮报「snapshot too old」(可重试),绝不静默返回更新的数据;软阈值(8MB)
  截断会等待活跃快照结束;
- 并发目录安全:catalog 改为 `Arc` 共享,写入端仅在快照读者持有旧版本时才深拷贝单表元数据
  (`Arc::make_mut`),独占写入时零拷贝。

### Aspire 集成与 NuGet 首发(2026-09-12)

#### 新增

- **Aspire 扩展三件套**(`Docsql.Aspire.Hosting` / `Docsql.Aspire.Client` / EF 容器级注册):
  AppHost 中 `AddDocsql` 以官方镜像编排 DocSQL 节点(随机 token、命名数据卷、TCP 健康检查、
  连接串注入),`AddDocsqlCluster` 一次拉起对称集群(DOCSQL_PEERS/CLUSTER_TOKEN/数据卷),
  `WithWebConsole` 附带 docsql-web 控制台伴生容器;消费侧 `AddDocsqlConnection` 注册连接与
  健康检查;EF 新增 `AddDocsqlDbContext<T>(connectionName)` 按名取注入连接串;
- **NuGet 发布流水线**:`nuget-publish.yml` 随 `v*` tag 将 Docsql.Client / Docsql.EntityFrameworkCore /
  Docsql.Aspire.Client / Docsql.Aspire.Hosting 推送 GitHub Packages;
- **示例**:`dotnet/samples/AspireSample/`(AppHost + Worker 参数化写读闭环,集群形态注释可切换)。

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
- **Oracle 兼容增强**:`DUAL` 哑表(大小写不敏感)、`ROWNUM` 伪列(取行后、WHERE 与
  ORDER BY 之前编号,被过滤行消耗编号,`SELECT *` 不含该列)、`FETCH FIRST n ROWS ONLY`
  (SQL 标准/Oracle 12c 分页;`WITH TIES`/`PERCENT` 显式不支持)、Oracle 风格函数族
  (`NVL`/`NVL2`/`DECODE`(NULL=NULL 匹配)/`INSTR`/`LPAD`/`RPAD`/`GREATEST`/`LEAST`/
  `TO_NUMBER`/`TO_CHAR`(单参)/`SYSDATE()`)、Oracle 数据字典兼容视图
  (`ALL_TABLES`/`USER_TABLES`/`ALL_TAB_COLUMNS`/`USER_TAB_COLUMNS`/`ALL_INDEXES`/
  `USER_INDEXES`,单全局命名空间下 USER 与 ALL 同数据,系统表不出现);
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
