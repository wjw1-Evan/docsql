# AGENTS.md

面向 AI 编码代理的开发指南。用户可见功能与部署手册见 [README.md](README.md);开发命令、机制约束与红线以本文为准,执行细节以脚本/CI 为准。

## 项目概览

DocSQL:Rust 原生文档数据库 + .NET 客户端栈。JSON 文档整体存储、完整 SQL(DDL/DML/JOIN/聚合/事务/约束/系统视图)、DocSQL Studio Web 控制台(纯管理工具、自身零存储)、ADO.NET + EF Core 提供程序、对称集群复制(任意节点可写)+ 主从写转发 + PROMOTE、持久化 pub/sub(先落盘再推送)。**运行仅 Docker**:镜像由 CI 发布到 GHCR,cargo 只用于开发测试。

| 模块 | 职责 |
|---|---|
| `crates/docsql-core` | 存储引擎(pager/WAL/B+树/heap)+ SQL 内核 + 协议帧 + JSON,无网络依赖 |
| `crates/docsql-server` | TCP 服务器、鉴权、复制、pub/sub、查询/同步日志、备份恢复 |
| `crates/docsql-cli` | 嵌入式/远程 SQL shell |
| `crates/docsql-web` | REST API + 内嵌单页控制台(`console.html` 经 `include_str!`) |
| `dotnet/` | Docsql.Client(ADO.NET)、Docsql.EntityFrameworkCore、Aspire 三件套(Hosting/Client/EF 容器级注册)、两套 xUnit、Sample/EfSample、samples/AspireSample(AppHost 示例) |
| `deploy/` | `docker-compose.yml`(dev,源码构建)/ `docker-compose.prod.yml`(prod,GHCR);均含 `single`/`cluster`/`join` profile;`run-tests.sh` 部署测试入口 |

关键文件:`core/engine.rs`(SQL 执行器/约束/事务/写单元,最大文件)、`core/btree.rs`、`core/encode.rs`+`core/value.rs`、`core/guid.rs`、`core/useradmin.rs`+`core/kdf.rs`(数据库用户/角色 + PBKDF2)、`core/meta.rs`、`core/stmt.rs`、`core/proto.rs`、`server/lib.rs`(连接循环/全部 REQ_* 帧/join-repair,模块头注释权威)、`server/pubsub.rs`(锁顺序权威)、`server/querylog.rs`、`server/backup.rs`、`web/lib.rs`、`web/auth.rs`、`web/console.html`、`dotnet/Docsql.Client/AdoNet.cs`、`DocsqlSubscriber.cs`。基准:`crates/docsql-core/examples/bench*.rs`。

## 常用命令

```bash
# 提交门禁(全部通过才能提交)
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                      # 473 用例

# 改 dotnet 或协议时(cargo build 先行:测试进程会启动 target/debug/docsql-server)
cargo build -p docsql-server
cd dotnet && dotnet test                    # Client 62 + EFCore 22

# 改复制/部署逻辑后必跑;默认先构建 :local 镜像(构建内含 cargo test 门禁)
./deploy/run-tests.sh                       # single 34 + cluster 81
DOCSQL_DEV_IMAGE_TAG=<tag> ./deploy/run-tests.sh   # 跳过构建,复用已有镜像

# 运行仅 Docker(dev 端口 1760x/1770x,prod 1860x/1870x;项目名/数据卷分离,两套可同时跑)
cd deploy && docker compose --profile cluster up -d --build                     # 本地开发集群
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d   # 生产单节点
```

- compose 必须带 profile(不带 = 空操作);数据卷 external,`down -v` 不清数据;清数据唯一入口 `./deploy/reset-data.sh`;首次部署需先建卷(见 README;run-tests.sh 自动重建 dev 卷)。
- 测试布局:Rust 单测在各模块内;e2e 在 `crates/docsql-server/tests/e2e.rs`(pub/sub、join/repair、备份恢复、故障转移)与 `crates/docsql-web/tests/e2e.rs`(真实 HTTP);.NET 为两个 xUnit 套件;部署测试 `deploy/single-test.sh` / `deploy/multinode-test.sh`。
- CI `.github/workflows/docker-image.yml`(Rust 1.98.1 / .NET 10)执行同样门禁 + dotnet 测试 → 多架构镜像 → main 分支部署测试;CI 失败等同门禁失败。
- NuGet 发布:`.github/workflows/nuget-publish.yml` 在 push `v*` tag(或手动)时 pack 四个 .NET 包并推 GitHub Packages(`dotnet/Docsql.sln` 含 Aspire 包与示例;Aspire 测试不起容器,CI 直接可跑)。

### Mimosa 安全门禁(本机 commit/push 钩子)

ZCode 的 Mimosa 插件对 commit/push 做 L3 静态扫描,**native 引擎对任何进程启动无差别报 high「命令注入」**(全字面量也报;`mimosa-ignore` 与 validate Oracle 均无效),Edit/Write 增量扫描会直接拦写(high=拦,不受 warn 档影响)。本机 `MIMOSA_GIT_GATE_MODE=warn`(launchctl 注入 + `~/Library/LaunchAgents/com.user.mimosa-gate-mode.plist` 持久化;`launchctl getenv MIMOSA_GIT_GATE_MODE` 验证,改后重启 ZCode)。约定:

- 生产代码(核心/服务器/web/cli)不启动子进程;今后必须参数列表传参、禁拼 shell、不得有用户可控输入流入。
- 测试启动 `docsql-server` 只复用既有辅助方法:C# `FindServer`/`StartServer` 形态(`ProcessStartInfo` + `UseShellExecute=false` + `ArgumentList`);Rust 进程内 `tokio::spawn`,`e2e.rs` 的 `spawn_node`/`spawn_node_handle` 是模板。Rust 重启节点用 `JoinHandle::abort()` + 等端口关闭 + 同数据文件重启,**不要起子进程**(增量扫描会拦);不要为绕过扫描改 API 形状(反射/P-Invoke)。
- 已知 12 条误报(EfTests ×4、TransportEncryptionTests ×2、AdoNet/AsyncCommit/Failover/QueryLog/SymmetricCluster/EfSample 各 1),改动这些文件时的告警属预期。

## 关键机制与红线(改代码前必读)

1. **WAL/pager 顺序** — 一切落盘经 pager;先写 WAL 再应用页面;事务内的页面回写延迟到 WAL fsync 之后(防数据页领先日志)。动 pager 提交路径保持该顺序。checkpoint 已后台化(pager.rs:软阈值 8MB 请求后台线程 fsync 数据文件,硬阈值 64MB 写路径停等一次覆盖全日志的 fsync 后内联截断;ckpt 线程只 fsync 自有 dup 句柄,不碰 WAL/池/pending_writes),**WAL 截断仍必须发生在覆盖全日志的数据文件 fsync 之后**;页数增长经 `persisted_pages` 记账即时持久化进页头,否则 WAL 截断后重启丢页数(2026-09-13 修复的既有缺陷)。**pager 全方法 `&self`**(WAL/pending/页数/页 LSN 均内部锁;写串行仍由引擎写锁保证),供 MVCC 阶段 B 快照读共享:`begin_snapshot`/`read_page_as_of`/`end_snapshot` + WAL epoch(checkpoint 归零 LSN,快照按 epoch+LSN 定位);**软截断在有活跃快照时让位**(快照的 as-of 页历史只在 WAL),硬阈值流控优先、照常截断,被截断的快照在其后响亮报 `SnapshotTooOld`,绝不静默回退新版本页;提交路径在 WAL 锁内完成「append+commit+读面应用」(池/pending/页 LSN),快照 begin 读 commit head 与页面可见性由此原子。**阶段 B 读级 = 无锁视图**:server 读级(`execute_read_sql`)只在微秒级读锁内 `Database::read_view()`(克隆 catalog Arc + 取快照),SELECT 全程锁外跑,读不阻塞写;SELECT 执行链整体在 `ReadCx`(pager 引用 + catalog 快照 + deadline + `Option<Snapshot>`)上,页读取统一走 `PageReader`(Current/as-of 双模,heap/btree 读函数收 `&PageReader`,写函数收 `&Pager`);catalog = `BTreeMap<String, Arc<TableMeta>>`,写路径改目录一律经 `Database::catalog_mut`(内部 `Arc::make_mut`,仅在 detached 读者持有旧版本时深拷贝)或整条 `Arc::new` 替换——**别退回对 Arc 的直接可变借用**;写路径访问共享读助手走 `*_cx` shim(`table_docs_cx`/`index_probe_cx`/`matches_cx`/`load_from_cx`/`exec_query_cx`/`subst_expr_cx`)。
2. **编码 ≠ 排序** — `core/encode.rs` 只保证往返一致;排序统一走 `Value::cmp_values`(B+树/ORDER BY/DISTINCT)。新增值类型必须同时扩展编码与 `cmp_values`,否则索引序被破坏。DISTINCT/UNION 按编码字节判重:Int(3) 与 Float(3.0) 比较相等但不会互相去重。**复合索引的键 = `Value::Array` 按列序**(cmp_values 逐元素字典序,设计见 `docs/design/001-composite-indexes.md`):index_roots 键已泛化为 root_key(单列=列名,复合=索引名,旧卷零迁移),一切索引键构造/唯一判定必须走 `TableMeta::index_columns_of` + `index_key_of` + `root_key_unique`,勿回退按列名直取;复合 UNIQUE 的树级判重不进 meta.unique;OR REPLACE/IGNORE 的位移机制只认约束列,复合唯一冲突直接报错(明示语义)。
3. **自动索引是派生展示** — PK/表声明 UNIQUE 的 B+ 树随建表创建(`index_roots`);`catalog()` 按 `primary_key`+`constraint_unique` 派生 `sqlite_autoindex_<表>_<n>`(不落 catalog,旧卷零迁移、各节点重算一致);`DROP INDEX`/`CREATE INDEX` 对该前缀显式报错;`sqlite_master` 不列自动索引(EF SchemaSync 只看 sqlite_master 且只回收 `IX_` 前缀)。
4. **B+ 树边界** — 分裂按序列化字节驱动(超页即分裂);单键编码超半页(~2KB)报 KeyTooLarge;等键 run 可横跨分裂点,查找/删除靠 `candidate_children` 对名义区间覆盖该键的子树兜底——不要假设等键同叶。
5. **UPDATE/DELETE 快速路径 = 两阶段索引维护** — 先删所有被更新行的旧索引项,再逐行写新像;in-page repack 会移动 locator,后续 `updates` 的 locator 必须跟随 `moved`。多行唯一键位移(`SET id = id + 1`、键互换)靠此顺序才合法;改回逐行「先删后插」会误报 UNIQUE。
6. **单全局事务有所有者** — 引擎单写者;并发 BEGIN 在服务端排队(`BEGIN_QUEUE_WAIT` 30s 上限,dotnet/EF 并发 SaveChanges 依赖该排队——别改成直接报错或吞错)。owner 的写缓冲在 `tx_pending`,COMMIT 时整批 journal 追加进**一个写单元**再逐条扇出;非 owner 的写与复制写排队等事务关闭(直接并入会被 owner 的 ROLLBACK 静默丢弃);COMMIT/ROLLBACK/SAVEPOINT 来自非 owner 直接报错;owner 断连自动 ROLLBACK。
7. **`write_order` 串行化写执行与扇出** — 保证对等节点按本节点执行顺序应用。写单元单一 fsync 失败则整条语句不确认;`DOCSQL_ASYNC_COMMIT=1` 时单元照常分组记账但不强制 fsync(PUBLISH 仍推送前强制 fsync,先落盘后推送契约不变)。
8. **非确定性生成值由写入节点定值下发** — auto-GUID 主键(PK 列 GUID/UUID/UNIQUEIDENTIFIER/UUIDV7 + AUTOINCREMENT)INSERT 省略/NULL 时引擎填 UUIDv7,并把语句回写成显式值(`Database::take_resolved_sql`,用户管理语句的密码哈希回写同槽),扇出与 `tx_pending` 一律用回写文本;auto-GUID 表 `INSERT ... SELECT` 显式报错。今后任何随机/时间类默认值同理(定值回写或确定性可重算),否则对端各自生成、静默分叉。UUIDv7 节点内严格单调、时钟回退不回退。
9. **pub/sub 顺序契约** — PUBLISH 必须先经引擎写路径提交(WAL)再 notify 本地订阅者、再扇出 peers(对端只落盘+推本地,不再转发);SUBSCRIBE 在注册表锁内完成 注册→快照水位→回放→`arm_filter`,`skip_through` 去重保证回放/实时不丢不重——动锁顺序前先读 `pubsub.rs` 模块注释。id 节点内单调(TRIM 强制 keep≥1,否则重启续推失锚),at-least-once,慢连接丢实时帧可凭 id 重订阅补回。**订阅必须专用连接 + 专职读线程**(CLI/DocsqlSubscriber 已是);普通一问一答连接会把 RESP_PUSH 当命令响应错位。
10. **系统表只读** — `_pubsub_messages`/`_cluster_log`/`_cluster_pos`/`_cluster_id` 允许 SELECT,写/DDL 按 AST 写目标表名分类拒绝(`stmt_write_targets`;别再退回子串扫描——会误拒字面量里含表名的用户写);不进摘要/快照/对象树用户区,单列只读「系统表」分支;`docsql_pubsub` 视图走正常 SQL 路径。
10a. **用户/角色存储表 = 内部但参与复制** — `docsql_users`/`docsql_roles`/`docsql_role_members`/`docsql_grants` 是保留名普通表:`is_system_table`(复制排除:摘要/快照/journal 兜底)与 `is_internal_table`(显示过滤 + 写门禁 + fresh 判定)**语义不同,别混用**——用户表必须进摘要/快照/journal/备份(集群对用户集合收敛),但不进对象树/totals,普通 DML 对其一律拒绝(含复制帧),唯一入口是用户管理语句族(CREATE/ALTER/DROP USER、CREATE/DROP ROLE、GRANT/REVOKE,`core/useradmin.rs` 手写解析,sqlparser 不认该文法)。**明文密码绝不离开执行节点**:引擎把 `PASSWORD '明文'` 回写成 `$pbkdf2-sha256$…` 的 resolved 形式(`take_resolved_sql`,与 auto-GUID 同机制),journal/扇出/tx_pending/dump/查询日志全走回写文本;查询日志再经 `redact_sql` 把明文打码。读路径(登录验证/授权解析/dump)容忍表缺失(表仅在首条用户管理写时惰性创建,保证无用户节点 catalog 干净、与旧节点摘要一致)。鉴权:REQ_AUTH_USER(JSON user/password,PBKDF2 校验在 spawn_blocking;错误统一 "bad username or password",未知用户也烧一次派生防时序枚举;失败按 IP 锁定同 token 语义)。授权执行点在 server `execute_sql`(`authorize_statement`):admin 全过;readonly=SELECT(系统表/兼容视图除外);readwrite=DML+PUBLISH/TRIM;DDL/用户管理=admin;自定义角色=表级 DML 位;SELECT 读目标 `stmt_read_targets` fail-closed(无法分类即拒)。REVOKE 经 grants_epoch 纪元即时生效——**epoch 刷新在帧分发前逐帧检查**(连接循环里、匿名门之前),勿挪回 REQ_SQL 分支内:否则非 SQL 帧(PUBLISH/TRIM/REQ_BACKUP/PROMOTE)永久沿用旧授权,被 DROP 的用户还会以无身份 legacy 会话执行一条满权限语句。`CREATE USER` 对已存在用户报错(绝不静默重置密码;dump/快照回放前先 DROP 用户表,复制/恢复不受影响);`REVOKE … ON 表` 的行匹配用大写 `name()` 原形(存储侧即该形式;一侧 lowercase 曾使 REVOKE 静默无效)。存在任一用户后匿名连接关闭(判定在连接建立时:建号会话不被自己锁死,新匿名连接拒绝)。join/repair 快照以规范化用户语句随 dump 传播,apply 后补 epoch/has_users 记账。CLI:`connect <addr> --user <name>`(密码走 DOCSQL_PASSWORD 或交互提示,不走 argv);dotnet 连接串 `user=...;password=...`(与 token 二选一,优先用户)。Web 控制台「视图 → 用户与角色」页经 `/api/users`(GET 聚合四表 + POST 动作派发):语句全部来自固定模板,标识符按引擎同一字符集白名单校验、密码单引号转义、表名双引号转义 —— **别往该端点加任意 SQL 拼接**;无用户节点合成内置角色行展示(授予随首用户创建落地)。
11. **协议要点** — v1 二进制(`core/proto.rs`),wire 一句一帧(`core/stmt.rs`);**REQ_SQL 执行全文不截断**(仅查询日志显示截断;曾有过 512 字符静默截断的回归)。复制帧:REQ_SQL_SEQ(seq+node_id 的带序号写)、REQ_CATCHUP(按位点增量拉取)、REQ_DIGEST(表摘要)、REQ_SYNC/HOLD/RELEASE(join/repair 快照通道)、REQ_STATUS(cluster_id/journal_head/journal_oldest/replay_failures/backup/**metrics**——metrics 对象 wire 可见,只许加字段)、REQ_BACKUP(list/trigger/restore)、REQ_META、REQ_LOGS;服务端 prepared statements:REQ_PREPARE/REQ_EXECUTE/REQ_CLOSE_STMT(`bind_params` 引号感知绑定,字符串值翻倍转义,**改绑定渲染必须保持注入面关闭**);其余见 `server/lib.rs` 模块头。**语句超时的作用域红线**:deadline 只从客户端 REQ_SQL/REQ_EXECUTE 臂传入——复制 apply(SEQ/catch-up/drain)与恢复重放一律不带,否则慢节点对不上主节点已确认的写即集群分叉;引擎是同步单写者,语句只能靠行循环内的协作式采样(`StmtDeadline`,每 1024 行)打断。复制两形态:对称集群(`DOCSQL_PEERS` 互扇出,任意可写)/ 主从写转发(`DOCSQL_REPLICATE_TO` 指主,副本只读),PROMOTE 提升副本。
12. **join(新节点自动同步)** — 全新节点(无用户表)配 `DOCSQL_PEERS` 启动即 bootstrap:持自身 write_order 向各 peer REQ_HOLD(任一失败整体中止;对端 5s 内排空在途写并冻结、60s 看门狗防泄漏;发起前先发 id=0 的 REQ_RELEASE 清扫上次残留 hold)→ 全网静止 `dump_script()` 取快照(DDL 在前、跳过系统表)→ REQ_RELEASE 并行解冻 → ≤4MB 块流式回传,joiner 在单事务内整体重放(失败回滚保持全新)。**加入期间复制写入队即确认(`sync_queue`)**——扇出在门上等待会与源节点 write_order 互锁(实测活锁);队列携带 origin+seq,快照已覆盖的带序号入队写 drain 时按位点跳过;每个终态都必须 drain。**drain 在不持锁时自取 write_order 并整场持有**,不能只信 `order_held`(已确认写会被并进客户端事务、被其 ROLLBACK 丢弃);watchdog 防任务死亡后门泄漏(门开着=无限确认永不落地的写);drain 重放失败计数进 `replay_failures`(确认过的写丢失必须可观察)。Applied 后摘要复验,不一致转 repair;本地已有客户端写则放弃 bootstrap 保留本地数据;空集群不互相 sync;动态注册只在内存(原节点重启丢失,长期成员写进各节点 `DOCSQL_PEERS`)。
13. **repair(重启反熵,两阶段)** — 持数据节点启动即探各 peer 摘要与期刊窗口,一致则不修、直接 drain 关 sync 门。阶段一增量:节点把本地写按序记入 `_cluster_log`(记账与数据同写单元同 fsync;无扇出目标的节点跳过记期刊),扇出 REQ_SQL_SEQ;重入方按 `_cluster_pos` 位点 REQ_CATCHUP 拉缺口后复验摘要。阶段二快照兜底:增量不可行(位点缺失/超出 `DOCSQL_CATCHUP_WINDOW`)或复验仍分歧时,按选举参考方快照整体重建(`wipe_user_tables()` + 单事务回放)。**安全规则**:位点只滞后不超前(一律 `advance_position` 单调推进,迟到的低 seq 不得压低);REQ_CATCHUP 先采样 head、流不越过它,服务端对 (after,head] 连续性审计(断号/中途终止回错误,防请求方采 head 静默跳段),请求方收错误走快照兜底;快照采纳时旧位点清理与快照同一事务、以探测头播种下限、drain 后重探各原点 head 抬到位(否则下次重入必退化快照);采纳事务内 `journal_void_all()` 作废本机期刊文本(防被裁决丢弃的独有写日后被拉回「复活」)。参考方选举全网格确定性:按摘要分组、最大组胜出(组内互见一致不修);无多数时行数多者优先、并列按摘要序列化序;与启动顺序无关;**空组永不采纳**(别因 peer 全空清掉本地唯一副本);所有 peer 不可达则重试数轮后按本地服务。代价:少数方独有写被覆盖(无行级合并);修复只由重启触发。
14. **备份/恢复** — 备份 = `dump_script()` 逻辑快照(整库一致点、不含系统表),落 `<db>/backups/backup-<UTCms>.sql`(.tmp+rename 原子;**同名 `.sha256` 校验和 sidecar 随写,restore 前强校验,缺失容忍旧备份**;保留 KEEP 份只删本命名模式并连带 sidecar;首拍仅在无备份或最新已超一个间隔时触发)。**锁纪律:backup 状态锁从不与 write_order/engine 嵌套**;dump 持 `lock_engine_for_write` 取,文件 IO 在全部锁释放后做。恢复 = 逐条走正常写路径重放(autocommit;整场持 write_order,客户端写不会插入恢复流),完成后 catchup 自愈 + 逐一比对可达 peer 摘要,状态暴露 `converged`/`note`;跨节点互斥(探测 peers 的 REQ_STATUS);启动 sync 门未关时 tick/trigger/restore 跳过/拒绝;trigger/restore 拒只读连接;仅接受 `backup-*.sql` 裸文件名;同名表覆盖,备份后新建的表保留。
15. **鉴权与身份** — token 常数时间比较;二进制启动校验 token(≥8、非单字符重复,否则 exit 2)。`DOCSQL_CLUSTER_TOKEN` 命中 → peer 身份,只有 peer 连接可发 `FLAG_REPLICATION` 帧且不能跑普通客户端语句,扇出优先用它认证。**未配置 cluster token = 完全向后兼容**(任意已认证连接可发复制帧)——改连接循环门禁时保持。`DOCSQL_READ_TOKEN` → 只读角色(SQL 写/PUBLISH/TRIM/PROMOTE 协议拒绝);与 client token 同值按 client 处理(匹配顺序 cluster→client→read)。认证失败按来源 IP 锁定(10 次/60s → 锁 60s),事件写同步日志。
16. **Web 控制台** — 自身零存储:启动参数/`DOCSQL_UPSTREAM`/首个 peer = 默认管理节点;`node` 参数只在 `DOCSQL_PEERS` 白名单内切换(SSRF 防护);`/api/parse` 恒本地静态检查;数据端点 in-band error(HTTP 200 + `{"error":...}`,传输层失败才 5xx,`api._do` 归一化);`console.html` 经 `include_str!`,改完必须重新 `cargo build`(浏览器/Playwright 验证)。账号门 `web/auth.rs`:自带 SHA-256/HMAC/PBKDF2(有已知答案测试,**勿引加密 crate 除非删除本实现**);`DOCSQL_WEB_AUTH_FILE` 空/未设 = 关闭,已设 = 首次强制 setup,**setup 前数据端点一律 401**,`DOCSQL_TOKEN` 可 API 旁路。改账号 `POST /api/auth/change`:会话(或 token 旁路)+ 当前密码双门(会话防路过的人乱试,当前密码防被劫持的页面静默换凭据),当前密码错误计入同一按 IP 登录锁定;成功后 `Sessions::keep_only` 只留发起会话(改密码必须踢掉其它在线者;token 旁路调用无会话 = 全踢),新密码留空 = 只改用户名;写文件成功后才动内存副本(写失败旧凭据仍权威)。
17. **EF Core** — 独立原生提供程序(基于 Docsql ADO.NET,不依赖 SQLite),不重写关系生成;EnsureCreated/惰性建表与索引同步默认开启(模型增删表、索引自动同步,无需 Migrations);`Database.Migrate()` 显式报错;EF 事务内 SaveChanges 依赖 SAVEPOINT 有限语义——相关改动必须跑两个 dotnet 套件。
18. **不支持的 SQL 显式报错** — 窗口函数(OVER)/DISTINCT ON/ON CONFLICT DO UPDATE/ON DUPLICATE KEY UPDATE/自定义 TRIM 字符集/FK 的 ON DELETE|UPDATE 动作/相关子查询/无 GROUP BY 的 HAVING;`PRAGMA` 是有意接受并忽略的兼容垫片。已支持 WITH(非递归)/CTAS/ON CONFLICT DO NOTHING|REPLACE/`SELECT *, expr`。新增不支持语法时在解析/执行层报错,不要静默吞掉。
19. **PK ≠ NOT NULL** — 主键当前不隐含 NOT NULL(与主流不同);动约束逻辑需全量回归约束测试。
20. **deploy 钉子与本机环境** — `deploy/multinode-test.sh` 断言固定 REST/协议字段,改协议/REST 先同步该脚本与 dotnet 客户端;分区重连必须 `docker network connect --alias node-c <net> docsql-c`(裸 connect 丢别名,DNS/healthcheck/扇出静默失效,restart 不恢复),节点侧查询走 127.0.0.1;`DOCSQL_DEV_IMAGE_TAG`(dev)与 `DOCSQL_IMAGE_TAG`(prod)刻意分开,`.env` 的 prod tag 不会泄漏到 dev;本机到 github.com:443 间歇阻断,推送失败用 `git -c http.version=HTTP/1.1 push` 重试;Docker 构建基于 mcr.microsoft.com/azurelinux(docker.io 不可达);dotnet 的 bin/obj 不入库,新项目沿用;镜像运行层必须预建全部 compose 挂载点并 `chown 1000:1000`(/data、/auth——全新命名卷的属主继承自镜像内同名目录,镜像缺该目录则挂载点归 root,uid 1000 进程写入即 os error 13,console 凭据写入曾栽在此)。

21. **堆溢出链(>4KB 文档)** — 超页文档主页槽存 `[0xFF][total:u32][chain_head:u32][内联前缀]`,其余沿 `0xFE` 链页(`next:u32`+`len:u16`+载荷);`0xFE/0xFF` 是 encode 永不产生的字节,读取端按槽首字节判别,**旧卷字节级兼容**;链页回收进 `TableMeta.overflow_free`(catalog 持久化)优先复用;硬上限 `MAX_DOC_SIZE`=16MiB。改 heap 相关代码:一切读槽走 `slot_document_bytes`(防环/截断校验),新存储布局禁止使用 encode tag 0..=7 的首字节。设计见 `docs/design/002-overflow-page-chains.md`。

## 工作流约定

- **修改完成后自动提交并推送源码,无需用户再下指令**:过完提交门禁与相关专项测试即 `git add` 本次改动 → `commit` → `push`(push 即触发 CI);只暂存本次改动涉及的文件,不要把工作区里其它在途修改一并提交。
- 小步提交直接在 `main`;提交信息风格见 git log(如 `M16: ...`),里程碑式概括。
- 完整流程:过提交门禁 → 相关专项测试 → **最后一步提交并推送源码**(push 即触发 CI:门禁 + dotnet 测试 → 多架构镜像发布 GHCR → main 分支部署测试)。
- 改动跨复制:本地 e2e 之外必须跑 `./deploy/run-tests.sh`;改动 EF/事务:跑两个 dotnet 套件。
- 文档分工:用户可见行为 → README;开发命令、机制约束与红线 → 本文。

## 运行时环境变量(开发/测试相关)

> 用途与安全含义的完整列表见 README;以下是代理改动代码/测试时最常碰到的。

- `DOCSQL_TOKEN` / `DOCSQL_READ_TOKEN` / `DOCSQL_CLUSTER_TOKEN`:客户端 / 只读 / 节点间凭据。匹配顺序 cluster→client→read;read 与 client 同值按 client;集群各节点 cluster token 必须同值且最好不同于 client(相同启动告警);未配置 cluster token = 旧行为。
- `DOCSQL_KEY`:AES-256-GCM 帧加密(含 token 与数据);绑定非回环且未配置时启动告警(传输加密测试用它)。
- `DOCSQL_PEERS`:对称集群节点表;全新节点自动 join;web 用它做集群探测 + 节点切换白名单;指向自身的条目启动时忽略。
- `DOCSQL_ADVERTISE`:join 时通告自身地址;不设置仍同步数据但不注册(注册只在内存)。
- `DOCSQL_UPSTREAM`:web 默认管理节点(启动参数 > 此变量 > `DOCSQL_PEERS` 首条);未配置时数据端点报「未配置管理目标节点」。
- `DOCSQL_WEB_AUTH_FILE`:控制台账号门路径;未设或空 = 关闭(**run-tests.sh 显式置空依赖此语义**);已设 = 首次强制 setup。配套 `DOCSQL_WEB_COOKIE_SECURE=1`(HTTPS 反代)、`DOCSQL_WEB_TRUST_PROXY=1`(按 X-Forwarded-For 锁定)。
- `DOCSQL_ASYNC_COMMIT=1`:组提交(断电丢最后 ~2ms);PUBLISH 推送前仍强制 fsync。
- `DOCSQL_CATCHUP_WINDOW`:期刊保留条数(默认 10 万;0=不限);决定离线多久还能增量补。
- `DOCSQL_BACKUP_INTERVAL_SECS` / `DOCSQL_BACKUP_KEEP` / `DOCSQL_BACKUP_DIR`:自动备份(默认 86400s / 7 份 / `<db>/backups`)。
- `DOCSQL_MAX_CONN` / `DOCSQL_IDLE_TIMEOUT`:连接上限(超限立即拒绝) / 空闲断开(**订阅客户端需定期 PING 保活**)。
- `DOCSQL_REPLICATE_TO` / `DOCSQL_READ_ONLY=1`:主从写转发 / 副本只读。
- `DOCSQL_LOG_FILE`(审计 JSONL 落盘)、`DOCSQL_SLOW_MS`(慢查询阈值,默认 100ms,写 stderr):测试要用时显式指定临时路径。数值型 env(`DOCSQL_MAX_CONN`/`DOCSQL_IDLE_TIMEOUT`/`DOCSQL_CATCHUP_WINDOW`/`DOCSQL_BACKUP_*`/`DOCSQL_SLOW_MS`/`DOCSQL_STATEMENT_TIMEOUT_MS`)非法值一律拒绝启动(exit 2),勿改回静默回退。
- `DOCSQL_DEV_IMAGE_TAG`(dev compose,默认 `local`;部署测试传 `ci` 复用 CI 缓存镜像)vs `DOCSQL_IMAGE_TAG`(prod compose,默认 `latest`):两栈互不影响,可同时运行。
- `DOCSQL_DEV_TOKEN`(dev compose;prod 走 `.env` 的 `DOCSQL_TOKEN`):客户端 token,同值下发全部 dev 节点与两个 web 控制台,控制台以它认证节点。**控制台建首个数据库用户前必须配置**——节点存在任一用户后匿名连接被拒(含控制台自身),且匿名连接连 DROP USER 都执行不了,未配 token 即自锁,只能配 token 重建容器解锁或 reset-data.sh 清卷;run-tests.sh 显式置空保测试确定性。
