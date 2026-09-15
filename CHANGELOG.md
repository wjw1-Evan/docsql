# 更新日志

本文件记录用户可见的功能、修复与行为变更。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 加固收尾批次(2026-09-15 第二轮)

#### 优化

- **pub/sub 热路径与订阅回放**:`PUBLISH` 不再回读 `SELECT MAX(id)`(引擎新增
  `last_insert_id` 语句内直取,50k 历史深度实测 ~30×),订阅水位走 AUTOINCREMENT
  计数缓存 O(1);订阅回放改为按字节预算自适应分块(窗口 ≤512 行/≈1MB 起步、
  自适应放大),回放内存从"整段历史物化"降为单个窗口,巨量积压跨窗续传语义不变;
- **join 同步窗口预算**:bootstrap 期间"先 ack 后落盘"的入队写封顶(20 万条/
  256MB),超限拒绝 ack —— 源节点看到扇出失败,分歧走既有的摘要→快照收敛,
  而不是一个窗口把内存钉死;

#### 修复

- **web 会话上限与绝对寿命**:会话表封顶 64 个(驱逐最先过期者),滑动 12h 之外
  加 24h 绝对硬顶(持续使用的被窃会话不再永不失效);控制台页附加
  CSP/nosniff/Referrer-Policy 响应头;账号门开启但尚未建号时启动告警
  (先到者得的一次性窗口应当被看见);
- **凭据盐改用 OS 熵**(/dev/urandom;无此设备时回退原 PRF 流,保持唯一性);
- **ADO.NET**:`ConnectionString` 在连接打开后默认掩去 password/token/key
  (Persist Security Info 语义,诊断与日志面不再携带凭据;连接串写
  `persist security info=true` 显式豁免);参数改写器跳过双引号标识符与
  `--`/`/* */` 注释(其中的 `@` 记号不再被误改写);订阅器控制请求超时即投毒
  连接并触发 OnError —— 帧内无序号的前提下,迟到应答从此不可能被下一个
  请求错认;
- **EF Core**:schema 同步计账键改存连接串的 SHA-256 摘要(原文含口令不再
  常驻进程内存);
- **`DOCSQL_PBKDF2_ITERATIONS`**:新建凭据的 PBKDF2 迭代数可配置(默认
  210000;e2e 置 1000)。修复上一批次迭代提升后 debug 构建登录变慢、拖穿
  60s 登录锁定窗口的时序回归;非法值拒绝启动。

### 全库安全审查修复批次(2026-09-15)

#### 修复

- **prepared statements 绕过只读与匿名门**:`REQ_EXECUTE` 不在帧级只读门的帧类型
  白名单里,只读 token 可以先 `REQ_PREPARE` 一条写语句再执行;匿名连接同理。现在
  `REQ_EXECUTE` 按模板语句分类走与 `REQ_SQL` 相同的门,并进入匿名门帧列表;
- **写语句的读源表不做授权**:`INSERT … SELECT` 的源表、`UPDATE` 赋值子查询、聚合
  `FILTER (WHERE …)` 子查询、`MERGE WHEN` 谓词与 CTAS/CREATE VIEW 源查询此前都不进
  读目标分类——持有任意表写授权的用户可以把 `docsql_users` 口令哈希复制进自己的表
  再读回。`stmt_read_targets` 补齐上述形状,服务端对写语句同样施加读授权;
- **用户连接经复制帧剥离授权**:用户登录连接发送带 `FLAG_REPLICATION` 的 `REQ_SQL`
  时,执行侧用户身份曾被置空(语句以 token 级权限执行);现在用户身份全程保留,
  授权照常生效。`REQ_HOLD`/`REQ_SYNC`/`REQ_DIGEST`/`REQ_CATCHUP`/`REQ_RELEASE`/
  `REQ_SQL_SEQ` 六个节点专属帧同时拒绝用户登录与只读 token(peer 不受影响);
- **审计日志与运维面收敛**:`docsql_log` 视图与 `REQ_LOGS`(语句级审计,仅密码字面量
  脱敏)、`REQ_STATUS`/`REQ_META`(拓扑/路径/全目录结构)与备份清单对非 admin 数据库
  用户一律拒绝;web 控制台 `/api/auth/status` 未认证时不再返回账号用户名;
- **两处单语句进程 abort(DoS)**:`SELECT LPAD('a', 2^62)` 经无界 `with_capacity`
  直接 abort 引擎;三个 12 元素 `CUBE` 的 GROUPING SETS 笛卡尔积同样打穿分配器。
  分别加上界(LPAD ≤ 1 Mi 字符、分组集 ≤ 16384)并在聚合路径接入语句超时采样;
- **复制可注入超界口令哈希**:存储型凭据此前接受任意 PBKDF2 迭代数与盐长,被入侵的
  peer 可以植入 `iterations = 2^32-1` 的用户,让每次登录烧数十亿轮 HMAC。存储格式
  现在上界校验(迭代 ≤ 200 万、盐 ≤ 64 字节),越界条目验证直接拒绝;新建凭据迭代数
  60 000 → 210 000(旧条目按格式内嵌值继续可验证);
- **字典视图泄漏内部表**:`sqlite_master` 与 `information_schema` 此前列出引擎系统表
  与用户/角色存储表(存在性/形状),现在与对象树同一过滤(`is_internal_table`);
  引擎层同时拒绝 `CREATE TABLE` 抢注系统表名(此前只有服务端网关拦截);
- **备份文件 0600**:备份是整库明文 SQL(含口令哈希),落盘从默认 0644 收紧为属主
  可读;web 控制台会话 cookie 在原生 TLS 下自动附加 `Secure`;
- **CLI**:未知 `-` 参数此前被静默当作数据库路径(`docsql --help` 会在当前目录创建
  名为 `--help` 的库与 WAL),现在报错退出并新增 `-h/--help`;交互式密码输入关闭
  终端回显;
- **EF Core 提供程序**:SchemaSync 标识符引用补内嵌双引号转义;唯一索引漂移判断从
  子串 `Contains("UNIQUE")` 收紧为 DDL `CREATE UNIQUE INDEX` 前缀(索引名含
  UNIQUE 字样的普通索引不再误判)。

### 复制快照与 WAL 恢复健壮性修复(2026-09-15)

#### 修复

- **快照分片切成 UTF-8 字符中间导致 join/repair panic**:同步 dump 超过分片预算
  且含多字节字符(中文)时,`&str[start..end]` 曾精确切在汉字中间,服务端 panic、
  接收方报 `early eof`,分歧节点无法通过快照收敛;分片端点现在推进到字符边界
  (修复提交 3168a27);
- **超大 WAL 使节点启动 OOM 崩溃循环**:WAL 打开、崩溃恢复与快照物化原先整条
  日志读入内存(并逐帧拷贝),被杀死的大事务(如中途失败的重试快照采纳)把日志
  堆到数 GB 后,重启在重放/截断之前就 OOM;三处改为流式(逐帧、两遍:committed
  事务集合 → 按 LSN 顺序直写页面),恢复结束照常截断日志,内存与日志大小解耦;
- **回滚不再让死帧无界增长**:`ROLLBACK`/abort 之后同样执行 checkpoint 阈值检查
  (此前只有提交路径触发),失败的大事务留下的帧会随数据文件覆盖同步被截断,
  而不再等下一次成功提交。

## [0.4.0] - 2026-09-14

### 分页与计数性能批次(2026-09-14)

#### 优化

- **`ORDER BY <索引键> + 常量 LIMIT/OFFSET` 走索引序窗口**:按树序只装载窗口内
  文档,不再全表扫描 + 全量排序(50 万行实测首屏/浅页 ~30x,深 OFFSET ~40x,
  keyset 双向 ~50x);WHERE 能探测同一棵树时按窗口截断树遍历(OFFSET 行只计数
  不解码),残余条件回退候选边扫边过滤;DESC 走反向遍历,严格下界在遍历内丢弃
  等值 run;
- **无索引 `ORDER BY` + 有界 LIMIT 走键提取 top-K 窗口**:只从编码字节提取排序
  字段、不物化整文档(50 万行 `ORDER BY name` 320→41ms,深 OFFSET 366→98ms);
- **`SELECT COUNT(*)` 免解码活槽计数**(50 万行 387→1.5ms);
- 新增 `bench_page` 分页基准(默认 50 万行,可传行数)。

### 标准 SQL 子句补齐与 T-SQL 兼容审计(2026-09-14)

#### 新增

- **标准查询子句**:`GROUP BY ROLLUP(...)`/`CUBE(...)`/`GROUPING SETS` + `GROUPING()`;
  聚合 `FILTER (WHERE ...)`;`FETCH FIRST n ROWS WITH TIES`;`FROM (VALUES ...) AS t(cols)`
  (含 CTE 别名列);`IS [NOT] DISTINCT FROM`;量化比较 `> ANY`/`<> ALL`(非相关子查询,
  改写为比较链);
- **T-SQL 兼容垫片**:`COUNT_BIG`/`ISNULL`/`CAST(... AS BIT)`;`INFORMATION_SCHEMA`
  大小写不敏感;`ORDER BY (SELECT 1)` 子查询替换(EF Core `Skip`/`Take` 形态)。

#### 变更

- **子句级静默忽略全部改显式报错**:NATURAL JOIN(曾按 CROSS JOIN)、LATERAL/
  `FOR UPDATE`·`FOR SHARE`/`SELECT INTO`·`SELECT TOP`/`TABLESAMPLE`/`WINDOW`·`QUALIFY`、
  ClickHouse/Hive 专有子句、`* EXCLUDE/REPLACE`、T-SQL 变量 `@p`/`@@VAR`(曾静默 NULL)、
  `OUTPUT`、表提示 `WITH(...)`、`#` 临时表(曾建持久表)、索引 `INCLUDE`/`WHERE`/`USING`/
  存储选项(曾丢子句)、`UPDATE`·`DELETE` 的 `ORDER BY`/`LIMIT`、基表别名列、`sys.*`
  提示——均显式报错,不再静默降级;`PRAGMA` 仍是有意接受并忽略的兼容垫片。

### SQL 参考文档重写与三处静默降级修正(2026-09-14)

#### 变更

- `docs/sql-reference.md` 按 MSDN(T-SQL)风格重写:约定/数据类型/运算符/逐语句
  语法·参数·备注·示例/系统视图/兼容性矩阵;README 能力表同步 SQL 面、JOIN 列表与
  .NET 用例数;
- 表级复合 `PRIMARY KEY`/`UNIQUE`(曾按单列声明)与 `GROUP BY ... WITH ROLLUP`
  修饰符(曾被丢弃)改为显式报错;
- `ON CONFLICT (cols)`/`ON CONSTRAINT name` 实现**定向冲突**语义:只跳过目标唯一
  约束的冲突,命中其他唯一约束仍报错;目标不匹配任何唯一约束时显式报错。

#### 测试

- 新增深 B+ 树内部节点分裂、JSONL 审计落盘与写失败告警、X-Forwarded-For 可信
  代理锁定键、DESC 索引窗口 Eq/复合前缀探针、加密传输门禁(明文/错钥拒绝)、
  语句切分注释与转义、语句写目标分类等回归;workspace 604 用例,覆盖率 92.9%/88.6%
  (行/函数)。

### EF 运行期连接串工厂(2026-09-14)

#### 新增

- **`UseDocsql(Func<string> connectionStringFactory)`**:每次创建物理连接时调用工厂,
  连接信息不参与模型/服务提供程序缓存键(扩展哈希恒为 0)——同一宿主内不同上下文可连
  不同节点而模型只建一次;测试夹具按用例路由到独立节点的机制基于此。
- 修复 `UseDocsql(DocsqlConnection)` 连接实例被忽略的缺陷(此前 `CreateDbConnection`
  只读连接串,实例落入默认 `127.0.0.1:7600`)。
- 新增 `ConnectionFactoryTests`(双节点路由互不可见)回归。

### EF schema 同步摊销(2026-09-14)

#### 变更

- **惰性建表改「先校验后同步」**:新上下文(EF 每个 DbContext 一条新连接)先做两条
  一次性校验查询——`information_schema.columns`(表×声明列)与 `sqlite_master`
  (命名索引及其 DDL)——模型要求的表/列/索引齐备即跳过整场 `SyncModel`;此前每条
  连接都全量同步(119 实体规模 = 数百次建表/列探测往返)。校验覆盖缺表、缺列、缺索引、
  unique 漂移(同名非 UNIQUE)与模型已移除的 `IX_` 索引(回收面),任一命中才回落全量
  同步;外部 DDL(裸连接删表/删列/删索引)在下一个上下文即被校验发现并恢复,语义与
  逐连接全量同步一致。新增 `SchemaSyncAccounting.FullSyncCount(connectionString)`
  观测口径(内部,供测试断言)与 `SchemaVerifyTests` 三条回归(重复上下文零全量同步/
  缺列回填/缺索引重建)。

### 部署测试不再清空开发数据(2026-09-14)

#### 变更

- `./deploy/run-tests.sh` 改为在一次性卷 `docsql-dev-testdata-*` 上运行(single/cluster
  两套 34+81 项断言仍从干净状态开始),不再删除开发数据卷 `docsql-dev-data-*` 与控制台
  账号卷;测试前停止、测试后自动恢复运行前的 dev stack(`down` 不再带 `-v`)。
  清数据仍只通过 `./deploy/reset-data.sh`;compose 新增 `DOCSQL_DEV_DATA_PREFIX`
  覆盖外置卷前缀(默认 `docsql-dev-data`,测试栈用 `docsql-dev-testdata`)。

### Web 控制台移除浏览器令牌输入(2026-09-14)

#### 变更

- **控制台不再要求填写令牌**:工具栏的 `X-Docsql-Token` 输入框与「连接」按钮移除;
  控制台连节点一律使用 Web 进程环境变量 `DOCSQL_TOKEN`(docker-compose 下发,与节点同值)。
  `DOCSQL_WEB_AUTH_FILE` 启用时账号门(用户名/密码 + HttpOnly 会话)是唯一浏览器访问门,
  `DOCSQL_TOKEN` 仍可作程序化 API 旁路;未启用账号门时 API 开放(不再用浏览器令牌门控)。
  这样节点启用数据库用户后不会再因浏览器未持有令牌而报 `authentication required`。

## [0.3.0] - 2026-09-14

### 实体集合查询 + 字典映射 + 异常分类(2026-09-14)

#### 新增

- **实体集合属性的服务端查询**:`List<string>`/`List<int>` 等集合属性(JSON 数组列)的
  `Contains` 由 EF 提供程序翻译为引擎新函数 `JSON_ARRAY_CONTAINS(json_text, value)`
  (成员判定;非数组/坏 JSON → false,NULL 文本 → NULL)。覆盖常量元素、跨实体列形态
  (`t.RoleIds.Contains(p.Id)` 权限查询)、取反、数值集合与空集合;不回落客户端求值。
- **`Dictionary<string, object>` 映射**:提供程序约定(实体/属性加入时)把字典映射为
  JSON 文本标量——此前模型校验直接失败("shared-type entity type"导航)。读取把 JSON
  反序列化为原生 CLR 值(整数保持 long),`ValueComparer` 以键排序的规范 JSON 做
  相等/哈希/快照,原地改写可被变更跟踪捕获;字典成员不参与 SQL 翻译(需要键过滤时提取独立列)。
- **异常分类**:`DocsqlException.IsUniqueViolation`(唯一约束冲突)与 `IsSyntaxError`
  (解析错误)——幂等写入(webhook 重放)可按属性分支,不必解析错误文本。

#### 文档

- `docs/features.md` / `docs/drivers.md` / README / EF 包 README 同步集合翻译、字典映射、
  异常分类与 NULL/软删语义说明(`x != TRUE` 命中 NULL/缺字段;单列 UNIQUE 允许多个 NULL)。

### Aspire 示例改用发布包 + 文档整理(2026-09-14)

#### 变更

- `dotnet/samples/AspireSample/` 与真实用户对齐:改为引用 **GitHub Packages 已发布包**
  (`Docsql.Aspire.Hosting` / `Docsql.Aspire.Client`,版本集中在示例的
  `Directory.Build.props`),并移出 `Docsql.sln`——主解决方案的自测不再依赖私有包;
  CI 在 dotnet job 用 `GITHUB_TOKEN` 认证后单独构建该示例(fork PR 跳过)。示例新增 README
  (包源凭据配置、`aspire start`、集群切换、发布产物)。
- 文档整理:README 新增「文档导航」并压缩为导航友好形态(Studio/部署/备份恢复/用户角色
  细节下沉到 docs,开发命令改由 CONTRIBUTING 承接);`docs/README.md` 改为按任务分类的
  索引;`docs/operations.md` 补齐数据持久化、扩容、离线补齐、备份恢复语义、环境变量
  (新增 Web 控制台变量表)与 CLI;`docs/features.md` 补全 Web 控制台细节与 REST API。

### 文档完善:Aspire 使用手册 + 功能总览(2026-09-14)

#### 文档

- 新增 `docs/aspire.md`(Aspire 集成指南):安装与包源、单节点/对称集群编排、
  Web 控制台行为、token/凭据管理与自锁风险、消费侧连接注册(ADO.NET/EF/健康检查)、
  运行期配置与镜像钉版、本地开发工作流、发布部署路径对比、故障排查、Hosting API 速览;
- 新增 `docs/features.md`(功能总览):数据库全部能力的完整清单——运行形态、数据模型与
  类型、SQL(DDL/DML/查询/函数/事务/MVCC)、索引、发布订阅、复制与集群、备份恢复、
  安全、Web 控制台与 REST API、客户端/CLI/线协议、可观测性、部署与边界摘要;
- `docs/sql-reference.md` 补齐交集/差集(INTERSECT/EXCEPT/MINUS)、`TRUNCATE TABLE`、
  `CASE`/`ILIKE`、`ALTER TABLE RENAME/DROP COLUMN`、`FETCH FIRST`,并新增独立
  「不支持」章节(含 `CREATE VIEW`/`TRIGGER`、外键动作、表达式/部分索引等);
- README、`docs/README.md` 与两个 Aspire 包 README 增加新文档入口与索引。

### 精确数值与二进制类型 + EF 映射补齐(2026-09-14)

#### 新增

- **DECIMAL 精确十进制类型**:值模型新增 `Value::Decimal`(rust_decimal,28~29 位有效
  数字),编码 tag 8、`cmp_values` 与 Int/Float 数值互比、ORDER BY/索引/复合键全链路支持;
  `CAST('123.45' AS DECIMAL)` 与 `TO_NUMBER` 非整数文本产出 DECIMAL,算术/`SUM`/`AVG`/
  `ROUND`/`ABS` 按十进制精确执行(混合运算中 Decimal 优先于 Float);`value_literal` 渲染为
  `CAST('…' AS DECIMAL)`,dump/备份/复制重放精度不丢;JSON 线协议用 `{"$dec":"…"}` 标记
  精确传输(解析回 Decimal)。EF Core `decimal` 映射从 TEXT 改为 `DECIMAL`
  (`HasPrecision` 进入列类型与 CAST 字面量),参数经 `$dec` 标记、服务端十进制聚合与比较,
  86 个金额/面积字段这类场景不再有 Float 精度取舍。
- **BLOB 二进制值**:`x'hex'` 字面量解析为 `Value::Bytes`(此前仅内部值、SQL 层不可达),
  `CAST(text AS BLOB)`、`LENGTH`、`value_literal`/dump/复制全链路;JSON 线协议用
  `{"$bytes":[…]}` 标记。EF Core `byte[]` 映射从"故意不映射"改为 `BLOB`
  (DocsqlDataReader 新增 `GetFieldValue<T>`/`GetBytes` 按类型读取),客户端 `byte[]` 参数从
  显式拒绝改为精确往返(单值 ≤16MiB 文档上限,无流式分块)。
- **EF Core `DateOnly`/`TimeOnly` 映射**(可排序 ISO 文本,参数与读取双向),补齐
  `Common_clr_types_roundtrip` 之外的 30 个 `DateOnly` 属性场景;
- **EF Core 模型复合索引**:`HasIndex(e => new { … })` 从"尽力而为跳过"改为按列序创建
  (引擎复合索引 B+ 树);`List.Contains` 翻译为 `IN (…)`(新增回归测试);
- Web 控制台查询网格/行编辑识别 `$dec`/`$bytes` 标记:精确显示十进制文本、BLOB 十六进制,
  行内编辑保存走 `CAST(… AS DECIMAL)`/`x'…'` 而非 JSON 文本。

#### 变更

- 客户端 `decimal` 参数不再降级为 IEEE double(此前 >15~16 位有效数字丢精度),`byte[]`
  参数不再抛 `NotSupportedException`;
- `docs/limitations.md`、`sql-reference.md`、`drivers.md` 与 README 能力表同步(DECIMAL/
  BLOB 从"已知边界/路线"移入已具备;`INSERT … SELECT` 本已支持,文档补记)。

#### 修复

- `ADO.NET ExecuteScalar` 对 decimal 先转 double 再判断整数性,17 位大数(
  如 `SUM(金额)`)会因精度丢失被误判为整数并静默截断为 long;改为 decimal 自身
  精确判断整数性(同步/异步两路径),`double`/`float` 行为不变。

#### 测试

- 补齐 DECIMAL/BLOB 全链路测试:精确 CAST 矩阵与一元负号(常量/行/聚合三条求值路径)、
  除零/取模/溢出语义、MIN/MAX/AVG/SUM(DISTINCT)、GROUP BY 编码判重与 scale 别名、
  UNIQUE/复合索引/UPDATE 索引位移、跨列 JOIN、INSERT … SELECT 与 auto-GUID 回写重放、
  JSON_TYPE/TO_CHAR 文本、x'hex' 畸形拒绝;服务端 REQ_EXECUTE `$dec`/`$bytes` 线协议
  往返与注入回退;Web `/api/sql` 标记透出;CLI 表格/JSON 渲染;客户端表级往返、
  GetBytes 偏移契约、200KB BLOB、异步标记解码;EF 更新/聚合/可空 decimal/Contains、
  复合唯一索引、64KB BLOB 与 DateOnly/TimeOnly 可空往返。workspace 567 用例,
  覆盖率 92.2%/88.2%。

## [0.2.0] - 2026-09-14

### 发布与文档:0.2.0(NuGet 四包 + Aspire 安装使用)(2026-09-14)

#### 变更

- **.NET 包版本 0.1.0 → 0.2.0**:Docsql.Client / Docsql.EntityFrameworkCore /
  Docsql.Aspire.Hosting / Docsql.Aspire.Client 统一升版,随 `v0.2.0` tag 推 GitHub Packages;
- **文档补齐 Aspire 安装与使用**:README「.NET 与 Aspire」与 `docs/drivers.md`(新增安装节与
  Aspire 集成节)、`docs/README.md`(Aspire 快速开始)、四个包 README 全部写明 GitHub Packages
  源配置(nuget.config + PAT `read:packages`)、`dotnet add package` 命令、包到项目的对应关系、
  AppHost/消费侧最小示例与 `aspire start` / `aspire publish` 用法;`docs/limitations.md`
  生态行与 README 结构/测试行同步(含客户端与 Aspire 示例项目)。

### 审查去重续批(2026-09-13)

#### 修复

- **查询日志在非 ASCII 语句上 panic**:`redact_sql`(每条被记录语句都会跑)用
  `to_lowercase()` 副本的字节下标去索引原文,而 Unicode 大小写映射会改变字节长度
  (如 U+212A KELVIN SIGN 三字节 → `k` 一字节),下标越界直接 panic 并中断该连接。
  改为在原文上做 ASCII 大小写不敏感匹配,并复用共享的字面量扫描助手(该 panic
  有回归测试钉住:修复前 exit 101)。

#### 重构

- **SQL 字符串字面量扫描收敛单点**:占位符绑定(`bind_params`)、`docsql_pubsub`
  视图重写、`redact_sql`、查询日志词法四处手写的「跳过 '' 转义字面量」循环统一走
  `stmt::sql_literal_end`(新增,带边界测试:转义、闭合引号在末字节、未终止到 EOF);
  `bind_params` 由字符状态机改为字节游标(语义逐字节等价,注入面单点)。
- 用户/角色执行侧:GRANT/REVOKE/加角色/撤角色四处镜像分支收敛为
  `rows_have`/`insert_row`/`delete_rows` 三个助手(存在性判定与存储行的规范大写
  形式保持原样);GRANT/REVOKE 解析的 TO/FROM 方向分支合并。
- btree 删除:按整键删与按 `(键, 定位符)` 删共享同一递归走子(内部节点走法原本
  逐行重复,等键跨分裂的 `candidate_children` 兜底语义不变)。
- server 侧对等探测:repair_sync 的轮首/追赶后复验与 join 收敛复验三处
  spawn+collect 收敛为 `probe_all_digests`/`probe_all_peer_reports` 两个助手
  (「摘要到、状态未到」仍可快照修复的降级判定保留)。
- web 控制台探测:状态/日志两个 probe 的连接与请求-响应骨架收敛为
  `connect_probe_stream`/`request_on_probe_stream`(可达性分类的 bool 语义逐处保留)。
- hash join 候选集由每左行排序改为对两个已升序的桶做线性归并(右行输出顺序不变),
  补文档键值降级(always 桶)的回归测试。
- WAL 启动只整读一遍(撕尾截断后的干净前缀直接复用,原来重读第二遍;硬阈值下最大
  省约 60MB 启动 IO);三段 frame 走查(撕尾定位/durable 扫描)收敛为一个
  `scan_prefix`;pager 页头编码(初始建库与页数持久化两处)收敛 `encode_header`。

### 全库审查修复与去重批次(2026-09-13)

#### 修复

- **等值 JOIN 丢行两处**:(1) 同侧等值合取(`ON l.x = l.y AND l.id = r.id`)曾被误纳入
  哈希计划、第二表达式在右行上求值导致真匹配被丢弃——纯侧分类(Left/Right/Both)后同侧/混合侧
  合取一律留在 residual;(2) 限定名列经 `lookup_col` 后缀回退可在合并行上跨表绑定
  (`l.foo` 静默命中 `r.foo`),单侧求值与合并行求值分叉漏行——`EquiPair` 记录精确键清单,
  行内键未精确命中只降级该行为全探针;
- **MVCC 快照两处**:(1) checkpoint 截断后 `page_lsn` 残留旧 epoch 大 LSN,例行截断后的所有
  新快照读"截断前最后写过"的页会被伪 `SnapshotTooOld` 打爆——截断在 WAL 锁内清表并发布
  epoch;(2) 快路径 page_lsn 判定与取像两段加锁,提交者可插在中间使快照读到快照之后的页版本
  ——seqlock 式双检(提交临界区池锁最后释放保证可检);
- **`SYSDATE()` 日期完全错误**(一直如此):civil 历法换算的纪元除数写错,输出年份在
  公元 74 万年(测试只断言了形状);收敛到与备份时间戳同一份 Hinnant 算法并加已知答案测试;
- 哈希连接候选循环补 deadline 协作采样(超时粒度从"每左行"恢复到与嵌套循环同级的"每对");
  hash 路径的行组装/补 NULL 逻辑与嵌套循环共享同一组助手,消除双路径手抄分叉。

#### 优化

- 快照慢路径(首次 as-of 读)的 WAL 全量扫描不再持 snaps/WAL 任一锁:独立只读句柄扫描、
  扫后 WAL 锁内复验 epoch,后台化承诺(读不阻塞写)在物化期间同样成立;
- 缓冲池冷未命中的磁盘 IO 移出池锁(此前一个冷读可卡住全部读者与提交路径);
  嵌套循环右行统一预限定(原先每对 format! 一次);
- INSERT 热路径表元数据改 Arc 引用(不再每语句整条深拷);DROP INDEX 定点写拷贝
  (原先对全库每张表做 `Arc::make_mut`);MERGE 目标扫描复用 `table_pairs`
  (原先手抄一遍带定位符扫描循环)。

#### 重构

- 密码学实现收敛:控制台账号门(~180 行 SHA-256/HMAC/PBKDF2 手写副本)与
  `constant_time_eq` 三份实现统一到 core `kdf` 一份,已知答案测试保留于 core;
- SQL 字面量/标识符转义(六处字符串、五处标识符手抄副本)统一为
  `stmt::sql_string_literal`/`sql_quote_ident` 注入面单点;
- `ReadView::execute` 与 `execute_read` 的纯 SELECT 门禁合并为
  `plain_read_query` 单份;删除自 stage A 起恒空的 `Database::ctes` 死字段;
- btree `range_from` = `range_bounded(None)` 特例化(删 40 行镜像递归);
  heap 溢出链页头解析统一 `chain_header`,`insert_overflow` 不再双重加载尾页;
  server 侧 `outcome_frame`(读写两级响应渲染)、`record_auth_failure`
  (token/用户两路失败记账)、`hold_peer` 复用 `write_frame_on` 去重;
  `file_bytes`/`now_ms` 包装收敛 core。

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
