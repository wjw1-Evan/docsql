# 更新日志

本文件记录用户可见的功能、修复与行为变更。格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Web 管理端完善轮:活动会话监视 + KILL、结果导出、查询历史、慢查询过滤、PITR UI(2026-09-29)

对标商业库管理端(SSMS 活动监视器 / Azure Data Studio)补齐运维与生产力面:

- **活动会话(新协议帧)**:`REQ_SESSIONS`(0x0019,admin)返回节点实时连接表
  (id/来源/身份/状态/当前语句截断 256 字符/已运行毫秒/连接时长/语句数,RESP_ROWS 通用网格);
  `REQ_KILL`(0x001A,admin)按 id 终止连接——**空闲连接立即唤醒关闭**(读等待 select 在
  watch 通道上,无丢唤醒竞态),运行中语句完成后关闭(引擎不可抢占,弹窗明示);自杀/未知
  id/畸形载荷响亮拒绝;连接清理段注销会话条目。服务端身份标签随 AUTH 流转(anonymous/
  client token/read-only token/cluster peer/user <name>)。
- **控制台「活动会话」页**(视图菜单 + 服务器右键):5s 自动刷新、状态徽章、
  KILL 双重确认(输入 KILL);GET/POST `/api/sessions` 代理到受管节点。
- **查询工作台**:结果导出 **CSV(RFC 4180)/ JSON / INSERT**(本次执行全部结果集,
  INSERT 表名从 FROM/INTO 推断默认值、标识符引号转义、字符串单引号翻倍);**查询历史**
  (localStorage 最近 100 条,成败+耗时,点击重填,可清空——修复:数组直写 localStorage 被
  隐式字符串化成 "[object Object]" 导致历史静默清零)。
- **表数据网格**:当前页 CSV/JSON 导出按钮。
- **日志页**:慢查询阈值过滤(>100ms/500ms/1s/5s)+ 按耗时降序排序。
- **备份页**:恢复对话框新增「恢复到时间点」ISO 时间戳字段(前端校验格式),
  `/api/backup/restore` 透传 `to` → 节点 REQ_BACKUP 的 PITR 重放。
- 浏览器端到端冒烟通过:会话列表/KILL 全流程(被杀连接从列表移除)、三格式导出内容、
  历史记录与弹层、慢查询过滤、PITR 非法时间戳拒绝;cargo e2e 新增
  `sessions_frame_lists_and_kill_closes`(列表/身份/终止/注销/自杀拒绝/只读 token 拒绝/
  畸形载荷)、web e2e 新增 `sessions_endpoint_lists_and_kills`。

### 数据面原生 TLS(商用化路线 · 传输安全)(2026-09-29)

此前数据面传输保护只有私有 AES-256-GCM 帧加密(`DOCSQL_KEY`)或 TLS 反代;本批次为
SQL 协议引入标准 TLS(rustls / SslStream),**标准工具链与跨机房部署获得可对接的传输层**:

- **节点监听**(`DOCSQL_TLS_CERT` + `DOCSQL_TLS_KEY`,PEM 成对、缺一半拒绝启动):接受连接
  先完成 TLS 握手再进协议;明文客户端响亮失败,**无协议探测降级**;握手失败计数进
  REQ_STATUS metrics 与 Prometheus(`docsql_tls_handshake_failures_total`),监听状态
  `docsql_tls_listener` / status `tls_listener` 字段;TLS 在 socket 层之下,协议帧与
  `DOCSQL_KEY` 帧加密语义不变、可叠加。
- **出站拨号**(`DOCSQL_TLS_CONNECT=1`,可选 `DOCSQL_TLS_CA`):扇出/追赶/join/hold/备份
  全部对端连接走 TLS;未配 CA = 只加密不验证(自签友好,启动告警明示),配 CA = 完整
  链 + 名称验证。CLI 远程模式与 Web 控制台连节点共用同一变量(compose 健康检查自动跟随)。
- **.NET 客户端**:连接串 `tls=true`(可选 `tls_ca=<PEM>`、`tls_host=<名称>`,按 IP 连接
  而 DNS 证书时用);池化重开不重复握手;`DocsqlSubscriber` 同路径。
- **CLI**:同步 rustls 客户端(会话 + 非阻塞轮询读写线程,专职读线程继续承接 RESP_PUSH
  实时推送);best-effort close_notify。
- **Web 控制台**:节点探测/日志/数据端点/备份/用户页全部走 TLS 拨号(与节点共用
  `DOCSQL_TLS_CONNECT`/`DOCSQL_TLS_CA`);其 HTTPS 监听的证书装载去重为 server 同一实现。
- 连接处理层泛化:`TcpStream` → `tls::BoxConn`(server 入站/出站、web 节点客户端),
  单一帧循环两种传输。
- 测试:cargo 新增 TLS e2e 2 项(TLS 监听全功能 + 明文拒绝;双节点 TLS 集群 bootstrap +
  写扇出复制)+ tls 模块单测 + 配置配对/布尔拼写测试;dotnet 新增 `TlsTests` 4 项
  (TLS 往返含池化重开/明文拒绝/tls_ca 同证书验证通过/错误 CA 拒绝);CLI 经实机冒烟
  (SQL 往返、订阅实时推送、明文拒绝)。文档同步(operations/security/drivers/features/
  limitations/AGENTS/compose)。

### 缺陷审查轮 15:deploy 部署面 + .NET EF 提供程序/客户端剩余面(2026-09-29)

#### 修复(.NET 客户端/EF 提供程序)

- **外部取消后连接未弃用,下一条命令读到被取消语句的应答(静默错数据)**:
  `SendAsync` 读阶段只把「预算超时」标记连接弃用,外部 `CancellationToken` 取消原样上抛
  且不毒化——服务端(每连接一问一答)仍会执行完被取消的语句并送出应答帧,同一连接对象上
  的下一条命令读到的是**旧语句的结果**(实测:取消大聚合后 `SELECT 12345` 返回被取消查询
  的和;池路径靠租借前 PING 验活侥幸兜底,直接复用连接对象时无兜底)。读阶段的任何取消
  现在都标记弃用并关 socket(发送前取消保持无害不毒化)。
- **EF 变量(参数化)StartsWith/EndsWith/Contains 翻译直接抛异常**:变量分支裸构造
  SQL 树、未对参数应用类型映射,EF 校验阶段抛 "does not have a type mapping assigned"
  —— 前缀/子串搜索整类查询失效(常量路径正常,LIKE 转义测试只钉了常量)。经
  `ISqlExpressionFactory.ApplyTypeMapping` 应用映射后正常生成 `LIKE CONCAT('%', @p, '%')`。
- **TimeSpan/DateTimeOffset 属性「写得进、读不出」**:EF 映射声明支持这两类(参数分别写
  "c" 文本 / `$ts` UTC 毫秒),但 `GetFieldValue<T>` 无对应分支,物化落到
  `Convert.ChangeType`(string→TimeSpan、DateTime→DateTimeOffset 均不支持)抛
  InvalidCastException——模型一经采用即困死。补读取分支与 `GetDataReaderMethod` 覆写;
  `$ts` 只存 UTC 毫秒,DateTimeOffset 往返时刻精确(偏移口径随物化层,文档明示)。
- **ADO 强类型 getter 对 NULL 静默返回 0/false/null**(违反 ADO.NET 契约,漏判 `IsDBNull`
  的调用方拿到错数据):`GetInt32/GetInt64/GetDouble/GetBoolean/GetString` 现在对 NULL
  抛 InvalidCastException;`GetOrdinal` 补大小写不敏感回退(ADO.NET 惯例)。
- 附带:手工建表无 AUTOINCREMENT 时 EF int 键回读 NULL 的既有测试前置按真实形态修正
  (外部存量表主键几乎总带自增)。

#### 修复(deploy 部署面)

- **`.env.example` 三个变量从未进容器(静默无效配置)**:`DOCSQL_READ_TOKEN`(文档中的
  最小权限凭据!)、`DOCSQL_MAX_CONN`、`DOCSQL_IDLE_TIMEOUT` 在两个 compose 的全部
  `environment:` 块中均无引用——按示例配置只读令牌的用户实际拿到的是开放节点,只读客户端
  验证失败;上限被静默忽略。两份 compose 的全部数据节点现在透传(空串与未设等价,server
  默认值不变),`.env.example` 注释同步。
- **healthcheck 无 `start_period`:大 WAL 节点恢复期被误判 unhealthy,`service_healthy`
  依赖让整个集群 `up` 失败**:监听在 WAL 恢复+启动同步**之后**才绑定,5s×10 次的重试预算
  约 50s——历史上真实出现过 9.5GB WAL(恢复远超 50s),此时 node-b/c/web 的
  `depends_on: service_healthy` 会中止整个启动;prod 的 `restart: unless-stopped` 还会在
  恢复中途反复重启容器。加 `start_period: 30s` + `retries: 60`(容忍约 5.5 分钟的正常
  慢恢复)。
- **Dockerfile 无依赖预热层**:每次源码修改都使 `COPY crates` 之后的所有层失效,依赖树
  全量重下重编(本机网络断流环境下既慢又脆)。新增 stub 源预热层(RUN_TESTS=true 时含
  测试依赖),源码修改只重建工作区成员。
- **run-tests.sh 栈恢复丢失用户自己的环境**:恢复用户栈时一律 unset 覆盖变量——用
  `DOCSQL_DEV_IMAGE_TAG=ci`/自定义数据前缀/账号门文件起的栈,恢复后静默变成 `:local`
  默认形态(或直接 up 失败留下一片停摆)。改为入口快照、恢复时重放
  (`DOCSQL_WEB_AUTH_FILE` 区分未设/空串:compose 对它用 `${VAR-}` 语义,空串=禁用账号门)。
- **multinode-test.sh 直接调用会重建真实 dev 数据卷**:节点 D 卷删除/重建以
  `DOCSQL_DEV_DATA_PREFIX` 默认展开,不经 run-tests.sh 直接运行即指向 `docsql-dev-data-d`
  (真实数据)——未设置该前缀时现在拒绝运行并指回 run-tests.sh;12.5 的过时注释
  (「c 无反熵追赶」已与 9.5/10.5 的修复章节矛盾)一并修正。
- **reset-data.sh 漏自定义前缀的 dev 卷**:`DOCSQL_DEV_DATA_PREFIX` 自定义前缀创建的卷
  在「删除全部 DocSQL 数据」后依然存活;按锚定前缀(`^docsql-dev-data-`/
  `^docsql-dev-testdata-`)清扫补齐,不触及其它项目与 prod 的 `docsql-data-*`。

#### 测试

- .NET 新增回归 6 项:取消后同连接复用必须响亮失败(Client)、GetFieldValue 的
  TimeSpan/DateTimeOffset 往返(Client+EF)、变量 StartsWith/Contains/EndsWith 翻译(EF)、
  强类型 getter NULL 抛契约异常、GetOrdinal 大小写回退;dotnet 174(Client 102 + EF 60 +
  Aspire 12)全绿。部署面经 compose 渲染校验 + 两个 compose 全键计数核对;部署测试
  (`./deploy/run-tests.sh`,含镜像内 cargo 门禁 + single 34 + cluster 81)在本轮提交后实跑。

### 缺陷审查轮 14:Web 模块(REST API / 账号门 / 控制台前端)(2026-09-29)

#### 修复(安全/可用性)

- **PBKDF2 长密码 CPU 放大(登录/建号/改密可被两个匿名请求瘫痪约 20 分钟)**:
  `pbkdf2_hmac_sha256` 每次迭代重算 HMAC 密钥垫片,>64 字节口令每轮都先 `sha256(password)`
  ——2 MiB 口令(HTTP body 上限)一次 21 万轮派生从 ~200ms 放大到 ~20 分钟;web 登录的全局
  2-permit 派生池被两个 fire-and-forget 请求钉死,期间所有 IP 的登录/设置/改密一律「认证繁忙」
  (失败计数每 20 分钟才记 1 次,锁定机制追不上)。根修:垫片在循环外预计算一次(RFC 2104
  对 K>64 本就等价于先 H(K),输出逐位不变,KAT 不动;服务端 REQ_AUTH_USER 同收益)。
- **原生 HTTPS 监听把瞬态 accept 错误当致命(未认证即可远程杀死控制台进程)**:TLS 路径
  `r?` 把任何 accept 错误上抛出 `run()`,`EMFILE`(攻击者用 30s 握手窗口耗尽 fd)或单个
  `ECONNABORTED` 即进程退出,配合重启策略成崩溃循环;明文路径(axum)本就重试。现在对齐
  axum 策略:连接类噪声立即重试,资源类错误记日志退避 1s;SIGTERM 也与明文路径一致——
  停止 accept 后给在途请求 10s 有界排空。
- **登录与改密的竞态架空密码轮换**:改密的「快照→锁外派生(~200ms)→写回+踢会话」窗口内,
  用**旧密码**登录成功的请求在 `keep_only` 之后建会话——躲过「踢掉所有其他会话」,可继续
  滑动续期 12h。登录在发会话前短锁复核凭据快照仍是派生所依据的那份,不等则按未登录拒绝
  (客户端重试即可,不计锁定)。
- **凭据文件 iterations 解析两处 fail-open**:u64→u32 先截断后校验,手改 `2^32+1000` 静默
  按 1000 生效(违背自述的 corrupt 拒绝意图);非数值(负数/浮点/字符串)经 `as_u64()→None`
  静默回落默认迭代数。均改为响亮 corrupt(键缺失才回落,兼容旧文件)。

#### 修复(健壮性)

- **`/api/sql` 批执行只界内存不界时间**:2 MiB 请求体可携带 ~13 万条微型语句,逐条在节点
  连接上往返(单帧 IO 各自有超时,整场无界),一个已认证请求可占用 worker 数分钟。加
  **1 万条语句上限**(连接建立前拒绝)+ **120s 整场墙钟 deadline**(逐语句采样,超时按
  in-band 错误返回已完成的分结果)。
- **用户/角色名长度按字节计数却报「1-64 个字符」**:22 个汉字(66 字节)撞长度上限的报错
  具有误导性;改按字符计数(白名单本就 ASCII-only,通过路径行为不变)。

#### 修复(控制台前端 console.html)

- **会话过期后登录表单打不完字**:集群/备份/日志页每 5s 轮询,遮罩后的每个 401 都重新调
  `authGate.show('login')`——清空正在输入的密码、抢焦点、弹错误 toast。遮罩同模式下幂等,
  轮询定时器在遮罩可见期间停摆。
- **切换节点不刷新「用户与角色」页**:页面渲染旧节点名单,删除用户/改密码等动作却发往新
  节点(集群分叉期可能删错实体);切换时用户页随仪表盘/数据/备份页一并重载。
- **对话框吞掉传输层异常**:「删除表/索引、新建用户/角色、改密码」等弹窗的确定按钮对网络
  错误零反馈(弹窗原地不动、无提示);异常现在落到弹窗错误行(onOk 已自行呈现的按 'cancel'
  约定跳过),恢复/角色成员对话框同类问题一并修复。
- **结果网格排序对 DECIMAL/TIMESTAMP/BLOB 列完全失效**:这三类以 `$dec/$ts/$bytes` 标记
  对象下发,比较器 `String()` 后全列相等——点列头无反应。解包标记后按类型比较:十进制走
  字符串精确比较(`0.1` 与 `0.1+28 位尾数`可区分,不降双精度)、时间戳按毫秒、BLOB 按字节序
  (十六进制字典序即字节序)、数值/DECIMAL 混排列统一降双精度。
- **翻页/执行查询无请求序列化**:慢的旧响应最后落地会覆盖新结果,行号区间标签与按钮禁用态
  和网格内容错位(数据页在 count 回包后才读偏移,连点两次甚至可能都取到新偏移)。请求序号
  淘汰旧响应,分页参数快照进本场。
- **大结果集无渲染上限**:64 MiB 结果帧的 SELECT 会把几十 MB HTML 同步拼进 DOM,长冻结
  甚至打挂标签页(单元格全文再复制进 `title` 让内存翻倍)。首屏渲染上限 5000 行 + 明示
  提示行,单元格显示 256 / title 2048 字符截断。
- **其余前端加固**:「立即备份」按钮取 `e.target`(命中内层 span)防重复失效 → `currentTarget`;
  日志页来源下拉每 5s 整体重建、展开即被合上 → 成员签名 + 焦点守卫;重登后 `startApp` 二次
  执行重复绑定分隔条监听(拖动执行两遍、双击弹两个 toast)→ 幂等守卫;右键「复制名称」
  剪贴板 Promise 无 catch(权限拒绝时谎报成功 + unhandled rejection)→ 统一 `copyText`
  如实回报;主脚本顶层裸读 localStorage(「阻止所有 Cookie」/企业策略下整个控制台白屏)
  → `lsGet/lsSet` 守卫;索引对话框占位符双重转义(列名含 `&` 显示 `&amp;`)。

#### 测试

- 新增回归 5 项:PBKDF2 长密码 RFC 向量(100/64/65 字节边界 + 多字节 UTF-8)+ 垫片预计算
  与逐迭代 HMAC 的等价性(kdf 2)、凭据文件 iterations 越界/非数值拒绝(auth)、批语句上限
  单测 + HTTP e2e(web 2);cargo 920(原 915)、web e2e 34 全绿,浏览器端到端冒烟通过
  (DECIMAL/TIMESTAMP 列排序、渲染上限提示行、遮罩幂等、节点切换刷用户页、弹窗错误行)。

## [0.7.0] - 2026-09-29

### 缺陷审查轮 13:随机抽 20 功能点(2026-09-28)

抽样的 20 个功能点:GUID 主键、MERGE、INSERT…SELECT、TRUNCATE CASCADE、RENAME/DROP COLUMN、
CREATE VIEW、DROP INDEX、SELECT DISTINCT/ROWNUM/DUAL、CASE、T-SQL 字符串扩展、哈希族、
T-SQL 转换、prepared statements、用户/角色/GRANT/REVOKE、Web 用户页、语句超时、
docsql_pubsub 视图、PITR、Web 对象树/meta/stats、EF Core 同步与翻译。

#### 修复(安全/授权)

- **MVCC 读层授权不展开视图基表(权限绕过)**:SELECT 走读层快路径时资格预检传 `db: None`,
  视图基表展开被跳过——readonly/readwrite 用户经「视图 over `docsql_users`」(旧卷 dump 回放
  遗留的合法视图)直接读到**密码哈希**(写层同样语句拒绝,读层漏了)。预检现在持微秒级读锁
  传 `Some(&db)`,与写层同一套展开逻辑。
- **事务内 GRANT/REVOKE 未提交即生效、回滚不恢复**:引擎按语句成功即抬授权纪元,事务内的
  useradmin 写被缓冲,但逐帧权限刷新在引擎写锁下读**未提交行**(提前吊销/提前授予);ROLLBACK
  恢复成员行却不回抬纪元,吊销/授予效果常驻到下一次用户管理写。现在 useradmin 语句在显式
  事务内显式报错(join/restore 的快照回放不经该路径,不受影响)。

#### 修复(结果错误/数据完整性)

- **TRUNCATE/DROP/全表重写泄漏溢出链页(违反红线 1)**:活文档的溢出链页只由槽头指针锚定,
  清空/删表/重写只释放堆页与 `overflow_free`——大文档表的 TRUNCATE churn 让文件无界增长
  (实测 5 轮 9→29 页),join/repair/恢复同中招。新增 `heap::free_overflow_chains`(先按读路径
  同标准全量校验再释放),三条释放路径全部接入。
- **RENAME COLUMN 悬空外键(备份/恢复断裂)**:被引用列改名后,子表 FK 的远程列名(rc)不跟随
  ——子表一切非 NULL 插入永久 FK 报错,`dump_script()` 回放中断(join 快照/恢复全断)。现在
  同表 FK 的 rc 与跨表引用方的 rc 一并改写(与 RENAME TABLE 改写 rt 同规则、同失败恢复次序);
  同语句 `RENAME a TO b, DROP b` 组合的守卫也按演化后拼写匹配。
- **RENAME COLUMN 把 CHECK 字符串字面量当标识符改写**:`CHECK (name <> 'name')` 改名列
  `name` 后约束变成 `title <> 'title'`——插入 'name' 由拒变收、'title' 反被拒,坏文本经
  catalog 持久化并随复制逐节点一致腐蚀。字面量改写现在按 `sql_literal_end` 跳过字符串区间
  (与绑定/脱敏/词法同源扫描器)。
- **RENAME COLUMN 落到已有动态字段名静默覆盖数据**:schemaless 文档里已有 `extra` 字段时
  `RENAME COLUMN a TO extra` 把两列数据合并(动态字段版「重复列名静默覆盖」),现 validate
  阶段扫描实测文档键响亮拒绝。
- **DROP COLUMN 不查视图依赖**:视图按名列引用的列被静默删除,视图 WHERE 读 NULL 恒假返回
  空集而 dump 回放「看起来成功」。现在按视图 SQL 的 literal-aware 词法扫描拒绝(未引用列照常删)。
- **MERGE ON 目标限定缺列静默取源值**:`ON s.tag = t.tag` 中目标没有 `tag` 时,`t.tag` 经
  CompoundIdentifier 裸名回退解析到**源的值** → 恒匹配 → 目标行被错误 UPDATE。合并行现在
  同时登记目标限定键,目标缺列钉为 NULL。
- **同语句「显式自增 id == 语句开始水位 + 省略 id 行」整句撞 UNIQUE**:INSERT 多行 VALUES
  与 MERGE INSERT 臂的显式 id 不在行循环内推进水位,后续省略 id 的行复用同值报错。显式
  `Value::Int` id 现按 mainstream 语义推进水位(`INSERT (3),(NULL)` 得 3、4)。
- **INSERT … SELECT 源为空整句报错**:`WHERE 1=0` 过滤后 0 行曾报「INSERT has no rows」
  (该检查本意护 VALUES 路径);现 SELECT 源 0 行 = Affected(0)(主流语义),空 VALUES 仍报错。
- **CAST/CONVERT 到 DATE 目标原样穿透**:`CAST('2026-09-28' AS DATE)` 返回**普通文本**
  (TYPEOF=text,wire 无 `$ts` 标记),非法日期文本也静默通过。现按 T-SQL 语义转换并截断到
  UTC 零点,非法文本响亮报错(TRY 变体返 NULL)。
- **BLOB → 数值目标静默穿透**:`CONVERT(INT, x'…')` 原样返回 BLOB,后续比较/算术永远错位
  (`WHERE CONVERT(INT, x'01') = 1` 静默 miss)。INT/FLOAT/DECIMAL 三臂对 Bytes 显式报错。
- **QUOTENAME 六个合法定界符返 NULL**:`]` `<` `>` `{` `}` 反引号按 T-SQL 文档表应有效
  (闭合定界符双写),此前落入 `_ => NULL`;第二参为 NULL 现返回 NULL(此前当「未提供」
  回退默认 `[`)。
- **CONCAT_WS 分隔符 NULL 语义反了**:T-SQL 沿 CONCAT 家族 NULL 当空串(`CONCAT_WS(NULL,
  'a','b')` = 'ab'),此前返回 NULL。
- **STRING_ESCAPE json 不转义 solidus**:T-SQL `a/b` → `a\/b`,逐字节比对/哈希的应用静默失配。
- **CONVERT style = NULL 被当「未提供」**:T-SQL 明文「For a style value of NULL, NULL is
  returned」,此前继续按默认 style 转换出值。
- **写语句谓词里的 ROWNUM 静默全表命中**:UPDATE/DELETE/MERGE 自身 WHERE/SET/ON 无行流可
  编号,ROWNUM 读 NULL 且 `NULL <= n` 恒真——Oracle 风格 `DELETE … WHERE ROWNUM <= 1` 会
  **清空全表**。现响亮拒绝(含 SET 赋值;子查询内的 ROWNUM 是子查询自己的行流,照常可用;
  refs_rownum 顺带补齐 BETWEEN/IN/LIKE/CASE 递归面)。
- **兼容字典视图名可被同名表占用(建得出读不回)**:`CREATE TABLE user_tables` 成功、写入
  成功,SELECT 永远返回字典行(FROM 解析字典视图优先)。`DUAL`/`sqlite_master`/
  `information_schema.*`/Oracle 字典视图名现于 CREATE TABLE/VIEW 保留拒绝。
- **`docsql_pubsub` 同名用户表被视图劫持**:上轮只修了 `docsql_log`,pubsub 改写点仍无条件
  换表——同名用户表写入成功、SELECT 静默返回消息行。现与 docsql_log 同规则:catalog 有同名
  用户表则不改写。
- **docsql_pubsub 视图在 T-SQL 批内失效**:批语句经 BatchPipeExec 直接执行、不过顶层改写臂,
  批内 `SELECT … FROM docsql_pubsub` 报「表不存在」。批执行器现在逐语句跑同一改写(带同名
  用户表守卫)。
- **CTE 名被误判视图自引用**(walk_query 分类面):`WITH v AS (…) SELECT * FROM v` 的 FROM v
  读的是 CTE,读取目标分类/授权基表收集不再把被本层 WITH 遮蔽的名字当基表。
- **EF 字符串 Contains 漏转义 `[`**:常量翻译转义了 `\ % _` 但漏 `[`,引擎按 T-SQL 字符类
  解释 `[..]` → `Contains("gam[ma]")` 漏报正主、误报字符集内行。补 `\[`(与已转义的 %/_
  同为 .NET 字面子串语义)。
- **$float 非有限浮点参数绑成裸标识符**:REQ_EXECUTE 的 `{"$float":"NaN"}` 渲染为裸 `NaN`
  文本,引擎当列引用(SELECT 返 NULL / INSERT 报未知列)。现按 `value_literal` 同形渲染
  `CAST('NaN' AS REAL)`。
- **PITR `to` 早于基准备份自身快照静默降级**:重放「基准(状态@快照时刻)+ 增量(全部被
  过滤)」= 把目标之后的写全部复活还报成功(轮 7「链缺失静默降级」修复的镜像缺口)。现于
  restore 前比对 header ts,早于基准响亮拒绝。

#### 修复(健壮性)

- **语句超时采样补齐两段**:表值函数物化(GENERATE_SERIES 100 万行级,物化段曾完全不设防,
  过冲可达预算 ~2×)现在逐行采样(`table_function_checked` 贯穿 STRING_SPLIT/OPENJSON/
  GENERATE_SERIES);CREATE INDEX 的 B+ 树回填循环(build_trees)补 `check()`。
- 上一轮已记录、本轮确认未复发:复制态 PUBLISH 绕过 sync gate 窗口、多表 TRUNCATE 逐表独立
  事务(风险>收益,维持已记录)。

#### 文档

- `sql-reference`:INSERT…SELECT 无列清单按位置映射(原「取查询输出列」与实现矛盾);
  视图干跑校验改为「基表必须存在,列按 schemaless 语义」(原「列必须存在」在 schemaless
  模型下不可判定);ROWNUM 写语句拒绝;兼容字典视图保留名;useradmin 事务内拒绝。
- `drivers.md`:`List.Contains` 翻译目标 `IN (…)` → `JSON_ARRAY_CONTAINS`(文档漂移)。

#### 测试

- 新增回归:engine 12 项(自增水位/空源/溢出链释放/MERGE 目标键/RENAME FK·字面量·动态字段/
  DROP 列视图守卫/CTE 遮蔽分类/ROWNUM 写拒/保留名/DATE·BLOB cast)、tsql 3 组(QUOTENAME
  定界符矩阵+NULL、CONCAT_WS、CONVERT style NULL + STRING_ESCAPE solidus)、server e2e 6 项
  (读层视图授权/$float 绑定/useradmin 事务拒绝/pubsub 同名表/批内视图/PITR 早于基准)、
  dotnet 1 项(LIKE `[` 转义)。

### 缺陷审查轮 12:随机抽 20 功能点(2026-09-27)

#### 修复(结果错误/数据完整性)

- **OUTER JOIN 空侧补显式 NULL 键**:LEFT JOIN 右表为空(或 RIGHT/FULL 左表为空)时,
  另一侧同名列经后缀回退**镜像对方表的值**——`SELECT a.id, b.id … LEFT JOIN b`(b 空)返回
  a.id 的值,`WHERE r.id IS NULL` 反连接在空右表时返回 0 行。现在空侧按表列清单补 NULL 键。
- **OUTER APPLY 空行集同因修复**:保留的左行补右列 NULL 键(此前同名左列冒充右值)。
- **表函数别名列改名落地**:`FROM STRING_SPLIT(..) AS s(v)`、`FROM/APPLY OPENJSON(..) AS j(k,v,t)`
  此前静默丢弃改名清单(`s.v` 读 NULL、`s.value` 仍有效);现按位置改名,宽度不符响亮报错
  (含 STRING_SPLIT 第三参关闭 ordinal 时两列清单报错)。
- **MERGE 的 AUTOINCREMENT 水位**:INSERT 臂显式给 id 或 UPDATE 臂 `SET id` 此前不推进/失效
  水位,下一次普通 INSERT 在显式值上撞 UNIQUE 报错;纯 `WHEN NOT MATCHED INSERT` 臂多行命中
  同一目标行此前误报「cannot update the same row twice」(没有 UPDATE 臂即无「两次更新」)。
- **ON CONFLICT DO NOTHING/OR IGNORE 的 FK 孤儿**:子侧外键此前对「过滤前的整批」校验,
  被跳过的父候选为同批子行背书,落盘即孤儿;现按最终落盘集统一校验(批内前向引用一并合法)。
- **幽灵约束拒绝**:表级 `PRIMARY KEY(b)`/`UNIQUE(b)`(b 不存在)、`UNIQUE(LOWER(a))`、
  `CHECK (y > 0)`(y 不存在)此前静默建成永不生效的约束(dump 还会丢 PK 声明),现 DDL 报错;
  表级复合 FK 此前静默拆成逐列 FK(削弱为各列独立判定),现显式报错;列级 FK 多被引列同理。
- **INSERT/CTAS/MERGE INSERT 重复列名报错**:此前 `INSERT INTO t (a, a, b)` 的首个值被
  `zip().collect()` 静默覆盖。
- **ADD COLUMN**:`DEFAULT` 回填值现在过 CHECK(含本语句新增的 CHECK,此前存量行可永久违反);
  `GENERATED ALWAYS AS` 显式报错(此前静默降级为普通列)。
- **标量函数 NULL 传播**:`UPPER/LOWER/TRIM/LTRIM/RTRIM/LENGTH/LEN/INSTR/LPAD/RPAD` 此前把
  NULL 静默转成 `''`/`0`/报错(污染 `COUNT(expr)`/`SUM(LENGTH(..))` 聚合、`WHERE UPPER(v) IS NULL`
  永不命中);现按 SQL 语义返回 NULL。
- **SUBSTR 对齐 SQLite 语义**:起点 0 位于虚拟首位(`substr('abcde',0,2)` 得 `'a'`,此前多取
  一位)、负长度取起点之前的字符(`substr('abcde',2,-1)` 得 `'a'`,此前返回空串)。
- **SUM 单元素也校验数值性**:单行 BLOB/文本的 `SUM` 此前原样返回该值,第二行到达才报错。
- **BLOB 文本化走规范形**:`CAST(x'0102' AS TEXT)`、`||` 拼接、`GROUP_CONCAT` 此前产出
  Rust Debug 形态 `Bytes([1, 2])`,现为 `x'0102'`。
- **B+ 树键上限计入页头**:cell 上限此前未含节点头(3/7 字节),三个接近上限的键分裂时左右
  两半都放不进一页,合法插入被误报「key too large」;上限收紧为 `(PAGE_SIZE-头)/2` 后分裂恒有解。
- **HAVING 支持谓词形态**:`HAVING k IS NULL`(ROLLUP 筛总计行的惯用写法)、`LIKE`/`BETWEEN`/`IN`
  此前被列引用白名单误拒(求值器本就支持)。

#### 修复(会话/运维)

- **REQ_EXECUTE(参数化 INSERT)喂会话身份**:.NET 参数化命令走的 prepared 路径此前不更新
  `@@IDENTITY`/`SCOPE_IDENTITY()`(滞留上一条快路径 INSERT 的旧值,拿错自增 id 关联外键即错数据);
  MERGE 的 INSERT 臂此前同样不喂(快路径与解释器皆是)。
- **`docsql_log` 视图不再劫持同名用户表**:用户建 `docsql_log` 表后写入走真表、SELECT 却被
  审计视图替换(客户端读到与自己写入不一致的数据);现 catalog 有同名表时视图让位。
- **redact_sql 跳过字符串字面量**:字面量内的 `PASSWORD` 字样此前被过度脱敏,审计日志记录的
  语句文本与实际执行的不一致(方向是多脱不是泄漏)。
- **DROP VIEW/DROP INDEX 的 catalog 回滚**:catalog 写页失败(存储 IO 错)时此前内存目录已
  被改,下一次成功写会把半删除静默持久化;现与 DROP TABLE 同样快照回滚。
- **语句超时采样补齐**:无 WHERE 的 SELECT(扫描/投影/DISTINCT)、GROUP BY 分组循环、
  UPDATE/MERGE 的 CHECK+FK 校验循环此前零采样,`DOCSQL_STATEMENT_TIMEOUT_MS` 对这些形态
  打不断。

### 相关子查询支持(2026-09-27)

#### 新增

- **相关子查询落地(限定名、行级表面)**:`EXISTS`/`NOT EXISTS`、相关标量、`IN`/`NOT IN`、
  `ANY`/`ALL` 的外层引用(`别名.列`/`表名.列`)在 SELECT 的 `WHERE`/投影/`ORDER BY` 与
  UPDATE·DELETE 的 `WHERE`、UPDATE 的 `SET` 逐行绑定外层行值求值;内层 FROM 同名别名正确
  遮蔽外层,嵌套相关深度上限 8(响亮报错)。此前外层别名引用静默得空结果、外层表名引用显式拒绝。
  EF Core 导航集合 `Any()`(默认生成相关 `EXISTS`)自此端到端可用。
- 边界:JOIN ON/GROUP BY/HAVING 内的相关引用、`APPLY` 子查询形式仍显式报错;
  未限定名的外层引用在 schemaless 下与缺列不可区分,按缺列语义读 NULL(请写限定名)。

#### 修复

- 写语句(UPDATE/DELETE/MERGE)子查询内的 `NEWID()`/`NEWSEQUENTIALID()`/`RAND()` 此前
  绕过拒绝——表达式扫描不递归子查询,各重放节点会各自掷随机值导致集群分叉;现已拦截。

### 全库缺陷审查轮 1–11 与工程加固(2026-09-21 ~ 2026-09-26,发布补记)

v0.6.0 与缺陷审查轮 12 之间落地的批次,发布切版时补记汇总;逐项明细见对应提交信息。

#### 修复

- **首轮全库审查 + T-SQL 缺陷批次(26+ 项)**:两个 DoS 级挂死、索引/探针静默错行、
  FK 语义、ROLLBACK 崩溃一致性;T-SQL SCOPE_IDENTITY 会话隔离、RAISERROR 消息提取、
  PIVOT 分组桶化、RAND 语句级折叠;
- **存储底层(轮 1–2、11,共 18 项)**:WAL 截断 epoch 撕裂窗口、abort_deferred 丢
  txid、fsync 失败 fail-stop 毒化、B+树分裂点误报 KeyTooLarge、JSON Float 往返降级
  Int(集群分叉)、孤立代理项前瞻、collect_pages 双入池等;
- **engine 读/写路径(轮 3–4,共 30 项)**:集合操作/嵌套查询/JOIN ON 的相关子查询
  漏检、探针静默错行、RENAME COLUMN/表 毁数据与半应用、MERGE 两臂 FK 终态校验、
  重名 SAVEPOINT 服务端对齐、ALTER 全操作预验证等;
- **T-SQL 层(轮 5–6a,共 25 项)**:`[方括号]` 列名被 wall-clock 折叠器当函数静默
  写坏数据、STR/FORMAT 宽度 panic、块扫描器误吃 CASE 的 END/BEGIN TRAN、IF/WHILE
  单语句体派发、CLI 词法态 CASE 深度等;
- **用户/授权(轮 6b,7 项)**:RENAME TABLE 同场改写 grants(旧授权复活)、
  GRANT/REVOKE 表名大小写原形、授予视图的 SELECT 边界、StoredPw 迭代数下界等;
- **server/backup/pubsub(轮 7,10 项)**:预认证 PING 超预算断连(slowloris)、
  T-SQL 批逐条查询日志审计、回退读脏读复检、PBKDF2 认证闸门 60s 截止、PITR 链
  完整性审计、备份 fsync 后 rename 等;
- **web/auth/cli(轮 8,7 项)**:CLI 把字符串字面量内的数据行当协议命令执行、外部
  文本 ANSI/C0 消毒、控制台批量 SQL 64MiB 结果预算、web TLS 握手 30s 截断等;
- **proto/crypto/启动(轮 9,6 项)**:布尔 env 严格解析(`READ_ONLY=1\r` 曾静默失效
  为可写)、DOCSQL_KEY 逐字节严格 hex、全零密钥拒绝启动、读缓冲守卫补 20 字节帧头等;
- **dotnet/横切一致性(轮 10,12 项)**:服务端 `$float` 参数解码(NaN/±∞ 绑定曾
  污染列)、keyed 握手共享超时预算(黑洞对端不再无限挂起)、ulong 走 `$dec` 精确
  标记、SchemaSync 并发 duplicate column 容忍、ALTER 演化索引态预演等;
- **T-SQL 会话状态(2 项)**:单句 SCOPE_IDENTITY()/IDENT_CURRENT() 路由判据漏识别
  (撞响亮报错),OPENJSON(NULL) 改返回空行集(APPLY over 稀疏 JSON 列不再拖垮整句)。

#### 安全

- **传输加密 nonce 加固**:nonce 改 8 字节进程前缀 + 4 字节计数器 + 2^32 硬上限——
  旧 4 字节前缀的生日界落在进程诞生数上(同 key 下约 6.5 万次重启即 ~50% 前缀碰撞,
  碰撞进程从首帧起复用 nonce 致密钥流 XOR 泄漏);越上限响亮报错绝不回绕,混版本
  第二帧起拒收。

#### 变更(工程)

- **镜像轻量化 159MB → 90MB → 31.6MB**:先 strip 符号表 + distroless 基座,后运行层
  改 `FROM scratch`(镜像 = 三个静态二进制 + 固定 nsswitch.conf,glibc 静态链);
  compose 健康检查改 CMD-exec 形态,容器内检查一律走 CLI / `docker cp`;
- long_read e2e 时序断言去脆弱化(写突发改比值制判定 + 读负载结构化扩到 4M 对);
- 文档新增 T-SQL 数据操作实例(窗口函数 top-N、MERGE 对账、PIVOT/UNPIVOT、APPLY
  拆 JSON、递归 CTE 路径、UPDATE FROM/DELETE USING、循环事务批共 8 组实测)。

## [0.6.0] - 2026-09-21

### 文档重组(2026-09-21)

#### 变更

- **README 精简重写**(323 → 128 行):首页只保留定位、快速开始、特性一览与文档导航,
  细节下沉至 `docs/` 各手册;补上此前遗漏的窗口函数能力与许可证说明;
- **等保 2.0 对照表迁至 `docs/security.md`**(原仅存于 README,现为其正式归属,
  标题「等保 2.0 对照」);NuGet 包源 `nuget.config` 配置方法迁至 `docs/drivers.md`
  安装节(原与 README 互相指向,配置内容两处皆无);
- 修复全库文档中 11 处死链或错链:旧 README 锚点 ×6(等保章节/.NET 安装/Docker 部署
  等已随重写移位)、四个 NuGet 包 README 的 GitHub 锚点 ×4、
  `features.md#8-复制与集群` 章节错号 ×1;
- `docs/README.md` 索引补上 `docs/design/` 三篇设计文档与 CHANGELOG / SECURITY 入口;
  AGENTS.md 文档分工补全(安全/运维/驱动/边界/设计的归属落点),环境变量完整清单
  指引从 README 改指运维手册。

### 功能缺口审查修复(2026-09-17)

#### 修复

- **`RETURNING *` 落地**:SQL 参考与功能总览一直声明支持,DML 实现却只认
  表达式/别名,INSERT/UPDATE/DELETE 带 `*` 一律报「unsupported RETURNING
  item」。现按 `SELECT *` 的动态投影展开(变更文档的字段并集、按名排序,
  `RETURNING *, expr` 把显式投影追加在星号列之后);
- **子句级静默忽略清零**:`CREATE TABLE ... INHERITS`/`WITHOUT ROWID`/
  `ON COMMIT`/`LOCATION`/`STORED AS`/`CLUSTERED BY`/Hive-Redshift 分布与分区、
  列/表级 `DEFERRABLE`/`INITIALLY DEFERRED`/`NOT ENFORCED`/
  `NULLS NOT DISTINCT`/`MATCH FULL|PARTIAL` 此前被解析后丢弃,现全部显式报错;
  等价的空操作写法(`NOT DEFERRABLE`/`INITIALLY IMMEDIATE`/`ENFORCED`/
  `MATCH SIMPLE`/`NULLS DISTINCT`)继续接受;
- **删除/清空的依赖完整性**:`DROP TABLE`/`DROP VIEW` 此前会留下指向已删对象的
  悬空视图(下次 SELECT 才报错),现在默认拒绝并列出依赖视图,`CASCADE` 连带
  删除(含视图套视图);`DROP TABLE ... CASCADE` 同时从引用子表移除指向该表的
  外键声明(子表保留);`TRUNCATE ... CASCADE` 现在真正连带清空外键子表
  (传递闭包;此前 CASCADE 被忽略、仍按 RESTRICT 报错);删除视图同步清理其
  授权记录(同名重建不再继承旧权限);`DROP ... PURGE`、
  `TRUNCATE ... CONTINUE IDENTITY`/`ONLY`/分区/`ON CLUSTER` 显式报错
  (此前部分被忽略);
- **文档同步**:SQL 参考补上缺失的 CREATE VIEW / DROP VIEW 章节、TRUNCATE/DROP
  依赖与 CASCADE 语义、CREATE TABLE 拒绝清单;修正「DROP VIEW / CREATE VIEW
  不支持」等过期描述与功能总览中表级复合 PK/UNIQUE、无 GROUP BY 的 HAVING
  等漂移条目。

### MVCC 快照跨纪元历史补全(2026-09-17)

#### 修复

- **消除跨 checkpoint 快照的伪 `SnapshotTooOld`**:此前快照在 WAL 截断
  (checkpoint)之后开始、期间又有「截断前最后写入的页」被新提交覆盖时,
  该页的 as-of 像只存在于内存读面,覆盖后纪元内 WAL 无处可寻,快照读
  只能报「历史早于 WAL 保留窗口」——尽管快照开始时该像明明可见。现在
  提交在覆盖此类页之前,把旧像直接存入全部同纪元活跃快照的缓存;快照
  注册与 commit-head 读取在 WAL 锁内原子,保证需要的快照必已注册。真正
  跨纪元被截断的快照仍响亮报 `SnapshotTooOld`(硬阈值流控语义不变),
  长查询在写密集库上不再对冷页频繁误报。

### 全库审查第六轮(2026-09-17,视图 / TIMESTAMP / PITR / 客户端加固)

#### 修复

- **视图(安全/正确性)**:`CREATE OR REPLACE VIEW` 可构造目录环,随后的
  `SELECT` 无界递归直至栈溢出打崩进程(DDL 随复制扩散即全集群中招)——
  现在建视图做传递闭包环检测,视图展开另设 16 层预算(建得成必查得了);
  `SELECT COUNT(*) FROM <视图>` 恒返回 0、`ORDER BY … LIMIT` 恒返回空集
  (堆快路径误吃视图空存储)——现回退展开;`MERGE INTO <视图>` 漏过写守卫
  会向视图私有堆写不可见僵尸行——现显式报错;`DROP VIEW a, b` 中途失败
  前半已提交、`TRUNCATE a, b`/`DROP INDEX` 中途失败留半应用——现全部
  先校验后动手;dump 中视图按字母序输出导致「视图套视图」的备份无法恢复
  ——现按依赖拓扑排序;
- **TIMESTAMP(正确性)**:时区偏移含多字节字符(`+1中`)触发切片 panic
  ——现按字符校验返回 NULL/报错;值域收口为 0001-01-01..=9999-12-31
  (UTC 毫秒),CAST 整型/`$ts` 线标记/算术结果超域即报错或 NULL,
  不再产生 `value_literal` 无法重放的值(曾让含该值的整库备份无法恢复);
  `BETWEEN`/`IN`/`CASE`/`IS [NOT] DISTINCT FROM` 与字符串字面量的时间
  比较现在与 `=`/`<` 走同一提升漏斗(此前静默判假丢行);复合索引
  (前导或后置 TIMESTAMP 列)的裸字符串边界探针现在逐元素提升
  (此前探空带静默漏行,含 UPDATE/DELETE 快路径);标量子查询内联补
  DECIMAL/TIMESTAMP/BLOB 三类字面量(此前整条查询报错);
- **非确定值(红线加固)**:wall-clock 折叠器跳过 `--`/`/* */` 注释
  (注释里的撇号曾让合法语句改写错乱)、容忍 `NOW ()` 空格拼写
  (此前逃过折叠,各副本各盖各的章静默分叉);`DEFAULT NOW()` 不再被
  DDL 折叠冻结成建表时刻——建表保留调用文本,插入时逐行定值并把
  解析后的显式值回写进期刊(与 auto-GUID 同机制);`CHECK` 含
  wall-clock 调用与 MERGE 应用 wall-clock DEFAULT 均显式报错;
- **PITR(数据完整性)**:restore-to 重放链现在逐条过滤 `seq > base_seq`
  并做断链审计——增量段与基准备份错位(同 tick 导出顺序)或期刊窗口
  裁剪/段剪枝造成的空洞,不再表现为「双重重放撞主键」或「静默漏写」,
  而是响亮报错并提示重拍全量;导出游标取增量段与基准的较大者;
  `restore` 的 `to` 参数先解析后置位(坏类型不再把本节点及全集群的
  备份/恢复面永久卡死),不可解析的时间戳显式报错而非静默退化为全量
  恢复;`export` 补 admin 与只读门禁及审计;
- **运维健壮性**:期刊(`_cluster_log`)新增 512MB 字节上限(与条数窗口
  并行,大文档负载/`CATCHUP_WINDOW=0` 不再无界增长);pub/sub 扇出接入
  对等节点熔断退避(分区节点不再拖慢每次 PUBLISH);join 的 REQ_HOLD
  地址校验与上限(免 token 兼容模式下不可再注入垃圾地址拖累扇出);
  `docsql_pubsub` 视图改写尊重标识符边界(名为 `docsql_pubsub2` 的用户表
  不再被误改写);
- **.NET 客户端**:同步读超时现在像异步路径一样标记连接 `Broken` 并关闭
  socket(此前超时后复用连接会把上一条语句的迟到应答错配成下一条的
  结果——静默错数据);非有限浮点(NaN/±∞)参数与读取双向走
  `$float` 标记;订阅器超时投毒后读线程停止投递(兑现 OnError 契约);
  `DbDataReader.IsClosed` 随 Dispose 置位;连接串 `port` 非法值报清晰
  错误;新增 `timestampformat=ts|iso` 连接串开关(默认 `ts`;
  `iso` 用于连接 79a94ba 之前的旧服务端或存量 ISO 文本时间列的过渡期);
  EF 模型声明复合主键或降序索引时显式报错(此前分别静默建成无约束
  堆表/反向索引);引擎对 `CREATE INDEX … DESC` 显式拒绝(此前静默建 ASC);
- **CLI 与控制台**:`--csv` 导出对以 `= + - @` 等开头的单元格加 `'` 前缀
  (CSV 公式注入防护);远程 shell 增加响应超时(`DOCSQL_CLI_TIMEOUT_MS`,
  默认 600s,0 关闭),卡死服务端不再挂死脚本;`/metrics` 抓取走探针预算
  (楔死节点报 `docsql_node_up 0`,不再拖垮整个抓取);控制台「编写表脚本」
  生成 DDL 一律加引号、注释内表名剥离换行;控制台拒绝以
  `$pbkdf2-sha256` 开头的密码(该前缀保留给引擎哈希回写,曾把账号
  静默变砖);改密码的凭据写失败不再回传宿主路径细节。

### PITR / 精确 TIMESTAMP / 用户视图(2026-09-16 三大特性)

#### 新增

- **精确 TIMESTAMP 类型**:`TIMESTAMP '2026-09-15T08:30:00Z'` 字面量、
  `NOW()`/`CURRENT_TIMESTAMP`、`CAST` 双向(ISO 文本/UTC 毫秒整数)、
  `TIMESTAMP ± INT`(毫秒)与 `TIMESTAMP - TIMESTAMP`(时长);
  与字符串比较自动按时间解析(不可解析 → NULL);索引探针自动把可解析
  字符串边界提升进时间带(混合序带列需显式 CAST);wire 以
  `{"$ts": 毫秒}` 无损承载;写语句中的 `NOW()/SYSDATE()/CURRENT_TIMESTAMP`
  在执行节点折叠为字面量后进期刊/扇出(副本重放零偏差,红线 #8 落地);
  .NET 驱动 DateTime/DateTimeOffset 参数与 EF 列类型映射同步切换为
  TIMESTAMP(毫秒精度;存量 ISO 文本列读取兼容);
- **PITR / 增量备份**:期刊(带提交时间戳)无条件记录(单节点也记);
  全量备份头部锚定 journal-seq;增量段 `incr-*.sql` 按游标导出期刊条目
  (REQ_BACKUP `{"action":"export"}`,定时任务每 tick 自动执行);
  `{"action":"restore","file":…,"to":"<ISO/毫秒>"}` 重放
  基准备份 + 期刊链中提交时间 ≤ 目标 的条目,恢复后照常收敛验证;
- **用户视图**:`CREATE VIEW / CREATE OR REPLACE VIEW / DROP VIEW`;
  SELECT 穿透(视图套视图、外层 WHERE/ORDER BY/LIMIT 组合);
  建视图时干跑校验 + 自引用检查;写/DDL 对视图显式报错;
  dump/恢复/集群复制全链路携带视图;授权:读视图只需视图授权(权限收口),
  写语句经视图读源需基表授权(fail-closed 展开)。

### 银行级合规增量(2026-09-16)

#### 修复

- **EF SchemaSync 吞错收窄**:「表已存在/只读副本」判定由子串扫描改为锚定
  引擎确切错误文本 —— 任何恰好含这两个词的无关失败(权限/磁盘/语法)不再
  被伪装成「建表失败已忽略」;
- **备份完整性可观察**:恢复旧版无 `.sha256` 校验和的备份时容忍不变,但
  响亮告警到 stderr(此前操作者无法区分「已验证恢复」与「无法验证恢复」);
- **DOCSQL_MAX_CONN 默认从无限制改为 1024**:连接是 tokio 任务 + 读缓冲
  预算 + 写通道,默认无上限曾让连接洪泛直达 fd 枯竭;显式 `=0` 恢复不限
  (README/启动帮助同步)。

#### 测试与证据

- 新增银行场景合规测试(账务语义端到端):DECIMAL 转账原子性与分毫不差、
  事务回滚与语句中途失败保持账目不变、MVCC 快照读任何时刻不见「撕裂转账」
  (旧快照整段可见旧状态)、崩溃(COMMIT 前被杀)后转账绝不落一半;
- 新增审计在场 e2e:账务 DML 与 PUBLISH/TRIM 操作全部留痕于 `docsql_log`
  且库内可检索;
- 性能采数(宿主负载 6.93/8 核,Android 模拟器并发,绝对值仅作环境注记):
  bench6 同进程交替 A/B,逐条 fsync 416-421 行/秒 vs 异步组提交
  17201-20101 行/秒(~45×);本提交未触碰 pager/WAL/执行器热路径,
  既有基线(同语句点查 6.3µs 等)不受影响。

### 金融级加固批次(2026-09-16 全库审查第五轮)

#### 修复

- **ROLLBACK TO 后全量回滚复活已丢弃修改(数据正确性,P0)**:回放
  ROLLBACK TO 的 undo 尾巴时,「事务内第一次被写且晚于该保存点」的页会把
  当前脏像误记为预像;随后的 ROLLBACK 将已回滚的修改**复活并持久化**(且
  WAL fsync 落盘、重开后即成事实)。回放期间现已抑制 undo 记账;顺带修复
  同根的页池搁浅(Alloc 补偿后页号永久泄漏直至重启);
- **pub/sub 回放/实时重复投递(P0)**:边发边订场景下,回放期间发布的消息
  会「实时推一次 + 回放一次」系统性重复(消费者双计)。现改为追赶期保持
  注册即全抑制(回放是唯一投递路径),仅在结束时以注册表锁内采样的水位
  原子放开实时——时序上不丢不重;新增 1500 条积压 × 200 条并发的
  多窗口回归;
- **回放伪停顿**:回放帧以 `try_send` 热循环灌满深度 32 的写通道,writer
  任务尚未调度即被判「消费者停顿」,整场追赶每 ~32 帧中断一次,客户端被迫
  反复重订阅;改为有界阻塞发送(活跃消费者微秒级排空,真死连接 500ms 内
  照旧判死),并发回归套件 34.6s → 9.7s;
- **SUM 整数溢出回绕**:标量 `+` 已是溢出出 NULL,SUM 仍是 wrapping——
  `SUM` 过 i64 上限静默变负数(账面错数);现与标量语义一致;
- **CAST(DECIMAL/Float AS INT) 溢出**:超界 Decimal 曾静默归 0、超界
  Float 曾饱和到 i64::MAX;现显式报错(区间内截断不变);
- **RENAME TABLE 失败丢表**:catalog 持久化 I/O 失败时旧名已删、新名未立,
  表从目录消失、数据页孤儿;现恢复旧名目条目;
- **MERGE 无树老文件缺唯一回退**:约束列早于索引树的 legacy 表上,
  MERGE 的 matched-update 可写入重复业务键(INSERT/UPDATE 均有整表回退);
  现补齐同款回退;
- **中止事务的新增页号泄漏**:autocommit 失败语句触发页增长时,新页号不
  回池、文件只涨不缩;现随 abort 归还复用;
- **提交点之后的 flush/checkpoint 失败误报**:WAL fsync 已过即提交点,
  此后数据文件写失败曾让语句报错——客户端重试即重复入账;现降级为后台
  重试 + stderr 告警,不改语句返回值;
- **DROP USER 后活连接降级匿名**:授权刷新发现账号已删即关闭连接,不再
  以无身份会话继续(token-less 兼容模式下该会话可答节点帧);
- **ADO.NET 同步路径忽略 CommandTimeout**:同步读超时在构造时定格 30s,
  每语句设置的 CommandTimeout 静默无效;现 setter 即时生效到 socket;
- **订阅器默认 keepalive 与读超时贴面**:30s PING 周期 = 30s 读超时,
  安静频道上必然偶发断订;默认改 10s(3 倍裕量);
- 控制台:语言切换后标签页标题双重转义(表名含 `&<>'"` 显示成实体);
  对象树错误横幅中节点名未转义(纵深防御补齐)。

#### 安全

- **REQ_SQL_SEQ 来源上限**:node_id 是帧载荷可控字符串,任意已认证连接
  (legacy 模式)可用随机 origin 无限膨胀 apply_locks 与持久化位点表;
  现 256 来源 / 128 字节上限,超限显式报错走快照收敛;
- **审计补全**:PUBLISH(频道 + 载荷大小)、PUBSUB TRIM(频道 + keep +
  删除数)、PROMOTE、备份 trigger/restore 均落查询审计 trail(含操作者
  身份;此前仅 SQL 语句有审计,发布/清消息/故障转移无据可查)。

#### 安全边界说明

- Aspire `WithWebConsole` 新增 `secureCookie` 参数(TLS 反代后下发
  `DOCSQL_WEB_COOKIE_SECURE=1`,会话 cookie 带 Secure 标志);
- EF 免迁移同步的列类型边界已在 `docs/limitations.md` 显式文档化
  (引擎为无类型文档模型,列类型不持久化,类型漂移需人工评估)。

## [0.5.0] - 2026-09-16

### Web 控制台多语言(2026-09-16)

#### 新增

- **中文 / English 界面切换**:工具栏地球按钮一键切换,即时生效、不刷新页面
  (已打开的标签页自动重渲染、标签页标题重算,查询编辑器内容原样保留);
  首次进入跟随浏览器语言,手动选择后记忆(localStorage),首帧前应用、无闪变;
  登录/账号门等全部界面文本同步本地化。控制台版本 1.2。

### 审查遗留项收尾:页回收/事务回滚重做/复制韧性(2026-09-16)

#### 优化

- **页回收**:删除/整表重写/表与索引重建归还的页进入可复用池(随事务提交生效、
  abort 归还),TRUNCATE 循环、全表 UPDATE、DROP+CREATE 不再无界增长数据文件;
- **事务回滚重做**:BEGIN/SAVEPOINT 不再深拷全库文档(catalog Arc 快照 +
  页级 undo 日志,ROLLBACK 逆序回放),回滚成本与写入量成正比,不再与数据库
  大小成正比;大表 TRUNCATE/全表删除回滚有回归测试;
- **扇出熔断**:对端传输失败进入指数退避(1s 起步、封顶 60s,退避期限 500ms
  试用预算),对端重启后即时恢复;应用层拒绝不触发;
- **pub/sub 存储自动保留**(20 万行/512MB,按 id 区间删最旧,大载荷自动收缩
  保留深度);查询日志落盘失败改 10s 重试 + 单次告警(不再一次失败永久闭锁);
  期刊单一过大条目显式报错转快照收敛,不再发客户端读不下的帧。

#### 修复

- **加密帧头绑定**:帧加密 seal/open 以帧类型 + 标志位作 AAD(MITM 改标志即
  失效);修复配置 `DOCSQL_KEY` 的集群自身无法通信的缺陷(对端响应从不解封);
  .NET 客户端同步该语义;
- `REQ_STATUS` 新增 user_state(仅建了用户、无数据表的节点也能参与 join 探测);
  只读 token 读取状态/日志/元数据与普通用户同权拒绝;
- CLI:token 支持 `DOCSQL_TOKEN` 环境变量(经 argv 传入打泄漏警告);
  Aspire 健康检查走 IPv6 端点(不再受 DNS 解析影响)。

### pubsub 锁外回放(2026-09-16)

#### 修复

- **SUBSCRIBE 回放不再全程持注册表锁**:from earliest 的大 backlog 曾把每一次
  `PUBLISH`(及其背后 write_order 上的写)卡满整个回放时长;现在注册后锁外
  分块回放,每轮在锁内原子完成 arm_filter + 水位采样,回放/实时仍不丢不重;
  新增并发回归(300 条积压 + 200 条边发边订,慢消费者按缺口重订阅后续传,
  500 条恰好各收到一次且严格递增)。

### 全库缺陷审查修复批次(2026-09-16)

#### 修复

- **延迟提交围栏**:显式事务的语句以 deferred 提交记账,SQL COMMIT 时追加
  fence 后才 fsync;恢复/快照物化只认 fence 之前的 deferred 提交 —— 进程被杀
  不再重放「半个事务」(此前 BEGIN..未 COMMIT 的语句会在恢复后出现);
- **Int↔Float 精确比较**:2^53 以上混合类型比较传递性被破坏(B+树查找/唯一
  判定会漏判),改为精确值比较;FK 比较与 JOIN/`=` 走同一 `cmp_values`;
- **UPDATE/DELETE JOIN 显式拒绝**(曾静默按 CROSS JOIN 语义执行);CHECK 约束
  改真三值逻辑(NULL 误拒/误收双向修复);HAVING 引用非分组裸列显式报错;
  MERGE 走与 UPDATE/INSERT 同一约束门(NOT NULL/CHECK/FK/DEFAULT/auto-GUID);
- **集群选举修复**:按最大组 + 行数 + 摘要序确定性裁决(原「任一 peer 同意即
  收敛」让偶数分裂永不愈合);dump/digest 与客户端事务互斥(快照不再包含未提交
  行);每源 apply 串行化(忙接收方的两个在途写不再乱序);autoinc 水位随
  DROP/RENAME/重建清理(重建表不再让集群分叉);
- **损坏盘页防护**:heap/btree/catalog 对越界区域、指针成环、目录下溢加护栏
  (恶意盘页不再驱动 GB 级分配或栈溢出);WAL 中段 CRC/断号损坏响亮报错而非
  当撕裂尾静默截断(丢后续已提交事务);WAL 追加改定位写,部分写不再吞掉后续帧;
- **web 控制台**:修复原生 HTTPS 下认证端点全部 500(缺 ConnectInfo);凭据文件
  原子替换(崩溃不再留半文件锁死控制台);`/api/backup` 触发必须 JSON 体
  (堵 CSRF);用户管理语句先脱敏再进日志;PBKDF2 遵守迭代数配置且验证不持锁;
- **非有限浮点不再丢数据**:JSON 序列化新增 `$float` 标记(NaN/±Inf 此前序列化
  成 null),`value_literal` 支持结构化值往返(备份/join 不再因 JSON 列直接失败);
- **ADO.NET**:`CommandTimeout` 生效(默认 30s,超时置 Broken 弃用连接,连接池
  不再被半帧连接卡死);订阅器内置 keepalive PING(空闲频道不再被空闲超时断开);
- **EF Core**:Guid/非整数主键不再声明 AUTOINCREMENT;索引校验比较表/列序;
  OwnsOne 同表列集合并(新增属性可被发现)。

### 性能优化批次(2026-09-15)

#### 优化

- **进程级语句解析缓存**:纯语法 AST 无失效语义 + 二次命中门,同语句点查
  实测 -35%;
- **热路径免拷贝免重编码**:heap insert/replace 免整文档深拷贝、btree 叶子
  点查/删除项二分、页字节复用 scratch;pager 页池 FIFO→LRU;release 构建
  LTO(thin)+ codegen-units=1;交替 A/B ≥3 轮实测:批量插入 +4~16%、
  异步提交插入 +11~19%、同语句点查 9.7→6.3µs;
- 容器镜像限制 glibc 线程 arena 上限(`MALLOC_ARENA_MAX=2`)。

### Web 控制台界面批次(2026-09-15)

#### 新增

- **浅色 / 深色主题**:工具栏切换、首帧前跟随系统偏好、手动选择后记忆;
  全量颜色收拢 CSS 变量,尊重 `prefers-reduced-motion`;
- **可拖动布局**:对象资源管理器宽度、编辑器/结果区高度分隔条拖动调整
  (双击恢复默认,尺寸按浏览器记忆);
- 服务器仪表盘新增**索引数量**卡片(含主键/唯一约束自动索引,旧版本节点
  自动回退逐表求和);「关于」弹窗重设计(品牌头 + 能力标签 + 连接与版本信息,
  可复制诊断信息)。

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
