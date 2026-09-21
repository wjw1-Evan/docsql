# 功能总览

DocSQL 能力的完整清单与索引入口。语法细节见 [SQL 参考](sql-reference.md);部署与运维见
[运维手册](operations.md);Aspire 编排见 [Aspire 集成指南](aspire.md);安全见
[安全指南](security.md);边界与不支持项见[已知边界](limitations.md)。

## 1. 运行形态

| 形态 | 说明 |
|---|---|
| Docker 单节点 | 一个 `docsql-server` 容器 + 可选 `docsql-web` 控制台;命名卷持久化 |
| 对等集群 | `DOCSQL_PEERS` 互扇出,任意节点可读写;写入自动复制到全部对等节点 |
| 主从复制 | `DOCSQL_REPLICATE_TO` 指向主节点,副本只读,主写自动转发 |
| 只读副本 / 提升 | `DOCSQL_READ_ONLY=1` 整节点只读;`PROMOTE`(协议帧/CLI/控制台)清除只读 |
| Aspire 编排 | `AddDocsql` / `AddDocsqlCluster`,连接串注入消费项目;见 [Aspire 指南](aspire.md) |
| 嵌入式(开发) | `docsql-cli <file.db>` 直接打开数据文件;运行形态仍以 Docker 为准 |

## 2. 数据模型与类型

- **文档式存储**:每行是一个 JSON 文档(BTreeMap 键序,编码确定);表只声明列名与约束,
  允许写入声明之外的字段(schemaless),`SELECT *` 按数据实测字段投影;
  对象资源管理器区分「声明列」(DDL)与「实测列」(数据)并显示漂移;
- **声明类型作文档/自省用途**(`information_schema` 上报),值模型按实际值分型:

  | 类型 | 说明 |
  |---|---|
  | NULL | 空值 |
  | BOOL | TRUE/FALSE |
  | INT | 64 位整数 |
  | FLOAT | 64 位双精度 |
  | DECIMAL | 精确十进制,28~29 位有效数字;算术/聚合/比较按十进制语义,混合运算优先于 Float |
  | TEXT | 字符串(JSON 文档也按文本存储) |
  | TIMESTAMP | 精确 UTC 毫秒时间:`TIMESTAMP '…'` 字面量、`NOW()`/`CURRENT_TIMESTAMP`、与字符串比较自动解析、`±` 毫秒算术;值域 0001..=9999 年 |
  | BLOB | 二进制,字面量 `x'hex'` |
  | ARRAY / OBJECT | 表达式层内部使用;SQL 层 JSON 字面量以文本入库 |

- **GUID 主键**:PK 列声明 `GUID`/`UUID`/`UNIQUEIDENTIFIER`/`UUIDV7` + `AUTOINCREMENT` 时,
  INSERT 省略或 NULL 自动填 **UUIDv7**(时序有序、节点内单调、跨节点定值下发);
- **大文档**:单文档 ≤16MiB,超页文档走溢出页链;链页可回收复用;
- **JSON 文档**:嵌套对象/数组整体存储,点读用 `JSON_EXTRACT/JSON_TYPE/JSON_VALID`。

## 3. SQL 能力

### 3.1 DDL

```sql
CREATE TABLE [IF NOT EXISTS] t (
    id      INT PRIMARY KEY,                 -- 单列主键(不隐含 NOT NULL)
    name    TEXT NOT NULL DEFAULT 'anon',
    email   TEXT UNIQUE,
    amount  DECIMAL(18,2),                   -- 类型声明为文档/自省用途
    uid     GUID AUTOINCREMENT,              -- 自动 UUIDv7
    ref     INT REFERENCES other(id),        -- FK(RESTRICT 式)
    CHECK (id >= 0)
);
CREATE TABLE dst AS SELECT ...;              -- CTAS
DROP TABLE [IF EXISTS] t;
ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'v'; -- 带 DEFAULT 时回填存量行;NOT NULL 必须有 DEFAULT
ALTER TABLE t RENAME COLUMN a TO b;
ALTER TABLE t DROP COLUMN c;                 -- 主键列不可删
TRUNCATE TABLE [IF EXISTS] t;                -- 清空保留表结构
TRUNCATE TABLE p CASCADE;                    -- 连带清空外键子表(传递闭包)
CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON t (col);   -- 单列
CREATE [UNIQUE] INDEX idx ON t (a, b);                  -- 复合(键按列序)
DROP INDEX idx;
CREATE [OR REPLACE] VIEW v AS SELECT ...;    -- 只读命名查询;SELECT 穿透
DROP VIEW [IF EXISTS] v [CASCADE];           -- 引用检查;CASCADE 连带删依赖视图
```

- 表级 `PRIMARY KEY` / `UNIQUE`(仅单列,复合请用 `CREATE UNIQUE INDEX`)/ `CHECK` / `FOREIGN KEY` 支持;`AUTOINCREMENT`/`AUTO_INCREMENT`;
- PK 与表声明 UNIQUE 的 B+ 树**随建表自动创建**(`sqlite_autoindex_<表>_<n>`,派生展示);
- `ADD COLUMN` 不支持 PK/UNIQUE/FK/自增;`CREATE TRIGGER`/`CREATE MATERIALIZED VIEW` 不支持(显式报错);
- `CREATE TABLE ... INHERITS`/`WITHOUT ROWID` 等存储布局子句与 `DEFERRABLE`/`NULLS NOT DISTINCT`/`MATCH FULL` 等约束装饰显式报错,不静默忽略;
- 删除基表/视图前做依赖检查(视图引用默认拒绝,`CASCADE` 连带删除);`DROP TABLE ... CASCADE` 移除引用子表的外键声明。

### 3.2 DML

```sql
INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y') RETURNING id;
INSERT INTO t (a, b) SELECT a, b FROM src;                 -- INSERT … SELECT
INSERT INTO t (...) ON CONFLICT DO NOTHING;                -- 目标列/ON CONSTRAINT 限定作用域
INSERT OR REPLACE INTO t ...;  INSERT OR IGNORE INTO t ...;
UPDATE t SET a = a + 1 WHERE b = 'x' RETURNING *;
DELETE FROM t WHERE a = 1 RETURNING id;

MERGE INTO stock USING feed ON stock.sku = feed.sku
WHEN MATCHED THEN UPDATE SET qty = feed.qty
WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (feed.sku, feed.qty);
```

- 批量 `VALUES`、`RETURNING`(INSERT/UPDATE/DELETE)、`INSERT … SELECT`(auto-GUID 表除外);
- `MERGE` 单语句原子、随复制扇出;限制(谓词/WHERE 子句/源侧 CTE)见 SQL 参考。

### 3.3 查询

```sql
SELECT [DISTINCT] *, expr AS alias
FROM t
LEFT JOIN u ON t.id = u.t_id            -- INNER/LEFT/RIGHT/FULL/CROSS;USING(col)
WHERE a BETWEEN 1 AND 9 AND b IN (1,2) AND c LIKE 'x%' ESCAPE '!'
GROUP BY dept HAVING COUNT(*) > 1        -- HAVING 配 GROUP BY 或聚合
GROUP BY ROLLUP(dept), GROUPING SETS ((a), (b)), CUBE(a, b);   -- 分组扩展 + GROUPING()
SELECT COUNT(*) FILTER (WHERE v > 2);    -- 聚合 FILTER
ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST]
LIMIT n OFFSET m;
FETCH FIRST n ROWS ONLY;                 -- Oracle 12c 分页
FETCH FIRST n ROWS WITH TIES;            -- 键相等行一并返回(需 ORDER BY)

SELECT ... UNION [ALL] SELECT ...        -- 集合运算
SELECT ... INTERSECT [ALL] SELECT ...
SELECT ... EXCEPT [ALL] SELECT ...       -- / MINUS
WITH cte AS (SELECT ...) SELECT * FROM cte;              -- 非递归 CTE
SELECT 1 IN (SELECT ...), EXISTS (SELECT ...);           -- 标量/IN/EXISTS 子查询
SELECT v > ANY (SELECT ...), v <> ALL (SELECT ...);      -- 量化比较(非相关)
SELECT a IS DISTINCT FROM b;                             -- NULL 安全比较
SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(id, s);      -- 行值构造器
SELECT CASE WHEN n > 0 THEN 'p' ELSE 'n' END FROM t;     -- CASE
SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM t;   -- 窗口函数
SELECT SUM(salary) OVER (PARTITION BY dept) FROM t;      -- 聚合窗口(默认帧)
SELECT 'A' ILIKE 'a';                                    -- 大小写不敏感 LIKE
SELECT * FROM t, u;                                      -- 逗号 FROM = 交叉连接
```

- 等值 JOIN 自动走 **hash join**(键按数值/编码归一化,索引只做超集、ON 逐候选终裁);
- **窗口函数**:`ROW_NUMBER/RANK/DENSE_RANK/NTILE` + `LAG/LEAD/FIRST_VALUE/LAST_VALUE` +
  聚合 OVER(默认 RANGE 帧,支持 FILTER);WHERE 后、DISTINCT/ORDER BY 前求值;
  与 GROUP BY 混用、自定义帧、`WINDOW`/`QUALIFY` 显式报错;
- 派生表(子查询作 FROM)、`SELECT *, expr`、`ROWNUM`、`DUAL` 支持;
- **NULL / 软删语义**:`x != TRUE` 命中 NULL 与缺失字段(与 Mongo `$ne: true` 的软删过滤一致);
  单列 UNIQUE 允许多个 NULL,`CREATE UNIQUE INDEX` 复合唯一任一列 NULL 的行跳过整键(不判重);
- 不支持(均显式报错,不静默吞掉):`WINDOW`/`QUALIFY`、自定义窗口帧、`DISTINCT ON`、
  相关子查询、`WITH RECURSIVE`、`NATURAL JOIN`、`LATERAL`、
  `TABLESAMPLE`、`FOR UPDATE`/`FOR SHARE`、`SELECT INTO`/`SELECT TOP`、
  `ON CONFLICT DO UPDATE`、`ON DUPLICATE KEY UPDATE`;
  T-SQL 专有形式(变量 `@p`/`@@ROWCOUNT`、`OUTPUT`、表提示 `WITH (NOLOCK)`、`#` 临时表、
  `IDENTITY(1,1)`、`CROSS/OUTER APPLY`、`sys.*`、`N'…'`、`[方括号]` 标识符)同样报错,
  兼容别名 `COUNT_BIG`/`ISNULL`/`CAST(... AS BIT)` 与 `INFORMATION_SCHEMA`(大小写不敏感)已支持。

### 3.4 函数

| 类别 | 函数 |
|---|---|
| 聚合 | `COUNT` `SUM` `AVG` `MIN` `MAX` `GROUP_CONCAT` `STRING_AGG`,均支持 `DISTINCT` |
| 字符串 | `UPPER/UCASE` `LOWER/LCASE` `LENGTH/LEN` `SUBSTR/SUBSTRING` `TRIM/LTRIM/RTRIM` `CONCAT` |
| 数值 | `ABS` `ROUND`(DECIMAL 精确、四舍五入半离零;FLOAT 保持浮点) |
| 空值/条件 | `COALESCE` `IFNULL` `NULLIF` `NVL` `NVL2` `DECODE` |
| 时间 | `NOW()` `CURRENT_TIMESTAMP`(TIMESTAMP 值);`TIMESTAMP '…'` 字面量、`CAST(... AS TIMESTAMP)`、`±` 毫秒算术(见 SQL 参考 · 时间值) |
| 窗口 | `ROW_NUMBER RANK DENSE_RANK NTILE`;`LAG LEAD FIRST_VALUE LAST_VALUE`;聚合 OVER(`COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/STRING_AGG`,支持 FILTER) |
| 类型/自省 | `TYPEOF` `CAST(expr AS INT/REAL/DECIMAL/TEXT/BOOL/BLOB/...)` |
| JSON 点读 | `JSON_EXTRACT(doc,'$.a.b[0]')` `JSON_TYPE` `JSON_VALID`(坏文本/缺路径返回 NULL) |
| JSON 数组 | `JSON_ARRAY_CONTAINS(json_text, value)` 数组成员判定(EF 实体集合 `Contains` 的翻译目标;非数组/坏 JSON → false,NULL 文本 → NULL) |
| Oracle 兼容 | `INSTR` `LPAD/RPAD` `GREATEST/LEAST` `TO_NUMBER`(非整数文本产 DECIMAL) `TO_CHAR` `SYSDATE()` |

### 3.5 事务

```sql
BEGIN; ... COMMIT;                    -- 单写者引擎;并发 BEGIN 排队(30s 上限)
SAVEPOINT sp; ... ROLLBACK TO sp; ... RELEASE sp;
```

- 事务归属打开它的连接;连接断开自动 ROLLBACK;
- 事务内写不扇出,COMMIT 时整批作为一个写单元提交并复制;
- `DOCSQL_ASYNC_COMMIT=1` 走组提交(约 2ms 丢失窗口);PUBLISH 推送前强制 fsync。

### 3.6 视图

```sql
CREATE [OR REPLACE] VIEW v AS SELECT ...;   -- 列清单不支持,给投影加别名
DROP VIEW [IF EXISTS] v [CASCADE];          -- 被引用时默认拒绝,CASCADE 连带删
```

- 只读命名查询:SELECT 穿透(支持视图套视图、外层 WHERE/JOIN/GROUP BY 组合);
  写语句(INSERT/UPDATE/DELETE/MERGE/TRUNCATE/ALTER/INDEX)对视图显式报错;
- 建视图干跑校验(基表/列必须存在;自引用与传递闭包成环被拒);展开预算 16 层;
- 授权按视图名发放:读视图只需视图授权(基表权限收口),写语句经视图读源需基表授权
  (fail-closed 展开);删除视图同步清理授权记录;
- 依赖完整性:删除基表/视图默认被依赖检查拒绝,`CASCADE` 连带删除依赖视图;
- 备份/dump/join 快照按依赖拓扑序携带视图定义。

### 3.7 并发读(MVCC 快照读)

- SELECT 在读锁微秒级建立**无锁视图**后全程锁外执行:长查询不阻塞写入,写不阻塞读;
- 快照依赖 WAL 历史;极端写压下快照被截断时读取**响亮报** `snapshot too old`(可重试),
  绝不静默读到新版本;
- 读-读之间天然并发;写-写仍由单写者串行化。

## 4. 索引

| 能力 | 说明 |
|---|---|
| 自动索引 | PRIMARY KEY / 表声明 UNIQUE 随建表创建 B+ 树(`sqlite_autoindex_*`) |
| 普通索引 | `CREATE INDEX` 单列;点查/范围探测走索引 |
| 复合索引 | `CREATE INDEX idx ON t (a, b)`,键 = `Value::Array` 按列序;`a=? AND b=?` 点查、前导列前缀探测 |
| 唯一索引 | `CREATE UNIQUE INDEX`;重复键在写入时拒绝 |
| 约束唯一 | 表级单列 UNIQUE 与 `CREATE UNIQUE INDEX`(含复合)由引擎判重;DECIMAL 按数值、Int/Float 跨类型数值比较 |
| 回收 | 模型/手工删索引即回收树;`DROP INDEX` 清理定义 |
| 边界 | 无表达式索引、部分索引、JSON 路径索引;非前导列条件不走复合索引(结果仍正确) |

## 5. 发布订阅(pub/sub,持久化)

- 命令面:PUBLISH / SUBSCRIBE / PSUBSCRIBE(glob `*` `?` `[...]`)/ UNSUBSCRIBE /
  PUNSUBSCRIBE / PUBSUB CHANNELS·NUMSUB·NUMPAT·TRIM;
- **先落盘再推送**(WAL 持久化,重启不丢);id 节点内单调,keep ≥ 1;
- 订阅起点:`latest`(默认)/ `earliest`(全量回放)/ 指定 id(断线续传);
  投递 at-least-once,慢连接丢实时帧可凭 id 补回;订阅必须专用连接 + 专职读线程;
- 集群内发布扇出到全部节点,各节点本地落盘并推送本地订阅者;
- 历史可 SQL 查询:`SELECT * FROM docsql_pubsub WHERE channel = 'news' ORDER BY id DESC`;
- 客户端:`DocsqlConnection.Publish` 返回 `(id, receivers)`;`DocsqlSubscriber` 回调式订阅;
  CLI 内联 `subscribe/publish/pubsub` 命令。

## 6. 复制与集群

| 能力 | 说明 |
|---|---|
| 对称集群 | `DOCSQL_PEERS` 互配;任意节点可写,SQL 写随写序扇出全体对等节点 |
| 主从写转发 | `DOCSQL_REPLICATE_TO` 指主,副本 `DOCSQL_READ_ONLY=1` 只读 |
| 故障转移 | `PROMOTE` 提升(协议帧 / CLI / 控制台);清只读后即可写 |
| 新节点加入 | 全新节点配 peers 启动即自动 bootstrap:冻结写→快照→回放→注册,加入期间写到 `sync_queue` 不丢 |
| 重启反熵修复 | 节点重启自动对比摘要;按复制日志**增量追赶**,超窗/仍分歧转整快照采纳(多数派裁决),完成后复验收敛 |
| 节点身份 | `DOCSQL_CLUSTER_TOKEN`:复制帧仅接受节点身份;客户端凭据无法伪造节点流量 |
| 观测 | 日志页(数据/同步/错误)、集群状态页(在线/只读/行数/LSN)、`REQ_STATUS`、`/api/cluster` |
| 边界 | 单写者(全库写互斥);无行级合并——分歧中少数方独有写在快照采纳时被覆盖;修复由重启触发 |

## 7. 备份与恢复

- **自动备份**:每节点独立,默认每日一次(间隔/保留可调),整库逻辑快照
  `dump_script()`(DDL 在前,跳过系统表),写 `<db>/backups/backup-<UTCms>.sql`;
- **校验和**:每份备份同名 `.sha256` sidecar,恢复前强校验(损坏/篡改直接拒绝;旧备份无 sidecar 容忍);
- **PITR(恢复到时间点)**:期刊无条件记录(单节点也记);自动备份带 journal-seq 锚点,
  `REQ_BACKUP restore` 请求带 `"to"`(ISO 时间戳或 UTC 毫秒)时重放
  「基准备份 + 增量段 `incr-*.sql`(期刊条目 + 提交时间戳)」至目标时间点,增量链带连续性审计
  (断号/裁剪洞显式报错,要求重拍全量);
- **手动触发**:控制台「备份管理」页 / `POST /api/backup` / REQ_BACKUP(控制台/API 仅整份恢复,
  时间点恢复走协议帧 `"to"` 参数);
- **恢复**:逐条经正常写路径重放,整场持写路径,完成后增量补拉 + 逐 peer 摘要比对,
  状态暴露 `converged`/`note`;跨节点互斥(同时只允许一个恢复);只读连接拒绝;
- **归档**:备份在数据卷内,`docker cp` 取出。

## 8. 安全与访问控制

- **三层凭据**(匹配顺序 cluster → client → read):`DOCSQL_CLUSTER_TOKEN`(节点间,仅可发
  复制帧)、`DOCSQL_TOKEN`(客户端管理员)、`DOCSQL_READ_TOKEN`(只读客户端);
  常数时间比较,启动校验复杂度(≥8 位、非单字符重复,违者 exit 2);
- **数据库用户与角色**:`CREATE/ALTER/DROP USER`、`CREATE/DROP ROLE`、`GRANT/REVOKE`
  (内置 `admin`/`readwrite`/`readonly` + 自定义角色表级 DML);定义随集群复制,
  授权以纪元逐帧刷新,**即时生效**;密码 PBKDF2-HMAC-SHA256(210000 轮),明文不离开执行节点;
- **登录锁定**:同 IP 60s 窗口 10 次失败锁 60s(token 与用户登录同桶,含 Web 登录门);
- **审计**:语句审计(文本/耗时/行数/复制标记/错误,密码脱敏)+ 认证事件 + 同步日志;
  `DOCSQL_LOG_FILE` 落 JSONL;慢查询 `DOCSQL_SLOW_MS` 写 stderr;
- **传输加密**:`DOCSQL_KEY`(64 hex)AES-256-GCM 帧加密;控制台原生 TLS(rustls)
  或反代 + `DOCSQL_WEB_COOKIE_SECURE=1`;默认端口仅绑 127.0.0.1;
- **注入防护**:ADO.NET/EF 默认服务端参数绑定(`REQ_PREPARE/REQ_EXECUTE`,引号感知、
  字符串翻倍转义,取值无法逃逸字面量);
- **Web 控制台账号门**:首次强制 setup,PBKDF2 凭据文件,HttpOnly 会话,改密码踢出其它会话,
  `node` 参数受 `DOCSQL_PEERS` 白名单约束(SSRF 防护);
- **等保对照**:等级保护第三级逐项映射见[安全指南 · 等保 2.0 对照](security.md#等保-20-对照)。

## 9. Web 控制台(DocSQL Studio)

参考 SQL Server Management Studio 的交互重新设计,纯管理工具、**自身零存储**,所有数据操作
以客户端身份转发到管理节点(二进制协议直连,认证用节点 `DOCSQL_TOKEN`;节点切换只允许
`DOCSQL_PEERS` 白名单内地址):

- **对象资源管理器**(左栏树):服务器 → 表(列含 PK/UQ/NN/AI 徽章、索引、键)→ 每表可双击
  打开数据网格;系统表分支(`_cluster_log`/`_cluster_pos`/`_cluster_id`/`_pubsub_messages`,
  只读)与系统视图(`information_schema.*`、`sqlite_master`);列节点区分**声明列**(DDL 定义)
  与**实测列**(数据中顶层字段并集),表外字段带「数据」徽章,`SELECT *` 按实测字段投影;
- **查询工作台**(多标签):SQL 高亮 + 行号编辑器,F5 执行 / Ctrl+F5 仅语法分析 / 执行所选;
  多语句批次逐结果集呈现(网格 + 受影响行数 + 总耗时);表头点击排序;
- **可调整布局**:左栏宽度与编辑器/结果区高度可拖动分隔条调整,双击恢复默认;尺寸存
  `localStorage`,窗口缩放时编辑器与结果区按设定比例重新分配;
- **浅色 / 深色主题**:工具栏太阳/月亮按钮切换;首帧前应用主题(无闪变),首次进入跟随系统
  `prefers-color-scheme` 且未手动选择时持续跟随系统变化,手动选择后记忆(存 `localStorage`);
- **多语言(中文 / English)**:工具栏地球按钮切换;首帧前应用语言(无闪变),首次进入跟随
  浏览器语言,手动选择后记忆(存 `localStorage`)。切换即时生效不刷新页面:静态骨架经
  `data-i18n` 重排,打开中的数据/管理页重拉重渲染,查询页仅重排壳层(编辑器内容保留),
  标签页标题同步重算;
- **右键任务**:新建查询、选择前 1000 行、查看数据、编辑行/删除行(按主键定位;主键列缺失的
  表无此入口)、编辑表、插入文档、编写 CREATE/DROP 脚本、删除表;
- **写入面**:列编辑网格建表/改表(列名/类型/默认值/PK/NOT NULL/自增;GUID 类型勾自增即
  UUIDv7 主键;已有列可改名/删除,主键列与在线约束修改受限);数据网格「插入文档…」接受
  JSON 对象(单行)或数组(批量),允许表外字段;保存按「重命名 → 删除 → 新增」生成 ALTER 批次,
  失败语句与已生效前缀明确提示;
- **行内编辑与删除**:数据网格按主键生成 UPDATE/DELETE(自增列只读、可勾选置 NULL;
  `$dec`/`$bytes` 精确标量按 `CAST(... AS DECIMAL)`/`x'...'` 保存);
- **仪表盘**:表/行数/索引数(含主键/唯一约束自动索引)/页与文件占用/运行时长;**集群状态**:按 `DOCSQL_PEERS` 只读探测各节点
  (PING 延迟 + REQ_STATUS),展示在线/离线/只读、行数收敛、存储、LSN 收敛,5s 自动刷新;
- **日志页**:数据日志(语句审计,含耗时/行数/错误/复制徽章)与同步日志(写扇出/PROMOTE/
  节点加入的逐目标成败)合并倒序,支持类别/来源/关键词过滤与 5s 刷新;
- **备份管理页**:状态/文件列表/立即备份/一键恢复(恢复需输入完整文件名确认);
- **用户与角色页**:建号改密删号、自定义角色与成员、按表勾选 SELECT/INSERT/UPDATE/DELETE
  (直接授予与角色携带分开展示);
- **控制台账号**:首次强制设置用户名/密码(≥8 位),PBKDF2 凭据文件 + HttpOnly 会话 Cookie,
  连续输错锁定;「文件 → 修改账号…」需当前密码确认,改密后其它会话全部退出;
  浏览器不持有节点令牌(连节点用服务端 `DOCSQL_TOKEN`,与 docker-compose 下发值同源),
  `DOCSQL_TOKEN` 作程序化 API 旁路;未启用 `DOCSQL_WEB_AUTH_FILE` 时 API 开放;
- **节点切换**:工具栏下拉切换管理目标;未配置 peers 时显示单机模式。

REST API(账号门激活时:会话 Cookie 或 `X-Docsql-Token` 程序化旁路;未启用账号门时开放):

  | 方法 | 路径 | 说明 |
  |---|---|---|
  | GET | `/healthz` | 无门禁存活探针 |
  | GET | `/metrics` | Prometheus 文本(逐节点抓取) |
  | POST | `/api/sql` | 执行 SQL(可带 `node`) |
  | POST | `/api/parse` | 仅语法检查(恒本地静态检查,不执行) |
  | GET | `/api/meta` | 对象树/列/索引/存储(可带 `node`) |
  | GET | `/api/stats` | 仪表盘统计(可带 `node`) |
  | GET | `/api/cluster` | 集群探测汇总 |
  | GET | `/api/logs` | 数据日志 + 同步日志 |
  | GET/POST | `/api/users` | 用户/角色/授权读取与动作派发 |
  | GET/POST | `/api/backup`,POST `/api/backup/restore` | 备份列表/触发/恢复 |
  | GET/POST | `/api/auth/status`·`setup`·`login`·`change`·`logout` | 控制台账号门 |

## 10. 客户端与工具

| 入口 | 能力 |
|---|---|
| ADO.NET(`Docsql.Client`) | 连接池(默认开启,池键含身份)、事务 + SAVEPOINT、全链路异步、`RETURNING`、服务端参数绑定(`@name`)、`Publish`/`DocsqlSubscriber`、`DOCSQL_KEY` 透传 |
| EF Core(`Docsql.EntityFrameworkCore`) | 原生提供程序(不依赖 SQLite):EnsureCreated + 惰性建表 + 模型/索引(含复合)自动同步、LINQ/Include、`decimal`→DECIMAL、`byte[]`→BLOB、`DateOnly/TimeOnly`、`List.Contains`→IN、字符串方法→LIKE;**实体集合属性 `List<T>`(JSON 数组)的 `Contains` 翻译为 `JSON_ARRAY_CONTAINS`(常量/跨列/取反/数值元素)**;`Dictionary<string,object>` 映射为 JSON 文本(读写与变更跟踪,成员不参与 SQL 翻译);`Database.Migrate()` 显式报错 |
| Aspire | `AddDocsql` / `AddDocsqlCluster` / `WithWebConsole` / `WithDataVolume` + 消费侧 `AddDocsqlConnection` / `AddDocsqlDbContext`;见 [Aspire 指南](aspire.md) |
| CLI(`docsql-cli`) | 嵌入式(直接开数据文件)与远程 shell;`--csv`/`--json` 导出;`-f script.sql` 批量(快速失败);内联 pub/sub 命令;`--user` 登录(密码走环境/提示) |
| 线协议(自研驱动) | v1 二进制一句话一帧;`REQ_PREPARE/REQ_EXECUTE/REQ_CLOSE_STMT` 服务端绑定;复制帧、`REQ_STATUS`、`REQ_BACKUP`、订阅帧;参数支持 `$dec`/`$bytes` 精确标记;帧定义见 `core/proto.rs` 模块头 |

## 11. 可观测性

- `GET /healthz`(无门禁)、`GET /metrics`(Prometheus:`docsql_node_up`、语句/连接/字节/
  认证失败计数、表与行数、存储/JOURNAL 指标、控制台自身请求计数)、`/api/stats`、`/api/cluster`;
- `REQ_STATUS` 节点报告(uptime/只读/peers/存储/LSN/表总量/备份/运行时计数器);
- 语句审计与慢查询日志;同步日志按目标记录复制成败与原因;
- WAL 后台 checkpoint(软阈值 8MB 请求、硬阈值 64MB 流控),崩溃恢复自动重放。

## 12. 部署

- compose(仓库维护):`deploy/docker-compose.yml`(开发,源码构建 `:local`)/
  `deploy/docker-compose.prod.yml`(生产,GHCR 镜像);profile `single`/`cluster`/`join`;
  数据卷 external,`down -v` 不清数据,清数据唯一入口 `deploy/reset-data.sh`;
- Aspire:AppHost 编排容器节点与控制台,`aspire start` 本地、`aspire publish` 出部署产物;
- 部署测试:多节点 81 项 + 单节点 34 项(写入复制/事务/收敛/GUID/控制台/pub-sub/离线补齐/
  分区恢复/新节点加入/备份恢复演练)。

## 13. 已知边界(摘要)

完整清单见[已知边界](limitations.md),高频项:

- 单写者引擎:全库写互斥(读不阻塞写);单文档 ≤16MiB;BLOB 无流式分块;
- 精确 DECIMAL 有效数字 28~29 位;TIMESTAMP 精确到毫秒(无 TIME/INTERVAL 类型,区间用毫秒整数);
- 无行级合并:集群修复以多数派快照覆盖少数方独有写;修复由重启触发;
- 无表达式/部分/JSON 路径索引;查询优化器为规则式(无 EXPLAIN/统计信息);
- 备份为逻辑全量 + 增量 PITR(PITR 窗口 = 本节点期刊保留;对称集群跨节点写不在本节点期刊);
  无 TDE(部署加密卷替代);
- 不支持 SQL 清单见 [SQL 参考 · 不支持的语法](sql-reference.md#不支持的语法)。
