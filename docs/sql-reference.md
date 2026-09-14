# SQL 参考

DocSQL 的 SQL 面:完整 DDL/DML、JOIN、聚合、事务与约束。所有未列出的语法在解析/执行层**显式报错**,不静默吞掉。`PRAGMA` 是有意接受并忽略的兼容垫片。

## 数据类型

声明列类型作文档/自省用途(information_schema 上报,`GUID` 同样可见)。实际值模型:

| 值类型 | 说明 |
|---|---|
| NULL | 空值 |
| BOOL | TRUE/FALSE |
| INT | 64 位整数 |
| FLOAT | 64 位双精度 |
| DECIMAL | 精确十进制(rust_decimal,28~29 位有效数字);存精确文本,算术/聚合/比较按十进制语义,**混合运算中优先于 Float** |
| TEXT | 字符串(JSON 文档也按文本存储) |
| BLOB | 二进制,字面量 `x'hex'` |
| ARRAY / OBJECT | 表达式层内部使用;SQL 层 JSON 字面量以文本入库 |

`CAST` 支持:`INT`、`CHAR/TEXT/STRING`、`BOOL`、`REAL/DOUBLE/FLOAT`、`DECIMAL/NUMERIC`、`BLOB/BYTES/BINARY`(文本按其 UTF-8 字节入库)。没有 TIMESTAMP 精确类型(时间按 ISO-8601 文本/整数毫秒约定,见[已知边界](limitations.md))。DECIMAL 的规范写法是 `CAST('123.45' AS DECIMAL)`;`DECIMAL(p,s)` 声明仅作文档/自省用途,不做存储截断。

## DDL

```sql
CREATE TABLE [IF NOT EXISTS] t (
    id      INT PRIMARY KEY,              -- 单列主键(不隐含 NOT NULL,需显式声明)
    name    TEXT NOT NULL DEFAULT 'anon',
    email   TEXT UNIQUE,
    price   FLOAT CHECK (price >= 0),
    uid     GUID AUTOINCREMENT,           -- UUID/UNIQUEIDENTIFIER/UUIDV7 别名:自动 UUIDv7
    ref     INT REFERENCES other(id)      -- 外键(RESTRICT 式;不支持 ON DELETE/UPDATE 动作)
);
CREATE TABLE dst AS SELECT ...;                           -- CTAS
CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON t (col);          -- 单列索引
CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON t (a, b);         -- 多列(复合)索引
DROP INDEX idx;
ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'v';            -- 带 DEFAULT 回填存量行;NOT NULL 必须带 DEFAULT
ALTER TABLE t RENAME COLUMN a TO b;
ALTER TABLE t DROP COLUMN c;                            -- 主键列不可删;不支持在线改约束
TRUNCATE TABLE [IF EXISTS] t;                           -- 清空数据,保留表结构与索引
DROP TABLE [IF EXISTS] t;
```

- 主键与 UNIQUE 约束的 B+ 树随建表自动创建(保留名 `sqlite_autoindex_<表>_<n>`,`DROP INDEX` 对该前缀报错,`sqlite_master` 不列出);
- **多列(复合)索引**:键为按列序的复合值,`WHERE a = 1 AND b = 2` 形态走索引点查,
  前导列等值可用前缀探测;非前导列单独条件不走该索引(仍正确,全表扫描);
  复合 UNIQUE 的唯一性按**完整键组合**判定,任一列 NULL 的行跳过整键(不判重);
- `CREATE VIEW` / `CREATE TRIGGER` 不支持(显式报错);
- auto-GUID 表不支持 `INSERT ... SELECT`(随机值无法跨节点收敛;用 VALUES);
- ADD COLUMN 带 DEFAULT 时自动回填存量行;`ADD COLUMN` 不支持 PK/UNIQUE/FK/自增。

## DML 与查询

```sql
INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y') RETURNING id;
INSERT INTO t (a, b) SELECT a, b FROM src;              -- INSERT … SELECT
INSERT INTO t (...) ON CONFLICT DO NOTHING;             -- 或 DO REPLACE;不支持 DO UPDATE
INSERT OR REPLACE INTO t ...;                           -- 或 OR IGNORE
UPDATE t SET a = a + 1 WHERE b = 'x' RETURNING *;
DELETE FROM t WHERE a = 1 RETURNING id;

MERGE INTO stock USING feed ON stock.sku = feed.sku
WHEN MATCHED THEN UPDATE SET qty = feed.qty
WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (feed.sku, feed.qty);

SELECT [DISTINCT] * , expr
FROM t LEFT JOIN u ON t.id = u.t_id                     -- INNER/LEFT/RIGHT/FULL/CROSS;USING(col)
WHERE a BETWEEN 1 AND 9 AND b IN (1,2) AND c LIKE 'x%' ESCAPE '!'
GROUP BY ... HAVING ...                                  -- HAVING 必须配 GROUP BY
ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST]
LIMIT n OFFSET m;
FETCH FIRST n ROWS ONLY;                                -- Oracle 12c 分页
FETCH FIRST n ROWS WITH TIES;                           -- 键相等行一并返回(需 ORDER BY)

SELECT ... UNION [ALL] SELECT ...                       -- 集合运算
SELECT ... INTERSECT [ALL] SELECT ...
SELECT ... EXCEPT [ALL] SELECT ...                      -- / MINUS(同义)

WITH cte AS (SELECT ...) SELECT * FROM cte;              -- 非递归 CTE;WITH RECURSIVE 报错
WITH c(id, s) AS (VALUES (1, 'x')) SELECT * FROM c;      -- 别名列按位置改名
SELECT 1 IN (SELECT ...), EXISTS (SELECT ...);           -- 标量/IN/EXISTS 子查询
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, s);   -- 行值构造器(默认 column1..n)
SELECT CASE WHEN n > 0 THEN 'p' ELSE 'n' END FROM t;     -- CASE
SELECT 'A' ILIKE 'a';                                    -- 大小写不敏感 LIKE
SELECT a FROM t1, t2 WHERE ...;                          -- 逗号 FROM = 笛卡尔积
```

标准 SQL 补充面(均已实现):

```sql
-- 分组扩展:ROLLUP / CUBE / GROUPING SETS + GROUPING() 区分汇总 NULL
SELECT g, SUM(v), GROUPING(g) FROM t GROUP BY ROLLUP(g);
SELECT g, h, SUM(v) FROM t GROUP BY GROUPING SETS ((g), (h));
SELECT g, h, SUM(v) FROM t GROUP BY CUBE(g, h);

-- 聚合 FILTER (WHERE …):先过滤行,再 DISTINCT/聚合
SELECT COUNT(*) FILTER (WHERE v > 2), SUM(v) FILTER (WHERE g = 'a') FROM t;

-- 量化比较(非相关子查询;空集 ANY=false / ALL=true);= 形式等价 IN
SELECT v FROM t WHERE v > ANY (SELECT v FROM t WHERE v > 3);
SELECT v FROM t WHERE v <> ALL (SELECT v FROM t);

-- NULL 安全比较
SELECT a IS DISTINCT FROM b, a IS NOT DISTINCT FROM b FROM t;
```

- `RETURNING` 支持 INSERT/UPDATE/DELETE;`INSERT … SELECT` 源为 SELECT 查询;
- 集合运算按列数对齐(列名取左);`EXCEPT ALL` 保留左多重集语义;
- 等值 JOIN 自动走 hash join(索引只做超集过滤,ON 逐候选终裁)。

## 函数

- **聚合**:`COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/STRING_AGG`(支持 `DISTINCT` 聚合);
- **标量**:`UPPER/UCASE、LOWER/LCASE、LENGTH/LEN、ABS、ROUND、COALESCE/IFNULL、NULLIF、SUBSTR/SUBSTRING、TRIM/LTRIM/RTRIM、CONCAT、TYPEOF`;
- **JSON**(文档点读,坏文本/缺路径返回 NULL 不中断扫描):
  ```sql
  JSON_EXTRACT(doc, '$.user.name')     -- 路径:$ 根、.成员、[索引];对象/数组保持结构
  JSON_TYPE(doc, '$.tags[0]')          -- object/array/text/integer/real/boolean/null
  JSON_VALID(text)                     -- 合法 JSON 文本判定
  ```
- **Oracle 风格**:
  ```sql
  NVL(a, b) / NVL2(a, b, c)            -- 空值替换
  DECODE(expr, s1, r1, s2, r2 [, d])   -- NULL=NULL 匹配;无命中且无 default 返回 NULL
  INSTR(str, sub)                      -- 1 起始位置,无则 0
  LPAD / RPAD(str, len [, pad])        -- 填充/截断到 len 字符
  GREATEST / LEAST(a, b, …)            -- 任一 NULL → NULL
  TO_NUMBER(text)                      -- 解析失败显式报错
  TO_CHAR(v)                           -- 值 → 文本(格式掩码不支持)
  SYSDATE()                            -- 当前 UTC 时间戳文本(固定形状)
  ```

## 不支持

以下语法在解析/执行层**显式报错**,不静默吞掉、不降级近似:

- 窗口函数(`OVER`)、`WINDOW` 子句、`QUALIFY`;
- `DISTINCT ON`、`SELECT TOP`、`SELECT INTO`、`SELECT AS VALUE/STRUCT`、`SELECT * EXCLUDE`;
- `NATURAL JOIN`、`LATERAL` 派生表、表函数/`UNNEST`、`TABLESAMPLE`、表时态(`AS OF`);
- `FOR UPDATE` / `FOR SHARE`、`FOR XML` / `FOR JSON`、`SETTINGS`、`FORMAT`;
- `ON CONFLICT DO UPDATE`、`ON DUPLICATE KEY UPDATE`(只支持 `DO NOTHING` / `DO REPLACE` / `OR REPLACE` / `OR IGNORE`);
- **相关子查询**(子查询引用外层列);非相关标量/IN/EXISTS/ANY/ALL 子查询支持;
- `WITH RECURSIVE`(非递归 CTE 支持);
- 无 `GROUP BY` 的 `HAVING`;
- `FETCH … PERCENT`(WITH TIES 支持,需 ORDER BY);
- 外键的 `ON DELETE` / `ON UPDATE` 动作(RESTRICT 式);
- `CREATE VIEW` / `CREATE TRIGGER`;
- 自定义 `TRIM` 字符集(单参数形式支持);
- 表达式索引、部分索引、JSON 路径索引。

`PRAGMA` 是有意接受并忽略的兼容垫片(兼容 ORM/驱动的探测语句),不产生任何效果。

## Oracle 兼容面

- `FROM DUAL`:哑表(单行零列,大小写不敏感);
- `ROWNUM` 伪列:行取出后、WHERE 与 ORDER BY **之前**编号;被 WHERE 过滤的行
  消耗编号(Oracle 语义);`SELECT *` 不含该列;
- `FETCH FIRST n ROWS ONLY`(Oracle 12c/SQL 标准分页);`WITH TIES` 同样支持(需 ORDER BY),`PERCENT` 显式报错;
- 数据字典兼容视图:`ALL_TABLES`/`USER_TABLES`/`ALL_TAB_COLUMNS`/`USER_TAB_COLUMNS`/
  `ALL_INDEXES`/`USER_INDEXES`(OWNER 恒为 `DOCSQL`;单全局命名空间,USER_* 与 ALL_* 同数据;
  系统内部表不出现)。

## MERGE INTO(Oracle/SQL 标准 upsert)

```sql
MERGE INTO stock USING feed ON stock.sku = feed.sku
WHEN MATCHED THEN UPDATE SET qty = feed.qty
WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (feed.sku, feed.qty);
```

- ON 条件为任意表达式(在合并命名空间上求值:目标列裸名、源列可用 `别名.列` 限定);
- 一个目标行被多个源行命中 → 显式报错(Oracle ORA-30926 语义);
- 语句作为整体原子应用并随复制扇出(对端确定性重放收敛);
- v1 不支持:`WHEN … AND <predicate>`、`UPDATE … WHERE/DELETE WHERE`、INSERT 谓词、
  `ROW` 形式、`BY SOURCE`;源侧不支持引用 CTE。

## 事务

```sql
BEGIN; ... COMMIT;            -- 单写者:同一时刻一个引擎事务;并发 BEGIN 排队(30s 上限)
SAVEPOINT sp1; ... ROLLBACK TO sp1; RELEASE sp1;   -- 可嵌套;重名取最近
```

事务归属打开它的连接;连接断开自动 ROLLBACK。注意:ROLLBACK TO 会把该保存点自身一并丢弃(异于 SQLite/PG),之后勿再 RELEASE。

## 系统视图

`information_schema`(tables/columns 等)、`sqlite_master`(表/索引 DDL,不列自动索引)、`docsql_pubsub`(消息历史)。系统表(`_cluster_log`/`_cluster_pos`/`_cluster_id`/`_pubsub_messages`)只读。

## 用户与角色

见 [安全指南](security.md#数据库用户与角色):`CREATE/ALTER/DROP USER`、`CREATE/DROP ROLE`、`GRANT/REVOKE`(表级 DML 位,即时生效)。
