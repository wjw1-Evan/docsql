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
| TEXT | 字符串(JSON 文档也按文本存储) |
| BLOB | 二进制,字面量 `x'hex'` |
| ARRAY / OBJECT | 表达式层内部使用;SQL 层 JSON 字面量以文本入库 |

`CAST` 支持:`INT`、`CHAR/TEXT/STRING`、`BOOL`、`REAL/DOUBLE/FLOAT`。没有 DECIMAL/TIMESTAMP 精确类型(见[已知边界](limitations.md))。

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
CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON t (col);          -- 单列索引
CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON t (a, b);         -- 多列(复合)索引
DROP INDEX idx;
ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'v';            -- 不支持 PK/UNIQUE/自增;不支持 DROP COLUMN 主键
DROP TABLE [IF EXISTS] t;
```

- 主键与 UNIQUE 约束的 B+ 树随建表自动创建(保留名 `sqlite_autoindex_<表>_<n>`,`DROP INDEX` 对该前缀报错,`sqlite_master` 不列出);
- **多列(复合)索引**:键为按列序的复合值,`WHERE a = 1 AND b = 2` 形态走索引点查,
  前导列等值可用前缀探测;非前导列单独条件不走该索引(仍正确,全表扫描);
  复合 UNIQUE 的唯一性按**完整键组合**判定,任一列 NULL 的行跳过整键(不判重);
- auto-GUID 表不支持 `INSERT ... SELECT`(随机值无法跨节点收敛;用 VALUES);
- ADD COLUMN 带 DEFAULT 时自动回填存量行。

## DML 与查询

```sql
INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y') RETURNING id;
INSERT INTO t (...) ON CONFLICT DO NOTHING;             -- 或 DO REPLACE;不支持 DO UPDATE
UPDATE t SET a = a + 1 WHERE b = 'x' RETURNING *;
DELETE FROM t WHERE a = 1;

SELECT [DISTINCT] * , expr
FROM t LEFT JOIN u ON t.id = u.t_id                     -- INNER/LEFT/RIGHT/FULL/CROSS;USING(col)
WHERE ...
GROUP BY ... HAVING ...                                  -- HAVING 必须配 GROUP BY
ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST]
LIMIT n OFFSET m;

WITH cte AS (SELECT ...) SELECT * FROM cte;              -- 非递归 CTE;WITH RECURSIVE 报错
SELECT 1 IN (SELECT ...), EXISTS (SELECT ...);           -- 标量/IN/EXISTS 子查询
SELECT a FROM t1, t2 WHERE ...;                          -- 逗号 FROM = 笛卡尔积
```

## 函数

- **聚合**:`COUNT/SUM/AVG/MIN/MAX/GROUP_CONCAT/STRING_AGG`(支持 `DISTINCT` 聚合);
- **标量**:`UPPER/UCASE、LOWER/LCASE、LENGTH/LEN、ABS、ROUND、COALESCE/IFNULL、NULLIF、SUBSTR/SUBSTRING、TRIM/LTRIM/RTRIM、CONCAT、TYPEOF`;
- **JSON**(文档点读,坏文本/缺路径返回 NULL 不中断扫描):
  ```sql
  JSON_EXTRACT(doc, '$.user.name')     -- 路径:$ 根、.成员、[索引];对象/数组保持结构
  JSON_TYPE(doc, '$.tags[0]')          -- object/array/text/integer/real/boolean/null
  JSON_VALID(text)                     -- 合法 JSON 文本判定
  ```

不支持:窗口函数(OVER)、DISTINCT ON、ON CONFLICT DO UPDATE、相关子查询、自定义 TRIM 字符集、递归 CTE。

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
