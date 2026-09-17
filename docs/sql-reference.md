# DocSQL SQL 参考

本参考按 Transact-SQL 参考（MSDN）的组织方式编写：每条语句给出**语法**、**参数**、**备注**与**示例**。

DocSQL 的 SQL 方言以 **SQL 标准**为基准，并兼容 SQLite（`sqlite_master`、`LIMIT` 变体、`REPLACE INTO`）、Oracle（`DUAL`、`ROWNUM`、`NVL` 函数族、字典视图、`MERGE`）与 SQL Server/EF 生态（`information_schema`、`x != TRUE` 软删语义、`JSON_ARRAY_CONTAINS`）。未实现的语法一律在解析/执行层**显式报错**，不静默忽略；完整清单见[不支持的语法](#不支持的语法)。

## 目录

- [本文约定](#本文约定)
- [数据类型](#数据类型)
- [运算符与谓词](#运算符与谓词)
- [SELECT](#select)
- [INSERT](#insert)
- [UPDATE](#update)
- [DELETE](#delete)
- [MERGE](#merge)
- [CREATE TABLE](#create-table)
- [ALTER TABLE](#alter-table)
- [TRUNCATE TABLE / DROP TABLE](#truncate-table--drop-table)
- [CREATE VIEW / DROP VIEW](#create-view--drop-view)
- [CREATE INDEX / DROP INDEX](#create-index--drop-index)
- [事务语句](#事务语句)
- [用户与角色语句](#用户与角色语句)
- [函数](#函数)
- [系统视图与元数据](#系统视图与元数据)
- [兼容性与限制](#兼容性与限制)
- [不支持的语法](#不支持的语法)
- [另请参阅](#另请参阅)

## 本文约定

### 语法图

| 写法 | 含义 |
|---|---|
| `[ ... ]` | 可选项 |
| `{ a \| b }` | 二选一 |
| `[ , ...n ]` | 逗号分隔的列表 |
| `;` | 语句结束符（CLI/驱动可省略，一次提交多条语句用分号分隔） |

### 标识符

- 未加引号的标识符由字母、数字、`_`、`$` 组成，不以数字开头；**表名与列名区分大小写**（`T` 与 `t` 是两张表），关键字不区分大小写。
- 加双引号的标识符保留大小写并可包含任意字符：`"weird name"`（内部 `""` 表示一个 `"`）。
- **用户与角色名统一规范化为小写**（`"Bob"` 与 `bob` 同名）。

### 字面量与注释

```sql
SELECT 42, -7, 3.14, 1e3;          -- 数字
SELECT 'it''s';                     -- 字符串（'' 转义单引号）
SELECT x'00ff', TRUE, FALSE, NULL;  -- 二进制 / 布尔 / 空值
-- 行注释
/* 块注释 */
```

### NULL 与比较语义

DocSQL 的排序是**全序**：`NULL` 是一个可排序的值（最小），因此：

- `NULL = NULL` 为 `TRUE`，`NULL != 1` 为 `TRUE`——**没有 SQL 标准的 UNKNOWN 三值逻辑**；
- `x != TRUE` 会命中值为 `NULL` 或字段缺失的行（EF 软删过滤依赖该语义）；
- `AND`/`OR` 的操作数必须是 BOOL，`NULL AND TRUE` 会报错，而不是返回 UNKNOWN；
- `NULLS FIRST|LAST` 显式控制排序位置；默认 NULL 最小（升序在前、降序在后）。

需要"NULL 不参与匹配"的场景请显式写 `IS NOT NULL`。

### 错误处理

- 解析错误、类型错误、不支持的语法均返回带 `parse error:` 或说明文本的错误，不做隐式降级；
- 未知函数、未知列、未知 ORDER BY 键显式报错；
- `PRAGMA` 是有意接受并忽略的兼容垫片（兼容 ORM/驱动的探测语句），不产生任何效果。

## 数据类型

DocSQL 文档模型不强制 schema：**列声明类型仅作文档与自省用途**（`information_schema.columns` 上报），值按实际写入的 JSON 值存储。`VALUES` 与表达式决定值的运行时类型。

### 值模型

| 值类型 | 说明 |
|---|---|
| NULL | 空值 |
| BOOL | `TRUE` / `FALSE` |
| INT | 64 位有符号整数（算术溢出显式报错） |
| FLOAT | IEEE-754 双精度 |
| DECIMAL | 精确十进制（`rust_decimal`，28~29 位有效数字）；存精确文本，算术/聚合/比较按十进制语义，混合运算优先于 FLOAT |
| TEXT | UTF-8 字符串（JSON 文档亦以文本入库；支持 `||` 连接） |
| BLOB | 二进制，字面量 `x'hex'` |
| ARRAY / OBJECT | 表达式内部与 JSON 函数使用；SQL 字面量写入时按文本存储 |

### 声明类型

`INT`/`INTEGER`、`TEXT`/`CHAR`/`VARCHAR`/`STRING`、`BOOL`/`BOOLEAN`、`FLOAT`/`REAL`/`DOUBLE`、`DECIMAL`/`NUMERIC[(p,s)]`、`BLOB`/`BINARY`/`BYTES`、`GUID`/`UUID`/`UNIQUEIDENTIFIER`/`UUIDV7`（自动 UUIDv7 主键的触发器，见 [CREATE TABLE](#create-table)）。

### CAST

```sql
CAST(expr AS type)
```

支持的目标类型：`INT`、`CHAR`/`TEXT`/`STRING`、`BOOL`、`BIT`（T-SQL，映射 BOOL）、`REAL`/`DOUBLE`/`FLOAT`、`DECIMAL`/`NUMERIC`、`BLOB`/`BYTES`/`BINARY`（文本按 UTF-8 字节入库）。DECIMAL 的规范写法是 `CAST('123.45' AS DECIMAL)`；`DECIMAL(p,s)` 声明不做存储截断。其他目标类型（如 `DATE`/`DATETIME`/`NVARCHAR`）按**声明类型**处理，值保持不变（时间统一按文本约定）。

### 时间值

无精确 `TIMESTAMP` 类型。时间按 **ISO-8601 文本**或**整数毫秒**约定存储，由应用层保持形状一致；`SYSDATE()` 返回当前 UTC 时间戳文本。

## 运算符与谓词

| 类别 | 运算符/谓词 | 说明 |
|---|---|---|
| 算术 | `+ - * / %` | 数值运算；DECIMAL 参与时按十进制精确计算 |
| 比较 | `= <> != < <= > >=` | 按值全序比较（数值间类型无关：`1 = 1.0` 为真） |
| 逻辑 | `AND OR NOT` | 操作数必须为 BOOL |
| 连接 | `\|\|` | 字符串连接（NULL 传播为 NULL） |
| 范围 | `BETWEEN a AND b` | 闭区间 |
| 集合 | `IN (v1, v2, ...)` / `NOT IN (...)` | |
| 模式 | `LIKE pat [ESCAPE 'c']`、`ILIKE pat` | `%`、`_` 通配；ILIKE 大小写不敏感；用 `ESCAPE` 指定转义字符 |
| 空值 | `IS NULL`、`IS NOT NULL` | |
| NULL 安全比较 | `IS DISTINCT FROM`、`IS NOT DISTINCT FROM` | 全序下的安全等价比较 |
| 存在量词 | `EXISTS (SELECT ...)`、`IN (SELECT ...)`、`ANY/SOME/ALL (SELECT ...)` | 仅支持**非相关**子查询 |
| 类型 | `TYPEOF(expr)` | 返回值类型名 |

**示例**

```sql
SELECT 1 + 2 * 3;                       -- 7
SELECT 'a' || 'b';                      -- 'ab'
SELECT 3 BETWEEN 1 AND 5;               -- TRUE
SELECT 'Abc' ILIKE 'a%';                -- TRUE
SELECT NULL IS NOT DISTINCT FROM NULL;  -- TRUE
SELECT v FROM t WHERE v > ANY (SELECT v FROM t WHERE v > 3);
SELECT v FROM t WHERE v <> ALL (SELECT v FROM t);
```

## SELECT

从表或表达式中检索行。

### 语法

```sql
[ WITH cte_name [(col, ...)] AS ( SELECT ... ) [ , ...n ] ]
SELECT [ DISTINCT | ALL ] { * | expr [ [ AS ] alias ] } [ , ...n ]
[ FROM table_source [ , ...n ] ]
[ WHERE condition ]
[ GROUP BY { expr | ROLLUP ( expr [ , ...n ] ) | CUBE ( expr [ , ...n ] )
             | GROUPING SETS ( ( expr [ , ...n ] ) [ , ...n ] ) } [ , ...n ] ]
[ HAVING condition ]
[ ORDER BY expr [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ] [ , ...n ] ]
[ LIMIT { n | offset , n } [ OFFSET m ] ]
[ FETCH { FIRST | NEXT } n ROWS { ONLY | WITH TIES } [ OFFSET m ROWS ] ]
```

`table_source`：

```sql
table_name [ [ AS ] alias [ ( col, ... ) ] ]
| ( SELECT ... ) [ AS ] alias [ ( col, ... ) ]
| ( VALUES ( expr, ... ) [ , ...n ] ) [ AS ] alias [ ( col, ... ) ]
| table_source [ INNER | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER] ] JOIN table_source
    { ON condition | USING ( col, ... ) }
| table_source CROSS JOIN table_source
| table_source , table_source
```

集合运算：

```sql
SELECT ... { UNION | INTERSECT | EXCEPT | MINUS } [ ALL | DISTINCT ] SELECT ...
```

### 参数

| 参数 | 说明 |
|---|---|
| `DISTINCT` | 按整行判重（按编码字节，`Int(3)` 与 `Float(3.0)` 比较相等但不会互相去重） |
| `ALL` | 默认值，返回全部行 |
| `*` | 展开为所有文档字段的并集（跨文档动态）；`SELECT *, expr` 允许，显式表达式追加在星号列之后 |
| `alias` | 输出列名；ORDER BY 可引用 |
| `FROM` | 见下节；省略 FROM 时为单行常量查询 |
| `WHERE` | 过滤条件；只放行求值为 `TRUE` 的行 |
| `GROUP BY` | 分组键为表达式；投影中的裸列必须出现在分组键或聚合中 |
| `ROLLUP / CUBE / GROUPING SETS` | 分组扩展，见[备注](#select-备注)；`GROUPING(expr)` 在投影中区分汇总 NULL（该列不在当前分组集合时为 1，否则为 0） |
| `HAVING` | 分组后过滤；需配合 GROUP BY 或聚合 |
| `ORDER BY` | 键可为输出列名、序号（如 `ORDER BY 2`）或任意表达式；默认 `ASC`，默认 NULL 最小 |
| `LIMIT n` | 最多返回 n 行；`LIMIT -1` 表示不限制；`LIMIT off, n` 为 MySQL 写法（等价 `LIMIT n OFFSET off`） |
| `OFFSET m` | 跳过 m 行；负值视为 0 |
| `FETCH FIRST \| NEXT n ROWS ONLY` | SQL 标准/Oracle 分页；`WITH TIES` 需 ORDER BY，会把与第 n 行排序键相等的行一并返回；`PERCENT` 报错 |

### FROM 子句

| 形式 | 说明 |
|---|---|
| `t [AS] x` | 表别名；列可用 `x.col` 限定，未限定列名回退到行内后缀匹配 |
| `(SELECT ...) AS x(a, b)` | 派生表；别名列按位置改名，宽度/重名不符报错 |
| `(VALUES ...) AS x(a, b)` | 行值构造器；无别名列时列名为 `column1..n` |
| `INNER/LEFT/RIGHT/FULL JOIN ... ON/USING` | 等值 JOIN 自动走 hash join；`USING(col)` 等价两侧同名列等值 |
| `CROSS JOIN`、`a, b` | 笛卡尔积 |
| `DUAL` | Oracle 哑表（单行零列，大小写不敏感） |

**不支持**：`NATURAL JOIN`、`LATERAL`、表函数/`UNNEST`、`TABLESAMPLE`、表时态（`AS OF`）、`PIVOT`——均显式报错。

### SELECT 备注

- **执行与优化**：`ORDER BY <索引键> + 常量 LIMIT/OFFSET` 走索引序窗口（首屏/深页只装载窗口内文档）；无索引但有界 LIMIT 走键提取 top-K；`SELECT COUNT(*) FROM t`（无 WHERE/GROUP BY/ORDER/LIMIT）走免解码活槽计数。查询计划为规则式，无 `EXPLAIN`。
- **分组扩展**：`ROLLUP(a,b)` 展开为 `(a,b),(a),()`；`CUBE(a,b)` 展开为全部子集；`GROUPING SETS` 为显式列表；多项扩展按笛卡尔积组合；重复/嵌套分组集合报错，CUBE 上限 12 个元素。缺失分组键在输出行中为 `NULL`，用 `GROUPING()` 区分。
- **聚合修饰**：`COUNT/SUM/... ( [DISTINCT] expr ) [ FILTER (WHERE condition) ]`；`FILTER` 先过滤行再做 DISTINCT 与聚合。
- **集合运算**：两臂列数必须一致（列名取左）；`UNION`/`INTERSECT`/`EXCEPT` 默认去重，`ALL` 保留多重集（`INTERSECT ALL`/`EXCEPT ALL` 按最小计数）。
- **CTE**：仅非递归；`WITH RECURSIVE` 报错。CTE 与派生表一样支持别名列。
- **子查询**：支持非相关标量、`IN`、`EXISTS`、`ANY/SOME/ALL`；**相关子查询报错**。
- **ROWNUM**：Oracle 伪列，在行取出后、WHERE 与 ORDER BY 之前编号；被 WHERE 过滤的行消耗编号；`SELECT *` 不含该列。
- **GROUP BY 限制**：分组键不能是输出别名或序号（与 HAVING/ORDER BY 不同，显式报错）。

### SELECT 示例

**A. 基本查询与动态字段**

```sql
SELECT id, name FROM users WHERE age >= 18 ORDER BY name LIMIT 10;

-- 文档模型：字段并集 + 追加表达式
SELECT *, age * 2 AS age2 FROM users;
```

**B. 分组与分组扩展**

```sql
SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 1;

SELECT g, SUM(v), GROUPING(g) AS is_total
FROM t GROUP BY ROLLUP(g) ORDER BY g;

SELECT g, h, SUM(v) FROM t GROUP BY GROUPING SETS ((g, h), (g), ());
```

**C. 聚合 FILTER**

```sql
SELECT COUNT(*) FILTER (WHERE v > 2)  AS big,
       SUM(v)   FILTER (WHERE g = 'a') AS a_sum
FROM t;
```

**D. 分页**

```sql
SELECT id FROM t ORDER BY id LIMIT 20 OFFSET 40;          -- 第 3 页
SELECT id FROM t ORDER BY id FETCH FIRST 20 ROWS ONLY;    -- Oracle 写法
SELECT id FROM t ORDER BY score DESC FETCH FIRST 3 ROWS WITH TIES;
```

**E. 集合运算与 CTE**

```sql
SELECT id FROM a UNION ALL SELECT id FROM b;
SELECT id FROM a EXCEPT SELECT id FROM b;                 -- MINUS 同义
SELECT id FROM a INTERSECT ALL SELECT id FROM b;

WITH big AS (SELECT id FROM t WHERE v > 100)
SELECT COUNT(*) FROM big;
```

**F. 派生表与 VALUES**

```sql
SELECT p.id, p.s
FROM (VALUES (1, 'x'), (2, 'y')) AS p(id, s)
WHERE p.id > 1;

SELECT c.id FROM (SELECT id FROM t) AS c;
```

**G. JOIN**

```sql
SELECT o.id, u.name
FROM orders AS o LEFT JOIN users AS u ON o.user_id = u.id
WHERE o.total > 100;

SELECT * FROM a JOIN b USING (id);
```

**H. 量化比较与 NULL 安全比较**

```sql
SELECT v FROM t WHERE v > ANY (SELECT v FROM t WHERE v > 3);
SELECT v FROM t WHERE v <> ALL (SELECT v FROM t);
SELECT a IS DISTINCT FROM b FROM t;
```

## INSERT

向表中插入一行或多行。

### 语法

```sql
INSERT [ OR { REPLACE | IGNORE } ] INTO table_name [ ( column [ , ...n ] ) ]
    { VALUES ( expr [ , ...n ] ) [ , ...n ] | SELECT ... }
    [ ON CONFLICT [ ( column [ , ...n ] ) | ON CONSTRAINT index_name ] DO NOTHING ]
    [ RETURNING { * | expr [ [ AS ] alias ] } [ , ...n ] ]

REPLACE INTO table_name ...          -- = INSERT OR REPLACE
```

### 参数

| 参数 | 说明 |
|---|---|
| 列清单 | 省略时按表的声明列序取全部列；`INSERT ... SELECT` 未给列清单时取查询的输出列 |
| `VALUES` | 常量表达式，多行用逗号分隔；`VALUES` 中的子查询会被先求值 |
| `SELECT` | `INSERT INTO t (...) SELECT ...`；**auto-GUID 表不支持**（随机会在对端分叉） |
| `OR REPLACE` / `REPLACE INTO` | 唯一键冲突时**先删除冲突行再插入** |
| `OR IGNORE` / `ON CONFLICT DO NOTHING` | 唯一键冲突时跳过该行；目标列（及 `ON CONSTRAINT index_name`）限定**只跳过该唯一约束**的冲突，命中其他唯一约束仍报错；不支持 `DO UPDATE` / `DO REPLACE` |
| `RETURNING` | 返回插入后的行（`*` 或表达式/别名） |

### 备注

- **auto-GUID 主键**：`GUID`/`UUID`/`UNIQUEIDENTIFIER`/`UUIDV7` 列声明 `AUTOINCREMENT` 后，INSERT 省略或写入 NULL 时由引擎生成 UUIDv7，并把语句回写成显式值以便跨节点确定性重放。
- `AUTOINCREMENT` 的整数列取当前最大值 +1（非触发器语义）。
- 整条语句原子；约束（NOT NULL/UNIQUE/PK/CHECK/FK）先全量校验后写入。
- **冲突检测**：无目标时覆盖所有唯一约束；给了目标（列集合或 `ON CONSTRAINT`）时只跳过该约束的冲突，未匹配到任何唯一约束会显式报错。

### INSERT 示例

```sql
INSERT INTO users (id, name) VALUES (1, 'ann'), (2, 'bob');
INSERT INTO users (id, name) SELECT id, name FROM staging;

INSERT INTO users (id, name) VALUES (1, 'ann') ON CONFLICT DO NOTHING;
INSERT OR REPLACE INTO users (id, name) VALUES (1, 'ann2');

INSERT INTO users (id, name) VALUES (3, 'carl') RETURNING id, name;
```

## UPDATE

更新满足条件的行。

### 语法

```sql
UPDATE table_name [ [ AS ] alias ]
SET column = expr [ , ...n ]
[ FROM table_source [ , ...n ] ]
[ WHERE condition ]
[ RETURNING { * | expr [ [ AS ] alias ] } [ , ...n ] ]
```

### 参数

| 参数 | 说明 |
|---|---|
| `SET` | 逐列赋值；右侧可引用目标列与 `FROM` 中的源表（`src.col`） |
| `FROM` | 多表更新：目标行与源行按 WHERE 关联 |
| `WHERE` | 省略时更新全部行 |
| `RETURNING` | 返回更新后的行 |

### 备注

- 目标表必须是简单表名；不能同时用同一别名自连接以外的复杂 FROM（显式报错）。
- WHERE 命中索引列时走两阶段索引维护快路径，其余为整表重写。

### UPDATE 示例

```sql
UPDATE users SET name = 'ann', age = age + 1 WHERE id = 1 RETURNING *;

UPDATE a SET v = b.w FROM b WHERE a.id = b.id;
```

## DELETE

删除满足条件的行。

### 语法

```sql
DELETE FROM table_name [ [ AS ] alias ]
[ USING table_source [ , ...n ] ]
[ WHERE condition ]
[ RETURNING { * | expr [ [ AS ] alias ] } [ , ...n ] ]
```

### 参数

| 参数 | 说明 |
|---|---|
| `FROM` | 恰好一个目标表 |
| `USING` | 附加表，与目标表在 WHERE 中关联 |
| `WHERE` | 省略时删除全部行 |
| `RETURNING` | 返回被删除的行 |

### DELETE 示例

```sql
DELETE FROM users WHERE id = 1;
DELETE FROM a USING b WHERE a.id = b.id RETURNING a.id;
```

## MERGE

按源集与目标的匹配结果执行 UPDATE/INSERT（Oracle/SQL 标准 upsert 的受限实现）。

### 语法

```sql
MERGE INTO target [ [ AS ] alias ]
USING source [ [ AS ] alias ]
ON condition
WHEN MATCHED THEN UPDATE SET column = expr [ , ...n ]
WHEN NOT MATCHED THEN INSERT [ ( column [ , ...n ] ) ] VALUES ( expr [ , ...n ] )
```

### 备注

- ON 为任意表达式：目标列用裸名，源列用 `别名.列` 限定。
- 一个目标行被多个源行命中时报错（Oracle ORA-30926 语义）。
- 语句整体原子应用并随复制扇出，对端按相同状态确定性重放。
- 不支持：`WHEN ... AND <predicate>`、`UPDATE ... WHERE` / `DELETE WHERE`、INSERT 谓词、`ROW` 形式、`BY SOURCE`、源侧引用 CTE。

### MERGE 示例

```sql
MERGE INTO stock AS s
USING feed AS f ON s.sku = f.sku
WHEN MATCHED THEN UPDATE SET qty = f.qty
WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (f.sku, f.qty);
```

## CREATE TABLE

创建表；可同时声明约束并创建约束索引。

### 语法

```sql
CREATE TABLE [ IF NOT EXISTS ] table_name
( { column_name data_type [ column_constraint [ ...n ] ]
  | table_constraint } [ , ...n ] )

CREATE TABLE [ IF NOT EXISTS ] table_name AS SELECT ...
```

`column_constraint`：

```sql
[ CONSTRAINT name ]
{ PRIMARY KEY
| UNIQUE
| NOT NULL | NULL
| DEFAULT expr
| CHECK ( condition )
| REFERENCES ref_table ( ref_column )
| { AUTOINCREMENT | AUTO_INCREMENT } }
```

`table_constraint`：

```sql
[ CONSTRAINT name ]
{ PRIMARY KEY ( column )
| UNIQUE ( column )
| CHECK ( condition )
| FOREIGN KEY ( column [ , ...n ] ) REFERENCES ref_table ( ref_column [ , ...n ] ) }
```

### 参数

| 参数 | 说明 |
|---|---|
| `IF NOT EXISTS` | 已存在时静默返回（0 行受影响） |
| `AS SELECT` | CTAS：列名与行来自查询；`TEMPORARY` 关键字接受并按普通表处理 |
| `PRIMARY KEY` | **仅单列**；主键**不隐含 NOT NULL**，需显式声明（EF 提供程序因此生成 `NOT NULL`） |
| `UNIQUE` | 单列约束；复合唯一请用 `CREATE UNIQUE INDEX`（表级复合形式显式报错） |
| `NOT NULL` | 写入 NULL 或缺字段时报错 |
| `DEFAULT expr` | INSERT 省略该列时求值填充；`ALTER TABLE ADD COLUMN` 带 DEFAULT 会回填存量行 |
| `CHECK` | 写入时校验；仅求值为 FALSE 时失败（含 NULL 引用的表达式视为未知通过） |
| `REFERENCES` | 外键（RESTRICT 式）；不支持 `ON DELETE`/`ON UPDATE` 动作 |
| `AUTOINCREMENT` | 整数列：插入省略/NULL 时取 max+1；GUID 列：生成 UUIDv7 并回写语句 |
| `GUID` 类型 | `GUID`/`UUID`/`UNIQUEIDENTIFIER`/`UUIDV7` |

### 备注

- 主键与 UNIQUE 约束的 B+ 树随建表创建，`sqlite_master` 以 `sqlite_autoindex_<表>_<n>` 派生展示（不落 catalog，不可 `DROP INDEX`）。
- `docsql_users`/`docsql_roles`/`docsql_role_members`/`docsql_grants` 为保留表名，普通 DDL/DML 拒绝。
- 存储/布局子句（`INHERITS`、`WITHOUT ROWID`、`ON COMMIT`、`LOCATION`、`STORED AS`、
  `CLUSTERED BY`、Hive/Redshift 分布与分区等）显式报错，不静默忽略。
- 约束装饰（`DEFERRABLE`、`INITIALLY DEFERRED`、`NOT ENFORCED`、
  `NULLS NOT DISTINCT`、`MATCH FULL/PARTIAL`）显式报错；等价的空操作写法
  （`NOT DEFERRABLE`、`INITIALLY IMMEDIATE`、`ENFORCED`、`MATCH SIMPLE`、
  `NULLS DISTINCT`）接受。
- `TEMPORARY` 关键字被接受：引擎没有会话级临时表，一律按普通持久表处理
  （与 CTAS 的 `TEMPORARY` 一致）。

### CREATE TABLE 示例

```sql
CREATE TABLE users (
    id    INT PRIMARY KEY NOT NULL,
    name  TEXT NOT NULL DEFAULT 'anon',
    email TEXT UNIQUE,
    age   INT CHECK (age >= 0),
    org   INT REFERENCES orgs (id)
);

CREATE TABLE t (
    id  TEXT PRIMARY KEY,
    uid GUID AUTOINCREMENT,          -- 自动 UUIDv7
    n   INT AUTOINCREMENT            -- max+1
);

CREATE TABLE archive AS SELECT * FROM users WHERE age > 65;
```

## ALTER TABLE

修改表结构。

### 语法

```sql
ALTER TABLE table_name { ADD [ COLUMN ] column_name data_type [ constraint ... ]
                       | DROP [ COLUMN ] column_name
                       | RENAME COLUMN old_name TO new_name
                       | RENAME TO new_table_name }
```

### 参数

| 操作 | 说明 |
|---|---|
| `ADD COLUMN` | 支持 `DEFAULT`（自动回填存量行）；`NOT NULL` 必须带 DEFAULT；不支持 PK/UNIQUE/FK/自增列 |
| `DROP COLUMN` | 主键列不可删除 |
| `RENAME COLUMN` | 重命名并同步索引/约束元数据 |
| `RENAME TO` | 重命名表 |

### 示例

```sql
ALTER TABLE users ADD COLUMN note TEXT DEFAULT 'n/a';
ALTER TABLE users RENAME COLUMN note TO remarks;
ALTER TABLE users DROP COLUMN remarks;
ALTER TABLE users RENAME TO members;
```

## TRUNCATE TABLE / DROP TABLE

清空或删除表。

### 语法

```sql
TRUNCATE TABLE [ IF EXISTS ] table_name [ , ...n ]
    [ RESTART IDENTITY | CONTINUE IDENTITY ] [ CASCADE | RESTRICT ]
DROP TABLE [ IF EXISTS ] table_name [ , ...n ] [ CASCADE | RESTRICT ]
```

### 备注

- `TRUNCATE` 清空数据、保留表结构与索引，可一次指定多张表。
- `TRUNCATE ... CASCADE`：引用目标表的外键子表（传递闭包）一并清空；
  默认/`RESTRICT` 在子表仍有引用行时报错。`CONTINUE IDENTITY` 显式报错
  （`AUTOINCREMENT` 水位由现存行推导，清空后必然重置）；分区 / `ON CLUSTER` /
  `ONLY` 同样显式报错。
- `DROP TABLE` 删除前会校验外键引用：仍被其他表引用时报错；多表删除先整体校验再应用。
  `DROP TABLE ... CASCADE` 连带删除依赖它的视图，并从引用子表移除指向该表的外键声明
  （子表本身保留）。
- `DROP ... PURGE` 显式报错。删除被视图引用的表请先 `DROP VIEW` 或用 `CASCADE`。

### 示例

```sql
TRUNCATE TABLE staging, staging2;
TRUNCATE TABLE parent CASCADE;      -- 连带清空外键子表
DROP TABLE IF EXISTS staging2;
DROP TABLE parent CASCADE;          -- 连带删依赖视图/移除子表外键声明
DROP VIEW IF EXISTS active_users;
```

## CREATE VIEW / DROP VIEW

把一条 SELECT 存为只读命名查询。

### 语法

```sql
CREATE [ OR REPLACE ] VIEW view_name AS SELECT ...
DROP VIEW [ IF EXISTS ] view_name [ , ...n ] [ CASCADE | RESTRICT ]
```

### 参数

| 参数 | 说明 |
|---|---|
| `OR REPLACE` | 同名视图已存在时整体替换定义；同名对象是表时显式报错 |
| `AS SELECT` | 建视图时干跑校验：基表/列必须存在，自引用与传递闭包成环被拒；`SELECT` 展开预算 16 层 |
| 列清单 | 不支持（给投影加别名即可） |
| `MATERIALIZED` / `SECURE` / `WITH NO SCHEMA BINDING` | 显式报错 |

### 备注

- 视图只存定义、不占存储；SELECT 穿透视图执行（支持视图套视图与外层
  WHERE/JOIN/GROUP BY 组合），外层写语句对视图显式报错
  （INSERT/UPDATE/DELETE/MERGE/TRUNCATE/ALTER/INDEX）。
- 授权按视图名发放：读视图只需视图授权（基表权限被收口），写语句经视图读源
  需要基表授权（fail-closed 展开）。
- `dump_script()`/备份/join 快照按依赖拓扑序携带视图；`sqlite_master`
  只列 `type='view'` 定义。
- 删除基表或视图前做依赖检查，默认拒绝；`CASCADE` 连带删除依赖视图；
  删除视图时同步清理其授权记录。

### 示例

```sql
CREATE TABLE users (id INT PRIMARY KEY NOT NULL, name TEXT);

CREATE VIEW active_users AS SELECT id, name FROM users WHERE id > 0;
CREATE OR REPLACE VIEW active_users AS SELECT id FROM users;

SELECT * FROM active_users WHERE id = 1;   -- 外层过滤穿透视图
DROP VIEW active_users;
```


## CREATE INDEX / DROP INDEX

创建/删除索引。索引名在数据库范围内唯一。

### 语法

```sql
CREATE [ UNIQUE ] INDEX [ IF NOT EXISTS ] index_name
    ON table_name ( column [ , ...n ] )
DROP INDEX [ IF EXISTS ] index_name [ , ...n ]
```

### 参数

| 参数 | 说明 |
|---|---|
| `UNIQUE` | 唯一索引；复合唯一按**完整键组合**判重，任一列 NULL 的行跳过整键（不判重） |
| 列清单 | 仅限普通列名，按列序构成复合键（前导列等值可走索引探测） |
| `IF NOT EXISTS` | 同名索引已存在时静默返回 |

### 备注

- 表达式索引、部分索引、JSON 路径索引不支持（显式报错）。
- `sqlite_autoindex_` 前缀为保留名；自动索引不可删除。
- `DROP INDEX` 会把非唯一索引从 `sqlite_master` 移除，但底层 B+ 树保留继续服务非唯一探测。

### 示例

```sql
CREATE INDEX ix_users_age ON users (age);
CREATE UNIQUE INDEX ux_org_name ON orgs (name);
CREATE INDEX ix_orders_multi ON orders (user_id, created_at);
DROP INDEX ix_users_age;
```

## 事务语句

DocSQL 为单写者引擎：同一时刻只有一个写事务；并发 `BEGIN` 在服务端排队（上限 30s）。只读语句走 MVCC 快照，不阻塞写、不被写阻塞。

### 语法

```sql
BEGIN [ TRANSACTION | WORK ]
COMMIT [ WORK ]
ROLLBACK [ WORK ] [ TO [ SAVEPOINT ] savepoint_name ]
SAVEPOINT savepoint_name
RELEASE [ SAVEPOINT ] savepoint_name
```

### 备注

- 事务归属打开它的连接；连接断开自动 `ROLLBACK`。
- 保存点可嵌套、可重名（取最近者）。
- `ROLLBACK TO` 会把该保存点自身一并丢弃（与 SQLite/PostgreSQL 不同），之后不要再 `RELEASE`。
- 非事务属主连接发起事务控制语句会报错；事务内的写缓冲在 `COMMIT` 时整批落盘。

### 示例

```sql
BEGIN;
INSERT INTO t VALUES (1);
SAVEPOINT sp1;
UPDATE t SET v = 2;
ROLLBACK TO sp1;
COMMIT;
```

## 用户与角色语句

语法由引擎手写解析（sqlparser 不认该文法）。所有语句仅 admin 可执行；效果立即生效（撤销随下一帧生效），并随复制/快照同步到集群。

### 语法

```sql
CREATE USER user_name PASSWORD 'plain_text_or_hash'
ALTER USER user_name PASSWORD 'plain_text_or_hash'
DROP USER user_name
CREATE ROLE role_name
DROP ROLE role_name
GRANT { role [ , ...n ] } TO user [ , ...n ]
REVOKE { role [ , ...n ] } FROM user [ , ...n ]
GRANT { SELECT | INSERT | UPDATE | DELETE | ALL } [ , ...n ]
      ON [ TABLE ] { table [ , ...n ] } TO user [ , ...n ]
REVOKE { SELECT | INSERT | UPDATE | DELETE | ALL } [ , ...n ]
      ON [ TABLE ] { table [ , ...n ] } FROM user [ , ...n ]
```

### 参数

| 参数 | 说明 |
|---|---|
| 用户名 | 规范化为小写；`"Bob"` 与 `bob` 同名；密码最少 8 字符 |
| `PASSWORD` | 明文永不落盘：引擎回写为 `$pbkdf2-sha256$...` 哈希后再落盘/复制/记录日志 |
| 内置角色 | `admin`（全部）、`readwrite`（DML + PUBLISH/TRIM）、`readonly`（SELECT） |
| 表级授权 | `SELECT/INSERT/UPDATE/DELETE/ALL`；`REVOKE ... ON 表` 按表名精确匹配 |
| 保留名 | `docsql_users`/`docsql_roles`/`docsql_role_members`/`docsql_grants` 不可直接读写 |

存在任一用户后，匿名连接被拒绝（`DOCSQL_TOKEN` 旁路不受影响，可用于首次建号）。

### 示例

```sql
CREATE USER analyst PASSWORD 'change-me-1';
CREATE ROLE auditor;
GRANT auditor TO analyst;
GRANT SELECT ON TABLE orders TO auditor;
REVOKE SELECT ON orders FROM auditor;
ALTER USER analyst PASSWORD 'new-secret-1';
DROP USER analyst;
```

## 函数

### 聚合函数

```sql
agg_name ( [ DISTINCT ] { expr | * } ) [ FILTER ( WHERE condition ) ]
```

| 函数 | 说明 |
|---|---|
| `COUNT(*)` | 行数（无 WHERE/GROUP BY/ORDER/LIMIT 时走免解码计数） |
| `COUNT(expr)` | 非 NULL 值个数 |
| `COUNT_BIG` | T-SQL 兼容，语义同 `COUNT` |
| `SUM` / `AVG` / `MIN` / `MAX` | 数值求和/均值/最小/最大；`SUM` 空集为 NULL |
| `GROUP_CONCAT` / `STRING_AGG` | 文本拼接，可选分隔符 `STRING_AGG(v, ',')` |
| `GROUPING(expr)` | 该表达式是否不在当前分组集合（1/0），仅 SELECT 列表可用 |

所有聚合支持 `DISTINCT` 与 `FILTER (WHERE ...)`；`COUNT(DISTINCT expr)` 先去重再计数。

### 标量函数

| 分类 | 函数 | 说明 |
|---|---|---|
| 字符串 | `UPPER/UCASE`、`LOWER/LCASE`、`LENGTH/LEN`、`SUBSTR/SUBSTRING(s, start [, len])`、`TRIM/LTRIM/RTRIM(s)`、`CONCAT(a, b, ...)` | 位置按字符计；TRIM 仅单参形式 |
| 数值 | `ABS`、`ROUND(x [, digits])` | ROUND 对 DECIMAL 精确四舍五入（半离零），FLOAT 保持浮点 |
| 空值/条件 | `COALESCE`/`IFNULL`/`ISNULL`、`NULLIF` | |
| 类型 | `TYPEOF(v)` | 返回 `null`/`bool`/`integer`/`float`/`decimal`/`text`/`blob`/`array`/`object` |
| JSON | `JSON_EXTRACT(doc, '$.a.b[0]')`、`JSON_TYPE(doc, path)`、`JSON_VALID(text)`、`JSON_ARRAY_CONTAINS(json_text, needle)` | 坏文本/缺路径返回 NULL（不中断扫描）；`JSON_ARRAY_CONTAINS` 非数组为 FALSE |
| Oracle 兼容 | `NVL(a,b)`、`NVL2(a,b,c)`、`DECODE(expr, s1, r1, ..., [default])`、`INSTR(str, sub)`、`LPAD/RPAD(str, len [, pad])`、`GREATEST/LEAST(...)`、`TO_NUMBER(text)`、`TO_CHAR(v)`、`SYSDATE()` | `DECODE` 用全序匹配（`NULL = NULL` 命中）；`TO_NUMBER` 解析失败显式报错；`TO_CHAR` 不支持格式掩码 |

### 条件表达式

```sql
CASE [ operand ] WHEN value THEN result [ ...n ] [ ELSE result ] END
CAST(expr AS type)
```

### 示例

```sql
SELECT UPPER(name), LENGTH(name) FROM users;
SELECT ROUND(price, 2), ABS(delta) FROM t;
SELECT COALESCE(note, 'none'), NVL2(v, 'has', 'empty') FROM t;
SELECT JSON_EXTRACT(doc, '$.user.name'), JSON_VALID('{"a":1}') FROM t;
SELECT CASE WHEN n > 0 THEN 'pos' WHEN n < 0 THEN 'neg' ELSE 'zero' END FROM t;
```

## 系统视图与元数据

### information_schema

| 视图 | 列 |
|---|---|
| `information_schema.tables` | `table_name TEXT`、`pages INT` |
| `information_schema.columns` | `table_name`、`column_name`、`is_nullable`、`data_type`（自动 GUID 列为 `GUID`，其余为 `ANY`） |

### sqlite_master

SQLite 兼容的 DDL 自省视图（EF Core schema 同步使用），行：`type`（`table`/`index`）、`name`、`tbl_name`、`sql`。自动索引（PK/UNIQUE）不列出；`sqlite_temporal_master` 为其同义视图。

### Oracle 字典视图

`ALL_TABLES`/`USER_TABLES`（`owner`、`table_name`、`num_rows`、`blocks`）、`ALL_TAB_COLUMNS`/`USER_TAB_COLUMNS`（`owner`、`table_name`、`column_name`、`data_type`、`nullable`、`column_id`）、`ALL_INDEXES`/`USER_INDEXES`（`owner`、`index_name`、`table_name`、`uniqueness`）。`OWNER` 恒为 `DOCSQL`；`USER_*` 与 `ALL_*` 同数据（单全局命名空间）；内部表不出现。

### docsql_pubsub

持久化 pub/sub 历史视图，映射系统表 `_pubsub_messages`：`id INTEGER`（节点内单调）、`channel TEXT`、`ts_ms INT`、`payload TEXT`；可 `SELECT`/`ORDER BY`/`LIMIT`。

### 系统表（只读）

| 表 | 列 | 说明 |
|---|---|---|
| `_pubsub_messages` | `id, channel, ts_ms, payload` | 优先通过 `docsql_pubsub` 视图访问 |
| `_cluster_log` | `seq INT PRIMARY KEY, sql TEXT` | 本节点写期刊（增量追赶） |
| `_cluster_pos` | `node_id TEXT PRIMARY KEY, seq INT` | 各来源节点已应用位点 |
| `_cluster_id` | `id TEXT` | 本节点持久身份 |

系统表允许 `SELECT`，写入/DDL 一律拒绝（按 AST 写目标分类）。`PUBLISH`/`TRIM` 是协议命令而非 SQL（见 README）。

## 兼容性与限制

### 单写者与并发

写路径全库串行（单写者），只读语句走 MVCC 快照，读不阻塞写、写不阻塞读；快照依赖 WAL 保留窗口，过旧时响亮报 `SnapshotTooOld`（可重试）。并发 `BEGIN` 在服务端排队，上限 30s。

### 与标准 SQL 的差异（重要）

| 主题 | DocSQL 行为 |
|---|---|
| NULL 逻辑 | 全序，无 UNKNOWN；`NULL = NULL` 为 TRUE |
| 主键与 NULL | `PRIMARY KEY` 不隐含 `NOT NULL` |
| 主键类型 | 仅单列；复合唯一用 `CREATE UNIQUE INDEX` |
| 相关子查询 | 不支持（显式报错） |
| 窗口函数 / 递归 CTE | 不支持（显式报错） |
| `SELECT t.*` | 不支持（用 `*`） |
| `GROUP BY` 输出别名 | 不支持（显式报错）；`GROUP BY 1` 按常量 1 处理，无序号语义 |
| 外键动作 | 仅 RESTRICT 式，`ON DELETE/UPDATE` 报错 |

### SQLite 兼容面

`sqlite_master`/`sqlite_temporal_master`、`LIMIT n` / `LIMIT off, n` / `LIMIT -1`、`REPLACE INTO`、`INSERT OR REPLACE/IGNORE`、`PRAGMA`（接受并忽略）。

### Oracle 兼容面

`FROM DUAL`、`ROWNUM`、`FETCH FIRST/NEXT n ROWS ONLY`、`MINUS`、`NVL/NVL2/DECODE/INSTR/LPAD/RPAD/GREATEST/LEAST/TO_NUMBER/TO_CHAR/SYSDATE`、`ALL_*`/`USER_*` 字典视图、`MERGE` 与 `DECODE` 的 `NULL = NULL` 匹配语义。

### T-SQL（SQL Server）兼容面

**已支持**：`COUNT_BIG`、`ISNULL`、`LEN`、`CAST(... AS BIT)`、`INFORMATION_SCHEMA.*`（大小写不敏感）、`sqlite_master`（大小写不敏感）、`UPDATE ... FROM`、`DELETE ... USING`、`MERGE`（受限）、`ORDER BY (SELECT 1)`（EF Core Skip/Take 形状）、`x != TRUE` 命中 NULL/缺失字段（软删过滤）、`JSON_ARRAY_CONTAINS`（实体原始集合 Contains）；`BIT`/`DATETIME2`/`NVARCHAR(MAX)` 等类型名按声明类型接受（值模型不变）。

**显式报错**（不静默、不降级）：

| 类别 | 语法 |
|---|---|
| SELECT | `TOP [n]`、`INTO`、`FOR XML/JSON`、`OPTION (...)`、`FOR SYSTEM_TIME` |
| 运算符/变量 | `@@ROWCOUNT`/`@@VERSION` 等 `@@` 系统变量、`@param` 变量与 `SELECT @x = 1` 赋值、`N'...'` 字面量、`[bracket]` 标识符、`'a' + 'b'` 字符串加法（用 `\|\|`） |
| FROM/联接 | `WITH (NOLOCK)` 等表提示、旧式 `t (NOLOCK)`、`CROSS/OUTER APPLY`、`PIVOT`/`UNPIVOT`、表函数、`TABLESAMPLE` |
| DML | `OUTPUT INSERTED/DELETED...`（用 `RETURNING`）、`MERGE ... OUTPUT`、`MERGE WHEN MATCHED THEN DELETE`、`UPDATE/DELETE ... ORDER BY/LIMIT`、`DELETE t FROM ...` 多表删除（用 `DELETE ... USING`） |
| DDL | `#temp`/`##temp` 临时表、`IDENTITY(1,1)`、`ROWGUIDCOL`、`CLUSTERED`/`NONCLUSTERED INDEX`、`INCLUDE`、`WHERE` 过滤索引、`USING` 索引类型、索引存储选项 |
| 目录 | `sys.*`/`sysobjects`（报错并指向 `information_schema`/`sqlite_master`） |
| 语句/过程 | `USE`、`SET`、`DECLARE`、`PRINT`、`EXEC`、`WAITFOR`、`IF`、`TRY/CATCH`、`DENY`、`CREATE PROCEDURE/TRIGGER/SCHEMA/SEQUENCE` |
| 函数 | `GETDATE`、`NEWID`、`DATEPART`、`DATEDIFF`、`CONVERT`/`TRY_CONVERT`、`IIF`、`CHARINDEX`、`REPLACE`、`LEFT`/`RIGHT`、`STRING_SPLIT`、`SERVERPROPERTY`、`OBJECT_ID`、`DB_NAME`、`HOST_NAME`、`SUSER_SNAME`、`SCOPE_IDENTITY` 等（均返回 `unknown function`） |

窗口函数（`OVER`）、递归 CTE、`CROSS APPLY` 等结构性缺口见[不支持的语法](#不支持的语法)。

## 不支持的语法

以下语法在解析/执行层**显式报错**（不静默吞掉、不降级近似）：

| 类别 | 语法 |
|---|---|
| 窗口 | `OVER(...)`、`WINDOW` 子句、`QUALIFY` |
| 查询结构 | `SELECT TOP`、`SELECT INTO`、`SELECT AS VALUE/STRUCT`、`SELECT * EXCLUDE/EXCEPT/REPLACE/RENAME`、`ORDER BY COLLATE`、`SELECT t.*` |
| 连接 | `NATURAL JOIN`、`LATERAL` 派生表、表函数/`UNNEST`、`TABLESAMPLE`、`PIVOT`、表时态 `AS OF` |
| 锁/伪指令 | `FOR UPDATE`/`FOR SHARE`、`FOR XML`/`FOR JSON`、`SETTINGS`、`FORMAT`、pipe 操作符 |
| 子查询/CTE | 相关子查询、`WITH RECURSIVE` |
| 分组 | `WITH ROLLUP`/`WITH TOTALS` 等 GROUP BY 修饰符（请写 `GROUP BY ROLLUP(...)`/`CUBE(...)`）、嵌套/重复分组集合、`CUBE` 超 12 元素、不配合 GROUP BY 或聚合的 `HAVING` |
| 事务/冲突 | `ON CONFLICT DO UPDATE`、`ON DUPLICATE KEY UPDATE`、`DEFAULT VALUES`、无匹配唯一约束的 `ON CONFLICT` 目标 |
| DDL | 复合 `PRIMARY KEY`/`UNIQUE` 表约束、表达式/部分/JSON 路径索引、`CREATE TRIGGER`、`CREATE MATERIALIZED VIEW`、`ALTER TABLE` 的改约束/改类型、CREATE TABLE 存储/布局子句（`INHERITS`/`WITHOUT ROWID`/`LOCATION`/`STORED AS`/`CLUSTERED BY` 等）与约束装饰（`DEFERRABLE`/`INITIALLY DEFERRED`/`NOT ENFORCED`/`NULLS NOT DISTINCT`/`MATCH FULL/PARTIAL`） |
| T-SQL 专有 | `OUTPUT`（用 `RETURNING`）、`@` 变量/参数、表提示 `WITH (...)`、旧式 `(NOLOCK)`、`#`/`##` 临时表、`IDENTITY(1,1)`、`ROWGUIDCOL`、`CLUSTERED`/`NONCLUSTERED`、索引 `INCLUDE`/`WHERE`/`USING`/存储选项、`sys.*`/`sysobjects`、`N'...'`、`[方括号]` 标识符、`'a' + 'b'`（用 `\|\|`）、`CROSS/OUTER APPLY`、`UPDATE/DELETE ... ORDER BY/LIMIT`、`DELETE t FROM ...` |
| 分页 | `FETCH ... PERCENT` |
| 外键 | `ON DELETE`/`ON UPDATE` 动作 |

`PRAGMA` 例外：有意接受并忽略，不产生任何效果。

## 另请参阅

- [功能清单](features.md)——按模块的完整能力列表
- [已知边界与定位](limitations.md)——架构边界与路线图
- [安全指南](security.md)——鉴权、用户与角色部署
- [运维手册](operations.md)——备份、恢复、集群运维
- [驱动与协议](drivers.md)——ADO.NET / EF Core / CLI / REST
