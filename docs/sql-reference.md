# DocSQL SQL 参考

本参考按 Transact-SQL 参考（MSDN）的组织方式编写：每条语句给出**语法**、**参数**、**备注**与**示例**。

DocSQL 的 SQL 方言以 **SQL 标准**为基准，并兼容 SQLite（`sqlite_master`、`LIMIT` 变体、`REPLACE INTO`）、Oracle（`DUAL`、`ROWNUM`、`NVL` 函数族、字典视图、`MERGE`）与 SQL Server/EF 生态（`information_schema`、`x != TRUE` 软删语义、`JSON_ARRAY_CONTAINS`、T-SQL 表达式层——函数族/`TOP`/方括号/`CONVERT` 等见 [T-SQL 兼容面](#tsql-sql-server兼容面)）。未实现的语法一律在解析/执行层**显式报错**，不静默忽略；完整清单见[不支持的语法](#不支持的语法)。

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

`INT`/`INTEGER`、`TEXT`/`CHAR`/`VARCHAR`/`STRING`、`BOOL`/`BOOLEAN`、`FLOAT`/`REAL`/`DOUBLE`、`DECIMAL`/`NUMERIC[(p,s)]`、`BLOB`/`BINARY`/`BYTES`、`TIMESTAMP`/`DATETIME`/`DATETIME2`、`GUID`/`UUID`/`UNIQUEIDENTIFIER`/`UUIDV7`（自动 UUIDv7 主键的触发器，见 [CREATE TABLE](#create-table)）。

### CAST

```sql
CAST(expr AS type)
```

支持的目标类型：`INT`、`CHAR`/`TEXT`/`STRING`、`BOOL`、`BIT`（T-SQL，映射 BOOL）、`REAL`/`DOUBLE`/`FLOAT`、`DECIMAL`/`NUMERIC`、`BLOB`/`BYTES`/`BINARY`（文本按 UTF-8 字节入库）、`TIMESTAMP`/`DATETIME`（Str 按任意可解析时间形解析、**坏文本显式报错**；Int 视为 UTC 毫秒并做值域检查；反向 `CAST(timestamp AS INT)` 得毫秒数）。DECIMAL 的规范写法是 `CAST('123.45' AS DECIMAL)`；`DECIMAL(p,s)` 声明不做存储截断。其他目标类型（如 `NVARCHAR`）按**声明类型**处理，值保持不变。

### 时间值

时间类型为**精确 `TIMESTAMP`（UTC 毫秒）**，内部 64 位毫秒数；值域 0001..=9999 年，毫秒精度，超域显式报错（无规范文本形的值不可重放）。

```sql
SELECT TIMESTAMP '2026-01-01T00:00:00Z';        -- 字面量;坏文本显式报错
SELECT DATETIME '2026-01-01 00:00:00', DATE '2026-01-01';   -- 同一解析
SELECT NOW(), CURRENT_TIMESTAMP;                -- 当前 UTC 时间(TIMESTAMP 值)
SELECT CAST('2026-01-01T08:30:00+08:00' AS TIMESTAMP);
SELECT ts + 1500, ts - ts2 FROM t;              -- ± 毫秒整数 / 毫秒差(INT)
```

- **谓词提升**：`WHERE ts > '2026-…'` 与字符串比较按时间自动解析（不可解析 → NULL，不报错）；`BETWEEN`/`IN`/`CASE`/`IS DISTINCT FROM` 同语义。与字面量（报错）不同，这是对**列数据宽容**的读语义；
- **索引**：索引探针自动把字符串边界提升为 Timestamp 边界（采样确认带内值全为 Timestamp 时），`WHERE ts > '2026-…'` 走索引；
- **排序**：Timestamp 自成排序带，**不与 Str 混排**（`cmp_values` 的带序：数值 < Timestamp < Str < Bytes）；混合列请显式 `CAST` 统一；
- **算术**：`TIMESTAMP ± INT`（毫秒）、`TIMESTAMP - TIMESTAMP`（毫秒差）；`TIMESTAMP + TIMESTAMP` 类型错误 → NULL；无独立 `TIME`/`INTERVAL` 类型（区间用毫秒整数表达）；
- **wire 协议**：响应/参数用 `{"$ts": 毫秒}` 标记精确往返（.NET 客户端 `DateTime`/`DateTimeOffset` 参数默认走 `$ts`，`timestampformat=iso` 为旧服务器兼容开关）；
- **`DEFAULT NOW()`**：定值在**插入时**并回写进复制扇出的语句（每个节点存同值，不随重放漂移）；`CHECK` 表达式与 `MERGE` 的 wall-clock DEFAULT 显式拒绝（无回写机制就不放行）；
- `SYSDATE()` 返回当前 UTC 时间戳**文本**（Oracle 兼容；`NOW()` 返回 TIMESTAMP 值）。

## 运算符与谓词

| 类别 | 运算符/谓词 | 说明 |
|---|---|---|
| 算术 | `+ - * / %` | 数值运算；DECIMAL 参与时按十进制精确计算 |
| 比较 | `= <> != < <= > >=` | 按值全序比较（数值间类型无关：`1 = 1.0` 为真） |
| 逻辑 | `AND OR NOT` | 操作数必须为 BOOL |
| 连接 | `\|\|` | 字符串连接（NULL 传播为 NULL） |
| 范围 | `BETWEEN a AND b` | 闭区间 |
| 集合 | `IN (v1, v2, ...)` / `NOT IN (...)` | |
| 模式 | `LIKE pat [ESCAPE 'c']`、`ILIKE pat` | `%`、`_` 通配与 T-SQL 字符类 `[a-z]`/`[^…]`；ILIKE 大小写不敏感；用 `ESCAPE` 指定转义字符 |
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
SELECT [ DISTINCT | ALL ] [ TOP (n) [ WITH TIES ] ] { * | expr [ [ AS ] alias ] } [ , ...n ]
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
| 字符串 | `UPPER/UCASE`、`LOWER/LCASE`、`LENGTH/LEN`（LEN 不计尾随空格）、`SUBSTR/SUBSTRING(s, start [, len])`、`TRIM/LTRIM/RTRIM(s)`（另支持 `TRIM('ab' FROM x)` 字符集形式）、`CONCAT(a, b, ...)`（NULL 当空串;`\|\|` 仍传染） | 位置按字符计;T-SQL 字符串函数族（LEFT/RIGHT/CHARINDEX/REPLACE/REPLICATE/REVERSE/SPACE/STR/QUOTENAME/ASCII/CHAR/NCHAR/UNICODE/CONCAT_WS/TRANSLATE/STUFF/STRING_ESCAPE/FORMAT）见[T-SQL 兼容面](#tsql-sql-server兼容面) |
| 数值 | `ABS`、`ROUND(x [, digits])` | ROUND 对 DECIMAL 精确四舍五入（半离零），FLOAT 保持浮点 |
| 空值/条件 | `COALESCE`/`IFNULL`/`ISNULL`、`NULLIF` | |
| 类型 | `TYPEOF(v)` | 返回 `null`/`bool`/`integer`/`float`/`decimal`/`text`/`timestamp`/`blob`/`array`/`object` |
| 时间 | `NOW()` / `CURRENT_TIMESTAMP` / `NOW_MS` | 当前 UTC 时间，返回 TIMESTAMP 值；`NOW_MS` 为别名 |
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

### 窗口函数

`… OVER ([PARTITION BY …] [ORDER BY …])`。窗口在 WHERE 之后、DISTINCT/ORDER BY/LIMIT 之前求值；
PARTITION BY 按 `cmp_values` 编码分组，ORDER BY 语义与外层 `ORDER BY` 一致
（同 `NULLS` 规则、稳定序，并列行保持扫描序）。

| 类别 | 函数 |
|---|---|
| 排名 | `ROW_NUMBER()` `RANK()` `DENSE_RANK()` `NTILE(n)` |
| 值 | `LAG(expr [, offset [, default]])` `LEAD(expr [, offset [, default]])` `FIRST_VALUE(expr)` `LAST_VALUE(expr)` |
| 聚合 | `COUNT(*)` `COUNT(x)` `SUM` `AVG` `MIN` `MAX` `GROUP_CONCAT`/`STRING_AGG`，支持 `FILTER (WHERE …)`；无 ORDER BY 聚整个分区，有 ORDER BY 聚「分区首行 → 当前行及其并列行」（标准默认 RANGE 帧） |

```sql
SELECT name, salary,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn,
       SUM(salary) OVER (PARTITION BY dept)        AS dept_total,   -- 整分区
       SUM(salary) OVER (ORDER BY salary)          AS running,      -- 默认帧
       LAG(salary, 1, 0) OVER (ORDER BY salary)    AS prev
FROM emp;
```

- 窗口调用可出现在 SELECT 列表中（含包在算术/CASE 里的复合表达式）；WHERE/HAVING/ORDER BY 内的窗口调用显式报错；
- 与 GROUP BY 或聚合投影**不可混用**（显式报错）；DISTINCT 在窗口之后求值；
- 不支持（显式报错）：`WINDOW` 命名子句、`QUALIFY`、自定义窗口帧
  （`ROWS/RANGE/GROUPS BETWEEN …`）、`IGNORE NULLS`；默认帧可显式拼写
  （`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` 等价）。

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
| 窗口函数 | 已支持（见[窗口函数](#窗口函数)）；`WINDOW` 子句、`QUALIFY`、自定义窗口帧不支持 |
| 递归 CTE | 不支持（显式报错） |
| `SELECT t.*` | 不支持（用 `*`） |
| `GROUP BY` 输出别名 | 不支持（显式报错）；`GROUP BY 1` 按常量 1 处理，无序号语义 |
| 外键动作 | 仅 RESTRICT 式，`ON DELETE/UPDATE` 报错 |

### SQLite 兼容面

`sqlite_master`/`sqlite_temporal_master`、`LIMIT n` / `LIMIT off, n` / `LIMIT -1`、`REPLACE INTO`、`INSERT OR REPLACE/IGNORE`、`PRAGMA`（接受并忽略）。

### Oracle 兼容面

`FROM DUAL`、`ROWNUM`、`FETCH FIRST/NEXT n ROWS ONLY`、`MINUS`、`NVL/NVL2/DECODE/INSTR/LPAD/RPAD/GREATEST/LEAST/TO_NUMBER/TO_CHAR/SYSDATE`、`ALL_*`/`USER_*` 字典视图、`MERGE` 与 `DECODE` 的 `NULL = NULL` 匹配语义。

### T-SQL（SQL Server）兼容面

**已支持**（表达式/查询/记号层，实现见 `core/tsql.rs`）：

| 类别 | 内容 |
|---|---|
| 查询 | `COUNT_BIG`、`SELECT TOP n` / `TOP (n)` / `TOP n WITH TIES`（改写为 LIMIT/FETCH；`PERCENT` 报错）、`dbo.` 前缀容忍（取末段）、`ORDER BY (SELECT 1)`（EF Skip/Take）、`x != TRUE` 软删语义 |
| 记号 | `N'...'` 字面量、`[方括号]` 标识符（`]]` 转义）、字符串 `+` 拼接（与数字混合按隐式数值转换，失败报错）、位运算 `& \| ^ ~`（64 位整数） |
| 模式匹配 | `LIKE '[a-z]'` / `[^…]` 字符类（`]` 前置为字面成员、未闭合 `[` 为字面括号；`%`/`_`/`ESCAPE` 语义不变） |
| 日期时间 | `GETDATE/GETUTCDATE/SYSDATETIME/SYSUTCDATETIME`（引擎仅 UTC）、`DATEADD/DATEDIFF/DATEDIFF_BIG`（边界跨越语义、月末钳制）、`DATEPART/DATENAME`（全部 datepart 缩写）、`YEAR/MONTH/DAY/DAYOFYEAR`、`EOMONTH`、`DATEFROMPARTS` 与 `*FROMPARTS` 族、`ISDATE/ISNUMERIC` |
| 字符串 | `LEFT/RIGHT`、`CHARINDEX`、`REPLACE`、`REPLICATE`、`REVERSE`、`SPACE`、`STR`、`QUOTENAME`、`ASCII/CHAR/NCHAR/UNICODE`、`CONCAT_WS`、`TRANSLATE`、`STUFF`、`STRING_ESCAPE`（json）、`FORMAT`（常用数字/日期 token，未知 token 报错） |
| 数学 | `FLOOR/CEILING/POWER/SQRT/SQUARE/EXP/LOG/LOG10/SIGN/PI` 与三角函数族 |
| 转换 | `CONVERT(type, value[, style])`（常用日期 style 双向：23/101/112/120/121/126 等；未知 style 报错）、`TRY_CAST/TRY_CONVERT/PARSE/TRY_PARSE`（失败 → NULL；PARSE 文化仅 en-US） |
| 逻辑 | `IIF`、`CHOOSE`、`ISNULL` |
| 标识/元数据 | `NEWID/NEWSEQUENTIALID`（UUIDv7；**仅 SELECT/INSERT** —— INSERT 走回写把生成值作为字面量扇出，UPDATE/DELETE/MERGE 显式报错）、`DB_NAME/DB_ID/SERVERPROPERTY`、`CHECKSUM/BINARY_CHECKSUM`、`HASHBYTES`（MD5/SHA1/SHA2_256） |
| 表值函数 | `FROM STRING_SPLIT(s, sep[, 1]) AS t`、`FROM GENERATE_SERIES(a, b[, step]) AS t`、`FROM OPENJSON(json) AS t`（默认 key/value/type 形状；`WITH` 子句报错） |
| 批/变量 | `DECLARE @x [类型] [= 初值]`、`SET @x = 表达式`、`SELECT @a = e1, @b = e2 [FROM …]`（取扫描末行，空扫描保持原值）、`IF … ELSE`、`BEGIN…END` 嵌套块、`WHILE` + `BREAK`/`CONTINUE`、`@@ROWCOUNT`/`@@ERROR`/`@@VERSION`、`PRINT 表达式`（CLI 打印消息）；变量是**逐连接会话状态**（GO 结束批次即清空），替换经 `value_literal` 渲染，写语句只以字面量形式进入日志/复制 |
| 错误处理 | `BEGIN TRY … END TRY BEGIN CATCH … END CATCH`（捕获后批继续；CATCH 内 `ERROR_MESSAGE()`/`ERROR_NUMBER()` 读被捕获错误，CATCH 外为 NULL）；`THROW [code, 'msg', state]`（CATCH 内裸 `THROW` 重抛原错误）、`RAISERROR('msg', sev, state)`；CATCH 内的错误继续上抛，TRY 内的 BREAK/CONTINUE 穿透到外层 WHILE |
| 递归 CTE | `WITH [RECURSIVE] c(n) AS (锚点 UNION [ALL] 递归臂)`：半朴素迭代（每轮只见上一轮行，SQL Server 工作表语义）；`UNION` 按编码字节去重可收敛循环图、`UNION ALL` 循环在 100 轮/10 万行预算处响亮报错；T-SQL 无关键字拼写（自引用 UNION 体即递归）同样识别 |
| APPLY | `CROSS APPLY 表函数 AS t` / `OUTER APPLY …`（STRING_SPLIT/GENERATE_SERIES/OPENJSON）：**逐左行求值**的相关化表函数——实参引用左表列，OUTER 空行集保留左行（右列读 NULL）；支持别名列改名与连续 APPLY；APPLY 子查询仍显式报错（相关子查询边界） |
| PIVOT/UNPIVOT | `FROM src PIVOT (SUM(x) FOR col IN (v1 [AS c1], v2)) AS p`：隐式分组（除透视列与聚合实参列外的全部字段），单聚合（SUM/AVG/MIN/MAX/COUNT），空单元格为 NULL，IN 子查询与 `DEFAULT ON NULL` 报错；`FROM src UNPIVOT (val FOR col IN (a [AS α], b)) AS u`：每行×每列出一行，NULL 单元格剔除（`INCLUDE|EXCLUDE NULLS` 报错） |
| 标识与随机 | `SCOPE_IDENTITY()`/`@@IDENTITY`（逐连接会话状态：连接内最后一条 INSERT 的自增 id,非 INSERT 语句不清除;服务端从引擎 `last_insert_id` 读取）;`RAND()`（[0,1) 浮点,xorshift64*）——**复制安全**:SELECT 读路径可用,INSERT 路径字面量化回写(同 NEWID),UPDATE/DELETE/MERGE 拒绝 |
| 会话身份 | `SUSER_SNAME()`/`ORIGINAL_LOGIN()`/`SYSTEM_USER`/`SESSION_USER`/`USER_NAME()`/`APP_NAME()`/`HOST_NAME()` —— 经连接身份上下文替换（服务端为登录用户，无上下文时报错） |
| 会话垫片 | `SET <已知选项> ON/OFF`、`USE <db>`、`PRINT <字面量>`、`GO` 批分隔（CLI/控制台多语句）—— 与 PRAGMA 同通道**接受并忽略** |
| 语义对齐 | `CONCAT` 把 NULL 当空串（`\|\|` 仍 NULL 传染）、`LEN` 不计尾随空格（`LENGTH` 计）、`TRIM('ab' FROM x)` 字符集裁剪、`ROUND(x, -n)` 负位数（十位/百位） |
| 复制确定性 | 写语句中的 `GETDATE()` 族在写入节点折叠成时间戳字面量（与 `DEFAULT NOW()` 同红线）；`DEFAULT NEWID()` 按行定值回写 |
| 其他 | `CAST(... AS BIT)`、`INFORMATION_SCHEMA.*`/`sqlite_master`（大小写不敏感）、`UPDATE ... FROM`、`DELETE ... USING`、`MERGE`（受限）、`JSON_ARRAY_CONTAINS`；`BIT`/`DATETIME2`/`NVARCHAR(MAX)` 等类型名按声明类型接受（值模型不变） |

**显式报错**（不静默、不降级）：

| 类别 | 语法 |
|---|---|
| SELECT | `INTO`、`FOR XML/JSON`、`OPTION (...)`、`FOR SYSTEM_TIME`、`TOP ... PERCENT` |
| CTE/连接 | `WITH RECURSIVE` 已支持（见上表）；递归体必须是 `锚点 UNION [ALL] 递归臂`（其他集合形状报错） |
| 运算符/变量 | `@@ROWCOUNT`/`@@VERSION` 之外的 `@@` 系统变量、`RAND()`（无写路径回写通道，拒绝） |
| FROM/联接 | `WITH (NOLOCK)` 等表提示、旧式 `t (NOLOCK)`、`APPLY` 子查询形式、未知表函数、`TABLESAMPLE`(`PIVOT`/`UNPIVOT` 已支持,见上表) |
| DML | `OUTPUT INSERTED/DELETED...`（用 `RETURNING`）、`MERGE ... OUTPUT`、`MERGE WHEN MATCHED THEN DELETE`、`UPDATE/DELETE ... ORDER BY/LIMIT`、`DELETE t FROM ...` 多表删除（用 `DELETE ... USING`） |
| DDL | `#temp`/`##temp` 临时表、`IDENTITY(1,1)`（用 `AUTOINCREMENT`）、`ROWGUIDCOL`、`CLUSTERED`/`NONCLUSTERED INDEX`、`INCLUDE`、`WHERE` 过滤索引、`USING` 索引类型、索引存储选项 |
| 目录 | `sys.*`/`sysobjects`（报错并指向 `information_schema`/`sqlite_master`）、`OBJECT_ID` |
| 语句/过程 | `EXEC`、`WAITFOR`、`RETURN`、`GOTO`、`DENY`、`CREATE PROCEDURE/TRIGGER/SCHEMA/SEQUENCE`（存储过程/游标层整体是架构边界；`DECLARE`/`SET @x`/`IF`/`WHILE`/`BEGIN…END`/`TRY…CATCH`/`THROW`/`RAISERROR` 批控制流已支持，见上表） |
| 函数 | `SCOPE_IDENTITY`、`HASHBYTES` 的 `SHA2_512`/`MD2`、`TIMEFROMPARTS`（无 TIME 类型）；会话身份函数无连接上下文时报错（有上下文时替换，见上表） |
| 会话垫片边界 | 未知 `SET` 选项、`GO <n>` 重复次数、非字面量 `PRINT`（PRAGMA 值语法装不下表达式） |

`WINDOW` 子句、`QUALIFY`、递归 CTE、`CROSS APPLY` 等结构性缺口见[不支持的语法](#不支持的语法)。

## 不支持的语法

以下语法在解析/执行层**显式报错**（不静默吞掉、不降级近似）：

| 类别 | 语法 |
|---|---|
| 窗口 | `WINDOW` 命名子句、`QUALIFY`、自定义窗口帧（`ROWS/RANGE/GROUPS BETWEEN …`） |
| 查询结构 | `SELECT INTO`、`SELECT AS VALUE/STRUCT`、`SELECT * EXCLUDE/EXCEPT/REPLACE/RENAME`、`ORDER BY COLLATE`、`SELECT t.*` |
| 连接 | `NATURAL JOIN`、`LATERAL` 派生表、表函数/`UNNEST`、`TABLESAMPLE`、`PIVOT`、表时态 `AS OF` |
| 锁/伪指令 | `FOR UPDATE`/`FOR SHARE`、`FOR XML`/`FOR JSON`、`SETTINGS`、`FORMAT`、pipe 操作符 |
| 子查询/CTE | 相关子查询（`APPLY` 子查询形式同此边界） |
| 分组 | `WITH ROLLUP`/`WITH TOTALS` 等 GROUP BY 修饰符（请写 `GROUP BY ROLLUP(...)`/`CUBE(...)`）、嵌套/重复分组集合、`CUBE` 超 12 元素、不配合 GROUP BY 或聚合的 `HAVING` |
| 事务/冲突 | `ON CONFLICT DO UPDATE`、`ON DUPLICATE KEY UPDATE`、`DEFAULT VALUES`、无匹配唯一约束的 `ON CONFLICT` 目标 |
| DDL | 复合 `PRIMARY KEY`/`UNIQUE` 表约束、表达式/部分/JSON 路径索引、`CREATE TRIGGER`、`CREATE MATERIALIZED VIEW`、`ALTER TABLE` 的改约束/改类型、CREATE TABLE 存储/布局子句（`INHERITS`/`WITHOUT ROWID`/`LOCATION`/`STORED AS`/`CLUSTERED BY` 等）与约束装饰（`DEFERRABLE`/`INITIALLY DEFERRED`/`NOT ENFORCED`/`NULLS NOT DISTINCT`/`MATCH FULL/PARTIAL`） |
| T-SQL 专有 | `OUTPUT`（用 `RETURNING`）、`@` 变量/参数、表提示 `WITH (...)`、旧式 `(NOLOCK)`、`#`/`##` 临时表、`IDENTITY(1,1)`、`ROWGUIDCOL`、`CLUSTERED`/`NONCLUSTERED`、索引 `INCLUDE`/`WHERE`/`USING`/存储选项、`sys.*`/`sysobjects`、`CROSS/OUTER APPLY`、`UPDATE/DELETE ... ORDER BY/LIMIT`、`DELETE t FROM ...`、`TOP ... PERCENT`、`RAND()` |
| 分页 | `FETCH ... PERCENT` |
| 外键 | `ON DELETE`/`ON UPDATE` 动作 |

`PRAGMA` 例外：有意接受并忽略，不产生任何效果。

## 另请参阅

- [功能清单](features.md)——按模块的完整能力列表
- [已知边界与定位](limitations.md)——架构边界与路线图
- [安全指南](security.md)——鉴权、用户与角色部署
- [运维手册](operations.md)——备份、恢复、集群运维
- [驱动与协议](drivers.md)——ADO.NET / EF Core / CLI / REST
