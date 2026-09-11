# 设计说明:多列(复合)索引

状态:已实施(与本文件同批落地)。本文是实现的权威依据;改复合索引相关代码前先读。

## 0. 背景与约束

v1 索引只有单列(`CREATE INDEX … ON t (col)`,多余列显式报错)。单列假设深度耦合在四处:
`index_roots` 键=列名、`reindex_*` 写维护按列取键、`probe_plan`/`index_probe` 按列探测、
`CREATE UNIQUE INDEX` 把列塞进 `meta.unique`(列级约束容器)。

硬约束(AGENTS 红线 #2):**编码只保证往返一致,排序统一走 `Value::cmp_values`**。
B+ 树键本来就是逻辑 `Value`——节点内比较全部走 `cmp_values`(btree.rs 模块注释),
encode 只是持久化载体(BSON 式,非序编码,也不需要是)。

## 1. 复合键 = `Value::Array`(不发明新字节序方案)

**结论:复合键直接取 `Value::Array([v1, …, vn])`,零新增编码。**

依据:
- `cmp_values` 对 Array 已是**逐元素字典序 + 长度前缀**(rank 5 保证 Array 互相比、不与标量混比;
  测试 `[1,2] > [1]` 钉住前缀序)——这正是复合键的全序;
- Array 的 encode/decode 往返一致,B+ 树持久化零改动;
- 半页 `KeyTooLarge` 上限自然沿用(数组编码即键长);
- 前缀序还免费给出**前缀范围界**:`Array([v]) ≤ k < Array([v2])` 对复合键成立(§4)。

被否决的方案:长度前缀拼接 / 0x00 转义续接(两者都要发明一套 order-preserving
per-type 编码,与"排序统一走 cmp_values"的既有架构重复且必然引入序 bug 面)。

**NULL 语义**:单列索引跳过 NULL 键。复合键规则统一为**任一列 NULL → 整键跳过**
(不进树、不参与唯一判重)。与 SQLite 的部分 NULL 组合语义不同,文档明示。

## 2. `index_roots` 键的泛化:root_key → 列集

- `index_roots: BTreeMap<String, u32>` 键从"列名"泛化为"**root_key**":
  - 单列索引与 PK/UNIQUE 约束树:沿用**列名**(旧卷零迁移,字节级兼容);
  - 复合索引:使用**索引名**。
- 新 helper `TableMeta::index_columns_of(root_key) -> Vec<String>`:
  先查 `index_defs`(name == root_key)取 `columns`;否则视为旧式列名 → `vec![root_key]`。
  写维护、判重、探测全部经此解析,**单列路径行为逐字节不变**。
- 新 helper `TableMeta::root_key_unique(root_key) -> bool`:替代 `is_constraint_col`——
  root_key 是 PK/UNIQUE 约束列(单列)→ true;`index_defs` 中同名且 unique → true。

### 唯一判重

- **单列 `CREATE UNIQUE INDEX` 保持现状**:列加入 `meta.unique`(列级约束容器),
  现有 drop-lift 语义与测试不变;
- **复合 `CREATE UNIQUE INDEX`**:树以 unique 模式建立并在维护时强制
  (`reindex_insert` 对 `root_key_unique` 的树传 unique 标志,重复即报错);
  不触碰 `meta.unique`(它没有"列组合"容器)。副作用:`OR REPLACE`/`OR IGNORE`
  的位移机制基于 `indexed` 列清单(约束列),对复合唯一冲突不做位移——
  INSERT 直接报唯一冲突错误,文档明示。

## 3. `IndexDef` 与 catalog 版本策略

- `TableMeta.index_defs` 从 `Vec<(name, column, unique)>` 元组升级为
  `Vec<IndexDef>`,`IndexDef { name, columns: Vec<String>, unique }`
  (`columns[0]` 即旧 `column`)。`IndexInfo`(展示结构)同样加 `columns`,
  `column` 字段保留 = 首列(控制台兼容)。
- **版本策略:不升 `CATALOG_MAGIC`。** catalog 内容是自描述 JSON-Value 对象
  (encode 序列化),键集开放;MAGIC 只表达"链式页布局"这一物理格式。
  兼容做法:
  - 写:每个 index_def 序列化为四元 `[name, col0, unique, [c1, c2, …]]`;
  - 读:长度 ≥4 取 `columns` 数组;否则(旧卷三元)→ `columns = [col0]`;
  - 升级路径:旧库打开即单列语义,首个复合索引落库后新格式生效;降级本就不支持
    (项目一贯策略)。相比 MAGIC 升版,这避免了两套读分支的长期维护。
- `schema_hash` 已整体纳入 `index_defs`(IndexDef 派生 Hash,columns 全集进 hash)
  ——**含复合列的摘要跨节点一致**:同 schema 的两库 hash 相等,列集不同 hash 必不同。

## 4. 探测:`probe_plan` 前导列匹配

`index_probe` 仅在单表无 JOIN、无 CTE 时启用(现状不变)。复合索引的第一版探测策略:

- 收集 WHERE 合取项中的**全部等值对**(col → literal;现状只留一个);
- 对每个 root_key 取列集 `cols`:
  - **全列等值**(每个 ci 都有等值字面量)→ 精确键
    (`cols.len()==1` → 标量值,现状;否则 `Array([v1..vn])`)→ `ProbePlan::Eq`;
  - **前导 k 列等值(k < n)** → `ProbePlan::Prefix(Array([v1..vk]))`:
    `range_bounded(lo=prefix, hi=None)` + 保留前缀匹配的键
    (`cmp_values` 前缀判断;前缀序保证连续区段);
  - 首列无等值 → 该 root_key 不可探测(范围条件留在 residual WHERE,正确性无损);
- 单列索引行为逐字节不变(`cols.len()==1` 时 Eq/Range 两分支与现状完全一致,
  复合索引不参与 Range 探测)。

## 5. 联动面清单

| 联动面 | 处理 |
|---|---|
| `exec_create_index` | 接受多列(Identifier;表达式列仍拒绝);列必须存在;重复列报错;重名/IF NOT EXISTS 语义不变;`sqlite_autoindex_` 前缀保留字不变 |
| 写维护 `reindex_remove/insert/repoint` | 签名改收 `&TableMeta` + root_keys;经 `index_columns_of` + `index_key` 构造键;唯一树按 `root_key_unique` |
| `DROP INDEX` | 单列:现有 drop-lift `meta.unique` 逻辑不变;复合:删 root + index_defs 条目即可(无列级约束牵连) |
| `rewrite_table`(表重建) | 重建按 root_key→列集(helper 统一后自动正确) |
| `dump_script` | `CREATE INDEX name ON t (c1, c2, …)` 导出全列 |
| `sqlite_master` | 索引行 SQL 文本带全列 |
| `schema_hash`/摘要 | IndexDef 派生 Hash,columns 全集进 hash → 跨节点收敛一致 |
| `/api/meta` | `index_defs` 每项加 `columns` 数组;`column` 字段保留为首列(控制台兼容) |
| 自动索引(`sqlite_autoindex_*`) | PK/UNIQUE 约束仍是单列 → 自动索引面零变化 |
| `OR REPLACE`/`OR IGNORE` | 复合唯一冲突不做位移,INSERT 报唯一冲突(明示) |

## 6. 测试计划(全部落地)

单元(engine):建复合索引/多列导出、重复列与列不存在报错、复合 UNIQUE 判重
((1,'a') 与 (1,'b') 共存、(1,'a') 重复拒绝)、NULL 整键跳过、UPDATE/DELETE 索引维护、
全列等值点查、前缀等值(ProbePlan::Prefix)、非前导列条件不走探测(行为等价)、
dump round-trip、catalog 重启往返、旧三元 catalog 兼容读取、schema_hash 联动、
`OR REPLACE` 复合冲突报错。

兼容回归:cargo 全量、dotnet 75+22、部署测试(single+multinode)。
