# 设计说明:JSON 路径索引(JSON_EXTRACT 走索引)

状态:**设计定稿;已实现**。本文是权威设计;任何索引/探测相关改动先对照
本文的不变量清单(§5)。对照:`docs/limitations.md`「单列之外的索引能力」
边界条目(JSON 点读非前导列条件走全表扫描)由此关闭。

## 0. 现状(精确事实)

- 索引键 = 顶层列值:`index_key_of(doc, cols)` 直取 `doc.get(col)`;
  复合键 = `Value::Array` 按列序(001 号设计);
- `JSON_EXTRACT(col, '$.a.b[0]')` 是行内标量函数(`json_path_lookup`,
  支持点段与数组下标;坏文本/缺路径返回 NULL,不报错)——`WHERE
  JSON_EXTRACT(doc,'$.name') = 'x'` 无法被 `probe_plan` 消费(Function
  不是 Identifier),全表扫描逐行求值;
- catalog 里 `index_defs` 持久化为 `[name, first_col, unique, [cols]]`
  (`TableMeta.index_defs`),`index_roots` 键 = root_key(单列=列名,
  复合=索引名,旧卷零迁移);重命名/重写/dump 全部经 `IndexDef` 与
  `idx_specs`(`root_key, cols, unique`)流转。

## 1. 目标语义

```sql
CREATE INDEX ix_doc_name ON docs (JSON_EXTRACT(doc, '$.name'));
SELECT * FROM docs WHERE JSON_EXTRACT(doc, '$.name') = 'svc-7';
--   → 索引探测(EXPLAIN: INDEX PROBE ON docs USING ix_doc_name),非全表扫描
SELECT * FROM docs WHERE JSON_EXTRACT(doc, '$.ts') > '2026-01-01';
--   → 范围探测(同一棵树,Range 边界;字符串↔Timestamp 混合带仍走
--     promote_plan_bounds 的采样守卫,不可安全探测即整体回退)
```

- 键提取:`doc` 列的**文本值**按 JSON 解析后走 `json_path_lookup`;列缺失、
  值非文本、文本非法、路径缺失 → 键为 NULL → 不进树(与标量列 NULL 键
  同语义;而 `NULL = 'x'` 永假,探测结果的语义恰好完备——探测是 exact);
- root_key = **索引名**(与复合索引同规;绝不复用列名树——路径树的键
  形状与列值树完全不同);
- 树内序 = `Value::cmp_values`(既有红线:编码 ≠ 排序,这里复用标量序)。

## 2. 语法与 catalog

- 语法面:`CREATE INDEX <name> ON <table> (JSON_EXTRACT(<col>, '$.path'))`
  (JSON_TYPE/多参/表达式实参一律显式报错);`<col>` 必须是表列;
- 路径校验(建索引时,响亮拒绝):必须 `$` 开头,仅 `.key` / `[n]` 段,
  key 非空且不含 `.` `[` `'` `\`(后两个是 DDL 文本安全);
- catalog:`index_defs` 条目扩为 `[name, col, unique, [cols], path?]`——
  第 5 元素仅在路径索引时写入;旧节点读新目录忽略多余元素(数组按位
  解析),新节点读旧目录 get(4) 缺失 → `None`,**双向零迁移**;
- `IndexDef` 增 `path: Option<String>`;ALTER RENAME 的 IndexDef 重建
  透传该字段(列名改的是 `columns`,路径不变)。

## 3. 执行路径

| 路径 | 处理 |
|---|---|
| CREATE INDEX | 识别 Function 形列;UNIQUE + 路径 → 显式报错(唯一性执行机制是列级的:`meta.unique`/`constraint_unique` 不认路径;树级唯一在 UPDATE 两阶段维护与 OR REPLACE 位移里也不完整——不放行);root 冲突 → 一律报错(不走标量同列复用——键形状不同) |
| DML 维护 | `IdxSpec` 增 `path`;`index_key_of_spec` 分支:Some(path) → `json_path_key`(JSON_EXTRACT 同一 lookup 函数,**提取与查询同源**,不会分叉);build_trees 增 `paths_per_key` 平行参数(三个调用点:表重写/建索引回填/表创建) |
| probe_plan | 识别 `JSON_EXTRACT(col,'$.p') op 常量`(含镜像):等值收集为 (col,path)→值(同键不同值 → 拒绝计划,同 `eq_conflict`),范围收集为边界;命中 `index_defs` 中 `columns==[col] && path==p` 且树存在 → `ProbePlan::Eq/Range`;**exact 仅当该条件是唯一合取元**(多条件/其余合取 → 残余过滤兜底,exact=false——保守正确,后续可收紧) |
| 有序窗口 | `order_walk_index` 与 `resolve_indexed_col` **排除路径索引**(`index_defs` 中该 root 带 path):`ORDER BY doc`/`WHERE doc = …` 若匹配到路径树会用提取值当列值排序/探测——静默错误结果;排除后回退通用路径 |
| dump/恢复 | DDL 生成:路径索引 emit `CREATE INDEX name ON t (JSON_EXTRACT(col, '$.p'))`;恢复重放经同一 CREATE 解析,catalog 往返自洽 |
| EXPLAIN | 经 probe_plan 同路,自动报告 `INDEX PROBE ON t USING <index_name>` |
| 集群复制 | 索引是 catalog/DDL 的一部分,随 DDL 文本复制;节点各自按本地数据建树,无新协议面 |

## 4. 明确不做

- 表达式索引一般化(任意标量函数)——仅 JSON_EXTRACT 一族,表达式索引
  的谓词代换/投影匹配是另一个量级;
- 复合路径索引(`JSON_EXTRACT(..) , JSON_EXTRACT(..)`)——前缀探测的
  (col,path) 键对泛化后置;
- 路径索引参与 ORDER BY 窗口/标量列探测(§3 的排除是正确性红线,不是
  能力缺口);
- `JSON_TYPE` / 数组整体 / 多值路径(`$[*]`)索引。

## 5. 不变量(改索引代码先对照)

1. **提取与查询同源**:树键提取与 `JSON_EXTRACT` 行内求值必须走同一
   `json_path_lookup`(改路径语法两处同步),否则探测静默漏行;
2. **路径树永不充当列树**:`resolve_indexed_col`/`order_walk_index`/
   标量同列复用(reuse_root)三处对 path 索引的排除是正确性要求;
3. 旧目录字节兼容:第 5 元素可选、忽略未知;旧卷打开零迁移;
4. UNIQUE + 路径显式报错,绝不静默降级为非唯一;
5. exact=false 时的残余过滤必须真的执行(`matches_in` 在非窗口路径恒跑)。
