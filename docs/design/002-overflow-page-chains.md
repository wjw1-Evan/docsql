# 设计说明:文档 >4KB 溢出页链

状态:已实施(与本文件同批落地)。权威依据;改 heap 相关代码前先读。

## 0. 背景

v1 文档必须装进单个 4KB 页(上限 ~4090 字节,`heap.rs::DocTooLarge`)。文档库的核心
卖点要求大文档(BSON 式嵌套、Base64 载荷)。

## 1. 总原则:溢出是 heap 层的分帧,encode/btree 零改动

- encode 不动:B+ 树键仍是逻辑 `Value`,文档载荷的编码格式不变;
- btree 不动:locator(页<<16|槽)语义不变,索引条目指向主页槽;
- 摘要/复制不受影响:digest 与复制都工作在**文档内容**(Value/SQL 文本)层面,
  与物理布局无关;dump/快照输出逻辑文档,布局无关。

## 2. 溢出页链格式

每个 pager 页是通用字节容器,heap 用**首字节页类型标记**区分(encode 的 tag 只用
0-7,任何 ≥8 的首字节对 `decode_prefix` 都是 `UnknownTag` 错——这个被拒绝的字节
空间正好用作 heap 层包装标记,读取端零歧义):

```text
主页槽内容(溢出文档):
  0xFF                      溢出标记(encode 永不产生)
  u32 total_len             完整文档编码字节长度
  u32 chain_head            首个溢出页的页号
  inline_bytes              文档编码的前缀,塞满主页剩余空间(0..~4KB)

溢出链页:
  0xFE                      链页标记
  u32 next                  下一链页页号(0 = 链尾)
  u16 len                   本页载荷字节数
  payload                   文档编码的后续分片(≤ PAGE_SIZE-7)
```

- 主页槽的 u16 len 字段 = `5 + inline_len`(≤u16 ✓);
- 文档解码:拼 inline + 沿链 payload → `decode_prefix`(截断即报错,防静默丢尾);
- 链遍历防环/防截断:访问页数上限 = `total_len / 最小块 + 2`,超出即
  `HeapError::Page(.., "overflow chain corrupt")`。

## 3. locator 与写入路径

- **locator 不变**:`pack_loc(page, slot)` 语义照旧——索引、复制位点、`moved`
  报告全部无需感知;
- **insert**:编码 ≤ 单页可用 → 原路径;否则走溢出布局——先分配全部链页
  (pager 页号单调分配,先拿到全部 pid 才能写前向 next 指针),再选主页写内联段
  + 溢出头;
- **replace**:旧槽溢出或新文档溢出时,先回收旧链(见 §4),再按 insert 溢出
  路径落新文档(现有"不 fit 则 tombstone+append"的既有语义,链回收插在前面);
- **文档上限**:16 MiB(`MAX_DOC_SIZE`,对齐主流文档库的默认文档上限;
  u32 长度本身允许 4GiB,但 16MiB 挡住 hostile 巨文档的内存/链长爆炸)。

## 4. 链页回收:表内 free 清单

pager 没有空闲页机制(allocate 只增;孤儿页是既有现状——rewrite_table 的旧页
同样孤儿化,VACUUM 是独立里程碑)。溢出链回收**不触 pager**:

- `TableMeta` 加 `overflow_free: Vec<u32>`(回收的链页页号),随 catalog 持久化
  (自描述对象加键,旧卷/旧代码忽略未知键,向后兼容读);
- 删除/替换溢出文档:沿链收集页号 → 清零写回 → 加入 `overflow_free`;
- 新建溢出链:优先从 `overflow_free` 取页(数量不足的差额新建;富余留在清单);
- 表删除随表消失;文件收缩留给 VACUUM。

## 5. 读取路径

`scan` / `page_docs` / `doc_at` 的读槽逻辑统一改走 `read_slot_bytes`:
- 槽首字节 < 8 → 正常 `decode_prefix`(旧文档路径,零开销);
- `0xFF` → 解析溢出头 + 沿链拼接 → `decode_prefix`;
- 事务可见性:链页读取与主页同样优先 tx staged 页(`staged_or_file_page` 语义),
  保证同一事务内"写链 → 读回"可见。

## 6. 跨节点复制与旧卷兼容

| 面 | 兼容性 |
|---|---|
| 复制/同步 | 复制的是 SQL 文本与逻辑文档,布局无关;大文档的 resolved 语句照常扇出 |
| 摘要 digest | 基于文档 Value,与存储布局无关 |
| dump/快照 | 逻辑文档输出,布局无关 |
| 旧卷 → 新代码 | 旧槽首字节全是 encode tag(0-7) ≠ 0xFF,正常路径;catalog 无 `overflow_free` 键 → 缺省空 ✓ |
| 新卷 → 旧代码 | 0xFF 槽 `decode` 报 UnknownTag(降级不支持,项目一贯策略) |

## 7. 测试计划

heap 单元:>4KB 插入/scan/doc_at 读回、跨多链页(>8KB)、链页回收后复用
(断言不再分配新页)、大→小/小→大 replace、截断链/防环错误、旧文档路径不受影响;
engine:SQL 层大文档 INSERT/SELECT、UPDATE 大→小回收、索引维护(locator 语义不变)、
digest 含大文档、dump round-trip。
兼容回归:cargo 全量 + dotnet 75+22 + deploy/run-tests.sh。
