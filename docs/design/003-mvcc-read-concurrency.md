# 设计说明:MVCC / 读写并发(分阶段路线)

状态:**设计定稿;阶段 A 已全部落地**(pager 读取去借用化 + SELECT 执行链
`&self` 化 + `ServerState.db: RwLock` 锁分级,读-读并发);**阶段 B 已全部
落地**——pager 层:全方法 `&self` 化、WAL checkpoint epoch、
`begin_snapshot`/`read_page_as_of`/`end_snapshot`(快速路径 = 页 LSN ≤ 快照
直接用当前版本,慢速路径 = 一次 WAL 扫描物化全部页的 as-of 镜像并缓存;软截
断给活跃快照让位,硬阈值流控优先、截断后旧快照响亮报 `SnapshotTooOld`)。
引擎/服务器层:`Database::read_view()` 产出无锁 `ReadView`(catalog Arc 克隆
+ pager Arc + 独立语句超时 + 快照令牌,Drop 注销快照),SELECT 执行链整体
迁移到 `ReadCx`(`PageReader` 双模读页),server 读级只持锁微秒级构建视图、
语句全程锁外执行——**读不阻塞写**。实现与 §3 兼容矩阵一致:写单元融合的
bookkeeping 在同一 commit LSN 原子可见(提交路径在 WAL 锁内完成读面应用);
复制 apply/restore 恒走写路径;快照读受语句超时约束(视图自带 deadline)。
本文是权威设计;任何并发/可见性相关改动先对照本文的兼容矩阵。

## 0. 现状(精确事实)

- `ServerState.db: Mutex<Database>`:**一切语句(读或写)都抢这一把锁**——
  读-读串行、读-写串行、写-写串行;
- `write_order: tokio::sync::Mutex`:写语句与复制 apply 的跨语句顺序保证
  (对等节点按本节点执行序应用);
- 引擎单写者:同一时刻至多一个打开的事务(`tx_owner`),其他连接的写
  排队(`BEGIN_QUEUE_WAIT` 30s),语句级冲突由 WAL 单 fsync 原子性兜底;
- 事务回滚 = 全表内存快照(`tx_snapshot`,BEGIN 时 `snapshot_all`);
- 因此现状语义已经是**可重复读**:事务打开后,其他连接的写被互斥挡住,
  本事务内重复读必然一致;缺的只是**并发**。

## 1. 目标语义(分两档)

| 阶段 | 语义 | 收益 |
|---|---|---|
| A. 读-读并发(RwLock 分离) | 多个只读语句在引擎上并发执行;写仍互斥、且与读互斥 | 多客户端只读负载(控制台+报表+驱动连接池)线性扩展读吞吐 |
| B. 快照读(页多版本) | 读拿历史版本快照,**读不阻塞写、写不阻塞读** | 单写者前提下的真 MVCC 读;长查询不再卡写路径 |

明确不做:多写者并发(单写者是有意设计——对等集群把写扩展放在节点维度,
见 README 架构说明;多写者带来的冲突解决与隔离复杂度不匹配本项目定位)。

## 2. 阶段 A:读-读并发(RwLock 分离)

### 2.1 锁结构

```text
ServerState.db: Mutex<Database>   →   RwLock<Database>
写路径(is_write/BEGIN/用户管理/备份恢复/复制 apply) → write()
只读路径(SELECT 探测/log 视图/meta/状态统计)      → read()
```

`RwLock<T>: Sync` 仅要求 `T: Send`——`StmtDeadline` 的 `Cell` 是 Send,
现有结构即可迁移,**无需先做引擎大改**(Cell 的内部可变性在读锁保护下
单写者语义不变;若未来读锁内并发 tick,再升 AtomicU64,见 §2.4)。

### 2.2 读方法 &self 化(阶段 A 的主要工程)

现状读路径经 `Database::execute(&mut self)`,&mut 的实际来源:

| 来源 | 处理 |
|---|---|
| `pager.read_page(&mut)`(buffer pool LRU 更新) | pool 访问改为内部锁或 &self + pool 计数原子化;WAL 只追加天然支持并发读 |
| `heap.scan/page_docs/doc_at(&self, pager: &mut)` | 随 pager &self 化自然变 &self |
| `exec_select` 链(CTE 写入、ORDER BY 临时结构) | CTE 已是语句级字段(&mut 的真实需求)——读路径把语句级可变状态收进 `RefCell`/局部参数 |
| `autoinc_cache` / catalog 缓存 | 只在写路径触碰;读路径不涉及 |

做法是**编译器驱动**:新增 `execute_read(&self, sql)`,仅接受分类为只读的
语句(写语句显式报错),逐个修复 &mut 借用点直至 SELECT 执行链在 &self 上
成立。预计触及 exec 链 10-15 个方法签名。

### 2.3 服务器侧分级

- `execute_sql_inner` 已有 `p.is_write` 分类:写 → `db.write()`;
  只读 → `db.read()`;
- `tx_owner`/`BEGIN_QUEUE`:BEGIN(写意图)仍拿 write 锁 + 排队;
  事务内的语句(无论读写)持有 tx_owner → 统一 write 锁(事务内的"读"
  必须看到本事务未提交写,不能进读锁);
- 复制 apply(REQ_SQL_SEQ/catch-up/drain)、restore 重放:恒 write 锁
  (与现状一致);
- 备份 dump:持 `lock_engine_for_write`(现状)→ write 锁下 dump,
  语义不变。

### 2.4 死锁与公平性红线

- 读锁内禁止再取 write_order;write 锁内禁止再取读锁(同语句内只需要
  一把);
- `StmtDeadline` 若升 Atomic,读锁内多线程并发 tick 是**预期行为**
  (计数器只需要近似采样);
- RwLock 写饥饿:采用 `parking_lot` 式公平锁或 tokio RwLock(写优先)
  ——落点在阶段 A 实现时选定,测试含"持续读流量下写延迟有界"用例。

### 2.5 测试计划(阶段 A)

- 并发读:多线程同时 SELECT,总耗时 < 串行版(证明真并行);
- 写饥饿有界:后台持续点查,INSERT 的 P99 延迟有上限;
- 语义回归:全部 477+ 现有测试(单写者/复制/事务/超时)零变化通过;
- 集群:multinode 套件(复制 apply 恒走写锁,收敛不受影响)。

## 3. 阶段 B:快照读(WAL 页版本管理)

阶段 A 之后"读-写互斥"仍是 RwLock 语义。阶段 B 让**只读语句拿历史快照**:

### 3.1 版本来源:WAL 即版本链

WAL 已经是"页 id → 按序页镜像"的日志(每个写单元一次 append,LSN 单调)。
快照读的定义:

```text
读事务拿 snap_lsn = 当前 WAL commit LSN。
读到的页版本 = snap_lsn 之前最后一次该页的镜像。
未提交写不在 WAL commit 记录中 → 天然不可见(写提交后才可见 = 读已提交)。
```

- 需要 WAL 记录携带**提交边界**(现有 `wal.commit(tx.id)` 已有事务分组,
  补一个 commit LSN 查询 `last_visible_lsn()`);
- 页查找:buffer pool(当前版本)→ 若页的最后修改 LSN ≤ snap_lsn 直接用;
  否则回放 WAL 到 snap_lsn 找该页镜像(或维护 `page → [lsn]` 倒排,
  内存开销 O(热页));

### 3.2 引擎接口

```rust
pub struct Snapshot { lsn: u64 }          // 读事务令牌
Database::begin_snapshot(&self) -> Snapshot
Database::execute_snapshot(&self, snap: &Snapshot, sql) -> …
```

- SELECT 的页读全部走 `read_page_as_of(snap.lsn, page)`;
- 与 `StmtDeadline`/复合索引探测/溢出链读取完全正交
  (溢出链页的 as-of 读取沿用同一 `read_page_as_of`);
- 快照读**不允许**触碰:catalog 保存、autoinc 缓存、journal 追加、
  `tx_pending`——分类与 §2.2 相同。

### 3.3 与单写者事务/write_unit/复制的兼容矩阵

| 机制 | 兼容策略 |
|---|---|
| 单写者事务(tx_owner) | 不变:写事务互斥依旧;快照读根本不进写路径 |
| write_unit 融合 | 写单元的 WAL append 照旧;快照读按 commit LSN 划界,**融合单元的 bookkeeping(data+position)在同一 LSN 可见**,不出现半可见 |
| 复制 apply(SEQ/catch-up/drain) | apply 恒走写路径;快照读看见的是"已提交"版本——与对端按 origin 序应用的一致性模型不冲突 |
| 恢复重放(restore) | 重放持写路径;重放期间的快照读看到旧快照(可接受:恢复本身是管理员操作) |
| `StmtDeadline` | 快照读同样受语句超时约束(在扫描循环内,现有机制照用) |
| ROLLBACK 快照(`tx_snapshot`) | 不变:写事务回滚机制与快照读正交 |

### 3.4 阶段 B 测试计划

- 读写并发:长查询执行期间并发 INSERT 全部即时提交完成(写不被读阻塞);
- 快照一致性:读事务两次读同一范围,期间并发写提交,两次结果一致且
  不含未提交数据;
- WAL 边界:融合写单元(data+journal+position)对快照读原子可见;
- 崩溃恢复后 as-of 读取正确(重放 LSN 对齐);
- 全量回归:cargo + dotnet + deploy(三个层级)。

## 7. 风险与回退

- 阶段 A 纯锁结构替换,回退 = 换回 Mutex(单点改动);
- 阶段 B 的 WAL 倒排索引内存开销可用"热页上限 + 超出回放"兜底;
- 任何阶段失败不影响单写者语义的正确性(写路径从未放松互斥)。
