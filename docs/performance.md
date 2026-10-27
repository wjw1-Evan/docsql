# 性能基准与容量参考

本文是 DocSQL 单节点引擎的实测性能基线（2026-10-27，commit `737cab0`），包含测试环境、方法、原始结果与容量解读。所有数字都是 `docsql-core` 嵌入引擎的进程内测量：不含 TCP/JSON 线协议、鉴权与 Docker 卷的额外开销；生产容器环境请在相同宿主上复测，不要跨机器比较绝对值。

## 结论摘要

| 场景 | 实测（本机中位数） | 说明 |
|---|---|---|
| 单条 autocommit 写 | 0.7–1.0 ms/条，约 **1,000–1,400 行/秒** | 每语句一次 WAL fsync，延迟由磁盘支配 |
| 单事务批量写 | **47 µs/行，约 21,000 行/秒** | 整批一次 fsync，CPU 路径 |
| 异步组提交 | **45 µs/行，约 21,000 行/秒** | 比逐条 fsync 快约 **18×**；断电丢最后约 2 ms |
| 主键点查 | **约 11–13 µs/次，约 9 万次/秒** | 3 万行到 30 万行只增加约 11% |
| 索引序分页首屏 / keyset | **0.018 ms / 0.038 ms** | 50 万行；深 OFFSET 与偏移量线性 |
| 无索引 `ORDER BY … LIMIT` | **42 ms**（首屏）、**87 ms**（OFFSET 半表） | top-K 窗口，50 万行 |
| `COUNT(*)` 50 万行 | **3.0 ms** | 免解码活槽计数 |
| 索引探针 `IN` / `BETWEEN` / `LIKE '前缀%'` | **1.5 / 4.2 / 0.13 ms** | 同条件全扫基线 178–306 ms |
| 等值 hash JOIN（9M 对） | **5.5 ms** | 约 16 亿对/秒 |
| 逐行 DELETE（事务内） | **约 655 行/秒** | 页内幸存行索引重指开销；见下文专项 |
| 单语句多行 DELETE | **8,200 行/秒**（122 µs/行） | 同一张表，比逐行删快约 12× |

## 测试环境与方法

| 项 | 值 |
|---|---|
| 日期 / 版本 | 2026-10-27 / commit `737cab0` |
| 宿主 | macOS（Darwin 27.2.0），Apple silicon arm64 |
| CPU / 内存 | 8 逻辑核 / 8 GiB |
| 磁盘 | 内置 APFS（`/dev/disk3s1`），非 Docker 卷 |
| 工具链 | rustc 1.99.0，`--release`（opt-level 3） |
| 基线构建 | `cargo build --release -p docsql-core --examples` |

**采样方法**：轻量基准（`bench`、`bench6`、`bench_micro`、`bench_write`、`bench_join`）各跑 4 轮，取中位数；`bench_page`、`bench_probe` 每轮都要装载 50 万行，各跑 3 轮取中位数。首轮起点 1 分钟负载均值 59.8（宿主另有任务收尾），其余轮次 1 分钟负载均值 2.5–8.9；首轮不参与轻量基准的中位数，其数值仍用于观察上界。本次是同一二进制的重复采样，不是新旧 A/B 对比，数值用于建立单版本基线。

**噪声判读**（按仓库基准规范）：fsync 主导指标（autocommit 写、WAL 批次）宿主卷抖动可达 ±30%，本机实测波动甚至更大（逐条插入 14.7–21.3 s）；CPU 主导指标（点查、分页、聚合、JOIN）对噪声更敏感，±10% 内视为噪声。所有数字保留到毫秒/微秒只是为了对齐脚本输出，不代表该精度下的可复现性。

复现命令：

```bash
cargo build --release -p docsql-core --examples
./target/release/examples/bench          # 写入/点查/更新/删除/扫描
./target/release/examples/bench6         # 逐条 fsync vs 异步组提交
./target/release/examples/bench_micro    # 解析/点查成本拆解
./target/release/examples/bench_write    # 写入成本拆解（含集群 journal 融合）
./target/release/examples/bench_join     # hash join vs 嵌套循环
./target/release/examples/bench_page     # 50 万行分页（可传行数）
./target/release/examples/bench_probe    # 50 万行谓词探针（可传行数）
```

## 写入路径

`bench`（2 万行，主键 + 一个二级索引）：

| 操作 | 中位数 | 吞吐 |
|---|---:|---:|
| 逐条 INSERT（autocommit，每语句 fsync） | 20,113 ms | 994 行/秒 |
| 批量 INSERT（单事务） | 1,412 ms | 14,164 行/秒 |
| 批量 UPDATE（单事务，2 万行） | 2,606 ms | 7,674 行/秒 |
| 批量 DELETE（单事务，逐行语句） | 30,521 ms | 655 行/秒 |

`bench_write`（成本拆解）：

| 操作 | 中位数 | 吞吐 / 备注 |
|---|---:|---|
| autocommit INSERT 5,000 条 | 3,619 ms | 724 µs/条，约 1,382 条/秒 |
| 事务批量 INSERT 20,000 条 | 944 ms | 47.2 µs/条，约 21,189 条/秒 |
| autocommit UPDATE 5,000 条 | 3,747 ms | 749 µs/条，约 1,334 条/秒 |
| 异步组提交 INSERT 5,000 条 | 232 ms | 46.4 µs/条，约 21,552 条/秒 |
| 集群写记账（未融合，3,000 条） | 4,378 ms | 1,459 µs/条（用户写 + journal 各一次 fsync） |
| 集群写记账（融合写单元，3,000 条） | 2,573 ms | 858 µs/条，约 **1.7×** 提升 |

`bench6`（2 万条自动提交，交替跑两遍）：

| 模式 | 中位数 | 吞吐 |
|---|---:|---:|
| 逐条 fsync（第一遍 / 第二遍） | 17,591 / 16,827 ms | 1,137 / 1,189 行/秒 |
| 异步组提交（第一遍 / 第二遍） | 909 / 962 ms | 21,998 / 20,793 行/秒 |

**解读**：单条 autocommit 写的成本几乎全在 `fsync`（本机约 0.7–1.0 ms/条），解析与执行只占十几微秒；批量事务与异步组提交把 fsync 摊销后回到 CPU 路径，约 45–47 µs/行。集群节点上“用户写 + journal 记账”各有一次 fsync，融合写单元合并成一次后可省约 40% 的写延迟。`DOCSQL_ASYNC_COMMIT=1` 的代价是崩溃丢最后约 2 ms 的已确认写，PUBLISH 仍在推送前强制 fsync。

## 查询与扫描

| 操作 | 规模 | 中位数 | 单次成本 |
|---|---:|---:|---:|
| 纯解析 | 20,000 次 | 84.8 ms | 4.2 µs/次 |
| 主键点查（解析 + 执行） | 20,000 次 | 215 ms | 10.8 µs/次 |
| 同一条语句反复执行（排除 format） | 20,000 次 | 138 ms | 6.9 µs/次 |
| 二级索引等值查（每查询约 200 行） | 20,000 次 | 2,220 ms | 111 µs/次 |
| 全表聚合 `COUNT(*), MAX(v)` | 20,000 行 | 9.5 ms | 0.48 µs/行 |
| 全表过滤 | 300,000 行 | 142 ms | 0.47 µs/行 |
| 主键点查随规模 | 3 万 / 30 万行 | 240 / 270 ms | 12.0 / 13.5 µs/次 |
| `COUNT(*)` | 500,000 行 | 3.0 ms | 6 ns/行（免解码） |

**解读**：点查成本 ≈ 解析 4 µs + B+ 树探针与文档解码约 7 µs；表从 3 万行增到 30 万行，点查只增加约 11%，索引高度增长的影响很小。二级索引等值查的 111 µs 主要是返回约 200 行结果集的解码与物化，不是探针本身，吞吐随结果行数变化。`COUNT(*)` 的微基准会出现 0.15–1.8 ms 的波动：热路径本身约 0.15 ms，后台 checkpoint / 页缓存竞争时会上到 1–2 ms，这是 20k 行小表上的瞬态，不是渐进复杂度变化。

## 分页窗口（50 万行）

`bench_page`：表 `page_t(id PK, name, v)`，`v` 上有二级索引；装载 50 万行约 37 s（异步组提交，约 13,500 行/秒）。

| 查询 | 中位数 |
|---|---:|
| 1 主键序首屏 `ORDER BY id LIMIT 20` | 0.018 ms |
| 2 主键序 `LIMIT 20 OFFSET 250000` | 4.337 ms |
| 3 主键序尾页 `LIMIT 20 OFFSET 499980` | 9.278 ms |
| 4 keyset `id > 250000 ORDER BY id LIMIT 20` | 0.038 ms |
| 5 keyset 尾声 `id > 499900 …` | 0.039 ms |
| 6 主键序 DESC 首屏 | 0.022 ms |
| 7 主键序 DESC `OFFSET 250000` | 4.384 ms |
| 8 keyset DESC `id < 250000 …` | 0.036 ms |
| 9 二级索引序首屏 `ORDER BY v LIMIT 20` | 0.018 ms |
| 10 二级索引序 `OFFSET 250000` | 5.664 ms |
| 11 残余过滤 keyset（`id > 100 AND id % 7 = 0`） | 9.240 ms |
| 12 无索引排序 `ORDER BY name LIMIT 20` | 41.751 ms |
| 13 无索引排序 `OFFSET 250000` | 87.111 ms |
| 14 `COUNT(*)` | 3.012 ms |

**解读**：首屏走索引序窗口，只装载窗口内文档；深 OFFSET 仍要按序跳过并计数前 N 行，成本与偏移量近似线性（半表 4.3 ms、尾页 9.3 ms）。keyset 分页在任意深度都保持 0.04 ms，是深分页的推荐形态。无索引排序走 top-K 堆：首屏 42 ms、半表 87 ms，明显高于索引序，但仍远低于“全量物化 + 全量排序”。残余过滤会退化为边走边过滤，耗时 9.2 ms。

## 谓词探针（50 万行）

`bench_probe`：表 `probe_t(id PK, name, v, plain)`，`v` 与 `name` 有索引，`plain` 无索引。

| 查询 | 中位数 |
|---|---:|
| 1 稠密 IN（`v IN (17,233,571)`） | 1.473 ms |
| 2 稀疏 IN（主键 4 个散点） | 0.030 ms |
| 3 `IN (SELECT …)` 改写 | 11.642 ms |
| 4 `BETWEEN 200 AND 209` | 4.204 ms |
| 5 `LIKE 'name1234%'`（前缀） | 0.132 ms |
| 6 `LIKE 'name1%4%'`（中段 `%`，全扫对照） | 306.289 ms |
| 7 全扫基线（无索引列 `plain IN …`） | 184.546 ms |
| 8 全扫基线（无索引列 `plain BETWEEN …`） | 178.206 ms |
| 9 IN + `ORDER BY id LIMIT 20` | 1.572 ms |
| 10 BETWEEN + `ORDER BY id DESC LIMIT 20` | 4.139 ms |

**解读**：索引探针把 `IN`/`BETWEEN`/前缀 `LIKE` 从 180–306 ms 的全扫降到 0.03–4.2 ms；稠密 `IN` 与 `BETWEEN` 的成本主要来自命中行数（约 1.5–5 千行）的解码。`LIKE` 只接受“字面量前缀 + 单个尾部 `%`”，中段 `%` 仍回退全扫。`IN (SELECT …)` 的 11.6 ms 包含子查询预 pass 与结果改写，仍远低于全扫。

## JOIN

3,000 × 3,000 行（笛卡尔积 900 万对）：

| 查询 | 中位数 |
|---|---:|
| 等值 INNER JOIN（匹配 1,500 行） | 5.5 ms（约 16 亿对/秒） |
| 等值 LEFT JOIN（结果 3,000 行） | 281 ms |
| 复合等值 JOIN（两列） | 4.0 ms |
| 非等值 JOIN（`x.id < 200 AND y.id < 200`） | 3,414 ms |

**解读**：可提取等值条件的 ON 走 hash join，900 万对 5.5 ms；复合等值同样走 hash。非等值 ON 回退嵌套循环，脚本里的“200×200”实际仍要扫描 3,000×3,000 对（谓词在连接后才过滤），3.4 s 是这条路径的代表值。LEFT JOIN 的计划同样是 hash join（`EXPLAIN` 可见），但 281 ms 的成本来自未匹配行的 NULL 扩展：当前实现对每个未匹配左行遍历整个右侧行集合并入右列键（`null_extended_left`），另一半左行未匹配时就是约 1,500 × 3,000 × 列数 次插入。这是已知成本，优化方向是预先计算右侧键并集、每行只复制一次；在修复前，大量未匹配行的 LEFT JOIN 应考虑改用 `UNION ALL` / 先过滤右侧再连接。

## DELETE 专项

逐行 DELETE 是这份基线里最值得注意的形态（2 万行表，主键 + 二级索引）：

| 形态 | 成本 |
|---|---:|
| `bench` 事务内逐行 `DELETE ... WHERE id = n`（2 万行） | 30,521 ms，655 行/秒（约 1.53 ms/行） |
| 同一事务内逐行删除，仅主键索引 | 630 µs/行 |
| 同一事务内逐行删除，主键 + 二级索引 | 1,740 µs/行 |
| 逐行删除，autocommit（额外一次 fsync） | 2,916 µs/行 |
| 单语句 `DELETE FROM t WHERE id < 2000` | 122 µs/行（约 8,200 行/秒） |
| 点查对照（同表） | 11.4 µs/次 |

根因在堆层：`Heap::remove_many` 删除一行后对整页 `repack`，`repack` 把**所有存活槽**都作为 `(old, new)` 位移返回（即使 `old == new`），`engine::exec_delete_fast` 随即对每个“位移”调用 `reindex_repoint`，在每个索引树里删除并重插一条索引项。删除一页中的一行，会让同页其余幸存行（数十行）各做一次索引重指；两个索引就是数十对 B+ 树删除 + 插入。逐行删因此是 O(页内行数 × 索引数) 的写放大。`Heap::replace` 的原地更新分支已经过滤了 `old != new`，所以 UPDATE 只有约 130 µs/行，没有同样的放大。

**使用建议**：删除多行时用一条 `DELETE ... WHERE ...`（每个受影响页只 repack 一次），避免在应用层逐行删除；尤其在带二级索引的表上，逐行 DELETE 的吞吐会比单语句低一个数量级。代码层面的优化方向是把 `repack` 的返回过滤为真实 locator 变化（`old != new`），或引入 locator 间接层；本文只记录基线，未改动引擎实现。

## 与 SQL Server 的对比

**先说明可比性边界**：截至 2026-10-27，本文没有给出 SQL Server 的并排实测数字。原因不是 SQL Server 不可测，而是本机环境无法产出可信的同机对比：

- 完整 SQL Server 2022 的 Linux 容器只有 **amd64** 镜像；本机是 arm64。用 QEMU/Rosetta 跑 amd64 SQL Server 测到的是模拟层开销，不能代表 SQL Server。
- 原生 arm64 的 **Azure SQL Edge** 是 SQL Server 引擎的边缘版（SQL Server 2019 血统，功能与资源上限不同），不能等价为完整 SQL Server。
- 本机 Docker Desktop 只有 **4 vCPU / 2 GiB**，与前面 8 核 / 8 GiB 的原生 DocSQL 基线不是同一资源档；即使跑起来也不能并排。

拿模拟层或边缘版的数字填“SQL Server”一列，会同时误导两个方向。因此下面只做**架构层面的对比**、**按场景的预期方向**和**可在 x86_64 主机上复现的实测方案**；真实数字应在同机、同资源、同负载下另行采集。

**总体判断**：低并发、简单 OLTP（单条写、主键点查、索引分页）两者预计同量级——DocSQL 的优势是进程内执行、文档整体存储与单二进制部署，SQL Server 的优势是日志缓冲/组提交、成本优化器、并行执行与多写者；复杂分析、非等值/外连接和高并发写入通常 SQL Server 占优。这个判断必须用下一节的同机实测替换成数字。

### 架构差异

| 维度 | DocSQL | SQL Server | 对性能的含义 |
|---|---|---|---|
| 执行模型 | 嵌入式单进程、单写者；读走 MVCC 快照，写串行 | 客户端-服务器、多线程、并行查询、锁/闩与行版本 | 低并发简单 OLTP 差距小；高并发混合负载 SQL Server 占优 |
| 写入与持久化 | WAL；每条 autocommit 语句一次 fsync，显式事务一次 fence | 事务日志 + 日志缓冲区；提交刷盘；多写者；组提交 | 单条写都受刷盘支配；SQL Server 的日志缓冲/组提交通常更有利，需同盘实测 |
| 存储布局 | 4 KiB 页、文档堆（整文档存储）+ 每个索引一棵 B+ 树 | 8 KiB 页、堆/聚集索引、行存，另有列存 | 范围扫描、覆盖索引、列存分析 SQL Server 更强；DocSQL 的文档整体读写更自然 |
| 查询优化 | 规则/探针路径，无统计信息与成本模型 | 成本优化器 + 统计信息 + 并行 + 多种连接算法 | 多表复杂查询、非等值/外连接通常 SQL Server 更优 |
| JOIN | 可提取等值条件走 hash join，否则嵌套循环 | hash / merge / nested loop，可并行 | DocSQL 的 LEFT JOIN 与不等值 JOIN 是实测弱项（见上节） |
| 聚合/窗口 | 单线程执行 | 并行、批处理模式 | 大数据量分析 SQL Server 通常更快 |
| 分页 | 索引序窗口 / top-K 堆；keyset 任意深度约 0.04 ms | `TOP` / `OFFSET … FETCH`；优化器选择索引或排序 | 浅页两者都好；深 OFFSET 两者都随偏移线性增长 |
| 事务/隔离 | WAL + SAVEPOINT；单写者，写事务排队 | 完整隔离级别、锁、行版本、并行事务 | SQL Server 的并发事务能力更完整 |
| JSON/文档 | JSON 文本 + JSON 路径索引，文档整体存储 | JSON 函数，底层仍是行存 | 文档读写 DocSQL 更自然；复杂关系查询 SQL Server 更成熟 |
| 集群 | 对等复制/主从转发，每个节点单写者 | Always On、FCI、读写分离等 | 拓扑不同，不能直接把集群吞吐放在一张表里比 |

### 按场景的预期方向

下表左列是前文的 DocSQL 实测（嵌入引擎、本机 APFS），右列是基于架构的**预期方向**，不是 SQL Server 实测值。真正对比时请按下一节的方案在 x86_64 上采数。

| 场景 | DocSQL 实测 | SQL Server 预期方向 | 说明 |
|---|---|---|---|
| 单条 autocommit 写 | 1,000–1,400 行/秒 | 同盘同核通常同量级或更高 | 日志缓冲、组提交、恢复模式都会影响；必须同盘实测 |
| 单事务批量写 | 约 21,000 行/秒 | 通常更高，取决于批量大小与最小日志 | SQL Server 批量导入/最小日志是强项 |
| 主键点查 | 11–13 µs/次（进程内） | 引擎侧同量级；端到端另加网络往返 | 不要把 DocSQL 进程内延迟与 SQL Server 客户端-服务器延迟直接相减 |
| 索引序首屏/keyset | 0.018 / 0.038 ms（50 万行） | 浅页与 keyset 通常都很快 | 深 OFFSET 两者都线性变慢 |
| 索引探针 IN/BETWEEN/LIKE 前缀 | 0.03–4.2 ms | 索引 seek 通常同量级，依赖统计信息 | 全扫基线 178–306 ms；SQL Server 有统计信息与更丰富的访问路径 |
| 等值 hash JOIN | 5.5 ms / 900 万对 | 通常同量级或更快 | SQL Server 可并行、可选 hash/merge |
| LEFT JOIN（大量未匹配行） | 281 ms | 通常明显更快 | DocSQL 当前 null 扩展逐右行扫描；SQL Server 优化器会选更合适的连接 |
| 非等值 JOIN | 3.4 s（嵌套循环） | 通常明显更快 | SQL Server 可用 hash/merge join 与并行；DocSQL 回退嵌套循环 |
| 逐行 DELETE | 655 行/秒（事务内） | 逐行也有语句开销，但通常高于 DocSQL | DocSQL 的页内索引重指是当前实现的固定成本；两边都应优先集合式删除 |
| 并发写 | 单写者串行 | 多写者并行 | 这是架构差异，不是调参能消除的 |
| 并发读 | MVCC 快照，读不阻塞写 | 快照/锁隔离，可并行 | 低并发差距小；高并发 SQL Server 更成熟 |

### 在 x86_64 主机上复现对比

要得到可信的 SQL Server 数字，按下面的约束在同一台 x86_64 Linux 主机上跑：

```bash
# 1) SQL Server 2022 Developer（amd64 原生，不要在 arm64 上模拟）
docker run -d --name mssql-bench \
  -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='Str0ng!Passw0rd' \
  -p 1433:1433 -v mssql-bench:/var/opt/mssql \
  --cpus 6 --memory 8g \
  mcr.microsoft.com/mssql/server:2022-latest

# 2) DocSQL 用同一台机器的同一块本地盘（release）
cargo build --release -p docsql-core --examples
./target/release/examples/bench
./target/release/examples/bench_page 500000
./target/release/examples/bench_probe 500000
```

SQL Server 2022 镜像不自带 `sqlcmd`；可在同一网络起 `mcr.microsoft.com/mssql-tools` 容器，或安装 `mssql-tools18` 后用 `sqlcmd -S localhost -U sa -P '…' -C -i compare.sql` 执行脚本（把下面的计时模板与各场景拼成 `compare.sql`）。最小的点查计时模板（服务端循环，避免客户端逐语句往返）：

```sql
SET NOCOUNT ON;
DECLARE @t DATETIME2 = SYSUTCDATETIME(), @i INT = 0, @x NVARCHAR(50);
WHILE @i < 20000
BEGIN
    SET @x = (SELECT name FROM bench WHERE id = @i);
    SET @i += 1;
END;
SELECT 'point-query-20k' AS metric,
       DATEDIFF_BIG(MICROSECOND, @t, SYSUTCDATETIME()) AS us;
```

写入、批量写、分页、探针、JOIN、删除按同样的模式计时；DocSQL 侧用等价的 `bench*` 脚本或 T-SQL 批循环，保证两边测的是同一件事。

对齐变量：

- **CPU/内存**：给两个引擎相同的 `--cpus` / `--memory`；DocSQL 若跑在容器里也加上同样的限制。不要用“原生 DocSQL vs 2 GiB 容器 SQL Server”。
- **存储**：同一块本地 SSD、同一文件系统/卷类型；不要一个在 APFS、一个在网络卷。SQL Server 用 Docker volume 时，DocSQL 也用 Docker volume 或同一块盘上的目录。
- **恢复模式**：SQL Server 默认 FULL；若改 SIMPLE，写入数字会变，必须在报告里注明。DocSQL 对应 `DOCSQL_ASYNC_COMMIT=0/1` 也要分开报。
- **并行度**：SQL Server 的 `MAXDOP` 默认可能使用多核；DocSQL 是单线程执行。要么固定 `MAXDOP 1` 做单线程引擎对比，要么明确报告“SQL Server 默认并行”的数字。
- **统计信息/索引**：SQL Server 先让统计信息就绪（或 `UPDATE STATISTICS`），并记录索引/填充因子；DocSQL 没有成本优化器，不要用 SQL Server 的“优化器自动纠正”来解释差异而不说明。
- **采样**：预热后至少 3 轮，取中位数；fsync 主导的写指标与 CPU 主导的查询指标分开报；记录宿主负载、profile、数据量。
- **客户端开销**：若要比较端到端延迟，SQL Server 用 `sqlcmd`/驱动，DocSQL 用 CLI/驱动，两边都走真实网络；若只比较引擎，使用服务端循环（SQL Server 的 `WHILE` 批、DocSQL 的 T-SQL 批）避免客户端逐语句往返。

对比时至少覆盖：autocommit 写入、单事务批量写、主键点查、二级索引等值、`IN`/`BETWEEN`/前缀 `LIKE`、50 万行首屏/深 OFFSET/keyset、无索引排序、`COUNT(*)`、等值/LEFT/非等值 JOIN、逐行 vs 集合式 DELETE、以及并发读写（多连接）。**不要**把本机模拟层、Azure SQL Edge 或不同资源档的数字标成“SQL Server 性能”。

## 容量解读与注意事项

- **写吞吐由 fsync 决定**：单条 autocommit 写在本地 APFS 上约 0.7–1.0 ms/条；生产 Docker 卷、云盘、网络存储的 fsync 延迟可能显著不同，必须用 `bench6` / `bench_write` 在目标环境复测。批量导入用单事务或 `DOCSQL_ASYNC_COMMIT=1`，可把吞吐从约 1 千行/秒提升到约 2 万行/秒。
- **集群写不是零成本**：每个写入节点还要记 `_cluster_log`；融合写单元（用户写 + journal 一次 fsync）实测约 1.7× 提升，未融合时接近两倍写延迟。
- **点查已足够快**：约 12 µs/次、9 万次/秒（单线程嵌入引擎）；从 3 万到 30 万行只增加约 11%，索引高度不是瓶颈。服务端 TCP + JSON + 鉴权会叠加额外开销，端到端延迟需用驱动/HTTP 基准测。
- **深分页用 keyset**：OFFSET 成本随偏移线性增长；`WHERE id > 上页最后值 ORDER BY id LIMIT n` 在任意深度约 0.04 ms。
- **分析查询优先等值连接**：等值 hash JOIN 与复合等值都在毫秒级；非等值连接回退嵌套循环，规模上去后是秒级。LEFT JOIN 在当前实现下对“大量未匹配行”不友好，尽量先缩小右侧或改写成可等值的形式。
- **批量删除用单语句**：逐行 DELETE 的页内索引重指是当前实现的固定成本；单语句删除同一批数据快约 12×。
- **本文数字是嵌入引擎基线**：不含网络、鉴权、Web 控制台与 Docker 卷；跨机器只比较倍率（如异步 vs 同步、索引 vs 全扫），不要比较绝对毫秒数。

## 已知性能改进线索

以下是本次实测确认、但未在本次改动中实施的方向，供后续优化排期：

| 线索 | 证据 | 预期收益 |
|---|---|---|
| `Heap::repack` 只上报真实 locator 变化（`old != new`） | 逐行 DELETE 的 1.5 ms/行中，大部分是 `reindex_repoint` 对未移动幸存行的无效重指；`Heap::replace` 已按此过滤 | 删除吞吐提升数倍；对页尾删除接近零重指 |
| `null_extended_left` 预计算右侧键并集 | LEFT JOIN 281 ms vs INNER 5.5 ms；未匹配左行各遍历整个右侧 | 未匹配行多时 LEFT JOIN 降到毫秒级 |
| 批量删除/批量更新合并语句 | 单语句 DELETE 122 µs/行 vs 逐行 1,740 µs/行 | 应用侧即可获得约 12×，无需引擎改动 |

新增或修改基准脚本后，按仓库规范交替多轮采数、按主导成本分档判断噪声，并在报告里带上宿主负载、profile 与数据量，避免把单轮差异读成趋势。
