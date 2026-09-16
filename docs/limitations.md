# 已知边界与定位

商用交付现状(2026-09-11 商用化加固批次之后)。诚实清单:哪些是设计内权衡,哪些是待补路线。

## 架构边界(当前设计的硬边界)

| 边界 | 现状 | 影响 |
|---|---|---|
| 单写者引擎 | 写路径全库互斥(有意设计,横向扩展靠集群分摊写入点);只读语句已 MVCC 快照化,**读不阻塞写、写不阻塞读** | 写吞吐仍是单线程串行;快照读依赖 WAL 保留窗口,极端写压下长读可响亮报「snapshot too old」 |
| 单文档 ≤ 16MiB | 溢出页链已支持超页文档(旧 4KB 上限解除);BLOB 值(`x'hex'`/byte[] 映射)走 `Value::Bytes`,仍是整值读写 | 超过 16MiB 显式报错;无 BLOB 流式/分块读写(大文件请按块存多行) |
| 单列之外的索引能力 | 多列(复合)索引已支持(复合 UNIQUE 判重、前导列等值探测);无表达式/部分/JSON 路径索引 | JSON 点读中非前导列条件走全表扫描(JSON_EXTRACT 不走索引) |
| 无精确 TIMESTAMP 类型 | 值模型含精确 DECIMAL(rust_decimal,28~29 位有效数字;算术/聚合/比较精确,混合运算优先于 Float);时间仍按整数毫秒/ISO-8601 文本约定 | 时间比较依赖文本规范形状;DECIMAL 有效数字上限 28~29 位 |
| **小数字面量按 Float 解析** | `0.1`/`1.5` 等含小数点的字面量走 f64(二进制近似);DECIMAL 只经 `CAST('…' AS DECIMAL)`、线协议 `$dec` 标记或驱动参数获得。混合运算 Decimal 优先,但字面量先转 f64 会带近似误差 | `CAST('9.99' AS DECIMAL) + 0.1` 得 28 位近似而非 10.09;金融计算一律参数化传 DECIMAL 或显式 CAST,勿裸写字面量。字面量改按 Decimal 解析会与旧节点副本分叉(同文本不同语义),故为显式边界 |
| **pub/sub 跨节点尽力而为** | 消息持久化(先落盘后推送)+ 实时扇出(失败退避重试)满足 at-least-once;但 `_pubsub_messages` 是系统表,不进摘要/快照/期刊修复——发布节点宕机期间错过的消息在其重连后**不会补投** | 单节点订阅者 at-least-once 成立(重订阅按 id 续推);跨节点「订阅者离线窗口」的消息缺口需业务侧按 id 对账发现。要求不丢消息的分发请以 SQL 写业务表 + pubsub 只作通知 |
| 集群修复无行级合并 | 重启反熵:增量追赶为主,超窗/分歧转快照采纳(多数派裁决) | 分歧中少数方独有写在快照采纳时被覆盖(文档化策略);修复由重启触发 |
| ALTER TABLE 多操作非原子 | `ALTER TABLE … ADD/DROP/RENAME` 多个操作逐条独立提交,中途失败(后续操作被拒)时已提交的操作不回滚 | 变更单里每个操作单独校验后再提交;要原子性请一条语句一个操作 |
| FK 目标唯一性建表不校验 | CREATE TABLE 不校验 REFERENCES 目标表/列存在且为 PK/UNIQUE(与 SQLite 一致,首个子插入才报错) | 目标非唯一时父侧删除检查可能误拒合法删除;建模期靠 schema 评审 |
| 查询优化器原始 | 规则式索引探测(单表无 JOIN 时生效);JOIN 等值条件走 hash join(键按 `cmp_values` 归一化,索引是超集、ON 逐候选终裁;等值 JOIN 千行级表毫秒级完成),非等值/无限定列/交叉连接仍嵌套循环 | 非等值 JOIN 成本高;无 EXPLAIN |
| 备份为逻辑全量 | dump 快照 + keep-N + sha256 校验和;恢复逐条重放、持 write_order 全程 | 无增量备份/PITR;大库备份/恢复 O(数据);恢复中途失败留下部分恢复态(converged=false 如实报告,重跑 restore 即可) |
| EF 免迁移同步不校验列类型 | 校验覆盖 表/列存在性、索引名/列序/唯一性;引擎为无类型文档模型,`information_schema.columns.data_type` 恒为 ANY,列类型本就不持久化 | 改实体属性类型(TEXT→DECIMAL 等)不会被告警也不会生效于存量数据;类型变更属破坏性变更,需人工评估/迁移新表 |

## 已具备的商用面

- 完整门禁与治理:双许可、SECURITY/CONTRIBUTING/CHANGELOG、CI(fmt/clippy/test → 多架构镜像 → 部署测试);
- 可观测性:`/metrics`(Prometheus)、`/healthz`、REQ_STATUS 运行时计数器、审计 JSONL、慢查询日志;
- 运维韧性:优雅停机(SIGTERM 排空)、TCP keepalive、配置快速失败、`DOCSQL_MAX_CONN`/`IDLE_TIMEOUT`/`STATEMENT_TIMEOUT_MS`;
- 数据完整性:WAL 先日志后数据、备份 sha256 校验和、恢复后跨节点摘要收敛验证、集群反熵自愈;
- 安全:三凭据 + 数据库用户/角色(即时撤销)、登录锁定、**服务端参数化绑定(驱动默认路径)**、
  **Web 控制台原生 TLS(rustls)**、等保三级能力对照;
- 生态:.NET ADO.NET(连接池默认开启 + 服务端绑定)+ EF Core + Aspire 编排集成(NuGet/GitHub Packages 发布)、
  CLI(csv/json/脚本)、Web 控制台、单语言驱动之外的服务端 prepared statements 线协议(第二语言驱动的基础)。

## 路线(按商用优先级)

1. **驱动扩展**:基于 REQ_PREPARE/REQ_EXECUTE 服务端绑定实现第二/第三语言驱动(JDBC/Python);
2. **MVCC/读写并发** — **阶段 A(读-读并发)与阶段 B(快照读,读不阻塞写)已落地**
   (`docs/design/003-mvcc-read-concurrency.md`):server 读级微秒级建视图后锁外执行,
   长查询不再卡写入;快照过旧(写流量把 WAL 推过硬阈值截断)时读取响亮报错可重试;
   附:TIMESTAMP 精确类型、JSON 路径索引、VACUUM(溢出链孤儿页回收);
3. **性能**:EXPLAIN、统计信息(JOIN 等值 hash join 已落地,`bench_join` 基准);
4. **数据安全**:TCP 协议层原生 TLS(控制台已原生支持;数据面走 AES-GCM 帧加密或 TLS 反代/加密卷)、
   增量备份/PITR;
5. **生态**:Kafka/CDC 连接器、视图与触发器、全文检索。

以上边界均为**显式行为**(报错或文档化策略),不存在静默数据风险;未列出的 SQL 语法一律显式报错。
