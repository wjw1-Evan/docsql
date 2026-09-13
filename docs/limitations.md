# 已知边界与定位

商用交付现状(2026-09-11 商用化加固批次之后)。诚实清单:哪些是设计内权衡,哪些是待补路线。

## 架构边界(当前设计的硬边界)

| 边界 | 现状 | 影响 |
|---|---|---|
| 单写者引擎 | 写路径全库互斥(有意设计,横向扩展靠集群分摊写入点);只读语句已 MVCC 快照化,**读不阻塞写、写不阻塞读** | 写吞吐仍是单线程串行;快照读依赖 WAL 保留窗口,极端写压下长读可响亮报「snapshot too old」 |
| 单文档 ≤ 16MiB | 溢出页链已支持超页文档(旧 4KB 上限解除);仍无无限大文档 | 超过 16MiB 显式报错;无 BLOB 流式读取 |
| 单列之外的索引能力 | 多列(复合)索引已支持(复合 UNIQUE 判重、前导列等值探测);无表达式/部分/JSON 路径索引 | JSON 点读中非前导列条件走全表扫描(JSON_EXTRACT 不走索引) |
| 无精确 DECIMAL/TIMESTAMP 类型 | 值模型 Int/Float/Str/…;JSON 函数族已补文档点读 | 金额用 Float 有精度取舍;时间按整数毫秒/文本约定 |
| 集群修复无行级合并 | 重启反熵:增量追赶为主,超窗/分歧转快照采纳(多数派裁决) | 分歧中少数方独有写在快照采纳时被覆盖(文档化策略);修复由重启触发 |
| 查询优化器原始 | 规则式索引探测(单表无 JOIN 时生效);JOIN 等值条件走 hash join(键按 `cmp_values` 归一化,索引是超集、ON 逐候选终裁;等值 JOIN 千行级表毫秒级完成),非等值/无限定列/交叉连接仍嵌套循环 | 非等值 JOIN 成本高;无 EXPLAIN |
| 备份为逻辑全量 | dump 快照 + keep-N + sha256 校验和 | 无增量备份/PITR;大库备份/恢复 O(数据) |

## 已具备的商用面

- 完整门禁与治理:双许可、SECURITY/CONTRIBUTING/CHANGELOG、CI(fmt/clippy/test → 多架构镜像 → 部署测试);
- 可观测性:`/metrics`(Prometheus)、`/healthz`、REQ_STATUS 运行时计数器、审计 JSONL、慢查询日志;
- 运维韧性:优雅停机(SIGTERM 排空)、TCP keepalive、配置快速失败、`DOCSQL_MAX_CONN`/`IDLE_TIMEOUT`/`STATEMENT_TIMEOUT_MS`;
- 数据完整性:WAL 先日志后数据、备份 sha256 校验和、恢复后跨节点摘要收敛验证、集群反熵自愈;
- 安全:三凭据 + 数据库用户/角色(即时撤销)、登录锁定、**服务端参数化绑定(驱动默认路径)**、
  **Web 控制台原生 TLS(rustls)**、等保三级能力对照;
- 生态:.NET ADO.NET(连接池默认开启 + 服务端绑定)+ EF Core(NuGet 可打包)、
  CLI(csv/json/脚本)、Web 控制台、单语言驱动之外的服务端 prepared statements 线协议(第二语言驱动的基础)。

## 路线(按商用优先级)

1. **驱动扩展**:基于 REQ_PREPARE/REQ_EXECUTE 服务端绑定实现第二/第三语言驱动(JDBC/Python);
2. **MVCC/读写并发** — **阶段 A(读-读并发)与阶段 B(快照读,读不阻塞写)已落地**
   (`docs/design/003-mvcc-read-concurrency.md`):server 读级微秒级建视图后锁外执行,
   长查询不再卡写入;快照过旧(写流量把 WAL 推过硬阈值截断)时读取响亮报错可重试;
   附:DECIMAL/TIMESTAMP 类型、JSON 路径索引、VACUUM(溢出链孤儿页回收);
3. **性能**:EXPLAIN、统计信息(JOIN 等值 hash join 已落地,`bench_join` 基准);
4. **数据安全**:TCP 协议层原生 TLS(控制台已原生支持;数据面走 AES-GCM 帧加密或 TLS 反代/加密卷)、
   增量备份/PITR;
5. **生态**:Kafka/CDC 连接器、视图与触发器、全文检索。

以上边界均为**显式行为**(报错或文档化策略),不存在静默数据风险;未列出的 SQL 语法一律显式报错。
