# 设计说明:自动故障转移(多数派写栅栏 + 自动 PROMOTE)

状态:**设计定稿;阶段 1(对称集群写栅栏 + 仲裁者)已全部落地**——
`crates/docsql-server/src/quorum.rs`(可见性记账 + 探测循环)、写门五站
(SQL 写/PUBLISH/TRIM/PROMOTE/backup trigger+restore,与 read_only 副本门
同层)、REQ_STATUS `quorum` 字段、`docsql_writes_fenced_total`/
`docsql_quorum_visible`/`docsql_quorum_probe_failures_total` 指标、
`DOCSQL_ARBITER` 模式(handle_connection 加 arbiter 门,除
AUTH/PING/STATUS 外全拒,内存库);配置 `DOCSQL_QUORUM`/
`DOCSQL_QUORUM_PROBE_MS`/`DOCSQL_QUORUM_K`/`DOCSQL_QUORUM_MEMBERS`/
`DOCSQL_QUORUM_ARBITERS`,默认关闭。§2 的开放问题按文中倾向拍板:仲裁者
不参加摘要选举;`DOCSQL_QUORUM_MEMBERS` 覆盖 PEERS 的成员表语义(扇出仍
以 PEERS 为准)。阶段 2(主从自动 PROMOTE)未开始,按 §4 推进。
**阶段 2(主从自动 PROMOTE + primary_epoch 降级)已落地**:
`DOCSQL_AUTO_PROMOTE=1`(要求 DOCSQL_QUORUM)的副本在「主失联 K 周期 + 多数派可见 +
日志滞后 ≤ DOCSQL_CATCHUP_WINDOW(滞后超窗→保持只读并告警一次)」三条件下执行与手动
PROMOTE 相同路径(epoch+1,sync_event 审计);REQ_STATUS 增 `primary_epoch`;PROMOTE(手动
或自动)递增 epoch;活跃主探测到更高 epoch 的活跃主即降级(只读 + 重指向对方)。实现注意:
比较只取「更高 epoch」,等值不动作——对称集群所有节点 epoch 恒 0,永不触发;独立手动提升造成
的等值窗口是 §4.3 已文档化的残余风险。部署要求:主从对的成员表互相列出对方(或经
DOCSQL_QUORUM_MEMBERS)+ 副本配仲裁者,使主失联后副本仍有多数派可见。
本文是权威设计;任何可用性/一致性语义相关改动先对照本文的语义矩阵(§6)。

## 0. 现状(精确事实)

- **对称集群**(`DOCSQL_PEERS`):任意节点接受客户端写,写提交后按
  本节点执行序以 REQ_SQL_SEQ(origin+seq)扇出全网(`write_order` 串行化);
  追赶走 REQ_CATCHUP 位点拉取,分歧走快照采纳(`decide_repair` 已实现
  确定性参考方选举:摘要分组多数派 → 行数多者优先 → 序列化序)。
  **网络分区时两侧都继续接受写**——少数派独有写在快照采纳时被参考方
  覆盖(无行级合并),这是文档化的 RPO>0 语义;
- **主从**(`DOCSQL_REPLICATE_TO` + `DOCSQL_READ_ONLY`):副本把写转发给
  主,自身拒绝客户端写;切换 = 人工对副本发 REQ_PROMOTE(清 read_only +
  detach 上游)+ 人工处理旧主(降级/重指向)。主宕机期间需要人工介入;
- **故障检测**:运行时为零——`probe_peer_info`(REQ_STATUS)只在启动
  同步/repair/restore 时使用;长分区期间没有任何节点知道对端是否存活;
- **栅栏基础设施**:副本模式 `state.read_only` 已经是"整节点拒绝客户端
  写"的现成形态;REQ_HOLD/REQ_RELEASE + `drain_sync_queue` 是 join 用的
  "排空在途写并冻结"机制(带 watchdog 防泄漏);
- 修复只由重启触发(设计内不变量,本文不改变它)。

## 1. 目标与非目标

目标(配置启用后,默认关闭 = 现行为):

| 场景 | 现行为 | 启用后 |
|---|---|---|
| 对称集群网络分区 | 两侧都收写,少数派写日后被覆盖(RPO>0) | 多数派侧照常;**少数派自动栅栏客户端写**(响亮报错)→ 不产生分叉写,RPO=0 |
| 主从:主宕机 | 副本只读,等人工 PROMOTE | 副本确认"主失联 + 自身仍有多数派可见"后**自动 PROMOTE** |
| 主从:旧主回流 | 无防护(人工纪律) | 旧主探测到更高 epoch 的主存在 → 自动降级只读并重指向新主(§5) |
| 节点失联抖动 | 无感知 | K 个探测周期失联才动作(防抖);STATUS/指标暴露可见性 |

非目标(明确不做):

- **行级合并**:始终不做;栅栏的意义正是让分叉写不发生,而不是发生后调和;
- **2 节点零 RPO**:无仲裁者的两成员集群,任一失联即双方都失多数——
  数学上只能在"可用性"与"一致性"里选一边;本文选择**栅栏(一致性)**,
  并提供仲裁者把 2 变 3(§4.4);
- **客户端路由**:驱动侧多端点 failover 连接串是独立工作(§7 阶段 3),
  本文只负责服务端的栅栏与提升语义;
- **修复自动化**:分区期间被栅栏的节点不需要"修复"(它没有分叉写);
  重启反熵原样保留,兜底语义不变。

## 2. 核心机制:多数派写栅栏(阶段 1)

### 2.1 成员与可见性

- 仲裁成员表 = `DOCSQL_PEERS`(对等)+ self。新增可选
  `DOCSQL_QUORUM_MEMBERS`(覆盖默认成员表,给"部分节点参与仲裁、
  全部节点参与复制"的部署留口)与 `DOCSQL_QUORUM_ARBITERS`(§4.4);
- 每个节点一个后台探测循环(默认 1s 一轮,`DOCSQL_QUORUM_PROBE_MS`
  可调):对全部成员**并发**发 REQ_STATUS(复用 `probe_frame`/
  `probe_peer_info` 的连接形态:cluster token 认证、单请求短连接,
  与 repair 的探测完全同构);记录每成员"最近一次成功响应"的
  **本地单调时刻**(绝不比较节点间墙上时钟);
- 可见性判定防抖:成员失联 = 连续 K 个探测周期(`DOCSQL_QUORUM_K`,
  默认 3)无成功响应。1s×3 → 约 3 秒检测窗口,成员重启的秒级窗口
  不会触发栅栏;
- 成员身份用 REQ_STATUS 已有的 `node_id` 区分"重启后的同一成员"
  与"新出现的节点"。

### 2.2 写门

- 栅栏判定:`visible_members() * 2 > member_count()`(self 恒计入);
  不满足 → 节点进入 **fenced** 状态;
- 检查点:与 `state.read_only` 相同的所有写入口——客户端 SQL 写、
  PUBLISH、PROMOTE、REQ_BACKUP trigger/restore、用户管理语句。实现上
  与 `read_only` 读标志同层(execute_sql 的写入口处),一次原子加载;
- **栅栏只挡客户端写**:复制 apply(REQ_SQL_SEQ 等 FLAG_REPLICATION
  帧)、订阅推送、全部读路径照常——少数派节点保持对订阅者与只读
  流量的服务,它只是不再"发明"新写;
- 报错形态:与副本模式同语义的响亮错误
  `quorum lost: {visible}/{members} members visible; writes fenced`
  (RESP_ERROR)。客户端按普通错误重试即可,多数派侧恢复可达后栅栏
  自动解除(探测循环持续运行);
- 在途写:栅栏检查在写入口(authorize 同层),进入 `write_order` 的
  写单元必然已在栅栏生效前通过检查——不需要打断在途事务;扇出失败
  的既有退避/追赶机制原样兜底(对端恢复后按位点拉取)。

### 2.3 为什么"少数派栅栏"后不需要新的恢复机制

栅栏的唯一目的是**消灭分叉写**:分区期间只有多数派产生写,少数派
没有独有写。于是:

- 分区愈合,少数派重连 → 正常扇出/追赶即收敛,快照采纳(覆盖语义)
  从"分区的常见结局"退化为"仅重启反熵的兜底";
- 重启修复原样保留:它处理的场景(离线追赶超窗、摘要分歧)不因
  栅栏而消失,只是不再被分区分叉触发。

### 2.4 仲裁者(arbiter)

- 形态:同一二进制加 `DOCSQL_ARBITER=1` 模式——只监听协议端口、只
  应答 REQ_STATUS(返回 node_id)、不打开数据文件、拒绝其余全部帧;
  零存储、零复制参与;
- 用途:2 数据节点的集群配 1 个仲裁者 = 3 仲裁成员,单点失联不再
  双侧栅栏;仲裁者应部署在与两数据节点都独立的故障域(第三台机器);
- 仲裁者自身失联 = 集群退化为 2 成员语义(§1 非目标里明示的取舍)。

## 3. 可观测性

- REQ_STATUS 增字段(wire 只许加字段的既有规则):
  `"quorum": {"members": M, "visible": V, "fenced": bool}`;
- Prometheus:`docsql_quorum_visible`(gauge)、
  `docsql_writes_fenced_total`(counter,栅栏拒绝的写请求数)、
  `docsql_quorum_probe_failures_total`(按成员维度的失联事件);
- 同步日志:进入/解除栅栏各记一条 sync_event(与 promote/restore 的
  审计形态一致);
- 控制台集群页把 `fenced` 渲染为状态徽章。

## 4. 主从自动 PROMOTE(阶段 2)

### 4.1 提升判定

配置 `DOCSQL_AUTO_PROMOTE=1` 的副本运行与阶段 1 相同的探测循环,当且
仅当同时满足:

1. 主(`DOCSQL_REPLICATE_TO`)连续 K 周期失联;
2. 仲裁成员多数可见(含 self;副本自己失联不成立——它得先能探测别人);
3. 本地 journal 位点在最近一次成功的主探测中不是明显落后
   (落后超过 `DOCSQL_CATCHUP_WINDOW` 时提升会产生无法增量补齐的缺口,
   此时保持只读并告警,交人工判断——这是刻意的保守)。

满足即执行与手动 PROMOTE 完全相同的代码路径(清 read_only + detach
上游 + sync_event 审计),并在事件里标注 `auto`。

### 4.2 防脑裂:epoch 与旧主降级

这是自动故障转移真正的新增状态,分两个可选强度:

- **探测式降级(默认)**:旧主恢复后,它的探测循环发现"集群里存在
  更高 epoch 的主"即自动进入 read_only 并把 `replicate_to` 指向新主。
  epoch 的传播载体:REQ_STATUS 增 `primary_epoch`(主从拓扑专用字段)。
  旧主能"看见新主"才降级——若旧主自己失了多数派,§2 的写栅栏已经先
  把它挡住,两个机制正交;
- **强栅栏(可选,`DOCSQL_FENCE_BY_EPOCH=1`)**:写转发帧(副本→主)
  与扇出帧携带 epoch,接收方 epoch 更低即拒绝。覆盖"旧主看得见仲裁
  者从而解除写栅栏、但其上游指向已过期"的窗口。默认不启用:它要求
  epoch 持久化(见下),而探测式降级 + 写栅栏在常见部署里已闭合。

### 4.3 epoch 状态放哪(刻意不持久化,附理由)

- epoch 存内存、重启归零。**正确性论证**:epoch 的唯一用途是让
  "恢复的旧主"识别出自己过时;重启后的旧主没有做任何提升动作
  (内存 epoch 归零 = 它从未 PROMOTE 过),它的 `replicate_to` 仍是
  配置值——指向已经不是主的原主地址。此时它收到的转发失败/探测
  失败会把配置值重新评估;运维语义:重启后的旧主按配置恢复原角色,
  若原主已死则它作为**带数据的只读或按 AUTO_PROMOTE 重新竞选**——
  AUTO_PROMOTE 的判定条件(§4.1)对它同样成立时它会提升,不成立则
  保持只读等待。该语义对"主从"拓扑是自洽的:同一时刻至多一个节点
  处于"主活跃"状态的保证来自 §4.1 的条件 2(多数派可见性)与
  §2 的写栅栏,epoch 只是把"谁最后提升的"讲给探测方听;
- 明示风险窗口:探测式降级依赖旧主能探测到新主。若旧主恢复到一个
  **把新旧主都隔离**的网络里且它拥有多数可见性(仲裁者在它这边),
  它会恢复接受写——这正是 §2 写栅栏的定义,该场景等价于"仲裁者
  见证了下一次多数派",属于合法的多数派裁决,不产生未裁决分叉。

### 4.4 与对称集群的关系

阶段 1/2 共用同一探测循环与可见性判定;主从拓扑的成员表默认 =
`[replicate_to]` + self(+ 仲裁者)。两拓扑不混跑(现状约束不变)。

## 5. 与现有机制的交互矩阵

| 机制 | 交互 |
|---|---|
| 重启反熵(repair) | 不变。栅栏减少分叉写 → 快照采纳从"分区常见结局"退为兜底;`decide_repair` 的选举语义不变 |
| join(bootstrap) | join 的 REQ_HOLD/排空在探测循环之下不受影响;join 期间写门照常(bootstrap 写经 sync 门,不是客户端写) |
| restore/PITR | 栅栏节点拒绝 restore(与只读副本同语义);restore 中失联多数 → restore 继续(它持 write_order 且本身是多数派侧发起的操作),完成后的收敛验证照常 |
| pub/sub | PUBLISH 与客户端写同门(栅栏节点拒绝);订阅推送/重订阅照常(读) |
| 优雅停机 | 停机即失联,对集群等价于一次失联事件;K 周期防抖保证滚动重启(重启窗口 < K×probe 周期)不触发栅栏 |
| 用户管理 | 用户表随复制走;栅栏不改变授权语义(execute_sql 里 authorize 在写门之后,顺序:身份 → 授权 → 栅栏 → 执行) |
| `DOCSQL_ASYNC_COMMIT` | 无交互(栅栏在语句入口,不在提交路径) |

## 6. 语义矩阵(启用后)

| 事件 | 数据面 | 一致性 |
|---|---|---|
| 单成员失联(其余健康) | 无感 | 无变化 |
| 分区,少数派侧 | 客户端写响亮报错;读/订阅照常 | 少数派零分叉写 |
| 分区,多数派侧 | 照常写(含扇出,扇出失败对离线侧退避) | 多数派内 RPO=0 |
| 分区愈合 | 栅栏自动解除,扇出/追赶补齐 | 收敛,无需快照 |
| 主宕机(主从) | 副本 K×probe 后自动提升;客户端重试落到新主 | 已确认写不丢(它们在主侧已 fsync 并扇出或可被拉取) |
| 旧主回流 | 探测到新主 → 自动降级只读 + 重指向 | 单写者恢复 |
| 仲裁者失联(2+1 部署) | 退化为 2 成员语义 | 与 2 节点同(明示取舍) |

## 7. 分阶段落地

| 阶段 | 内容 | 依赖 |
|---|---|---|
| 1 | 对称集群写栅栏:探测循环 + 写门 + REQ_STATUS `quorum` 字段 + 指标/审计 + 仲裁者模式;配置 `DOCSQL_QUORUM_PROBE_MS`/`DOCSQL_QUORUM_K`/`DOCSQL_QUORUM_MEMBERS`/`DOCSQL_QUORUM_ARBITERS`/`DOCSQL_ARBITER` | 无(纯增量,默认关闭) |
| 2 | 主从自动 PROMOTE + `primary_epoch` 探测式降级(`DOCSQL_AUTO_PROMOTE`) | 阶段 1 的探测循环 |
| 3(可选) | 强栅栏 `DOCSQL_FENCE_BY_EPOCH` | 阶段 2;需评估 epoch 内存语义的运维接受度 |
| 4(可选) | .NET 连接串多端点 failover(`server=a,b;retry=failover`),落 `docs/drivers.md` | 阶段 1 的栅栏错误语义(客户端按错误重试即可,阶段 4 是体验优化) |

每阶段附带:

- cargo e2e:失联模拟(abort peer 任务/停止探测应答)+ 栅栏判定 +
  愈合解除 + 防抖窗口(探测周期时间可注入);主从轮:杀主 → 副本
  自动提升 → 旧主回归降级;
- `deploy/multinode-test.sh`:docker network disconnect 的真实分区
  用例(既有分区测试基建复用,断言少数派写被栅栏、愈合后收敛);
- 配置非法值拒绝启动(exit 2,数值型 env 一贯规则)。

## 8. 风险与开放问题

- **探测风暴**:成员多时每秒全网状探测的连接开销——成员表按部署
  规模几十以内,REQ_STATUS 短连接成本可忽略;探测循环与 repair 的
  `probe_all_peer_reports` 共享连接构造,不新增机制;
- **抖动误栅栏**:K×probe 的防抖窗口是可用性/检测速度的直接权衡,
  默认 3s 起步并明示调参方向;
- **时钟**:全程本地单调时钟,无跨节点时钟依赖(与 UUIDv7 单调、
  PITR 时间戳的既有取舍一致);
- **栅栏期间的人工恢复**:被长期分区的少数派若有数据面流量但禁止
  写,运营方可能被误导"节点坏了"——同步日志的 fenced 事件与控制台
  徽章是刻意的显性化设计;
- **开放问题(实现前需定)**:仲裁者是否需要参加 REQ_DIGEST/摘要
  分组选举(倾向:不参加,它无数据);`DOCSQL_QUORUM_MEMBERS` 与
  `DOCSQL_PEERS` 部分重叠时的扇出语义(倾向:扇出仍以 PEERS 为准,
  仲裁成员表只影响栅栏判定)。
