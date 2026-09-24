# 运维手册

运行仅依赖 Docker。本文覆盖部署拓扑、数据持久化、扩容与修复、备份恢复、环境变量、监控与
CLI;日常使用入口见 [README](../README.md),Aspire 编排见 [Aspire 集成指南](aspire.md),
安全配置见[安全指南](security.md)。

## 部署拓扑

两个 compose 文件完全分离(项目名/端口/数据卷/镜像 tag 变量互不相同,可同时运行):

| profile | 节点 | 开发端口(`docsql-dev`) | 生产端口(`docsql-prod`) |
|---|---|---|---|
| `single` | 独立单节点 + 独立 web 控制台 | 17600 / 17710 | 18600 / 18710 |
| `cluster` | node-a + node-b + node-c 对等集群 + web 控制台 | 17601-17603 / 17700 | 18601-18603 / 18700 |
| `join` | node-d(加入运行中的集群) | 17604 | 18604 |

```bash
# 开发(源码构建 :local,构建期内置 cargo test 门禁;换版本加 --build)
cd deploy && docker compose --profile single up -d
cd deploy && docker compose --profile cluster up -d

# 生产(拉取 CI 发布的 GHCR 镜像;先配 .env 的 DOCSQL_IMAGE_TAG 与 DOCSQL_TOKEN)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d
```

- compose 必须带 profile(不带 = 空操作);
- 控制台是纯管理工具、自身零存储:启动参数即默认管理节点(`DOCSQL_UPSTREAM` 或首个 peer),
  数据操作全部转发到节点执行;
- 节点与控制台镜像 tag 分别由 `DOCSQL_DEV_IMAGE_TAG`(默认 `local`)与 `DOCSQL_IMAGE_TAG`
  (生产 `.env`,默认 `latest`)控制,两栈互不影响。

## 数据持久化与清理

数据卷是 external 卷(`docsql-data-*` / `docsql-dev-data-*`),发布换镜像、重建容器乃至
`down -v` 都**不会**删数据。首次部署前先建卷(一次性):

```bash
cd deploy && for v in a b c single; do docker volume create docsql-data-$v; done && docker volume create docsql-prod-data-d  # 生产
cd deploy && for v in a b c d single; do docker volume create docsql-dev-data-$v; done                                       # 开发
```

彻底清数据唯一入口是 `./deploy/reset-data.sh`(删除并重建全部数据卷)。备份文件在卷内
`/data/backups`,可 `docker cp docsql-prod-single:/data/backups .` 归档。

## 扩容(新数据节点自动同步)

集群已有数据时,起一个指向现有节点的全新节点即可——自动拉取全量历史(schema、约束、索引、
数据、GUID 值),注册进各节点扇出列表,随后与其它节点互相同步:

```bash
cd deploy && docker volume create docsql-dev-data-d    # 一次性建卷(生产 join 用 docsql-prod-data-d)
docker compose --profile cluster --profile join up -d node-d
```

新节点需要 `DOCSQL_PEERS`(现有节点地址表)与 `DOCSQL_ADVERTISE`(其它节点回连自己的地址)。
注意:动态注册保存在原节点内存中,原节点重启即丢失——要把 node-d 变成长期成员,请把它写进
各节点的 `DOCSQL_PEERS` 并重建(数据保留,不会重复同步)。

## 离线补齐与重启反熵

节点离线/分区期间错过的写不会实时补发,但离线节点**重启时自动修复**:对比各节点表摘要,
按复制日志位点从各原点**增量拉取缺失操作**(只读对端日志,不冻结集群、不重传已有数据),
补齐后复验摘要;超出 `DOCSQL_CATCHUP_WINDOW`(默认 10 万条)或增量后仍有分歧时,回退为
整体快照采纳(多数派裁决)。两点边界:无行级合并,分歧中少数方独有写会被参考方快照覆盖;
修复由重启触发,仅分区未重启不会自动收敛。语义细节与测试覆盖见
[功能总览 · 复制与集群](features.md#6-复制与集群)。

## 备份与恢复

- **自动备份**:每节点独立,默认每日一次(`DOCSQL_BACKUP_INTERVAL_SECS`,0=关闭),
  整库逻辑 SQL 快照 `backup-<UTCms>.sql` 写 `<db>/backups`(`DOCSQL_BACKUP_DIR` 可改),
  保留 `DOCSQL_BACKUP_KEEP` 份(默认 7);节点重启后若无备份或最新已超一个间隔则立即出一份;
- **校验和**:每份备份带同名 `.sha256` sidecar,恢复前强校验(损坏/被篡改拒绝重放;
  旧备份无 sidecar 仍可恢复);保留清理连带 sidecar;
- **PITR(恢复到时间点)**:期刊无条件记录(单节点也记),自动备份带 journal-seq 锚点;
  周期性备份之间自动追加**增量段** `incr-<UTCms>.sql`(期刊条目 + 提交时间戳,同样带 sidecar);
  `REQ_BACKUP` 恢复请求带 `"to"` 字段(ISO 时间戳文本或 UTC 毫秒整数)时,重放
  「基准备份 + 增量段链」至目标时间点——增量链带连续性审计,断号/裁剪洞显式报错
  (要求重拍全量),坏 `"to"` 值显式拒绝(不静默退化为整份恢复);
  控制台「备份管理」页与 `POST /api/backup/restore` 目前仅整份恢复,时间点恢复走协议帧;
  窗口 = 本节点期刊保留:对称多写集群中他节点发起的写不在本节点期刊里,
  **集群级 PITR 需在承载全部写的主节点上执行**(主从拓扑天然满足)或以全量兜底;
- **手动触发**:控制台「备份管理」页、`POST /api/backup` 或协议帧 `REQ_BACKUP`;
- **恢复**:备份是完整 SQL 脚本(多表 `DROP TABLE IF EXISTS` 开头,幂等),逐条经正常写路径
  重放并扇出全网,整体收敛到备份时点。语义:
  - 覆盖备份中包含的所有表(整表替换);备份后新建的表保留,如需完全对齐先手动删除;
  - 恢复重放期间发起节点持写路径:本地新写排队(超 30s 报错),恢复完成后照常落库;
  - 完成后自动增量补拉并逐一比对各可达 peer 摘要,状态暴露 `converged`/`note`;仍有分歧时
    让对应节点重启一次即自动修复;
  - AUTOINCREMENT 计数器按恢复后现存最大值 +1 续推;
  - 全网同时只允许一个恢复(发起节点探测 peers,他节点恢复中即拒绝);只读连接/副本拒绝;
  - 控制台/API 调用要求 `confirm` 字段逐字重复文件名;
- **手工归档/重放**:

  ```bash
  bk=$(docker exec docsql-prod-single ls /data/backups | grep -E '^backup-.*\.sql$' | sort | tail -1)
  docker exec docsql-prod-single cat "/data/backups/$bk" | docker exec -i docsql-prod-single docsql-cli connect 127.0.0.1:7600
  ```

备份状态在 `REQ_STATUS.backup` 与控制台备份页可见;每次备份成败写入同步日志。

## 环境变量(运维相关)

### 认证与传输

| 变量 | 默认 | 说明 |
|---|---|---|
| `DOCSQL_TOKEN` | 无 | 客户端凭据(恒为管理员身份);Web 控制台以它连接节点(与节点同值),并作为账号门的 API 旁路;≥8 位且非单字符重复,违者拒绝启动 |
| `DOCSQL_READ_TOKEN` | 无 | 只读客户端凭据(可查不可写) |
| `DOCSQL_CLUSTER_TOKEN` | 无 | 节点间凭据(复制帧仅接受节点身份) |
| `DOCSQL_KEY` | 无 | 64 位 hex → AES-256-GCM 帧加密(客户端与节点间同用);全零 key 拒绝启动(公开密钥加密形同虚设,与弱 token 同策略) |

### 资源与执行

| 变量 | 默认 | 说明 |
|---|---|---|
| `DOCSQL_MAX_CONN` | 0(不限) | 并发连接上限,超限立即拒绝 |
| `DOCSQL_IDLE_TIMEOUT` | 0(不限) | 空闲会话秒数,超时主动断开 |
| `DOCSQL_STATEMENT_TIMEOUT_MS` | 0(不限) | 客户端语句墙钟预算,超时报错回滚;复制 apply 与恢复重放不受限 |
| `DOCSQL_ASYNC_COMMIT` | 0 | 1 = 组提交(~2ms 丢失窗口;PUBLISH 仍推送前强制 fsync) |

### 集群与日志

| 变量 | 默认 | 说明 |
|---|---|---|
| `DOCSQL_PEERS` | 无 | 对等节点表(逗号分隔);全新节点配它启动即自动 join |
| `DOCSQL_ADVERTISE` | 无 | join 时通告自身地址;不设仍同步数据但不注册 |
| `DOCSQL_REPLICATE_TO` | 无 | 主从写转发目标 |
| `DOCSQL_READ_ONLY` | 0 | 1 = 整节点只读副本 |
| `DOCSQL_CATCHUP_WINDOW` | 100000 | 追赶日志保留条数(0 = 不限) |
| `DOCSQL_BACKUP_INTERVAL_SECS` / `_KEEP` / `_DIR` | 86400 / 7 / `<db>/backups` | 自动备份节奏/保留份数/目录 |
| `DOCSQL_LOG_FILE` / `DOCSQL_SLOW_MS` | 无 / 100 | 审计 JSONL 落盘 / 慢查询阈值(stderr) |

### Web 控制台

| 变量 | 默认 | 说明 |
|---|---|---|
| `DOCSQL_UPSTREAM` | 无 | 默认管理节点(启动参数 > 此变量 > `DOCSQL_PEERS` 首条) |
| `DOCSQL_WEB_AUTH_FILE` | 无 | 控制台账号门凭据文件;空/未设 = 关闭(API 开放,浏览器无令牌输入),已设 = 首次强制 setup |
| `DOCSQL_WEB_COOKIE_SECURE` | 0 | HTTPS 反代下置 1(会话 Cookie Secure) |
| `DOCSQL_WEB_TRUST_PROXY` | 0 | 1 = 按 `X-Forwarded-For` **末跳**分桶(登录锁定/审计);多级反代需在最近一层重写 XFF 为仅客户端地址 |
| `DOCSQL_WEB_TLS_CERT` / `_KEY` | 无 | PEM 证书/私钥路径,同时设置即以 HTTPS 服务(只设其一拒绝启动) |

**配置快速失败**:以上数值变量非法值一律拒绝启动(exit 2),不静默回退。

## 监控

- `GET /healthz` — 无门禁存活探针,只反映控制台进程自身;
- `GET /metrics` — Prometheus 文本,账号门激活时抓取认证与其它 API 一致(会话 Cookie 或 `X-Docsql-Token` 旁路),未启用账号门时开放:

  ```yaml
  scrape_configs:
    - job_name: docsql
      static_configs:
        - targets: ["web:18710"]
  ```

  指标族(节点标签 `node`):`docsql_node_up`、`docsql_node_info`、`docsql_uptime_seconds`、
  `docsql_sql_statements_total` / `docsql_sql_statement_errors_total`、
  `docsql_connections_total/_active/_rejected_total`、`docsql_publishes_total`、
  `docsql_auth_failures_total`、`docsql_network_bytes_total`、`docsql_tables` / `docsql_rows`、
  `docsql_storage_bytes` / `docsql_wal_bytes`、`docsql_journal_head` /
  `docsql_replay_failures_total`;控制台自身 `docsql_web_http_requests_total`;
- `GET /api/stats`、`GET /api/cluster` — 同数据的 JSON 形式(仪表盘/集群页数据源);
- 语句审计、认证事件与慢查询日志见[安全指南 · 审计](security.md#审计)。

## 健康检查与优雅停机

- compose healthcheck 用镜像自带 CLI 对节点 PING(interval 5s / retries 10);
- 节点与控制台处理 SIGTERM/SIGINT:停止接受新连接 → 存量连接限时排空(节点最长 10s)→ 进程退出;
  未收尾事务由断连回滚 + WAL 恢复兜底。`docker stop` 与编排器滚动发布安全。

## CLI

```
docsql <file.db>                    嵌入模式
docsql connect <addr> [token]       远程模式(--user 走用户登录,密码走 DOCSQL_PASSWORD/交互)
  --csv | --json                    行导出格式(RFC 4180 / JSON 对象数组)
  -f script.sql                     脚本批执行(错误即退出 1,容忍缺失末尾分号)
  help;                             内联帮助(远程模式含 pub/sub 命令面)
```

部署测试(`./deploy/run-tests.sh`)在一次性卷(`docsql-dev-testdata-*`)上同时验证两个
profile,不触碰开发数据卷(`docsql-dev-data-*`)与控制台账号卷,结束后恢复运行前的
stack;接口细节见[客户端与驱动](drivers.md#clidocsql-cli)。
