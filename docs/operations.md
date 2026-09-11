# 运维手册

## 部署拓扑

运行仅依赖 Docker。两套 compose 完全分离(项目名/端口/数据卷/镜像 tag 互不相同,可同时运行):

| 拓扑 | 命令 | 端口(生产) |
|---|---|---|
| 单节点 | `docker compose -f docker-compose.prod.yml --profile single up -d` | 18600 + web 18710 |
| 三节点对等集群 | `... --profile cluster up -d` | 18601-18603 + web 18700 |
| 扩容第四节点 | `... --profile cluster --profile join up -d node-d` | 18604 |

- 数据卷 external:`down -v` 不清数据;清数据唯一入口 `deploy/reset-data.sh`;
- 新节点配 `DOCSQL_PEERS` 启动即自动拉取全量快照加入;动态注册在内存,长期成员写进各节点 `DOCSQL_PEERS`;
- 节点离线期间错过的写,在**重启时**自动修复(增量追赶,超窗转快照采纳);详见 README。

## 环境变量(运维相关)

| 变量 | 默认 | 说明 |
|---|---|---|
| `DOCSQL_TOKEN` | 无 | 客户端凭据(恒为管理员身份);≥8 位且非单字符重复,违者拒绝启动 |
| `DOCSQL_READ_TOKEN` | 无 | 只读客户端凭据(可查不可写) |
| `DOCSQL_CLUSTER_TOKEN` | 无 | 节点间凭据(复制帧仅接受节点身份) |
| `DOCSQL_KEY` | 无 | 64 位 hex → AES-256-GCM 帧加密(客户端与节点间同用) |
| `DOCSQL_MAX_CONN` | 0(不限) | 并发连接上限,超限立即拒绝 |
| `DOCSQL_IDLE_TIMEOUT` | 0(不限) | 空闲会话秒数,超时主动断开 |
| `DOCSQL_STATEMENT_TIMEOUT_MS` | 0(不限) | **客户端语句墙钟预算**,超时报错回滚;复制 apply 与恢复重放不受限 |
| `DOCSQL_ASYNC_COMMIT` | 0 | 1 = 组提交(~2ms 丢失窗口;PUBLISH 仍推送前强制 fsync) |
| `DOCSQL_CATCHUP_WINDOW` | 100000 | 追赶日志保留条数(0 = 不限) |
| `DOCSQL_BACKUP_INTERVAL_SECS` / `_KEEP` / `_DIR` | 86400 / 7 / `<db>/backups` | 自动备份节奏/保留份数/目录 |
| `DOCSQL_LOG_FILE` / `DOCSQL_SLOW_MS` | 无 / 100 | 审计 JSONL 落盘 / 慢查询阈值(stderr) |
| `DOCSQL_PEERS` / `DOCSQL_ADVERTISE` / `DOCSQL_REPLICATE_TO` / `DOCSQL_READ_ONLY` | — | 集群拓扑 |

**配置快速失败**:以上数值变量非法值一律拒绝启动(exit 2),不静默回退。

## 监控

- `GET /healthz` — 无门禁存活探针,只反映控制台进程自身;
- **原生 TLS**:`DOCSQL_WEB_TLS_CERT` + `DOCSQL_WEB_TLS_KEY`(PEM 证书/私钥路径)同时设置即以
  HTTPS 服务控制台全 API 面(rustls;只设其一会拒绝启动);TLS 节点建议配
  `DOCSQL_WEB_COOKIE_SECURE=1`;未配置为明文 HTTP,生产亦可置于 TLS 反代之后;
- `GET /metrics` — Prometheus 文本。抓取认证与其它 API 一致(带 `X-Docsql-Token`):

  ```yaml
  scrape_configs:
    - job_name: docsql
      basic_auth: {}   # 或自定义 header;header 方式见采集器配置
      params: {}
      static_configs:
        - targets: ["web:18710"]
  ```

  指标族(节点标签 `node`):`docsql_node_up`、`docsql_node_info`、`docsql_uptime_seconds`、
  `docsql_sql_statements_total` / `docsql_sql_statement_errors_total`、`docsql_connections_total/_active/_rejected_total`、
  `docsql_publishes_total`、`docsql_auth_failures_total`、`docsql_network_bytes_total`、
  `docsql_tables` / `docsql_rows`、`docsql_storage_bytes` / `docsql_wal_bytes`、
  `docsql_journal_head` / `docsql_replay_failures_total`;控制台自身 `docsql_web_http_requests_total`;
- `GET /api/stats`、`GET /api/cluster`:JSON 形式的同一数据(仪表盘/集群页数据源)。

## 健康检查与优雅停机

- compose healthcheck 用镜像自带 CLI 对节点 PING(interval 5s / retries 10);
- 节点与控制台处理 SIGTERM/SIGINT:停止接受新连接 → 存量连接限时排空(节点最长 10s)→ 进程退出;
  未收尾事务由断连回滚 + WAL 恢复兜底。`docker stop` 与编排器滚动发布安全。

## 备份与恢复

- 每节点独立自动备份:整库逻辑快照,写 `backups/backup-<UTCms>.sql` + **同名 `.sha256` 校验和 sidecar**;
- 保留 keep-N,清理连带 sidecar;
- **恢复前强校验**:sidecar 与文件不匹配(损坏/篡改)直接拒绝重放;旧备份无 sidecar 仍可恢复;
- 恢复经正常写路径逐条重放并扇出全网,完成后自动增量补拉 + 摘要收敛验证(`converged` 字段);
- 手工归档:`docker cp docsql-prod-single:/data/backups .`;恢复演练见 `deploy/single-test.sh` 第 8 章。

## CLI

```
docsql <file.db>                    嵌入模式
docsql connect <addr> [token]       远程模式(--user 走用户登录,密码走 DOCSQL_PASSWORD/交互)
  --csv | --json                    行导出格式(RFC 4180 / JSON 对象数组)
  -f script.sql                     脚本批执行(错误即退出 1,容忍缺失末尾分号)
  help;                             内联帮助(远程模式含 pub/sub 命令面)
```
