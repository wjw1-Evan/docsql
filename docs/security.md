# 安全指南

身份、访问控制、审计与传输安全。等级保护第三级逐项对照见文末[等保 2.0 对照](#等保-20-对照)。

## 身份模型

三层凭据 + 数据库内用户,匹配顺序 cluster → client → read:

| 身份 | 配置 | 能力 |
|---|---|---|
| 节点间 | `DOCSQL_CLUSTER_TOKEN` | 复制帧(`FLAG_REPLICATION`)仅接受节点身份 |
| 客户端管理员 | `DOCSQL_TOKEN` | 全部能力;控制台程序化旁路同一凭据 |
| 只读客户端 | `DOCSQL_READ_TOKEN` | SELECT + 订阅;一切持久化写协议层拒绝 |
| 数据库用户 | `CREATE USER ... PASSWORD` | 角色/表级授权(见下) |

- 所有协议凭据常数时间比较;启动时校验复杂度(≥8 位、非单字符重复,违者 exit 2);
- 存在任一数据库用户后,新建匿名连接即被拒绝(判定在连接建立时刻,运营者建号会话不会被自己锁死);
- 明文密码绝不离开执行节点:引擎把 `PASSWORD '明文'` 回写为 PBKDF2-HMAC-SHA256 哈希形式再进
  复制流/日志/备份;查询日志二次脱敏。

## 数据库用户与角色

```sql
CREATE USER analyst PASSWORD '至少8位密码';     -- 重名报错,绝不静默重置
GRANT readonly TO analyst;                      -- 内置:admin / readwrite / readonly
CREATE ROLE reporting;
GRANT SELECT, UPDATE ON orders TO reporting;    -- 表级 DML 位
REVOKE UPDATE ON orders FROM reporting;         -- 即时生效(grants 纪元逐帧刷新)
DROP USER analyst;                              -- 级联清理授权与成员关系
```

权限面:admin=全部(DDL/用户管理/备份恢复/PROMOTE);readwrite=表 DML + PUBLISH/TRIM;
readonly=SELECT;自定义角色=授予的表级 DML。读目标按 AST 分类 fail-closed(无法分类即拒)。

## 登录失败锁定

同一来源 IP 60 秒窗口内认证失败 10 次(token 与用户登录同桶)即锁定 60 秒,期间任何凭据(含正确值)均拒绝;
锁定事件写审计日志。Web 控制台登录门同策略;`DOCSQL_WEB_TRUST_PROXY=1` 时按 X-Forwarded-For 的**最后一跳**分桶(该跳是可信反代实际观测到的地址;取首跳会被追加式代理下的伪造头轮换绕过)。多级反代需在最近一层把 XFF 重写为仅客户端地址。

## SQL 注入防护(双层)

1. **驱动层**:ADO.NET/EF Core 参数在客户端转义为类型化字面量(字符串单引号强转义、二进制 hex 字面量);
2. **服务端 prepared statements**(推荐给自研驱动):`REQ_PREPARE` 提交含 `?` 占位符的模板,
   `REQ_EXECUTE` 传参数数组——**绑定在服务端完成**,引号感知(字符串字面量内的 `?` 是数据),
   字符串值单引号翻倍转义,任何取值都无法逃逸字面量;授权/超时/审计与普通语句同路径。

## 传输与静态数据

- 传输:`DOCSQL_KEY`(64 hex)启用 AES-256-GCM 帧加密(token 与数据同密);绑定非回环地址未配置 key 时启动告警;
  默认端口仅映射 127.0.0.1;Web 控制台生产部署置于 TLS 反代之后(`DOCSQL_WEB_COOKIE_SECURE=1`);
- 静态:TDE 未内置(部署于加密卷之上);备份为明文 SQL + sha256 sidecar(防损坏/篡改,不保密——保护备份卷);
- Web 控制台:首次使用强制设置账号(盐化 PBKDF2 存储,HttpOnly 会话 Cookie),
  改密码踢掉其它全部在线会话;浏览器不持有节点令牌(控制台以服务端 `DOCSQL_TOKEN`
  连接节点,该 token 也可作程序化 API 旁路);
  `node` 参数受 `DOCSQL_PEERS` 白名单约束(SSRF 防护)。

## 审计

- 语句审计:语句文本(密码脱敏、显示截断不影响执行)/耗时/影响行数/是否复制/错误;`DOCSQL_LOG_FILE` JSONL 落盘;
- 认证事件:成功与失败均记录(来源 IP + 授予身份/失败原因),锁定事件单独标记;
- 慢查询:`DOCSQL_SLOW_MS` 阈值写 stderr;
- 环形缓冲在内存(重启丢失),长期留存必须配 `DOCSQL_LOG_FILE`。

## 等保 2.0 对照

面向国内数据库安全检测(等级保护第三级 / GB/T 20273 数据库管理系统安全技术要求)的能力对照:

| 控制项 | DocSQL 实现 |
|---|---|
| 身份标识与鉴别 | 协议层 token 认证(REQ_AUTH,常数时间比较防时序侧信道);三种凭据:`DOCSQL_TOKEN`(客户端)、`DOCSQL_READ_TOKEN`(只读客户端)、`DOCSQL_CLUSTER_TOKEN`(节点间,`FLAG_REPLICATION` 复制帧仅接受节点身份);**数据库用户**(REQ_AUTH_USER,盐化 PBKDF2-HMAC-SHA256 存储,常数时间校验,未知用户等价计算防枚举),角色与表级权限随集群复制;Web 控制台账号门:首次使用强制设置用户名/密码,HttpOnly 会话 Cookie,`DOCSQL_TOKEN` 可作为程序化旁路 |
| 登录失败处理 | 同一来源 IP 在 60 秒窗口内认证失败达 10 次(阈值可按部署调严)即锁定 60 秒,期间任何 token(含正确值)均被拒绝;锁定事件写入审计日志;Web 控制台登录门同策略(按来源 IP) |
| 口令/凭据复杂度 | 服务器启动时校验所有已配置凭据:长度不足 8 或单一字符重复即拒绝启动(进程退出码 2) |
| 访问控制(最小权限) | `DOCSQL_READ_TOKEN` 只读身份:可查询、可订阅,一切持久化写在协议层拒绝;**数据库角色**:内置 `admin`/`readwrite`/`readonly` + 自定义角色表级 DML 授权(GRANT/REVOKE 即时生效,DDL 与管理操作仅 admin,读目标含子查询 fail-closed);存在任一用户后匿名连接关闭;副本模式 `DOCSQL_READ_ONLY=1` 整节点只读;Web 控制台独立 token 门禁 |
| 安全审计 | 语句审计(`docsql_log` 环形缓冲,含语句文本/耗时/影响行数/是否复制/错误)、认证事件审计(成功与失败均记录,来源 IP + 授予身份/失败原因,web 控制台日志页可见)、`DOCSQL_LOG_FILE` 可同步落 JSONL 文件留存 |
| 资源控制 | `DOCSQL_MAX_CONN` 并发连接数上限(默认 1024,超限立即拒绝不排队;0 = 不限);`DOCSQL_IDLE_TIMEOUT` 空闲会话超时(服务端主动断开,订阅客户端需定期 PING 保活);`DOCSQL_STATEMENT_TIMEOUT_MS` 客户端语句墙钟预算(超时即报错回滚;复制 apply 与恢复重放不受限,慢节点不偏离已确认写入);单帧 64MB 上限;对端 IO 预算(连接 3s/读写 10s);TCP keepalive + NODELAY(NAT/防火墙后的长会话不被静默掐断);`DOCSQL_PBKDF2_ITERATIONS` 新建凭据哈希迭代数(默认 210000;存量凭据按各自存储值校验,非法值拒绝启动) |
| 传输保密性 | `DOCSQL_KEY` AES-256-GCM 帧加密(含认证 token 与数据);每条 keyed 连接以服务端随机挑战(RESP_HELLO)开头并折入双向 GCM AAD——**跨连接重放录制的整段会话必因挑战不同而验签失败**,连接内另有单调计数器重放闸;数据文件/WAL/审计 JSONL 均以 0600 落盘(与备份同口径);绑定非回环地址且未配置 `DOCSQL_KEY` 时启动显式告警;默认端口映射仅绑定 `127.0.0.1`;**Web 控制台原生 TLS**(`DOCSQL_WEB_TLS_CERT`/`DOCSQL_WEB_TLS_KEY`,rustls),或置于 TLS 反代之后(`DOCSQL_WEB_COOKIE_SECURE=1`) |
| SQL 注入防护 | **ADO.NET/EF Core 默认服务端参数绑定**(REQ_PREPARE/REQ_EXECUTE:占位符在服务端引号感知绑定,字符串值翻倍转义,任何取值都无法逃逸字面量);服务端不拼接外部输入;系统表 `_pubsub_messages` 对 SQL 客户端隐藏 |
| 数据完整性 | WAL 先写日志后落数据、崩溃恢复;节点间复制依赖独立集群凭据防伪造 |
| 口令策略 | 数据库用户、服务 token、Web 控制台账号三处统一策略:长度 ≥8 且拒绝单字符重复;PBKDF2 迭代默认 210000(`DOCSQL_PBKDF2_ITERATIONS` 低于 10000 时启动告警;存量凭据按各自存储值校验) |
| 数据备份 | 自动定时备份(默认每日,`DOCSQL_BACKUP_INTERVAL_SECS`/`DOCSQL_BACKUP_KEEP` 可调):整库一致点逻辑快照,随数据卷持久;**每份备份带 sha256 校验和 sidecar,恢复前强校验**(损坏/被篡改的转储在重放前被拒,旧备份无 sidecar 仍可恢复);恢复为整库重放,控制台备份页可手动触发;备份成败计入审计日志。注意:备份与 `_cluster_log` 期刊包含用户口令的 PBKDF2 哈希(复制/恢复所需)——备份目录的访问边界即哈希的暴露边界,目录权限应与数据卷同口径 |

已知边界:审计环形缓冲在内存(重启丢失,需要长期留存请启用 `DOCSQL_LOG_FILE` 外发);静态数据加密(TDE)暂未内置(可部署在加密卷之上);备份为明文逻辑快照(请保护备份目录/卷)。

## 漏洞报告

不要走公开 Issue——见 [SECURITY.md](../SECURITY.md)。
