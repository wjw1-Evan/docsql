# 安全指南

身份、访问控制、审计与传输安全。等级保护第三级逐项对照见 [README 安全章节](../README.md#安全对照等保-20--gbt-20273-数据库管理系统安全技术要求)。

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
锁定事件写审计日志。Web 控制台登录门同策略;`DOCSQL_WEB_TRUST_PROXY=1` 时按 X-Forwarded-For 分桶。

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
  改密码踢掉其它全部在线会话;`node` 参数受 `DOCSQL_PEERS` 白名单约束(SSRF 防护)。

## 审计

- 语句审计:语句文本(密码脱敏、显示截断不影响执行)/耗时/影响行数/是否复制/错误;`DOCSQL_LOG_FILE` JSONL 落盘;
- 认证事件:成功与失败均记录(来源 IP + 授予身份/失败原因),锁定事件单独标记;
- 慢查询:`DOCSQL_SLOW_MS` 阈值写 stderr;
- 环形缓冲在内存(重启丢失),长期留存必须配 `DOCSQL_LOG_FILE`。

## 漏洞报告

不要走公开 Issue——见 [SECURITY.md](../SECURITY.md)。
