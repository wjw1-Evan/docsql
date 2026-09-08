# AGENTS.md

面向 AI 编码代理与贡献者的开发指南(原 DEVELOPMENT.md 已并入本文)。用户侧功能见 [README.md](README.md)。

## 项目是什么

docsql:Rust 实现的原生文档数据库 + .NET 客户端栈。约 1.3 万行 Rust + 约 4 千行 C#:

- **文档式存储**:JSON 文档整体存储,无强制 schema
- **完整 SQL**:DDL/DML/JOIN/聚合/事务/约束/系统视图
- **Web 管理控制台**:docsql Studio(SSMS 风格)
- **.NET 生态**:ADO.NET 提供程序 + EF Core 提供程序
- **集群**:对称集群复制(任意节点可写)、主从写转发、PROMOTE 故障转移

**运行环境仅 Docker**:镜像由 GitHub Actions 自动构建发布到 `ghcr.io/wjw1-evan/docsql`;文档与部署不再提供本地二进制/cargo run 运行方式(cargo 仅用于开发测试)。

## 常用命令

```bash
# 构建与测试(开发门禁,非运行方式)
cargo build --workspace
cargo test --workspace              # Rust 全量(约 200 用例)

# .NET 测试(需先 cargo build 出 server 二进制)
cd dotnet && dotnet test

# 运行(仅 Docker;镜像来自 GHCR,由 CI 自动发布)
# 单节点部署(compose single profile:独立节点 :17600 + web :17710)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600    # SQL shell

# 多节点部署(compose cluster profile:3 节点对等集群 + web)
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d   # 生产(GHCR 镜像 + .env)
cd deploy && docker compose --profile cluster up -d --build   # 本地开发集群(源码构建镜像 :local)
# 本地开发与生产同端口互斥;停止清理带相同 profile 参数:--profile single --profile cluster down -v

# 部署测试(改复制/部署逻辑后必跑;用本地开发 compose 文件):
# 默认构建 :local 镜像(内置 cargo test 门禁);传 DOCSQL_IMAGE_TAG 复用已有镜像
./deploy/run-tests.sh            # 同时拉起 single + cluster 两个 profile:多节点 19 项 + 单节点 14 项
```

## 提交门禁(全部通过才能提交)

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

改动 dotnet 或协议相关时,加跑 `cd dotnet && dotnet test`。

CI(`.github/workflows/docker-image.yml`,push/PR 触发)执行同样三门禁 + dotnet 测试,通过后构建多架构镜像(amd64/arm64,`RUN_TESTS=false`)发布到 `ghcr.io/wjw1-evan/docsql`;main 分支另跑 compose 部署测试(多节点 19 项 + 单节点 14 项)。

测试层次(约 200 个 Rust 用例):

- **单元/内核**:core 的 pager/WAL/B+树/engine 各模块内测试
- **SQL 集成 / 协议**:各 crate tests
- **端到端**:`crates/docsql-server/tests/e2e.rs`
- **多节点部署**:Docker compose 内 19 项测试(CI 对 main 分支在镜像发布后执行;本地 `./deploy/run-tests.sh` 会先构建带测试门禁的本地镜像),改动部署/复制相关逻辑后必跑;重部署先 `docker compose down -v` 清卷,避免旧状态干扰
- **.NET**:`dotnet test`(ADO.NET Client 34 项 + EF Core 22 项)

## 仓库结构与模块地图

```
Cargo.toml              # workspace
crates/
  docsql-core/          # 存储引擎 + SQL 内核(无网络依赖,可嵌入式)
  docsql-server/        # TCP 服务器、鉴权、复制
  docsql-cli/           # 嵌入式 shell 与远程客户端
  docsql-web/           # Web 控制台(REST API + 内嵌单页 UI)
dotnet/                 # Docsql.Client(ADO.NET)、Docsql.EntityFrameworkCore、测试与示例
deploy/                 # docker-compose.yml(本地开发,源码构建)、docker-compose.prod.yml(生产,GHCR 镜像 + .env);均含 single/cluster 两个 profile;测试脚本 single-test.sh(14 项)与 multinode-test.sh(19 项)
.github/workflows/      # CI:cargo/dotnet 测试 → 构建多架构镜像发布 GHCR → 部署测试
target/                 # 构建产物(git 忽略)
```

**docsql-core**(存储 + SQL 内核,自底向上):

| 文件 | 职责 |
|---|---|
| `value.rs` / `encode.rs` | 值类型与磁盘编码(可比较的有序编码,索引键序依赖它) |
| `pager.rs` | 手写分页器,页面读写与文件布局 |
| `wal.rs` | WAL 日志与崩溃恢复 |
| `heap.rs` | 堆文件:表的文档存取 |
| `btree.rs` | 页式 B+ 树索引(索引接入执行器、单事务提交、原地更新/删除) |
| `json.rs` | JSON 文档模型与编解码 |
| `engine.rs` | SQL 执行器:AST → 计划 → 执行;约束、事务、SAVEPOINT、RETURNING、DISTINCT、外键检查、批量执行 |
| `proto.rs` | 自定义二进制协议 v1 帧编解码(预留拓扑版本/重定向字段) |
| `lib.rs` | AST/解析器入口 |

**docsql-server**:

| 文件 | 职责 |
|---|---|
| `lib.rs` / `main.rs` | tokio TCP 服务器:SQL 会话、REQ_AUTH token 鉴权、REQ_PROMOTE 故障转移提升、副本协议 |
| `crypto.rs` / `querylog.rs` | 鉴权辅助与查询日志 |

**docsql-web**:

| 文件 | 职责 |
|---|---|
| `lib.rs` / `main.rs` | REST API(`/api/sql` `/api/parse` `/api/meta` `/api/stats`)、`DOCSQL_TOKEN` 认证 |
| `console.html` | docsql Studio 单页 UI(通过 `include_str!` 内嵌进二进制,改完必须重新 `cargo build`) |

**dotnet/**:

| 项目 | 职责 |
|---|---|
| `Docsql.Client` | ADO.NET 提供程序(`DocsqlConnection/Command/DataReader` 等),token 认证(REQ_AUTH 原始 token) |
| `Docsql.EntityFrameworkCore` | 复用 SQLite 管线 + Docsql ADO.NET;`UseDocsql(connectionString)`;惰性建表/索引同步默认开启(`AutoCreate`/`SchemaSync`/`Infrastructure/`) |
| `Docsql.Client.Tests` / `Docsql.EntityFrameworkCore.Tests` | xUnit 套件 |
| `Docsql.EfSample` | 一键端到端示例 |

## 关键内部机制

### 存储:页面 + WAL + B+ 树

- 一切落盘经由 `pager`;WAL 先写日志再应用页面,启动时恢复;事务内的页像延迟到 WAL fsync 后才落数据文件(pager `pending_writes`),动 pager 提交路径时保持该顺序
- 索引键序依赖 `encode.rs` 的可比较有序编码——**新增值类型必须同步扩展编码,否则索引序被破坏**
- 索引接入执行器(点查走索引探针)、UPDATE/DELETE 走快速路径原地更新、单事务一次提交

### 会话事务与复制缓冲

- SQL 事务 `BEGIN/COMMIT/ROLLBACK` + `SAVEPOINT`(`SAVEPOINT/ROLLBACK TO/RELEASE`);引擎是单全局事务(单写者),并发连接的 BEGIN 在服务端排队(`BEGIN_QUEUE_WAIT` 30s 上限)
- 会话事务内执行的写语句缓冲在 `tx_pending`,提交时按执行顺序转发给上游/对等节点,回滚即丢弃
- 写执行与扇出经 `write_order` 互斥串行化,保证对等节点按本节点执行顺序应用

### 网络与复制

- 自定义二进制协议 v1(`core/proto.rs`),帧头预留拓扑版本/重定向字段;请求帧:REQ_SQL、REQ_AUTH(token 认证)、REQ_PREPARE/REQ_EXECUTE/REQ_CLOSE_STMT(参数化语句)、REQ_PING、REQ_PROMOTE(故障转移提升)
- 复制两种形态:**对称集群**(`DOCSQL_PEERS` 互相扇出,任意节点可写)与**主从写转发**(`replicate_to` 指向主,副本只读);`PROMOTE` 提升副本为主
- 鉴权:token(常数时间比较);集群节点间扇出自动先认证

### EF Core 提供程序

- 策略是**复用 SQLite 管线** + Docsql ADO.NET,不重写关系生成
- `EnsureCreated`/惰性建表默认开启,模型增删表、索引(含唯一索引)自动同步,无需 Migrations;`Database.Migrate()` 显式报错并指引改用 EnsureCreated;重复 EnsureCreated 前需先 DROP 旧表
- 已知边界:SAVEPOINT 语义有限,EF 事务内 SaveChanges 依赖它——相关改动需跑两个 dotnet 测试套件验证

## 红线与已知坑

1. **索引排序不依赖编码字节序**——`core/encode.rs` 只保证往返一致;排序统一走 `Value::cmp_values`(B+树/ORDER BY/DISTINCT)。新增值类型必须同时扩展编码与 `cmp_values`,否则索引序被破坏。注意 Int(3) 与 Float(3.0) 比较相等但编码不同,DISTINCT/UNION 去重按编码字节判重,两者不会互相去重。
2. **`docsql-web/src/console.html` 通过 `include_str!` 内嵌**——改 UI 后必须重新 `cargo build` 才生效;开发循环用 Playwright 验证(对象树单击选中、双击打开数据网格)。
3. **deploy 有兼容性钉子**——`deploy/multinode-test.sh` 断言固定的 REST/协议接口;改协议或 REST 字段前先同步该脚本与 dotnet 客户端。
4. **PK ≠ NOT NULL**:主键当前不隐含 NOT NULL,与主流数据库不同;动约束逻辑需全量回归约束测试。
5. **不支持的 SQL 必须显式报错**——窗口函数(OVER)/DISTINCT ON/ON CONFLICT DO UPDATE/ON DUPLICATE KEY UPDATE/自定义 TRIM 字符集/FK 的 ON DELETE|UPDATE 动作均已显式报错;`PRAGMA` 是有意兼容垫片(接受并忽略)。新增不支持语法时在解析/执行层报错,不要静默吞掉。WITH(非递归)/CTAS/ON CONFLICT DO NOTHING|REPLACE 已支持。
6. **会话事务是单全局事务**(单写者引擎):并发连接的 BEGIN 在服务端排队等待(`BEGIN_QUEUE_WAIT` 30s 上限)而非立即报错,dotnet 端事务错误如实上抛——并发 EF SaveChanges 依赖该排队,别改成直接报错或吞错。
7. **dotnet 的 bin/obj 不入库**(已在 .gitignore);新建 dotnet 项目注意沿用。
8. 重部署 Docker 集群先带 profile 参数 `docker compose --profile single --profile cluster down -v` 清卷,避免旧状态干扰测试(所有服务都在 profile 内,不带参数的 down 不会清理)。

## 工作流约定

- 小步提交直接在 `main`;提交信息风格见 git log(如 `M16: ...`、`Engine milestones: ...`),里程碑式概括。
- 完整流程:过提交门禁 → 相关专项测试 → **最后一步提交并推送源码**;push 即触发 CI(门禁 + dotnet 测试 → 多架构镜像发布 GHCR → main 分支部署测试);CI 失败等同门禁失败。
- 改动跨复制:本地 e2e 之外必须跑 `./deploy/run-tests.sh`。
- 改动 EF 相关(SAVEPOINT 语义敏感):跑两个 dotnet 测试套件验证。
- 文档分工:用户可见行为 → README;开发向内容与代理工作所需的命令、红线 → 本文件。
- 本机到 github.com:443 间歇阻断,推送失败用 `git -c http.version=HTTP/1.1 push` 重试。
- 环境:macOS/arm64;Docker 构建基于 mcr.microsoft.com/azurelinux(docker.io 不可达)。

## 运行时环境变量

- `DOCSQL_TOKEN`:server(协议端口,REQ_AUTH)与 web 控制台共用的认证 token;集群节点复制扇出会自动先认证。
- `DOCSQL_PEERS`:对称集群节点表。注意:当前为对称集群、无反熵追赶,新加入副本不会自动补历史数据;指向自身的条目会在启动时被忽略(防双写)。PROMOTE(故障转移提升)走 REQ_PROMOTE 帧。
- `DOCSQL_IMAGE_TAG`:compose 使用的镜像标签(默认 `latest`;本地测试用 `local`/`ci`)。
- compose 发布端口默认绑定 `127.0.0.1`(无认证部署不暴露到网络);对外服务需改端口映射并设置 `DOCSQL_TOKEN`。
