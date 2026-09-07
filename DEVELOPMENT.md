# docsql 开发文档

面向贡献者的开发指南:架构总览、模块地图、构建与测试门禁、关键内部机制、已知坑与开发流程。用户侧功能介绍见 [README.md](README.md)。

## 1. 项目概览

docsql 是一个原生多模数据库,用 Rust 实现:

- **文档式存储**:JSON 文档整体存储,无强制 schema
- **完整 SQL**:DDL/DML/JOIN/聚合/事务/约束/系统视图
- **KV 命令**:字符串/列表/哈希/集合/有序集合 + MULTI/EXEC + TTL
- **发布订阅**:SUBSCRIBE/PUBLISH,服务端主动推送
- **Web 管理控制台**:docsql Studio(SSMS 风格)
- **.NET 生态**:ADO.NET 提供程序 + EF Core 提供程序
- **集群**:主从复制、只读副本、PROMOTE 故障转移、16384 哈希槽分片
- **运行环境**:仅 Docker(镜像由 GitHub Actions 自动构建发布到 GHCR)

代码规模约 1.8 万行 Rust + .NET 客户端栈。

## 2. 仓库结构

```
Cargo.toml              # workspace
crates/
  docsql-core/          # 存储引擎 + SQL 内核(无网络依赖,可嵌入式)
  docsql-kv/            # KV/集合命令、发布订阅总线(依赖 core)
  docsql-server/        # TCP 服务器、复制、分片路由、鉴权
  docsql-cli/           # 嵌入式 shell 与远程客户端
  docsql-web/           # Web 控制台(REST API + 内嵌单页 UI)
dotnet/                 # Docsql.Client(ADO.NET)、Docsql.EntityFrameworkCore、测试与示例
deploy/                 # docker-compose.yml(本地开发,源码构建)、docker-compose.prod.yml(生产,GHCR 镜像)、多节点测试脚本
.github/workflows/      # CI:cargo/dotnet 测试 → 构建多架构镜像发布 GHCR → 部署测试
target/                 # 构建产物(git 忽略)
```

### 2.1 模块地图

**docsql-core**(存储 + SQL 内核,自底向上):

| 文件 | 职责 |
|---|---|
| `value.rs` / `encode.rs` | 值类型与磁盘编码(可比较的有序编码,索引键序依赖它) |
| `pager.rs` | 手写分页器,页面读写与文件布局 |
| `wal.rs` | WAL 日志与崩溃恢复(M1 起支撑) |
| `heap.rs` | 堆文件:表的文档存取 |
| `btree.rs` | 页式 B+ 树索引(M4/M16 性能里程碑:索引接入执行器、单事务提交、原地更新/删除) |
| `json.rs` | JSON 文档模型与编解码 |
| `engine.rs` | SQL 执行器:解析后的 AST → 计划 → 执行;约束、事务、SAVEPOINT、RETURNING、DISTINCT、外键检查、批量执行 |
| `proto.rs` | 自定义二进制协议 v1 帧编解码(预留拓扑版本/重定向字段) |
| `lib.rs` | AST/解析器入口 |

**docsql-kv**:

| 文件 | 职责 |
|---|---|
| `lib.rs` | KV 字符串命令、TTL(惰性 + 定期清理)、`_kv` 系统表与 SQL 双向互通 |
| `collections.rs` | LIST/HASH/SET/ZSET 命令(集合值以 JSON 形态存于 `_kv`) |
| `pubsub.rs` | 发布订阅总线 |

**docsql-server**:

| 文件 | 职责 |
|---|---|
| `lib.rs` / `main.rs` | tokio TCP 服务器:SQL/KV 会话、token 鉴权、副本协议、PROMOTE |
| `shard.rs` | 16384 哈希槽分片路由(SQL 键与 KV 键统一路由) |
| `kvproto.rs` | KV 命令服务端分发 |
| `crypto.rs` / `querylog.rs` | 鉴权辅助与查询日志 |

**docsql-web**:

| 文件 | 职责 |
|---|---|
| `lib.rs` / `main.rs` | REST API(`/api/sql` `/api/parse` `/api/meta` `/api/keys` `/api/kvkey` `/api/kv` `/api/stats` `/api/publish` `/api/events`)、`DOCSQL_TOKEN` 认证 |
| `console.html` | docsql Studio 单页 UI。**注意:通过 `include_str!` 内嵌进二进制,改完必须重新 `cargo build` 才生效** |

**dotnet/**:

| 项目 | 职责 |
|---|---|
| `Docsql.Client` | ADO.NET 提供程序(`DocsqlConnection/Command/DataReader` 等),支持 key 认证 |
| `Docsql.EntityFrameworkCore` | EF Core 提供程序:复用 SQLite 管线;`UseDocsql(connectionString)`;惰性建表/索引同步默认开启(`AutoCreate`/`SchemaSync`/`Infrastructure/`) |
| `Docsql.Client.Tests` / `Docsql.EntityFrameworkCore.Tests` | xUnit 套件 |
| `Docsql.EfSample` | 一键端到端示例 |

## 3. 构建与运行

**运行环境统一为 Docker**:镜像由 GitHub Actions 自动构建并发布到 `ghcr.io/wjw1-evan/docsql`(`.github/workflows/docker-image.yml`,push 到 `main` / `v*` 标签触发;`latest`、`main`、`v1.2.3`、`sha-*` 等标签)。

```bash
# 单节点部署(compose `single` profile:独立节点 :17600 + web 控制台 :17710)
cd deploy && docker compose --profile single up -d --build         # 本地开发(源码构建,:local)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d   # 生产(GHCR 镜像)

# 多节点部署(compose `cluster` profile:3 节点对等集群 :17601-17603 + web :17700)
cd deploy && docker compose --profile cluster up -d --build        # 本地开发
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d  # 生产(配置见 .env.example)

# 停止与清理(带相同 profile 参数)
cd deploy && docker compose --profile single --profile cluster down -v

# 本地改动后的部署验证:本地构建镜像(内置全量 cargo test 门禁)+ 42 项部署测试
./deploy/run-tests.sh            # 多节点 24 项 + 单节点 18 项;或 DOCSQL_IMAGE_TAG=<已有tag> 跳过构建
```

开发门禁(仅测试,不作为产品运行方式):

```bash
cargo build --workspace
cargo test --workspace
cd dotnet && dotnet test         # 需先 cargo build 出 server 二进制
```

环境变量:`DOCSQL_TOKEN`(web/server 认证)、`DOCSQL_PEERS`(对称集群节点表;注意当前为对称集群、无反熵追赶,新加入副本不会自动补历史数据)、`DOCSQL_IMAGE_TAG`(compose 使用的镜像标签)。

## 4. 测试与门禁

提交前必须全绿:

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

测试层次(约 160 个 Rust 用例):

- **单元/内核**:core 的 pager/WAL/B+树/engine 各模块内测试
- **SQL 集成 / KV 语义 / 协议**:各 crate tests
- **端到端**:`crates/docsql-server/tests/e2e.rs`
- **多节点部署**:Docker compose 内 24 项测试(CI 对 main 分支在镜像发布后执行;本地跑 `./deploy/run-tests.sh` 会先构建带测试门禁的本地镜像),改动部署/复制相关逻辑后必跑;重部署用 `docker compose down -v` 清卷后再 up,避免旧状态干扰
- **.NET**:`dotnet test`(ADO.NET 合规 + EF Core CRUD/LINQ/Include)
- **CI**(`.github/workflows/docker-image.yml`,push/PR 触发):上述三门禁 + `dotnet test` → 构建多架构镜像(amd64/arm64,`RUN_TESTS=false` 因测试已原生跑过)发布到 `ghcr.io/wjw1-evan/docsql` → main 分支另加载 amd64 镜像跑同一套部署测试

注意:`deploy` 相关 API 有兼容性钉子(部署测试脚本断言固定接口),改 REST/协议字段前先同步 `deploy/multinode-test.sh`。

## 5. 关键内部机制

### 5.1 存储:页面 + WAL + B+ 树

- 一切落盘经由 `pager`;WAL 先写日志再应用页面,启动时恢复
- 索引键序依赖 `encode.rs` 的可比较有序编码——**新增值类型必须同步扩展编码,否则索引序被破坏**
- M16 之后:索引接入执行器(点查走索引探针)、UPDATE/DELETE 走快速路径原地更新、单事务一次提交

### 5.2 SQL 与 KV 统一事务

- KV 的 `MULTI/EXEC/DISCARD` 与 SQL 的 `BEGIN/COMMIT/ROLLBACK` 共用同一会话事务系统;SQL 侧另有 `SAVEPOINT`(`SAVEPOINT/ROLLBACK TO/RELEASE`)
- KV 数据本质存于 `_kv` 系统表(SQL 可直接查),集合类型(LIST/HASH/SET/ZSET)以 JSON 形态存储,`_kv` 查询结果的 JSON 形状是有测试钉住的
- 会话事务内执行写操作时注意复制路径:写转发发生在主节点提交时

### 5.3 网络、复制与分片

- 自定义二进制协议 v1(`core/proto.rs`),帧头预留拓扑版本/重定向字段
- 主从复制为**写转发**:副本接受读,写转发给主;`PROMOTE` 提升副本为主
- 分片:16384 哈希槽,SQL 表键与 KV 键统一按槽路由,跨分片数据隔离;事务不跨分片
- 鉴权:token;KV 写复制与 .NET 客户端均带 key 认证

### 5.4 EF Core 提供程序

- 策略是**复用 SQLite 管线** + Docsql ADO.NET,不重写关系生成
- `EnsureCreated`/惰性建表默认开启,模型增删表、索引(含唯一索引)自动同步,免迁移;重复 EnsureCreated 前需先 DROP 旧表
- 已知边界:SAVEPOINT 语义有限,EF 事务内 SaveChanges 依赖它——相关改动需跑两个 dotnet 测试套件验证

## 6. 已知坑与注意事项

1. **PK ≠ NOT NULL**:当前主键约束不隐含 NOT NULL 语义,与主流数据库不同,改动需全量回归约束测试
2. **静默错误的 SQL**:部分不支持的结构(如 `WITH`/CTAS/`ON CONFLICT`)会被**忽略而非报错**,这是已知债务;清理reject 列表时在解析器层显式报错,不要静默吞掉(详见 SQL 支持边界探查记录)
3. **console.html 是 `include_str!` 内嵌**:改 UI 后必须重新编译;开发循环用 Playwright 验证(对象树单击选中、双击打开数据网格)
4. **dotnet bin/obj 不入库**:已在 .gitignore,新项目注意补
5. **GitHub 推送**:本机到 github.com:443 间歇阻断,推送失败用 `git -c http.version=HTTP/1.1 push` 重试

## 7. 开发流程约定

- 分支:小步提交在 `main`;里程碑式提交信息格式见 git log(如 `M16: ...`、`Engine milestones: ...`)
- 提交前:第 4 节三门禁 + 相关 dotnet 测试
- **最后一步:提交并推送源码**(`git push`;443 间歇阻断时用 `git -c http.version=HTTP/1.1 push` 重试)——push 即触发 CI
- push 到 GitHub 后 CI 自动执行:三门禁 + dotnet 测试 → 构建多架构镜像发布 `ghcr.io/wjw1-evan/docsql` → main 分支跑部署测试;CI 失败等同门禁失败
- 改动跨复制/分片时:本地 e2e 之外必须跑 `./deploy/run-tests.sh`(先 `docker compose down -v`)
- 改协议/REST 字段:同步更新 `deploy/multinode-test.sh` 的兼容性断言与 dotnet 客户端
- 文档:用户可见行为更新 README,开发向内容更新本文件
