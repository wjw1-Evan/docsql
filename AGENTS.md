# AGENTS.md

面向 AI 编码代理的工作指南。用户侧功能见 [README.md](README.md),架构细节与模块地图见 [DEVELOPMENT.md](DEVELOPMENT.md)。

## 项目是什么

docsql:Rust 实现的原生多模数据库(文档式存储 + 完整 SQL + KV 命令/发布订阅 + Web 控制台 + EF Core 提供程序 + 对等集群复制与分片)。约 1.8 万行 Rust + .NET 客户端栈。

**运行环境仅 Docker**:镜像由 GitHub Actions 自动构建发布到 `ghcr.io/wjw1-evan/docsql`;文档与部署不再提供本地二进制/cargo run 运行方式(cargo 仅用于开发测试)。

## 常用命令

```bash
# 构建与测试(开发门禁,非运行方式)
cargo build --workspace
cargo test --workspace              # Rust 全量(约 160 用例)

# .NET 测试(需先 cargo build 出 server 二进制)
cd dotnet && dotnet test

# 运行(仅 Docker;镜像来自 GHCR,由 CI 自动发布)
# 单节点部署(compose single profile:独立节点 :17600 + web :17710)
cd deploy && docker compose -f docker-compose.prod.yml --profile single up -d
docker exec -it docsql-prod-single docsql-cli connect 127.0.0.1:7600    # SQL/KV shell

# 多节点部署(compose cluster profile:3 节点对等集群 + web)
cd deploy && docker compose -f docker-compose.prod.yml --profile cluster up -d   # 生产(GHCR 镜像 + .env)
cd deploy && docker compose --profile cluster up -d --build   # 本地开发集群(源码构建镜像 :local)
# 本地开发与生产同端口互斥;停止清理带相同 profile 参数:--profile single --profile cluster down -v

# 部署测试(改复制/分片/部署逻辑后必跑;用本地开发 compose 文件):
# 默认构建 :local 镜像(内置 cargo test 门禁);传 DOCSQL_IMAGE_TAG 复用已有镜像
./deploy/run-tests.sh            # 同时拉起 single + cluster 两个 profile:多节点 24 项 + 单节点 18 项
```

## 提交门禁(全部通过才能提交)

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

改动 dotnet 或协议相关时,加跑 `cd dotnet && dotnet test`。

CI(`.github/workflows/docker-image.yml`,push/PR 触发)执行同样三门禁 + dotnet 测试,通过后构建多架构镜像(amd64/arm64,`RUN_TESTS=false`)发布到 `ghcr.io/wjw1-evan/docsql`;main 分支另跑 compose 部署测试(多节点 24 项 + 单节点 18 项)。

## 仓库结构

| 路径 | 职责 |
|---|---|
| `crates/docsql-core/` | 存储引擎(pager/WAL/B+树/heap)+ SQL 解析执行 + 协议帧;无网络依赖,可嵌入式 |
| `crates/docsql-kv/` | KV/集合命令、TTL、`_kv` 系统表、发布订阅总线 |
| `crates/docsql-server/` | TCP 服务器、token 鉴权、复制、16384 哈希槽分片路由 |
| `crates/docsql-cli/` | 嵌入式 + 远程 shell |
| `crates/docsql-web/` | REST API + 内嵌单页 UI(console.html) |
| `dotnet/` | Docsql.Client(ADO.NET)、Docsql.EntityFrameworkCore、xUnit 测试、示例 |
| `deploy/` | `docker-compose.yml`(本地开发,源码构建)、`docker-compose.prod.yml`(生产,GHCR 镜像 + `.env`);均含 `single`/`cluster` 两个 profile;测试脚本 `single-test.sh`(18 项)与 `multinode-test.sh`(24 项) |
| `.github/workflows/` | CI:测试门禁 → 构建多架构镜像发布 GHCR → 部署测试 |

## 红线与已知坑

1. **索引键序依赖 `core/encode.rs` 的可比较有序编码**——新增值类型必须同步扩展编码,否则索引序被破坏。
2. **`docsql-web/src/console.html` 通过 `include_str!` 内嵌**——改 UI 后必须重新 `cargo build` 才生效。
3. **deploy 有兼容性钉子**——`deploy/multinode-test.sh` 断言固定的 REST/协议接口;改协议或 REST 字段前先同步该脚本与 dotnet 客户端。
4. **PK ≠ NOT NULL**:主键当前不隐含 NOT NULL,与主流数据库不同;动约束逻辑需全量回归约束测试。
5. **部分不支持的 SQL 会被静默忽略而非报错**(如 `WITH`/CTAS/`ON CONFLICT`)。清理时应在解析器层显式报错,不要继续静默吞掉。
6. **SQL 与 KV 共用同一会话事务系统**(`BEGIN/COMMIT` ≡ `MULTI/EXEC`);`_kv` 的 JSON 形状有测试钉住,改存储格式需同步测试。
7. **dotnet 的 bin/obj 不入库**(已在 .gitignore);新建 dotnet 项目注意沿用。
8. 重部署 Docker 集群先带 profile 参数 `docker compose --profile single --profile cluster down -v` 清卷,避免旧状态干扰测试(所有服务都在 profile 内,不带参数的 down 不会清理)。

## 工作流约定

- 小步提交直接在 `main`;提交信息风格见 git log(如 `M16: ...`、`Engine milestones: ...`),里程碑式概括。
- 完整流程:过提交门禁 → 相关专项测试 → **最后一步提交并推送源码**;push 即触发 CI(门禁 + dotnet 测试 → 多架构镜像发布 GHCR → main 分支部署测试)。
- 改动跨复制/分片:本地 e2e 之外必须跑 `./deploy/run-tests.sh`。
- 改动 EF 相关(SAVEPOINT 语义敏感):跑两个 dotnet 测试套件验证。
- 文档分工:用户可见行为 → README;开发向内容 → DEVELOPMENT.md;本文件只维护代理工作所需的命令与红线。
- 本机到 github.com:443 间歇阻断,推送失败用 `git -c http.version=HTTP/1.1 push` 重试。
- 环境:macOS/arm64;Docker 构建基于 mcr.microsoft.com/azurelinux(docker.io 不可达)。

## 运行时环境变量

- `DOCSQL_TOKEN`:web/server 认证 token。
- `DOCSQL_PEERS`:对称集群节点表。注意:当前为对称集群、无反熵追赶,新加入副本不会自动补历史数据。
- `DOCSQL_IMAGE_TAG`:compose 使用的镜像标签(默认 `latest`;本地测试用 `local`/`ci`)。
