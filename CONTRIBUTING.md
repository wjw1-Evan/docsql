# 贡献指南

感谢关注 DocSQL。本文面向人类贡献者;面向 AI 编码代理的开发指南见 [AGENTS.md](AGENTS.md)。

## 行为准则

保持专业与善意。提交即表示你同意以 [MIT OR Apache-2.0](LICENSE-MIT) 双许可发布你的贡献。

## 报告问题

- **安全漏洞**不要走公开 Issue,见 [SECURITY.md](SECURITY.md);
- 功能缺陷请附:版本/镜像 tag、最小复现(SQL 脚本或复现步骤)、期望与实际行为;
- 集群类问题请附部署形态(单机/集群/主从)与各节点日志关键行。

## 开发环境

服务端与集群部署推荐 Docker;本地开发用本机工具链(cargo / dotnet),嵌入式
SQLite 式使用(库/CLI)完全不需要 Docker。

```bash
# 提交门禁(全部通过才能提交)
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# 改 dotnet 或协议时(cargo build 先行)
cargo build -p docsql-server
cd dotnet && dotnet test

# 改复制/部署逻辑后必跑
./deploy/run-tests.sh

# 本地起开发集群
cd deploy && docker compose --profile cluster up -d --build
```

compose 必须带 profile(不带 = 空操作);数据卷 external,`down -v` 不清数据。

## 提交约定

- 小步提交,直接在 `main`;提交信息用里程碑式概括,说明**动机与机制变化**而非流水账;
- 只暂存本次改动涉及的文件,不要把工作区其它在途修改一并提交;
- 用户可见行为改 README,开发约束/红线改 AGENTS.md;
- 改协议、REST 字段时必须同步 `deploy/multinode-test.sh` 断言与 dotnet 客户端。

## 发布流程

三条发布通道,**全部由 `v*` tag 驱动**(tag 即发布):

| 通道 | 产物 | 触发 |
|---|---|---|
| GHCR 镜像(`docker-image.yml`) | 多架构容器镜像(dev/prod compose 消费) | v* tag 推送 + main 分支 CI |
| GitHub Releases 二进制(`release-binaries.yml`) | 各平台 tar.gz(`docsql-server/cli/web`)+ SHA256SUMS | v* tag 推送(手动 dispatch 只产 artifact) |
| NuGet 包(`nuget-publish.yml`) | 四个 .NET 包 → GitHub Packages | v* tag 推送(或手动) |

**crates.io(Rust 库 `docsql-core`)是唯一人工步骤**:注册 crates.io 账号后执行

```bash
cargo login                     # 一次性:crates.io 个人 API token
cargo publish --dry-run -p docsql-core   # 验证打包面(examples 会在包内编译)
cargo publish -p docsql-core
```

- `docsql-server` / `docsql-cli` / `docsql-web` 已标 `publish = false`,不会被 `publish --all`
  误发(二进制产品走 Releases);`docsql-core` 版本单独钉在 `crates/docsql-core/Cargo.toml`,
  发版时随 CHANGELOG/dotnet 包版本对齐与推进;
- 首次发布后 README/Cargo 的 `docsql-core = "x.y"` 版本引用即生效,发布前用
  `docsql-core = { git = "https://github.com/wjw1-Evan/docsql" }` 引用 main。

## 代码约定(要点)

- 生产代码不启动子进程;一切落盘经 pager(先 WAL 后数据页);
- 新增值类型必须同时扩展编码与 `Value::cmp_values`;
- 不支持的 SQL 显式报错,禁止静默吞掉;
- 系统表只读;用户/角色存储表的复制与显示语义见 AGENTS.md 第 10a 条。

完整机制约束与红线清单见 [AGENTS.md](AGENTS.md) —— 提交前请通读。
