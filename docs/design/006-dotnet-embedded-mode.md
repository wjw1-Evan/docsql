# 设计说明:.NET 嵌入式模式(零守护进程的 SQLite 式体验)

状态:**评估稿(未实现;短期交付 = 文档与本地 server 组合)**。回应方向:「数据应能像
SQLite 一样快速在项目里用,不依赖 Docker」。Rust 侧与 CLI 侧的嵌入式已交付
(`docsql-core` 库 / `docsql-cli <file.db>`,见 README「无 Docker 快速开始」);
本文只回答 **.NET 栈如何达到同等体验**,给出方案矩阵、红线核对与推荐路线。

## 0. 现状(精确事实)

- .NET 侧只有网络客户端:`Docsql.Client`(ADO.NET)+ `Docsql.EntityFrameworkCore` +
  Aspire 三件套,连接串 `host=host;port=port` —— **没有进程内路径**;
- 引擎是 Rust(`docsql-core`,同步单写者 + WAL,无 tokio/网络依赖)。进程内单写者
  模型与「一个文件归属一个进程」的家规已经落进文档(limitations「单数据文件单进程
  归属」);
- 本仓库红线(AGENTS.md「Mimosa 安全门禁」节):**生产代码不启动子进程**;Mimosa
  静态扫描对任何进程启动无差别报 high「命令注入」。当前 C# 只有测试在
  `FindServer`/`StartServer` 形态起 `docsql-server`(已知误报白名单);
- SQLite 在 .NET 的既有先例:`Microsoft.Data.Sqlite` 走 `sqlite3.dll` P/Invoke ——
  「native 库 + NuGet runtimes 资产包」是成熟分发模式,不经过任何子进程。

## 1. 目标语义

```csharp
// 理想终态(方案 B 落地后):
// builder.Services.AddDbContext<TodoDb>(o => o.UseDocsql("Data Source=todos.db"));
//   → 无 server、无端口、无容器;文件就是数据库。多连接并发受引擎单写者纪律约束(进程内)。
```

同时坚守:**嵌入式不提供复制/集群**(那是另一种部署形态,由 server 形态承担);
嵌入式更不提供 pub/sub(依赖网络协议)。嵌入式 = 引擎 + 单文件库 + 单进程。

## 2. 方案矩阵

| | A. 驱动内自动拉起本地 `docsql-server` 子进程 | B. FFI:C ABI 包装 `docsql-core` 进 .NET 进程 | C. 维持现状(本地 server 手动一行) |
|---|---|---|---|
| 体验 | 连接串像嵌入(`Data Source=app.db`) | 真 SQLite 体验(同上) | `./docsql-server app.db` + `host=127.0.0.1` |
| 进程/端口 | 多一个子进程 + 回环端口 | 零 | 用户自起的一个进程 |
| 红线(Mimosa) | **直接违反**「生产代码不启动子进程」;静态扫描逐处 high「命令注入」 | 不涉及进程启动;(P/Invoke 语境:红线禁的是**用反射/P-Invoke 技巧绕过扫描**,不是禁止真实 FFI 库) | 无 |
| 安全面 | 自动开监听(即使回环)扩大面;生命周期(孤儿进程/端口泄漏/谁关谁)复杂 | 无网络监听;崩溃域 = 整个 .NET 进程(SQLite for .NET 同款接受度) | 显式、可控 |
| 工程量 | 中(驱动 + 三平台验证) | 大(`docsql-core-ffi` crate + C ABI + NuGet runtimes 包 + 版本同步纪律) | 零(已交付) |
| 崩溃域 | 独立进程,崩溃可观察 | native 崩溃带走宿主进程 | 独立进程 |
| 结论 | **不推荐**:需 owner 显式重定红线才可立项 | **推荐路线**(分阶段) | 短期交付 |

## 3. 方案 B 分阶段路线

1. **短期(已交付)**:文档定调「本地 server 一行启动」(README 无 Docker 1c +
   operations「无容器运行」);.NET 侧无代码改动。
2. **阶段 0 · C ABI 设计稿**:`docsql-core-ffi` crate 草案,最小面
   `docsql_open(path) / execute / prepare / step / close`,错误以错误码 + UTF-8 文本
   返回,结果集游标前进式(不回调 —— 回调跨 FFI 与 GC/异常面复杂度不成比例);
   引擎同步单写者纪律不变;适配 .NET 侧多连接共存:读走进程内 ReadView 快照,写全库互斥不变。
3. **阶段 1 · NuGet 双包**:`Docsql.Client.Native`(管理面,引用默认向后兼容) +
   `runtime-*` 资产包(linux-x64 / linux-arm64 / osx-arm64 / win-x64),`Microsoft.Data.Sqlite`
   同款 `.targets` 复制语义;连接串开关 `mode=embedded` 才启用(不静默分叉);
   EF 与 ADO.NET 复用现有对象模型(传输后端替换为进程内引擎,不再拨号)。
4. **阶段 2 · 体验收口**:`:memory:` 路径、测试与 CI(三平台矩阵并入
   release-binaries 产出 `.dll/.so/.dylib`;dotnet 套件增加嵌入式连接串用例)。

## 4. 红线核对表(改代码前再看一遍)

- 方案 B **不触碰** Mimosa 红线:核心红线针对进程启动/命令注入/用户输入流入 shell;
  P/Invoke 消费稳定 ABI 同 `Microsoft.Data.Sqlite` 模式,但**不要**用反射/P-Invoke
  去「绕过」任何扫描 —— ABI 面单独成 crate,全部函数签名进文档;
- FFI 层不得引入 tokio/runtime 线程池(引擎同步;"async 包装"在 .NET 侧做);
- 错误传播:任何 panic 必须在 C ABI 边界 `catch_unwind` 收敛为错误码(panic 跨
  FFI = UB);损坏数据文件一律走引擎既有"响亮报错"路径(link AGENTS 第 20 条
  「损坏的数据文件只许响亮报错,不许 panic」);
- 嵌入式连接串 `Data Source=…` 与网络串 `host=…` 双模并存时,错误信息必须能
  自证模式(分叉路径可诊断);
- 版本纪律:native 资产包与 `Docsql.Client` 同 tag 发布、版本一锁 —— 复制语义上
  双向解析旧目录的新引擎已保证旧卷兼容(引擎目录编解码向后兼容是其既有承诺)。

## 5. 明确的非目标

- 不在嵌入式形态里做复制/集群/快照修复(server 形态职能);
- 不做「驱动自动拉起 server」(方案 A):**需 owner 显式重定「生产代码不启动子进程」
  红线才可立项**,在那之前文档与 CI 一律不引入生产路径的 Process.Start;
- 不做跨进程文件锁:单写者模型不变,同进程多句柄受支持(引擎既有形态),跨进程
  打开仍由「文件单进程归属」家规约束(限制条目不变),FFI 层不新增锁机制。
