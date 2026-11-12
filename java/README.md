# DocSQL Java SDK(JDBC 驱动)

零第三方依赖的 Java 8+ JDBC 驱动:TLS 与 AES-256-GCM 帧加密走 JDK 自带库(javax.net.ssl / javax.crypto),JSON 编解码手写(约 300 行,`io.docsql.Json`),构建与测试零 Maven/Gradle 依赖(javac + jar)。

## 快速开始

`build.sh` 一次产出 `build/docsql-jdbc-0.9.0.jar`(约 72KB):

    ./build.sh

用法(DriverManager 自动发现,或手动 `Class.forName("io.docsql.jdbc.Driver")`):

    String url = "jdbc:docsql://127.0.0.1:7600?token=DOCSQL_TOKEN值";
    try (Connection c = DriverManager.getConnection(url)) {
        try (Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT 6 * 7");
            rs.next();
            assert rs.getInt(1) == 42;
        }
        // 服务端绑定预编译(qmark):值永远进不了字面量
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO t VALUES (?, ?, ?)")) {
            ps.setLong(1, 1);
            ps.setString(2, "it's safe");
            ps.setBigDecimal(3, new BigDecimal("19.99"));
            ps.executeUpdate();
        }
    }

连接参数(URL query,或 `Driver.getPropertyInfo` / Properties 同名键):

| 参数 | 说明 |
|---|---|
| `token` | 节点客户端 token(REQ_AUTH) |
| `user` / `password` | 数据库用户登录(REQ_AUTH_USER,两者同给时优先于 token) |
| `tls` | `true` 启用原生 TLS(server 侧需 `DOCSQL_TLS_CERT/KEY`) |
| `tlsCa` | CA 证书 PEM 路径(验证服务器证书 + 主机名);缺省 = 仅加密不验证(自签名集群) |
| `tlsHostname` | 按证书 DNS 名拨 IP 时的 SNI/校验名覆盖 |
| `key` | `DOCSQL_KEY` 64 位 hex,AES-256-GCM 帧加密(RESP_HELLO 挑战绑定 AAD) |
| `timeout` | 连接与每操作超时秒数(默认 15;超时毒化连接,须重连) |
| `autocommit` | 默认 `true`;`false` = 块首自动 BEGIN,`commit()/rollback()` 结束块 |

超时报错后连接已毒化:server 仍会完成在途语句并写回应答,复用 socket 会错位一帧(Python/.NET 同纪律),捕获后重建连接即可。

## 原生 API(io.docsql 包)

JDBC 层之下是同一份协议核心,也可直接用:

    ConnectConfig cfg = ConnectConfig.builder()
            .host("127.0.0.1").port(7600)
            .token("...").tls(true)         // 或 .key(hex)
            .build();
    try (DocsqlConnection c = new DocsqlConnection(cfg)) {
        Result r = c.execute("SELECT 1");
        long[] pub = c.publish("news", "hello");   // {id, receivers}
        Map<String, Object> st = c.status();
    }
    // pub/sub:专用连接 + 专职读线程,断线自动重订、按最后 id 续传
    Subscriber sub = new Subscriber(cfg, msg -> System.out.println(msg.get("channel")));
    sub.subscribe("news", "earliest");   // from: latest / earliest / 具体id
    ...
    sub.close();

## 构建 / 测试

    ./build.sh          # 编译 + jar + 81 个单元测试(无需 server)
    ./run-tests.sh      # 上一步 + 66 个 e2e(自动 cargo build 起真实 server,
                        #   平文/TLS/DOCSQL_KEY/匿名 四种形态;DOCSQL_SKIP_CARGO_BUILD=1 跳过 cargo)

零依赖直构:JDK 查找顺序 `JAVA_HOME` → macOS `/usr/libexec/java_home` → 常见路径
(`/tmp/docsql-jdk/jdk-*`、Homebrew openjdk@17、/Library/Java)。JDK 17 编译,`--release 8` 字节码。

## 范围与限制

- JDBC 面:Statement/PreparedStatement/ResultSet/ResultSetMetaData/DatabaseMetaData(getTables/getColumns 走 sqlite_master 真查询)、事务 + SAVEPOINT、`setAutoCommit(true)` 提交未决块(JDBC 语义)。
- 列类型推断自首行非空值(线协议只回列名),空结果退化为 `Types.OTHER/"JSON"`。
- 不支持:`CallableStatement`、`getGeneratedKeys`(线协议无此能力)、流/LOB 定位器、语句取消、result-set 更新。
- 嵌入式(`Data Source=`)是 .NET 独有能力(FFI 原生库),Java 侧无进程内引擎。
- CI 暂未包含 Java 套件(与 Python 驱动一致,本地门禁运行)。
