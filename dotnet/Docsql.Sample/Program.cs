// DocSQL .NET 数据操作实例(ADO.NET:Docsql.Client)。
//
// 与 Docsql.EfSample(EF Core 视角)互补:本示例用原生 ADO.NET 完整走一遍
// DocSQL 的数据操作面 —— CRUD、参数化与 CLR 类型、GUID 时序主键、事务与
// SAVEPOINT、RETURNING、JOIN/聚合/子查询、文档式无 schema 存储、DataReader
// 用法、持久化 pub/sub、错误处理与异步 API。
//
// 运行前置:一个已启动的 DocSQL 节点(本示例只做客户端,不拉起进程):
//
//   cd deploy && docker compose -f docker-compose.yml --profile cluster up -d
//   dotnet run --project dotnet/Docsql.Sample -- 127.0.0.1:17601
//
// 也可以用环境变量:DOCSQL_CONN=host=..;port=..;token=.. 或
// DOCSQL_HOST / DOCSQL_PORT / DOCSQL_TOKEN,然后直接 dotnet run。

using System.Collections.Concurrent;
using Docsql.Client;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var cs = BuildConnectionString(args);
        Console.WriteLine($"DocSQL .NET 数据操作实例 → {Describe(cs)}");
        Console.WriteLine(new string('─', 60));

        try
        {
            await CleanupAsync(cs);
            await BasicCrudAsync(cs);
            ParametersAndClrTypes(cs);
            GuidTimeOrderedKeys(cs);
            TransactionsAndSavepoints(cs);
            ReturningInsertedIds(cs);
            QuerySurface(cs);
            DocumentModel(cs);
            ReaderTechniques(cs);
            Pubsub(cs);
            ErrorHandlingAndAsyncApi(cs);
        }
        catch (DocsqlException ex)
        {
            Console.WriteLine($"\n❌ 服务端报错: {ex.Message}");
            return 1;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ 无法连接或执行: {ex.Message}");
            Console.WriteLine("   提示:本地开发集群是 127.0.0.1:17601-17603(deploy compose),");
            Console.WriteLine("   生产集群是 127.0.0.1:18601-18603;容器内默认 127.0.0.1:7600。");
            return 1;
        }

        Console.WriteLine("\n✅ 全部示例执行完毕(表留在节点上,可自行查看,重跑自动清理)");
        return 0;
    }

    // ---- 端点解析:args > DOCSQL_CONN > DOCSQL_HOST/PORT > 默认 7600 ----

    private static string BuildConnectionString(string[] args)
    {
        string raw;
        if (args.Length > 0)
        {
            raw = args[0];
        }
        else if (Environment.GetEnvironmentVariable("DOCSQL_CONN") is { } conn && conn.Length > 0)
        {
            raw = conn;
        }
        else
        {
            var host = Environment.GetEnvironmentVariable("DOCSQL_HOST") ?? "127.0.0.1";
            var port = Environment.GetEnvironmentVariable("DOCSQL_PORT") ?? "7600";
            raw = $"{host}:{port}";
        }

        // "host=..;port=.." 原样作为连接串;"host:port" 展开成连接串
        var cs = raw.Contains('=') ? raw : $"host={raw.Split(':')[0]};port={raw.Split(':')[^1]}";
        var token = Environment.GetEnvironmentVariable("DOCSQL_TOKEN");
        if (!string.IsNullOrEmpty(token) && !cs.Contains("token="))
        {
            cs += $";token={token}";
        }
        return cs;
    }

    private static string Describe(string cs)
    {
        var b = new DocsqlConnectionStringBuilder { ConnectionString = cs };
        return $"{b.Host}:{b.Port}";
    }

    private static DocsqlConnection Open(string cs)
    {
        var c = new DocsqlConnection(cs);
        c.Open();
        return c;
    }

    private static void Chapter(string title) =>
        Console.WriteLine($"\n── {title} {new string('─', Math.Max(2, 46 - title.Length))}");

    private static async Task Exec(string cs, string sql)
    {
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task CleanupAsync(string cs)
    {
        foreach (var t in new[] { "sample_products", "sample_orders", "sample_entities", "sample_events" })
        {
            await Exec(cs, $"DROP TABLE IF EXISTS {t}");
        }
    }

    // ---- 1) 建表与基础 CRUD ----

    private static async Task BasicCrudAsync(string cs)
    {
        Chapter("1) 建表与基础 CRUD");
        await Exec(cs, """
            CREATE TABLE sample_products (
                id     INT PRIMARY KEY,
                name   TEXT NOT NULL,
                price  DOUBLE,
                stock  INT DEFAULT 0
            )
            """);

        using (var conn = Open(cs))
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """
                INSERT INTO sample_products (id, name, price, stock) VALUES
                    (1, '键盘', 399.0, 120),
                    (2, '鼠标', 199.5, 300),
                    (3, '显示器', 1599.0, 45)
                """;
            var affected = cmd.ExecuteNonQuery();
            Console.WriteLine($"   多行 INSERT 影响行数 = {affected}");

            cmd.CommandText = "UPDATE sample_products SET stock = stock - 5 WHERE id = 1";
            Console.WriteLine($"   UPDATE 影响行数 = {cmd.ExecuteNonQuery()}");

            cmd.CommandText = "SELECT name, price FROM sample_products WHERE stock < 100";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine($"   低库存: {reader.GetString(0)} ¥{reader.GetDouble(1)}");
            }
        }
    }

    // ---- 2) 参数化与 CLR 类型(客户端字面量化,防注入 + 类型保真) ----

    private static void ParametersAndClrTypes(string cs)
    {
        Chapter("2) 参数化与 CLR 类型");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();
        var p = (DocsqlParameterCollection)cmd.Parameters;

        // 字符串里的单引号会被正确转义,O'Brien 安全入库
        cmd.CommandText = "INSERT INTO sample_products (id, name, price, stock) VALUES (@id, @name, @price, @stock)";
        p.AddWithValue("id", 4);
        p.AddWithValue("name", "O'Brien's Notebook");
        p.AddWithValue("price", 9999.9m);
        p.AddWithValue("stock", 10);
        Console.WriteLine($"   插入参数化行,影响 {cmd.ExecuteNonQuery()} 行");

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT @b, @i, @d, @m, @t, @n";
        p.AddWithValue("b", true);      // bool  → TRUE/FALSE
        p.AddWithValue("i", 42L);       // 整数
        p.AddWithValue("d", 3.5d);      // 浮点(保持小数,不会被取整)
        p.AddWithValue("m", 1234.56m);  // decimal
        p.AddWithValue("t", new DateTime(2026, 9, 10, 12, 34, 56)); // 不变文化文本往返
        p.AddWithValue("n", DBNull.Value); // NULL
        using (var r = cmd.ExecuteReader())
        {
            r.Read();
            Console.WriteLine($"   bool={r.GetBoolean(0)}, int={r.GetInt64(1)}, double={r.GetDouble(2)}, " +
                              $"decimal={r.GetDecimal(3)}, time={r.GetDateTime(4):yyyy-MM-dd HH:mm:ss}, null={r.IsDBNull(5)}");
        }

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT name FROM sample_products WHERE name = @n";
        p.AddWithValue("n", "O'Brien's Notebook");
        Console.WriteLine($"   WHERE 参数化命中: {cmd.ExecuteScalar()}");
    }

    // ---- 3) GUID 时序主键:省略 id 即自动生成 UUIDv7,字符串序 = 时间序 ----

    private static void GuidTimeOrderedKeys(string cs)
    {
        Chapter("3) GUID 时序主键(UUIDv7 自动生成)");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "CREATE TABLE sample_entities (id GUID PRIMARY KEY AUTOINCREMENT, name TEXT)";
        cmd.ExecuteNonQuery();

        // 省略 id 列 → 服务端生成 UUIDv7;显式 NULL 同样生成;显式值原样保留
        cmd.CommandText = "INSERT INTO sample_entities (name) VALUES ('自动一'), ('自动二')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO sample_entities (id, name) VALUES (NULL, '显式 NULL') ";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT id, name FROM sample_entities ORDER BY id";
        using (var r = cmd.ExecuteReader())
        {
            var first = true;
            while (r.Read())
            {
                Console.WriteLine($"   {r.GetString(0)}  {r.GetString(1)}{(first ? "  ← ORDER BY id 即生成序" : "")}");
                first = false;
            }
        }
    }

    // ---- 4) 事务与 SAVEPOINT ----

    private static void TransactionsAndSavepoints(string cs)
    {
        Chapter("4) 事务与 SAVEPOINT");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();

        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "INSERT INTO sample_products (id, name, price, stock) VALUES (10, '事务内提交', 1.0, 1)";
            cmd.ExecuteNonQuery();
            tx.Commit();
        }
        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "INSERT INTO sample_products (id, name, price, stock) VALUES (11, '事务内回滚', 1.0, 1)";
            cmd.ExecuteNonQuery();
            tx.Rollback();
        }

        // SAVEPOINT:部分回滚到命名点,外层事务照常提交。
        // 语义备注:引擎的 ROLLBACK TO 会连同命名保存点自身一起丢弃
        // (SQLite/PostgreSQL 保留),所以回滚后不需要也不能再 RELEASE。
        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "INSERT INTO sample_products (id, name, price, stock) VALUES (12, '保留', 1.0, 1)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "SAVEPOINT before_oops";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO sample_products (id, name, price, stock) VALUES (13, '这行会被回滚', 1.0, 1)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "ROLLBACK TO SAVEPOINT before_oops";
            cmd.ExecuteNonQuery();
            tx.Commit();
        }

        // RELEASE:只忘掉保存点,其间的修改照常保留
        using (var tx = conn.BeginTransaction())
        {
            cmd.CommandText = "SAVEPOINT keep_me";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO sample_products (id, name, price, stock) VALUES (14, '保存点间提交', 1.0, 1)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "RELEASE SAVEPOINT keep_me";
            cmd.ExecuteNonQuery();
            tx.Commit();
        }

        cmd.CommandText = "SELECT id, name FROM sample_products WHERE id >= 10 ORDER BY id";
        using (var r = cmd.ExecuteReader())
        {
            while (r.Read())
            {
                Console.WriteLine($"   留存: {r.GetInt64(0)} {r.GetString(1)}");
            }
        }
        Console.WriteLine("   (id=11 整体回滚、id=13 部分回滚,均已丢弃)");
    }

    // ---- 5) RETURNING:插入的同时取回 AUTOINCREMENT 生成的 id ----

    private static void ReturningInsertedIds(string cs)
    {
        Chapter("5) RETURNING 取回生成的主键");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO sample_products (id, name, price, stock)
            VALUES (20, 'A', 9.9, 5), (21, 'B', 19.9, 5)
            """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE sample_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INT, qty INT)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO sample_orders (product_id, qty) VALUES (@p, @q) RETURNING id, qty";
        var p = (DocsqlParameterCollection)cmd.Parameters;
        p.AddWithValue("p", 20);
        p.AddWithValue("q", 3);
        using (var r = cmd.ExecuteReader())
        {
            while (r.Read())
            {
                Console.WriteLine($"   新订单 id={r.GetInt64(0)}(自动生成), 数量={r.GetInt64(1)}");
            }
        }
    }

    // ---- 6) 查询面:JOIN / 聚合 / 子查询 / UNION / 分页 ----

    private static void QuerySurface(string cs)
    {
        Chapter("6) 查询面:JOIN / 聚合 / 子查询 / 分页");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "INSERT INTO sample_orders (product_id, qty) VALUES (20, 1), (21, 2), (20, 4)";
        cmd.ExecuteNonQuery();

        // JOIN + 聚合 + HAVING
        cmd.CommandText = """
            SELECT p.name, SUM(o.qty) AS sold
            FROM sample_orders o JOIN sample_products p ON o.product_id = p.id
            GROUP BY p.name HAVING SUM(o.qty) > 3
            ORDER BY sold DESC
            """;
        using (var r = cmd.ExecuteReader())
        {
            while (r.Read())
            {
                Console.WriteLine($"   热销: {r.GetString(0)} × {r.GetInt64(1)}");
            }
        }

        // 标量子查询 + LIMIT/OFFSET 分页
        cmd.CommandText = "SELECT name, price FROM sample_products ORDER BY price DESC LIMIT 2 OFFSET 1";
        using (var r = cmd.ExecuteReader())
        {
            var line = new List<string>();
            while (r.Read())
            {
                line.Add($"{r.GetString(0)} ¥{r.GetDouble(1)}");
            }
            Console.WriteLine($"   按价格分页(跳过最贵 1 个): {string.Join(" | ", line)}");
        }

        // UNION 去重(只取最初四个演示商品,价格两段)
        cmd.CommandText = """
            SELECT name FROM sample_products WHERE id <= 4 AND price < 300
            UNION
            SELECT name FROM sample_products WHERE id <= 4 AND price > 1000
            """;
        var names = new List<string>();
        using (var r = cmd.ExecuteReader())
        {
            while (r.Read())
            {
                names.Add(r.GetString(0));
            }
        }
        Console.WriteLine($"   UNION 两段价格段: {string.Join(", ", names)}");
    }

    // ---- 7) 文档式存储:同一张表的行可以有不同形状(无强制 schema) ----

    private static void DocumentModel(string cs)
    {
        Chapter("7) 文档式存储(无强制 schema)");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE sample_events (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT)";
        cmd.ExecuteNonQuery();

        // 除公共列外,各行携带不同字段 —— 引擎按文档存储,列是"观测到的键并集"
        cmd.CommandText = "INSERT INTO sample_events (type, url, referrer) VALUES ('visit', '/home', 'google')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO sample_events (type, duration_ms, code) VALUES ('build', 4200, 0)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO sample_events (type, email) VALUES ('signup', 'a@b.c')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT * FROM sample_events ORDER BY id";
        using (var r = cmd.ExecuteReader())
        {
            Console.WriteLine($"   SELECT * 列并集 = {Enumerable.Range(0, r.FieldCount).Select(r.GetName).Aggregate((a, b) => $"{a},{b}")}");
            var idOrd = r.GetOrdinal("id"); // 无固定 schema:列按观测到的键并集排列,按名取列
            while (r.Read())
            {
                var cells = Enumerable.Range(0, r.FieldCount)
                    .Where(i => !r.IsDBNull(i))
                    .Select(i => $"{r.GetName(i)}={r.GetValue(i)}");
                Console.WriteLine($"   [{r.GetInt64(idOrd)}] {string.Join(" | ", cells)}");
            }
        }
        Console.WriteLine("   (缺失字段读出 NULL —— 适合事件/日志类异形数据)");
    }

    // ---- 8) DataReader 技巧:名字访问、DBNull、GetSchemaTable、GetValues ----

    private static void ReaderTechniques(string cs)
    {
        Chapter("8) DataReader 技巧");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, price, stock FROM sample_products WHERE id IN (1, 4) ORDER BY id";
        using var r = cmd.ExecuteReader();

        // 列名 → 序号,类型元数据
        var nameOrd = r.GetOrdinal("name");
        var cols = Enumerable.Range(0, r.FieldCount).Select(i => $"{r.GetName(i)}:{r.GetFieldType(i)?.Name}");
        Console.WriteLine($"   列: {string.Join(" ", cols)}");

        var schema = r.GetSchemaTable();
        foreach (System.Data.DataRow col in schema!.Rows)
        {
            Console.WriteLine($"   schema: {col["ColumnName"]} 可空={col["AllowDBNull"]}");
        }

        while (r.Read())
        {
            var values = new object[r.FieldCount];
            var n = r.GetValues(values); // NULL → DBNull.Value,返回填充列数
            Console.WriteLine($"   行 {r["id"]}: {values[nameOrd]} 库存={values[3]}(GetValues 填 {n} 列)");
        }
        Console.WriteLine($"   HasRows={r.HasRows},读完后 NextResult={r.NextResult()}(单结果集)");
    }

    // ---- 9) 持久化 pub/sub:发布落盘、实时推送、离线回放与 id 续传 ----

    private static void Pubsub(string cs)
    {
        Chapter("9) 持久化 pub/sub");
        var inbox = new ConcurrentQueue<DocsqlMessage>();

        // 订阅:专用连接 + 后台分发;"latest" 只收新消息
        long lastId;
        using (var sub = new DocsqlSubscriber(cs))
        {
            sub.Subscribe("sample.news", m => inbox.Enqueue(m));
            using (var pub = Open(cs))
            {
                var (id, receivers) = pub.Publish("sample.news", "你好,订阅者");
                Console.WriteLine($"   发布 id={id}, 实时送达 {receivers} 个连接");
            }
            var live = WaitFor(inbox);
            lastId = live.Id;
            Console.WriteLine($"   实时收到: [{live.Id}] {live.Payload}");
        }

        // 订阅断开期间发布的消息不会丢:用最后收到的 id 重新订阅即补回缺口
        using (var pub = Open(cs))
        {
            pub.Publish("sample.news", "断线期间的这条");
        }
        using (var sub = new DocsqlSubscriber(cs))
        {
            sub.Subscribe("sample.news", m => inbox.Enqueue(m), lastId.ToString());
            var resumed = WaitFor(inbox);
            Console.WriteLine($"   从 id={lastId} 续传,恰好补回: [{resumed.Id}] {resumed.Payload}");
        }

        // "earliest" 则全量回放历史(语义 at-least-once,历史随 WAL 持久化)
        using (var sub = new DocsqlSubscriber(cs))
        {
            sub.Subscribe("sample.news", m => inbox.Enqueue(m), "earliest");
            var seen = new List<string> { "你好,订阅者", "断线期间的这条" };
            var replayed = new List<string>();
            while (replayed.Count < seen.Count)
            {
                replayed.Add(WaitFor(inbox).Payload);
            }
            Console.WriteLine($"   earliest 回放全量历史: {string.Join(" → ", replayed)}");
        }

        using (var conn = Open(cs))
        {
            // docsql_pubsub 视图可当表查;TRIM 只保留每频道最新 N 条
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM docsql_pubsub WHERE channel = 'sample.news'";
            Console.WriteLine($"   docsql_pubsub 视图可见 {cmd.ExecuteScalar()} 条历史");
            var trimmed = conn.PubsubTrim("sample.news", 1);
            Console.WriteLine($"   TRIM 保留最新 1 条,删除 {trimmed} 条");
        }
    }

    private static DocsqlMessage WaitFor(ConcurrentQueue<DocsqlMessage> q)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < 5000)
        {
            if (q.TryDequeue(out var m))
            {
                return m;
            }
            Thread.Sleep(20);
        }
        throw new TimeoutException("5 秒内未收到推送");
    }

    // ---- 10) 错误处理与异步 API ----

    private static void ErrorHandlingAndAsyncApi(string cs)
    {
        Chapter("10) 错误处理与异步 API");
        using var conn = Open(cs);
        using var cmd = conn.CreateCommand();

        // 约束冲突以 DocsqlException 上抛,连接仍可继续使用
        cmd.CommandText = "INSERT INTO sample_products (id, name) VALUES (1, '重复主键')";
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (DocsqlException ex)
        {
            cmd.CommandText = "SELECT COUNT(*) FROM sample_products";
            Console.WriteLine($"   主键冲突被拒:「{ex.Message}」");
            Console.WriteLine($"   连接仍可用,当前产品数 = {cmd.ExecuteScalar()}");
        }

        // 同一套 ADO.NET 面提供 *Async 版本
        var count = cmd.ExecuteScalarAsync().GetAwaiter().GetResult();
        Console.WriteLine($"   ExecuteScalarAsync = {count}");
    }
}
