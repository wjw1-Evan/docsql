// SQL 全覆盖测试:按 README 能力矩阵逐项验证 DocSQL 支持的 SQL 面。
// 通过 ADO 客户端直连服务端执行,每个用例使用独立表名避免相互干扰。

using Docsql.Client;
using System.Data.Common;
using Xunit;

public sealed class SqlSurfaceTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public SqlSurfaceTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var c = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        c.Open();
        return c;
    }

    private object Scalar(string sql)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar()!;
    }

    private long Long(string sql) => Convert.ToInt64(Scalar(sql));

    /// <summary>读单列全部值,按行序返回。</summary>
    private List<string> Column(string sql)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var values = new List<string>();
        while (r.Read()) values.Add(r.IsDBNull(0) ? "NULL" : r.GetValue(0)!.ToString()!);
        return values;
    }

    /// <summary>读全部行全部列,逐列展平成字符串。</summary>
    private List<string> RowsFlat(string sql)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var values = new List<string>();
        while (r.Read())
            for (var i = 0; i < r.FieldCount; i++)
                values.Add(r.IsDBNull(i) ? "NULL" : r.GetValue(i)!.ToString()!);
        return values;
    }

    /// <summary>执行写语句,返回受影响行数。</summary>
    private long NonQuery(string sql)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }

    private void Exec(params string[] sqls)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        foreach (var sql in sqls)
        {
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }

    private static void RowsOf(DbCommand cmd, string sql, Action<DbDataReader> assertRow)
    {
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        while (r.Read()) assertRow(r);
    }

    // ---------- DDL:CREATE / ALTER / DROP + 索引 ----------

    [Fact]
    public void Ddl_lifecycle_create_alter_drop_index_and_table()
    {
        Exec(
            "DROP TABLE IF EXISTS ddl_t",
            "CREATE TABLE ddl_t (id INT, name TEXT)",
            "INSERT INTO ddl_t VALUES (1, 'a'), (2, 'b'), (3, 'a')",
            "CREATE INDEX idx_ddl_name ON ddl_t (name)");
        Assert.Equal(3, Long("SELECT COUNT(*) FROM ddl_t"));

        // ALTER ADD COLUMN,带 DEFAULT 回填旧行
        Exec("ALTER TABLE ddl_t ADD COLUMN flag INT DEFAULT 1");
        Assert.Equal(1L, Scalar("SELECT flag FROM ddl_t WHERE id = 2"));

        Exec("DROP INDEX idx_ddl_name");
        Exec("DROP TABLE ddl_t");
        Assert.Throws<DocsqlException>(() => Long("SELECT COUNT(*) FROM ddl_t"));
    }

    [Fact]
    public void Column_constraints_pk_unique_notnull_are_enforced()
    {
        Exec(
            "DROP TABLE IF EXISTS cons_t",
            "CREATE TABLE cons_t (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL)");
        Exec("INSERT INTO cons_t VALUES (1, 'a@x', 'ann')");

        Assert.Throws<DocsqlException>(() =>
            Exec("INSERT INTO cons_t VALUES (1, 'b@x', 'dup_pk')"));      // 主键冲突
        Assert.Throws<DocsqlException>(() =>
            Exec("INSERT INTO cons_t VALUES (2, 'a@x', 'dup_uniq')"));    // 唯一冲突
        Assert.Throws<DocsqlException>(() =>
            Exec("INSERT INTO cons_t VALUES (3, 'c@x', NULL)"));          // 非空冲突
        Assert.Equal(1L, Long("SELECT COUNT(*) FROM cons_t"));
    }

    [Fact]
    public void Autoincrement_generates_increasing_ids()
    {
        Exec(
            "DROP TABLE IF EXISTS auto_t",
            "CREATE TABLE auto_t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
            "INSERT INTO auto_t (v) VALUES ('x'), ('y')");
        Assert.Equal(new[] { "1", "2" }, Column("SELECT id FROM auto_t ORDER BY id"));
    }

    // ---------- DML:INSERT / UPDATE / DELETE + RETURNING ----------

    [Fact]
    public void Insert_returning_returns_inserted_rows()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS ins_t";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE TABLE ins_t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)";
        cmd.ExecuteNonQuery();

        RowsOf(cmd, "INSERT INTO ins_t (v) VALUES ('a'), ('b') RETURNING id, v", r =>
        {
            Assert.Equal(r.GetString(1) switch { "a" => 1L, "b" => 2L, _ => -1L }, r.GetInt64(0));
        });
    }

    [Fact]
    public void Update_and_delete_returning()
    {
        Exec(
            "DROP TABLE IF EXISTS upd_t",
            "CREATE TABLE upd_t (id INT, v INT)",
            "INSERT INTO upd_t VALUES (1, 10), (2, 20), (3, 30)");
        Assert.Equal(2, Long("SELECT COUNT(*) FROM upd_t WHERE v >= 20"));
        Assert.Equal(2, NonQuery("UPDATE upd_t SET v = v + 1 WHERE id <= 2"));
        Assert.Equal(2, NonQuery("DELETE FROM upd_t WHERE id >= 2"));
        Assert.Equal(1L, Long("SELECT COUNT(*) FROM upd_t"));
    }

    // ---------- SELECT:WHERE 运算符 ----------

    [Fact]
    public void Where_operators_comparison_logic_like_between_in_null()
    {
        Exec(
            "DROP TABLE IF EXISTS where_t",
            "CREATE TABLE where_t (id INT, name TEXT, n INT)",
            "INSERT INTO where_t VALUES (1, 'apple', 10), (2, 'apricot', 20), (3, 'banana', NULL)");

        Assert.Equal(1L, Long("SELECT COUNT(*) FROM where_t WHERE n = 20"));
        // AND 优先于 OR:(id<>3 AND n<30) OR id=3 → 三行全部命中
        Assert.Equal(3L, Long("SELECT COUNT(*) FROM where_t WHERE id <> 3 AND n < 30 OR id = 3"));
        Assert.Equal(2L, Long("SELECT COUNT(*) FROM where_t WHERE name LIKE 'ap%'"));
        Assert.Equal(1L, Long("SELECT COUNT(*) FROM where_t WHERE n BETWEEN 15 AND 25"));
        Assert.Equal(2L, Long("SELECT COUNT(*) FROM where_t WHERE id IN (1, 3)"));
        Assert.Equal(1L, Long("SELECT COUNT(*) FROM where_t WHERE n IS NULL"));
        Assert.Equal(2L, Long("SELECT COUNT(*) FROM where_t WHERE n IS NOT NULL"));
    }

    [Fact]
    public void Order_by_limit_offset()
    {
        Exec(
            "DROP TABLE IF EXISTS page_t",
            "CREATE TABLE page_t (id INT)",
            "INSERT INTO page_t VALUES (5), (1), (4), (2), (3)");
        Assert.Equal(new[] { "1", "2", "3" },
            Column("SELECT id FROM page_t ORDER BY id LIMIT 3"));
        Assert.Equal(new[] { "3", "4" },
            Column("SELECT id FROM page_t ORDER BY id LIMIT 2 OFFSET 2"));
        Assert.Equal(new[] { "5", "4" },
            Column("SELECT id FROM page_t ORDER BY id DESC LIMIT 2"));
    }

    // ---------- 聚合与 GROUP BY / HAVING ----------

    [Fact]
    public void Aggregates_count_sum_avg_min_max()
    {
        Exec(
            "DROP TABLE IF EXISTS agg_t",
            "CREATE TABLE agg_t (v INT)",
            "INSERT INTO agg_t VALUES (10), (20), (30)");
        Assert.Equal(3L, Long("SELECT COUNT(*) FROM agg_t"));
        Assert.Equal(60L, Long("SELECT SUM(v) FROM agg_t"));
        Assert.Equal(20L, Long("SELECT AVG(v) FROM agg_t"));
        Assert.Equal(10L, Long("SELECT MIN(v) FROM agg_t"));
        Assert.Equal(30L, Long("SELECT MAX(v) FROM agg_t"));
    }

    [Fact]
    public void Group_by_with_having()
    {
        Exec(
            "DROP TABLE IF EXISTS grp_t",
            "CREATE TABLE grp_t (dept TEXT, pay INT)",
            "INSERT INTO grp_t VALUES ('eng', 100), ('eng', 120), ('ops', 60), ('ops', 80), ('hr', 40)");
        var rows = RowsFlat(
            "SELECT dept, COUNT(*) FROM grp_t GROUP BY dept HAVING SUM(pay) > 100 ORDER BY dept");
        // eng=220、ops=140 入选,hr=40 被过滤
        Assert.Equal(new[] { "eng", "2", "ops", "2" }, rows);
    }

    // ---------- JOIN:INNER / LEFT / CROSS / USING ----------

    [Fact]
    public void Joins_inner_left_cross_using()
    {
        Exec(
            "DROP TABLE IF EXISTS dept", "DROP TABLE IF EXISTS emp",
            "CREATE TABLE dept (id INT, name TEXT)",
            "CREATE TABLE emp (id INT, dept_id INT, name TEXT)",
            "INSERT INTO dept VALUES (1, 'eng'), (2, 'ops')",
            "INSERT INTO emp VALUES (10, 1, 'ann'), (11, 1, 'bob'), (12, 9, 'orphan')");

        // INNER:孤儿行不出现
        Assert.Equal(2L, Long(
            "SELECT COUNT(*) FROM emp e INNER JOIN dept d ON e.dept_id = d.id"));
        // LEFT:保留左表全部行,无匹配处为 NULL
        Assert.Equal(3L, Long(
            "SELECT COUNT(*) FROM emp e LEFT JOIN dept d ON e.dept_id = d.id"));
        Assert.Equal("NULL", Column(
            "SELECT d.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id WHERE e.id = 12")[0]);
        // CROSS:笛卡尔积
        Assert.Equal(6L, Long("SELECT COUNT(*) FROM emp CROSS JOIN dept"));
        // USING(同名列):等值连接
        Exec("DROP TABLE IF EXISTS t1", "DROP TABLE IF EXISTS t2",
            "CREATE TABLE t1 (k INT, a TEXT)", "CREATE TABLE t2 (k INT, b TEXT)",
            "INSERT INTO t1 VALUES (1, 'x')", "INSERT INTO t2 VALUES (1, 'y')");
        Assert.Equal(1L, Long("SELECT COUNT(*) FROM t1 JOIN t2 USING (k)"));
    }

    // ---------- 子查询与 UNION ----------

    [Fact]
    public void Subqueries_in_where_and_from()
    {
        Exec(
            "DROP TABLE IF EXISTS sub_t",
            "CREATE TABLE sub_t (id INT, v INT)",
            "INSERT INTO sub_t VALUES (1, 100), (2, 50), (3, 200)");
        // WHERE 中的标量/IN 子查询
        Assert.Equal(200L, Long("SELECT v FROM sub_t WHERE v = (SELECT MAX(v) FROM sub_t)"));
        Assert.Equal(2L, Long(
            "SELECT COUNT(*) FROM sub_t WHERE id IN (SELECT id FROM sub_t WHERE v > 60)"));
        // FROM 中的派生表
        Assert.Equal(2L, Long(
            "SELECT COUNT(*) FROM (SELECT id, v FROM sub_t WHERE v > 60) d"));
    }

    [Fact]
    public void Union_and_union_all()
    {
        Exec(
            "DROP TABLE IF EXISTS u1", "DROP TABLE IF EXISTS u2",
            "CREATE TABLE u1 (v INT)", "CREATE TABLE u2 (v INT)",
            "INSERT INTO u1 VALUES (1), (2)",
            "INSERT INTO u2 VALUES (2), (3)");
        // UNION ALL 保留重复(包一层 COUNT 统计全部行)
        Assert.Equal(4L, Long(
            "SELECT COUNT(*) FROM (SELECT v FROM u1 UNION ALL SELECT v FROM u2) u"));
        // UNION 去重
        var rows = Column("SELECT v FROM u1 UNION SELECT v FROM u2 ORDER BY v");
        Assert.Equal(new[] { "1", "2", "3" }, rows);
    }

    // ---------- 目录视图与 PRAGMA ----------

    [Fact]
    public void Catalog_views_and_pragma()
    {
        Exec("DROP TABLE IF EXISTS cat_t",
            "CREATE TABLE cat_t (id INT PRIMARY KEY, name TEXT)",
            "CREATE INDEX idx_cat_name ON cat_t (name)");
        Assert.Contains("cat_t", Column("SELECT name FROM sqlite_master WHERE type = 'table'"));
        Assert.Contains("idx_cat_name", Column("SELECT name FROM sqlite_master WHERE type = 'index'"));
        Assert.Contains("cat_t", Column("SELECT table_name FROM information_schema.tables"));
        var cols = Column(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'cat_t' ORDER BY column_name");
        Assert.Contains("id", cols);
        Assert.Contains("name", cols);
        // PRAGMA 兼容:被接受但不返回数据(引擎的兼容垫片,返回 affected=0)
        Assert.Empty(Column("PRAGMA table_info('cat_t')"));
    }

    // ---------- 事务 ----------

    [Fact]
    public void Transaction_commit_persists_and_rollback_discards()
    {
        Exec("DROP TABLE IF EXISTS stx_t", "CREATE TABLE stx_t (v INT)");
        using (var conn = Open())
        using (var cmd = conn.CreateCommand())
        {
            using (var tx = conn.BeginTransaction())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT INTO stx_t VALUES (1)";
                cmd.ExecuteNonQuery();
                tx.Commit();
            }
            using (var tx = conn.BeginTransaction())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT INTO stx_t VALUES (2)";
                cmd.ExecuteNonQuery();
                tx.Rollback();
            }
        }
        Assert.Equal(1L, Long("SELECT COUNT(*) FROM stx_t"));
    }
}
