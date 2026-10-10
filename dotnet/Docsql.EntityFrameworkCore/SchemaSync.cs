// 模型 → schema 同步:按 EF 模型建表/补列/建索引。
// 拦截器(惰性建表)与 DocsqlDatabaseCreator(EnsureCreated)共用。

using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Docsql.EntityFrameworkCore;

internal static partial class SchemaSync
{
    /// <summary>整个模型的建表/补列/建索引(拦截器与 EnsureCreated 共用)。
    /// 按表聚合而非按实体:TPH/table-splitting 下同一张表映射多个实体 ——
    /// 建表列集取并集、主键取首个声明,索引回收面取全部实体 wanted 的聚合
    /// 集合(按单实体划定回收面时,兄弟实体的索引会被逐轮互 DROP,含
    /// UNIQUE)。先全部建表(含重建),再同步索引:重建路径 DROP TABLE 会
    /// 连带删索引,必须让索引同步在所有表形态确定之后进行。</summary>
    public static void SyncModel(DbConnection conn, IModel model)
    {
        var byTable = new Dictionary<string, List<IEntityType>>(StringComparer.OrdinalIgnoreCase);
        var order = new List<string>();
        foreach (var entity in model.GetEntityTypes())
        {
            var table = entity.GetTableName();
            if (table is null) continue;
            if (byTable.TryGetValue(table, out var list))
            {
                list.Add(entity);
            }
            else
            {
                byTable[table] = [entity];
                order.Add(table);
            }
        }
        foreach (var table in order)
        {
            SyncTable(conn, table, byTable[table]);
        }
        foreach (var table in order)
        {
            SyncIndexes(conn, table, byTable[table]);
        }
    }

    public static void SyncTable(DbConnection conn, string table, IReadOnlyList<IEntityType> entities)
    {
        // 复合主键显式报错:引擎只支持单列 PRIMARY KEY(表级复合 PK 报错),
        // 此前静默建成无约束堆表——重复键行能插入,EF 的身份解析与并发判定
        // 在运行期以难排查的方式炸。与 Migrations 桩同一显式失败哲学。
        foreach (var entity in entities)
        {
            if (entity.FindPrimaryKey() is { Properties.Count: > 1 })
            {
                throw new NotSupportedException(
                    "composite primary keys are not supported; use a single-column key " +
                    "or a CREATE UNIQUE INDEX over the key columns");
            }
        }

        // 聚合该表全部实体的列(重名列以先声明者为准)与主键声明。
        var seenColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var modelColumns = new List<(string Name, string Type, bool NotNull)>();
        IProperty? pkProperty = null;
        string? pkColumn = null;
        foreach (var entity in entities)
        {
            var pk = entity.FindPrimaryKey();
            if (pkProperty is null && pk is { Properties.Count: 1 })
            {
                pkProperty = pk.Properties[0];
                pkColumn = pkProperty.GetColumnName(
                    StoreObjectIdentifier.Table(table, entity.GetSchema()));
            }
            foreach (var p in entity.GetProperties())
            {
                var column = p.GetColumnName(StoreObjectIdentifier.Table(table, entity.GetSchema()));
                if (column is null || !seenColumns.Add(column)) continue;
                modelColumns.Add((column, p.GetColumnType() ?? "TEXT", !p.IsNullable));
            }
        }

        var cols = new List<string>();
        foreach (var (column, type, notNull) in modelColumns)
        {
            var nullSuffix = notNull ? " NOT NULL" : "";
            // 单列值生成主键:仅整数类型自增 —— 引擎把 AUTOINCREMENT 视为
            // 整数 max+1;给 TEXT/GUID 主键加上它会让维护脚本插入整数,
            // EF 物化 Guid.Parse("1") 直接炸。GUID 主键改用引擎的 UUIDv7
            // 自动生成路径(Guid 列本身按 TEXT 建列,不再带 AUTOINCREMENT)。
            if (pkProperty is { } pk
                && pkColumn is not null
                && string.Equals(column, pkColumn, StringComparison.OrdinalIgnoreCase))
                cols.Add(
                    $"{Quote(column)} {type}{nullSuffix} PRIMARY KEY{(IsIntegerClrType(pk.ClrType) ? " AUTOINCREMENT" : "")}");
            else
                cols.Add($"{Quote(column)} {type}{nullSuffix}");
        }
        if (cols.Count == 0) return;

        // 复用该连接自己的 ADO 管线直接发 DDL;表已存在时转入列同步。
        // 只读副本(未提升)拒绝 DDL:静默跳过,交给后续只读命令正常执行。
        var created = true;
        try
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE TABLE {Quote(table)} ({string.Join(", ", cols)})";
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) when (IsIgnorableDdlError(ex))
        {
            created = false;
        }

        if (created) return;

        // 表已存在:先核对模型主键是否按提供程序形状强制(与 VerifyModel 同
        // 一判据,门禁一致 —— 校验看得见的漂移必须能被同步收敛,否则每个
        // 上下文都全量重同步)。引擎没有 ALTER ADD PRIMARY KEY,未强制时走
        // 保数据重建;只读副本上与建表路径同一静默守卫。
        if (pkProperty is { IsNullable: false } && pkColumn is not null
            && !PkColumnEnforced(conn, table, pkColumn))
        {
            try
            {
                RebuildTableWithPrimaryKey(conn, table, string.Join(", ", cols));
            }
            catch (Exception ex) when (IsIgnorableDdlError(ex))
            {
                // 只读副本:重建被拒,与 CREATE TABLE 路径同一守卫。
            }
            return;
        }
        AddMissingColumns(conn, table, modelColumns);
    }

    /// <summary>模型主键列是否已按提供程序形状强制。引擎的 SQL 面不暴露 PK
    /// 存在性(sqlite_master 的表行 sql 为空文本、PRIMARY KEY 的自动索引不
    /// 出列),提供程序自身的建表形状是「非空主键列 NOT NULL PRIMARY KEY」
    /// —— 以该列 is_nullable=NO 为弱判定:列缺失或可空都判未强制,能排除
    /// 「完全无 PK 的外部裸表」场景(裸建表列可空)。VerifyModel 用同一
    /// 判据,两侧门禁一致。可空主键列的模型(PK ≠ NOT NULL)无法区分,
    /// 两侧同样跳过。</summary>
    private static bool PkColumnEnforced(DbConnection conn, string table, string pkColumn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT is_nullable FROM information_schema.columns WHERE table_name = '" +
            table.Replace("'", "''") + "' AND column_name = '" + pkColumn.Replace("'", "''") + "'";
        var result = cmd.ExecuteScalar();
        return result is string s && s.Equals("NO", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>引擎没有 ALTER TABLE ADD PRIMARY KEY:「表已存在但模型主键列
    /// 未按提供程序形状强制」的唯一收敛路径是保数据重建 —— 建目标形状(含
    /// PRIMARY KEY)→ 拷贝全部既有列(含 schemaless 数据字段:引擎允许向未
    /// 声明列名 INSERT,目录按观察记账)→ 换名 → 重放原表命名索引的 DDL
    /// (手工索引不随重建丢失)。幂等:先清掉上次中断残留的临时表;拷贝失败
    /// (存量主键列重复值被唯一树拒绝等)异常上抛,原表与数据保持原样。</summary>
    private static void RebuildTableWithPrimaryKey(
        DbConnection conn, string table, string createColumns)
    {
        var temp = table + "__efpksync";
        // 崩溃自愈:上次重建在「DROP 原表之后、RENAME 之前」被中断时,数据
        // 全在临时表 —— 「原表缺失 + 临时表在场」即该状态,直接换名接管。
        if (!TableExists(conn, table) && TableExists(conn, temp))
        {
            Exec(conn, $"ALTER TABLE {Quote(temp)} RENAME TO {Quote(table)}");
            return;
        }
        // 换名后按原文重放:索引名全库唯一,DROP 原表即释放旧名,DDL 文本里
        // 的表名在 RENAME 之后重新成立。
        var indexDdls = ExistingIndexSqls(conn, table).ToList();
        // Table-level constraints (hand-made UNIQUE(...)/CHECK/FOREIGN KEY on
        // the external table) exist ONLY in the original CREATE TABLE text —
        // sqlite_master exposes it now. Re-attach them to the rebuilt shape:
        // losing them silently dropped enforcement (duplicate emails stopped
        // failing after a keep-data rebuild).
        var extraConstraints = ExtractTableLevelConstraints(conn, table);
        var targetColumns = extraConstraints.Length > 0
            ? createColumns + ", " + extraConstraints
            : createColumns;
        Exec(conn, $"DROP TABLE IF EXISTS {Quote(temp)}");
        Exec(conn, $"CREATE TABLE {Quote(temp)} ({targetColumns})");
        var copyColumns = ExistingColumns(conn, table).ToList();
        if (copyColumns.Count > 0)
        {
            var list = string.Join(", ", copyColumns.Select(Quote));
            try
            {
                Exec(conn,
                    $"INSERT INTO {Quote(temp)} ({list}) SELECT {list} FROM {Quote(table)}");
            }
            catch (Exception ex)
                when (ex.Message.Contains("does not exist", StringComparison.Ordinal))
            {
                // 并发上下文同场重建,原表已被对方换掉:本方作废,以对方为准。
                TryDrop(conn, temp);
                return;
            }
        }
        try
        {
            Exec(conn, $"DROP TABLE {Quote(table)}");
            Exec(conn, $"ALTER TABLE {Quote(temp)} RENAME TO {Quote(table)}");
        }
        catch (Exception ex)
            when (ex.Message.Contains("does not exist", StringComparison.Ordinal)
                || ex.Message.Contains("already exists", StringComparison.Ordinal))
        {
            // 同上:破坏性阶段发现对方已完成,本方清理退场。
            TryDrop(conn, temp);
            return;
        }
        catch (Exception ex)
            when (ex.Message.Contains("referenced by VIEW", StringComparison.Ordinal)
                || ex.Message.Contains("referenced by view", StringComparison.Ordinal))
        {
            // The engine's view-dependency guard refuses the DROP. A keep-data
            // rebuild can never succeed while a user view names this table:
            // rethrow an actionable error (drop the view or add the missing
            // primary key by hand) INSTEAD of letting every future context
            // redo the full table copy and fail the same way.
            TryDrop(conn, temp);
            throw new InvalidOperationException(
                $"cannot rebuild table '{table}' to enforce its model primary key: a VIEW " +
                "depends on the table (drop the view first, or create the primary key " +
                $"manually). Inner error: {ex.Message}", ex);
        }
        foreach (var ddl in indexDdls)
        {
            try
            {
                Exec(conn, ddl);
            }
            catch (Exception ex) when (IsIgnorableDdlError(ex))
            {
                // 并发方已建同名索引。
            }
        }
    }

    /// <summary>该表全部命名索引的 DDL 文本(sqlite_master.sql)。</summary>
    private static IEnumerable<string> ExistingIndexSqls(DbConnection conn, string table)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = '" +
            table.Replace("'", "''") + "'";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            if (reader.IsDBNull(0))
            {
                continue;
            }
            var sql = reader.GetString(0);
            if (sql.Length > 0)
            {
                yield return sql;
            }
        }
    }

    /// <summary>原表 CREATE TABLE 文本里的表级约束子句(UNIQUE(...)/
    /// CHECK(...)/FOREIGN KEY ...):列定义之外的 parts,原样保留。旧的
    /// 引擎把表行 sql 留空,保数据重建后这些约束静默消失;现在从 DDL
    /// 文本重新挂回目标形状。逗号按括号深度切分,字符串/引号标识符不透明。</summary>
    private static string ExtractTableLevelConstraints(DbConnection conn, string table)
    {
        string? ddl = null;
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText =
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '" +
                table.Replace("'", "''") + "'";
            ddl = cmd.ExecuteScalar() as string;
        }
        if (string.IsNullOrWhiteSpace(ddl))
        {
            return string.Empty;
        }
        var open = ddl.IndexOf('(');
        var close = ddl.LastIndexOf(')');
        if (open < 0 || close <= open)
        {
            return string.Empty;
        }
        var body = ddl.Substring(open + 1, close - open - 1);
        // Split top-level commas: parens nest, quoted identifiers ("a,b") and
        // string literals ('a,b') are opaque.
        var parts = new List<string>();
        var depth = 0;
        var sb = new System.Text.StringBuilder();
        char quote = '\0';
        foreach (var ch in body)
        {
            if (quote != '\0')
            {
                sb.Append(ch);
                if (ch == quote) quote = '\0';
                continue;
            }
            if (ch is '"' or '\'')
            {
                quote = ch;
                sb.Append(ch);
                continue;
            }
            if (ch == '(') depth++;
            if (ch == ')') depth--;
            if (ch == ',' && depth == 0)
            {
                parts.Add(sb.ToString());
                sb.Clear();
                continue;
            }
            sb.Append(ch);
        }
        if (sb.Length > 0) parts.Add(sb.ToString());
        var kept = parts
            .Select(p => p.Trim())
            .Where(p => p.StartsWith("UNIQUE", StringComparison.OrdinalIgnoreCase)
                || p.StartsWith("CHECK", StringComparison.OrdinalIgnoreCase)
                || p.StartsWith("FOREIGN KEY", StringComparison.OrdinalIgnoreCase)
                || p.StartsWith("PRIMARY KEY", StringComparison.OrdinalIgnoreCase))
            .ToList();
        return string.Join(", ", kept);
    }

    private static bool TableExists(DbConnection conn, string table)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '" +
            table.Replace("'", "''") + "'";
        return cmd.ExecuteScalar() is string;
    }

    private static void TryDrop(DbConnection conn, string table)
    {
        try { Exec(conn, $"DROP TABLE IF EXISTS {Quote(table)}"); }
        catch { /* best effort:并发清理 */ }
    }

    private static void Exec(DbConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>表已存在,或连的是只读副本(表由复制通道创建)。
    /// 锚定引擎的确切错误文本(table/index … already exists、read-only …),
    /// 不做子串扫描 —— 任何恰好含这两个词的无关失败(权限、磁盘、语法)
    /// 都曾被静默吞掉,让「建表失败」伪装成「表已存在」。</summary>
    private static bool IsIgnorableDdlError(Exception ex)
    {
        var msg = ex.Message;
        return (msg.StartsWith("table ", StringComparison.Ordinal)
                && msg.EndsWith(" already exists", StringComparison.Ordinal))
            || (msg.StartsWith("index ", StringComparison.Ordinal)
                && msg.EndsWith(" already exists", StringComparison.Ordinal))
            || msg.StartsWith("read-only", StringComparison.Ordinal);
    }

    /// <summary>
    /// 模型加字段免迁移(类似 EF MongoDB):对已存在的表补齐模型中新增的列,
    /// 只增不减 —— 旧数据保持原样,旧行新列读出来为 NULL。
    /// </summary>
    private static void AddMissingColumns(
        DbConnection conn, string table,
        IEnumerable<(string Name, string Type, bool NotNull)> modelColumns)
    {
        var existing = new HashSet<string>(ExistingColumns(conn, table), StringComparer.OrdinalIgnoreCase);
        foreach (var (name, type, _) in modelColumns)
        {
            if (existing.Contains(name)) continue;
            // 补列不加 NOT NULL(旧行无值),除主键外均可空写入。
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"ALTER TABLE {Quote(table)} ADD COLUMN {Quote(name)} {type}";
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex) when (IsIgnorableDdlError(ex))
            {
            }
            catch (Exception ex) when (IsIgnorableAddColumnError(ex))
            {
                // 并发上下文刚补了同一列(校验快照过期):列已在即目标
                // 达成。只放宽这一处,建表路径的失败仍原样抛出。
            }
        }
    }

    /// <summary>补列路径专用的可忽略错误:另一并发上下文刚补了同一列
    /// (两个并行 DbContext 的校验快照都缺该列),后到的 ALTER 报
    /// duplicate column name —— 列已在即目标已达成。</summary>
    private static bool IsIgnorableAddColumnError(Exception ex)
    {
        return ex.Message.StartsWith("duplicate column name:", StringComparison.Ordinal);
    }

    private static IEnumerable<string> ExistingColumns(DbConnection conn, string table)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT * FROM {Quote(table)} LIMIT 0";
        using var reader = cmd.ExecuteReader();
        for (var i = 0; i < reader.FieldCount; i++)
            yield return reader.GetName(i);
    }

    /// <summary>
    /// 模型索引双向同步(免迁移,与列同步配套):
    /// - 模型新增的索引(含 [Index] 特性、外键惯例索引与复合索引)自动创建,
    ///   IF NOT EXISTS 保证幂等;唯一索引由引擎强制执行重复检查;
    /// - 模型里删掉的索引自动 DROP。为避免误删用户手工建的索引,
    ///   只回收 EF 惯例命名(IX_ 前缀)且不在当前模型中的索引。
    /// 复合索引按列序创建(引擎以列序构造复合键)。
    ///
    /// 回收面 = 该表<b>全部</b>映射实体(TPH/table-splitting)的聚合 wanted
    /// 集合:按单实体划定回收面时,兄弟实体刚建的索引会被下一个实体的同步
    /// 轮次当作「模型已移除」逐轮互 DROP(含 UNIQUE),并让校验永远过不去、
    /// 每个上下文全量重同步。
    /// </summary>
    public static void SyncIndexes(
        DbConnection conn, string table, IEnumerable<IEntityType> entities)
    {
        var wanted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var created = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var entity in entities)
        {
            foreach (var index in entity.GetIndexes())
            {
                var iname = index.GetDatabaseName();
                if (iname is null) continue;
                wanted.Add(iname);
                var columns = new List<string>();
                foreach (var p in index.Properties)
                {
                    var column = p.GetColumnName(StoreObjectIdentifier.Table(table, entity.GetSchema()));
                    if (column is null) break;
                    columns.Add(column);
                }
                if (columns.Count == 0 || columns.Count != index.Properties.Count) continue;
                // 同名索引(继承层次重复声明等)只建一次;wanted 已聚合,回收
                // 面不受影响。
                if (!created.Add(iname)) continue;
                // 模型声明降序索引:引擎对 CREATE INDEX ... DESC 显式报错,这里
                // 静默建 ASC 会得到一个与模型方向相反的索引——显式失败。
                // IsDescending 在只读优化模型上对"未存储方向"直接抛
                // InvalidOperationException(EF 契约:该情况即升序),按契约捕获。
                bool hasDescending;
                try { hasDescending = index.IsDescending is { } d && d.Any(x => x); }
                catch (InvalidOperationException) { hasDescending = false; }
                if (hasDescending)
                {
                    throw new NotSupportedException(
                        $"descending index {iname} is not supported; declare ascending indexes");
                }
                var columnList = string.Join(", ", columns.Select(Quote));

                try
                {
                    // Unique drift, BOTH directions: an existing same-named
                    // index whose UNIQUE-ness disagrees with the model would
                    // be silently kept by IF NOT EXISTS. Missing UNIQUE
                    // leaves the constraint unenforced; a stale UNIQUE (the
                    // model dropped IsUnique) keeps rejecting legitimate
                    // duplicate values forever. Drop and recreate either way.
                    if (ExistingIndexSql(conn, table, iname) is { } ddl
                        && ddl.TrimStart().StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase)
                            != index.IsUnique)
                    {
                        using var drop = conn.CreateCommand();
                        drop.CommandText = $"DROP INDEX IF EXISTS {Quote(iname)}";
                        drop.ExecuteNonQuery();
                    }
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText =
                        $"CREATE {(index.IsUnique ? "UNIQUE " : "")}INDEX IF NOT EXISTS " +
                        $"{Quote(iname)} ON {Quote(table)} ({columnList})";
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex) when (IsIgnorableDdlError(ex))
                {
                }
            }
        }

        // 模型中已移除的 EF 命名索引:回收(手工/自定义命名索引不动)。
        foreach (var existing in ExistingIndexes(conn, table))
        {
            if (wanted.Contains(existing)) continue;
            if (!existing.StartsWith("IX_", StringComparison.OrdinalIgnoreCase)) continue;
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"DROP INDEX IF EXISTS {Quote(existing)}";
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex) when (IsIgnorableDdlError(ex))
            {
            }
        }
    }

    private static IEnumerable<string> ExistingIndexes(DbConnection conn, string table)
    {
        using var cmd = conn.CreateCommand();
        // tbl_name 按字符串比较:表名是值不是标识符,必须单引号。
        cmd.CommandText =
            $"SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '{table.Replace("'", "''")}'";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            yield return reader.GetString(0);
    }

    /// <summary>命名索引的 DDL(sqlite_master.sql),不存在返回 null。</summary>
    private static string? ExistingIndexSql(DbConnection conn, string table, string indexName)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = '" +
            indexName.Replace("'", "''") + "' AND tbl_name = '" + table.Replace("'", "''") + "'";
        var result = cmd.ExecuteScalar();
        return result is string s && s.Length > 0 ? s : null;
    }

    // Embedded quotes escaped by doubling, same rule as the ADO.NET
    // layer's QuoteIdent; string.Format keeps the nesting readable.
    private static string Quote(string id) => string.Format("\"{0}\"", id.Replace("\"", "\"\""));

    /// <summary>CLR 类型是否映射到引擎的整数自增语义(见调用处的列声明)。</summary>
    private static bool IsIntegerClrType(Type t)
    {
        t = Nullable.GetUnderlyingType(t) ?? t;
        return t == typeof(int) || t == typeof(long) || t == typeof(short)
            || t == typeof(byte) || t == typeof(sbyte)
            || t == typeof(uint) || t == typeof(ulong) || t == typeof(ushort);
    }
}
