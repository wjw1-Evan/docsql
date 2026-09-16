// 模型 → schema 同步:按 EF 模型建表/补列/建索引。
// 拦截器(惰性建表)与 DocsqlDatabaseCreator(EnsureCreated)共用。

using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Docsql.EntityFrameworkCore;

internal static partial class SchemaSync
{
    /// <summary>整个模型的建表/补列/建索引(拦截器与 EnsureCreated 共用)。</summary>
    public static void SyncModel(DbConnection conn, IModel model)
    {
        foreach (var entity in model.GetEntityTypes())
        {
            SyncTable(conn, entity);
            SyncIndexes(conn, entity);
        }
    }

    public static void SyncTable(DbConnection conn, IEntityType entity)
    {
        var table = entity.GetTableName();
        if (table is null) return;

        var pk = entity.FindPrimaryKey();
        var cols = new List<string>();
        var modelColumns = new List<(string Name, string Type)>();
        foreach (var p in entity.GetProperties())
        {
            var column = p.GetColumnName(StoreObjectIdentifier.Table(table, entity.GetSchema()));
            if (column is null) continue;
            var type = p.GetColumnType() ?? "TEXT";
            modelColumns.Add((column, type));
            // 单列值生成主键:仅整数类型自增 —— 引擎把 AUTOINCREMENT 视为
            // 整数 max+1;给 TEXT/GUID 主键加上它会让维护脚本插入整数,
            // EF 物化 Guid.Parse("1") 直接炸。GUID 主键改用引擎的 UUIDv7
            // 自动生成路径(Guid 列本身按 TEXT 建列,不再带 AUTOINCREMENT)。
            if (pk is { Properties.Count: 1 } && pk.Properties[0] == p && p.IsPrimaryKey())
                cols.Add(
                    $"{Quote(column)} {type}{(p.IsNullable ? "" : " NOT NULL")} PRIMARY KEY{(IsIntegerClrType(p.ClrType) ? " AUTOINCREMENT" : "")}");
            else
                cols.Add($"{Quote(column)} {type}{(p.IsNullable ? "" : " NOT NULL")}");
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

        if (!created)
            AddMissingColumns(conn, table, modelColumns);
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
        DbConnection conn, string table, IEnumerable<(string Name, string Type)> modelColumns)
    {
        var existing = new HashSet<string>(ExistingColumns(conn, table), StringComparer.OrdinalIgnoreCase);
        foreach (var (name, type) in modelColumns)
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
        }
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
    /// </summary>
    public static void SyncIndexes(DbConnection conn, IEntityType entity)
    {
        var table = entity.GetTableName();
        if (table is null) return;

        var wanted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
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
            var columnList = string.Join(", ", columns.Select(Quote));

            try
            {
                // Unique drift: an existing index with the same name but
                // without UNIQUE would be silently kept by IF NOT EXISTS,
                // so the constraint never gets enforced. Drop and recreate.
                if (index.IsUnique && ExistingIndexSql(conn, table, iname) is { } ddl
                    && !ddl.TrimStart().StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase))
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
