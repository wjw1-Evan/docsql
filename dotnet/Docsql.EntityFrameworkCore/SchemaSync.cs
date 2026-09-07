// 模型 → schema 同步:按 EF 模型建表/补列/建索引。
// 拦截器(惰性建表)与 DocsqlDatabaseCreator(EnsureCreated)共用。

using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Docsql.EntityFrameworkCore;

internal static partial class SchemaSync
{
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
            // 单列值生成主键:整数自增,其余 NOT NULL 照常声明。
            if (pk is { Properties.Count: 1 } && pk.Properties[0] == p && p.IsPrimaryKey())
                cols.Add($"{Quote(column)} {type} PRIMARY KEY AUTOINCREMENT");
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

    /// <summary>表已存在,或连的是只读副本(表由复制通道创建)。</summary>
    private static bool IsIgnorableDdlError(Exception ex) =>
        ex.Message.Contains("already exists") || ex.Message.Contains("read-only");

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
    /// - 模型新增的索引(含 [Index] 特性与外键惯例索引)自动创建,
    ///   IF NOT EXISTS 保证幂等;唯一索引由引擎强制执行重复检查;
    /// - 模型里删掉的索引自动 DROP。为避免误删用户手工建的索引,
    ///   只回收 EF 惯例命名(IX_ 前缀)且不在当前模型中的索引。
    /// 引擎目前只支持单列索引;多列索引按"尽力而为"跳过 —— 索引是
    /// 加速手段,除唯一索引的约束语义外不影响查询结果。
    /// </summary>
    public static void SyncIndexes(DbConnection conn, IEntityType entity)
    {
        var table = entity.GetTableName();
        if (table is null) return;

        var wanted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var index in entity.GetIndexes())
        {
            if (index.Properties.Count != 1) continue;
            var column = index.Properties[0]
                .GetColumnName(StoreObjectIdentifier.Table(table, entity.GetSchema()));
            if (column is null) continue;

            var iname = index.GetDatabaseName();
            if (iname is null) continue;
            wanted.Add(iname);
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"CREATE {(index.IsUnique ? "UNIQUE " : "")}INDEX IF NOT EXISTS " +
                    $"{Quote(iname)} ON {Quote(table)} ({Quote(column)})";
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

    private static string Quote(string id) => $"\"{id}\"";
}
