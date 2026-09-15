// schema 校验:确认模型声明的表/列/命名索引都在库中(子集判定,
// 忽略库中多出的对象)。通过时可跳过整场 SyncModel —— 后者对每条
// 新连接都要逐表「建表探测 + 列探测」与索引同步,119 实体规模下是
// 数百次往返;EF 每个 DbContext 都是一条新连接,不能按此计价。
//
// 校验用两条一次性查询:
//   information_schema.columns  → 全部 表 × 声明列(catalog 直出)
//   sqlite_master(type=index)   → 全部命名索引(自动索引不列出)
//
// 校验失败(缺表/缺列/缺索引,或外部删表)才回落全量同步,
// 语义与逐连接同步一致:外部 DDL 在下一个上下文即被恢复。

using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Docsql.EntityFrameworkCore;

internal static partial class SchemaSync
{
    /// <summary>
    /// 模型要求的表/列/索引是否都已存在。子集判定:库中多出的表/列/索引不影响
    /// 结果(同步本身只增不减,索引回收另走 SyncIndexes)。
    /// </summary>
    public static bool VerifyModel(DbConnection conn, IModel model)
    {
        var expectedTables = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var expectedIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var expectedUniqueIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var entity in model.GetEntityTypes())
        {
            var table = entity.GetTableName();
            if (table is null) continue;
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var p in entity.GetProperties())
            {
                var column = p.GetColumnName(StoreObjectIdentifier.Table(table, entity.GetSchema()));
                if (column is not null) columns.Add(column);
            }
            if (columns.Count == 0) continue;
            expectedTables[table] = columns;
            foreach (var index in entity.GetIndexes())
            {
                if (index.GetDatabaseName() is not { } iname) continue;
                expectedIndexes.Add(iname);
                if (index.IsUnique) expectedUniqueIndexes.Add(iname);
            }
        }
        if (expectedTables.Count == 0) return true;

        var actualColumns = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT table_name, column_name FROM information_schema.columns";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var table = reader.GetString(0);
                var column = reader.GetString(1);
                if (!actualColumns.TryGetValue(table, out var set))
                    actualColumns[table] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                set.Add(column);
            }
        }
        foreach (var (table, columns) in expectedTables)
        {
            if (!actualColumns.TryGetValue(table, out var actual)) return false;
            foreach (var column in columns)
            {
                if (!actual.Contains(column)) return false;
            }
        }

        // 即便模型没有索引,也要取实际索引:模型表上残留的 IX_ 索引属于回收面。
        var actualIndexes = new Dictionary<string, (string Table, string Sql)>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'index'";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                actualIndexes[reader.GetString(0)] = (reader.GetString(1), reader.GetString(2));
            }
        }
        foreach (var iname in expectedIndexes)
        {
            if (!actualIndexes.TryGetValue(iname, out var actual)) return false;
            // unique 漂移(同名但非 UNIQUE)由 SyncIndexes 以「先删后建」修复,
            // 校验必须看得见,否则约束永远不会被强制。
            if (expectedUniqueIndexes.Contains(iname)
                && !actual.Sql.TrimStart().StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase))
                return false;
        }
        // 模型已移除的 EF 命名索引是回收对象(SyncIndexes 只回收模型表上的
        // IX_ 前缀):留在库里即需要整场同步回收。
        foreach (var (name, actual) in actualIndexes)
        {
            if (name.StartsWith("IX_", StringComparison.OrdinalIgnoreCase)
                && expectedTables.ContainsKey(actual.Table)
                && !expectedIndexes.Contains(name))
                return false;
        }
        return true;
    }
}
