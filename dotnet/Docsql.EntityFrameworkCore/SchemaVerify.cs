using System.Linq;
// schema 校验:确认模型声明的表/列/命名索引都在库中(子集判定,
// 忽略库中多出的对象),且模型主键列按提供程序形状强制(NOT NULL,
// 弱判定 —— 引擎 SQL 面不暴露 PK 本身,见 VerifyModel 注释)。
// 通过时可跳过整场 SyncModel —— 后者对每条新连接都要逐表「建表探测 +
// 列探测」与索引同步,119 实体规模下是数百次往返;EF 每个 DbContext 都
// 是一条新连接,不能按此计价。
//
// 校验用两条一次性查询:
//   information_schema.columns  → 全部 表 × 声明列 × is_nullable(catalog 直出)
//   sqlite_master(type=index)   → 全部命名索引(自动索引不列出)
//
// 校验失败(缺表/缺列/缺索引/主键未强制,或外部删表)才回落全量同步,
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
    /// 另核主键列的强制形状:引擎的 SQL 面不暴露 PK 存在性(sqlite_master 表行
    /// 的 sql 是空文本、自动索引不出列),提供程序建表形状是「非空主键列
    /// NOT NULL PRIMARY KEY」—— 以 is_nullable=NO 为弱判定。此前只比列名子集,
    /// 外部建的无 PK 同名表(列名恰好齐)被判齐备,EF 的身份/唯一性保证静默
    /// 失效。判据与 SyncTable 完全同一(门禁一致):校验失败 ⇒ 全量同步能收敛
    /// (SyncTable 走保数据重建)。
    /// </summary>
    public static bool VerifyModel(DbConnection conn, IModel model)
    {
        var expectedTables = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var expectedIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var expectedUniqueIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        // 期望的非空主键列(table → pk column):取该表首个声明单列非空主键的
        // 实体(TPH 层次共享同一主键);可空主键(PK ≠ NOT NULL)与无键实体
        // 无法在 SQL 面区分,两侧(verify/sync)同样跳过。
        var expectedPkColumns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // Index column lists (and owning table), in model order: with an
        // explicit HasDatabaseName EF keeps the same index name when its
        // definition changes, so a name-only check left the stale index
        // (wrong columns / wrong uniqueness) enforced forever.
        var expectedIndexColumns = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var expectedIndexTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
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
            // Union, not overwrite: owned types (OwnsOne) and sibling entity
            // types can map onto the same table, and the old assignment let
            // one type's column set hide the other's — a property added to
            // the owner was never detected and SyncModel never ran.
            if (expectedTables.TryGetValue(table, out var existing))
            {
                existing.UnionWith(columns);
            }
            else
            {
                expectedTables[table] = columns;
            }
            if (!expectedPkColumns.ContainsKey(table)
                && entity.FindPrimaryKey() is { Properties.Count: 1 } pk
                && !pk.Properties[0].IsNullable)
            {
                var pkColumn = pk.Properties[0].GetColumnName(
                    StoreObjectIdentifier.Table(table, entity.GetSchema()));
                if (pkColumn is not null)
                {
                    expectedPkColumns[table] = pkColumn;
                }
            }
            foreach (var index in entity.GetIndexes())
            {
                if (index.GetDatabaseName() is not { } iname) continue;
                expectedIndexes.Add(iname);
                if (index.IsUnique) expectedUniqueIndexes.Add(iname);
                // 引擎只有 ASC 索引,模型声明 IsDescending 由 SyncIndexes 显式
                // 抛 NotSupportedException(红线:DESC 必须响亮报错)。校验阶段
                // 看不见方向:存量库里同名 ASC 索引会让校验通过、整场同步被
                // 跳过,降序声明被静默忽略 —— 声明了降序就回落全量同步。
                // IsDescending 在只读优化模型上对"未存储方向"直接抛
                // InvalidOperationException(EF 契约:该情况即升序),按
                // SchemaSync 同一契约捕获。
                bool hasDescending;
                try
                {
                    hasDescending = index.IsDescending is { } d && d.Any(x => x);
                }
                catch (InvalidOperationException)
                {
                    hasDescending = false;
                }
                if (hasDescending)
                {
                    return false;
                }
                var idxCols = new List<string>();
                foreach (var p2 in index.Properties)
                {
                    var column = p2.GetColumnName(
                        StoreObjectIdentifier.Table(table, entity.GetSchema()));
                    if (column is not null) idxCols.Add(column);
                }
                expectedIndexColumns[iname] = idxCols;
                expectedIndexTable[iname] = table;
            }
        }
        if (expectedTables.Count == 0) return true;

        var actualColumns = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var actualNotNullColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText =
                "SELECT table_name, column_name, is_nullable FROM information_schema.columns";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var table = reader.GetString(0);
                var column = reader.GetString(1);
                var nullable = reader.GetString(2);
                if (nullable.Equals("NO", StringComparison.OrdinalIgnoreCase))
                {
                    // 同库内表名+列名组合不区分大小写(引擎标识符语义)。
                    actualNotNullColumns.Add(table + "\u0001" + column);
                }
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
        foreach (var (table, pkColumn) in expectedPkColumns)
        {
            // 主键列缺失或可空 = 未按提供程序形状强制(外部无 PK 裸表),
            // 与 SyncTable 的 PkColumnEnforced 同判据。
            if (!actualNotNullColumns.Contains(table + "\u0001" + pkColumn)) return false;
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
            // 反向漂移:实际仍是 UNIQUE 而模型已取消 IsUnique —— 免迁移
            // 部署的常规操作。校验看不到它,旧约束就永远强制(合法的
            // 重复值写入持续报 UNIQUE 冲突,且每个上下文都判「已收敛」)。
            if (!expectedUniqueIndexes.Contains(iname)
                && actual.Sql.TrimStart().StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase))
                return false;
            // 表与列序漂移:同名索引换了表/列时 EF 视为同一索引,只有比较
            // sqlite_master 的 SQL 才能发现(否则旧约束永远留着)。
            if (expectedIndexTable.TryGetValue(iname, out var expectedTable)
                && !string.Equals(actual.Table, expectedTable, StringComparison.OrdinalIgnoreCase))
                return false;
            if (expectedIndexColumns.TryGetValue(iname, out var expectedCols))
            {
                var actualCols = ParseIndexColumns(actual.Sql);
                if (actualCols is null
                    || !actualCols.SequenceEqual(expectedCols, StringComparer.OrdinalIgnoreCase))
                    return false;
            }
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

    /// <summary>Extract the column list from a `CREATE INDEX ... (a, b)` SQL
    /// text, in order; null when the shape is unexpected.</summary>
    private static List<string>? ParseIndexColumns(string sql)
    {
        int open = sql.LastIndexOf('(');
        int close = sql.LastIndexOf(')');
        if (open < 0 || close <= open) return null;
        return sql[(open + 1)..close]
            .Split(',')
            .Select(c => c.Trim().Trim('"', '`').Trim())
            .Where(c => c.Length > 0)
            .ToList();
    }
}
