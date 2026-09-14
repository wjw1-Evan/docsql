// 惰性建表 + 免迁移:像 EF MongoDB 提供程序那样,不需要 EnsureCreated,
// 模型加字段也不需要迁移。
//
// 命令拦截器在每条连接的首个命令前做一次廉价 schema 校验(SchemaSync.
// VerifyModel,两条一次性查询);校验发现缺表/缺列/缺索引才整场同步
// (建表/补列/建索引,数百次往返),之后正常执行原命令。用户代码只管
// new DbContext + LINQ。校验通过时新上下文成本 = 2 次查询,而不是
// 每次全量同步 —— EF 每个 DbContext 都是一条新连接,全量同步不能按
// 连接计价(Web 每请求一次上下文)。

using System.Collections.Concurrent;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Docsql.EntityFrameworkCore;

internal sealed class DocsqlAutoCreateInterceptor : DbCommandInterceptor
{
    // 每条 (连接, 模型) 组合只做一次校验/同步(EF 默认每个 DbContext 一条
    // 连接,但 UseDocsql(DocsqlConnection) 允许多个不同模型的上下文共用
    // 一条连接 —— 只按连接记账会让第二个模型的表永远建不上)。
    private static readonly ConditionalWeakTable<DbConnection, ConditionalWeakTable<object, object>>
        Done = new();

    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        EnsureTables(command, eventData);
        return result;
    }

    public override InterceptionResult<int> NonQueryExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        EnsureTables(command, eventData);
        return result;
    }

    public override InterceptionResult<object> ScalarExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        EnsureTables(command, eventData);
        return result;
    }

    // Async commands dispatch to the *Async hooks only — without these
    // overrides ToListAsync/SaveChangesAsync would skip schema sync entirely
    // and fail with "table does not exist" on fresh databases.
    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        EnsureTables(command, eventData);
        return ValueTask.FromResult(result);
    }

    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
        DbCommand command, CommandEventData eventData, InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        EnsureTables(command, eventData);
        return ValueTask.FromResult(result);
    }

    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(
        DbCommand command, CommandEventData eventData, InterceptionResult<object> result,
        CancellationToken cancellationToken = default)
    {
        EnsureTables(command, eventData);
        return ValueTask.FromResult(result);
    }

    private static void EnsureTables(DbCommand command, CommandEventData eventData)
    {
        if (eventData.Context?.Model is not { } model) return;
        if (command.Connection is not { } connection) return;
        // 显式 schema 管理时让位(CREATE/DROP/ALTER/PRAGMA/事务控制/
        // sqlite_master 探测):拦截器若抢跑,显式 DDL 会撞 already exists。
        var text = command.CommandText?.TrimStart();
        if (text is not null
            && (text.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase)
                || text.StartsWith("DROP", StringComparison.OrdinalIgnoreCase)
                || text.StartsWith("ALTER", StringComparison.OrdinalIgnoreCase)
                || text.StartsWith("PRAGMA", StringComparison.OrdinalIgnoreCase)
                || text.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase)
                || text.StartsWith("COMMIT", StringComparison.OrdinalIgnoreCase)
                || text.StartsWith("ROLLBACK", StringComparison.OrdinalIgnoreCase)
                || command.CommandText.Contains(
                    "sqlite_master", StringComparison.OrdinalIgnoreCase)))
            return;
        var done = Done.GetValue(
            connection,
            static _ => new ConditionalWeakTable<object, object>());
        if (done.TryGetValue(model, out _)) return;

        // 先校验后同步:模型对象齐备时零 DDL;外部删表/加字段在下一个
        // 上下文被校验发现,语义与逐连接全量同步一致。
        if (!SchemaSync.VerifyModel(connection, model))
        {
            SchemaSync.SyncModel(connection, model);
            SchemaSyncAccounting.RecordFullSync(connection.ConnectionString ?? string.Empty);
        }
        done.GetValue(model, static _ => new object());
    }
}

/// <summary>
/// schema 同步记账(测试与诊断用):按连接串键控的全量同步次数。
/// 不参与判定,只做观测 —— 校验路径是否生效可直接断言。
/// </summary>
internal static class SchemaSyncAccounting
{
    private static readonly ConcurrentDictionary<string, int> FullSyncs = new(StringComparer.Ordinal);

    internal static void RecordFullSync(string connectionString) =>
        FullSyncs.AddOrUpdate(connectionString, 1, static (_, v) => v + 1);

    internal static int FullSyncCount(string connectionString) =>
        FullSyncs.TryGetValue(connectionString, out var v) ? v : 0;

    internal static void Reset(string connectionString) =>
        FullSyncs.TryRemove(connectionString, out _);
}
