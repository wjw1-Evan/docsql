// 惰性建表 + 免迁移:像 EF MongoDB 提供程序那样,不需要 EnsureCreated,
// 模型加字段也不需要迁移。
//
// 命令拦截器在每条连接的首个命令前调用 SchemaSync(建表/补列/建索引),
// 之后正常执行原命令。用户代码只管 new DbContext + LINQ。

using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Docsql.EntityFrameworkCore;

internal sealed class DocsqlAutoCreateInterceptor : DbCommandInterceptor
{
    // 每条 (连接, 模型) 组合只做一次建表检查(EF 默认每个 DbContext 一条
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
            command.Connection!,
            static _ => new ConditionalWeakTable<object, object>());
        if (done.TryGetValue(model, out _)) return;

        SchemaSync.SyncModel(command.Connection!, model);
        done.GetValue(model, static _ => new object());
    }
}
