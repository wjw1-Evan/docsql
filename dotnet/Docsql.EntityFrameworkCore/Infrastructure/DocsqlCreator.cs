// EnsureCreated:表由惰性建表拦截器按模型创建(类似 EF MongoDB);
// 这里只需把模型索引同步一遍,让 EnsureCreated 后索引立即可用。

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlDatabaseCreator(
    IRelationalConnection connection,
    IModel model)
    : IRelationalDatabaseCreator
{
    public bool HasTables() => true;
    public Task<bool> HasTablesAsync(CancellationToken ct = default) => Task.FromResult(true);
    public bool Exists() => false;
    public Task<bool> ExistsAsync(CancellationToken ct = default) => Task.FromResult(false);
    public void Create() { }
    public Task CreateAsync(CancellationToken ct = default) => Task.CompletedTask;

    public void Delete() => throw new NotSupportedException("DocSQL: 请用 DROP TABLE 管理对象");
    public Task DeleteAsync(CancellationToken ct = default) => throw new NotSupportedException("DocSQL: 请用 DROP TABLE 管理对象");

    public void CreateTables()
    {
        if (connection.DbConnection.State != System.Data.ConnectionState.Open)
        {
            connection.DbConnection.Open();
        }
        SchemaSync.SyncModel(connection.DbConnection, model);
    }

    public Task CreateTablesAsync(CancellationToken ct = default)
    {
        CreateTables();
        return Task.CompletedTask;
    }

    public string GenerateCreateScript() => string.Empty;
    public Task<string> GenerateCreateScriptAsync(CancellationToken ct = default) => Task.FromResult(string.Empty);

    public bool EnsureCreated()
    {
        CreateTables();
        return true;
    }

    public async Task<bool> EnsureCreatedAsync(CancellationToken ct = default)
    {
        await CreateTablesAsync(ct);
        return true;
    }

    public bool EnsureDeleted() =>
        throw new NotSupportedException("DocSQL: 请用 DROP TABLE 管理对象");
    public Task<bool> EnsureDeletedAsync(CancellationToken ct = default) =>
        throw new NotSupportedException("DocSQL: 请用 DROP TABLE 管理对象");

    public bool CanConnect()
    {
        // Close on BOTH paths: a probe that leaves the pooled connection
        // checked out pins a pool slot until the context is disposed.
        try
        {
            connection.Open();
            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            try { connection.Close(); } catch { /* best effort */ }
        }
    }

    public async Task<bool> CanConnectAsync(CancellationToken ct = default)
    {
        // 真异步:健康检查每轮探测都走这里,同步 Open()(TCP 建连+认证,
        // 上限 15s)会在线程池上阻塞一个线程;节点宕机时的高频探测把
        // 线程饥饿放大给整个服务。
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            connection.Close();
        }
    }
}
