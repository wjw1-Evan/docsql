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
        foreach (var entity in model.GetEntityTypes())
        {
            SchemaSync.SyncTable(connection.DbConnection, entity);
            SchemaSync.SyncIndexes(connection.DbConnection, entity);
        }
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

    public bool EnsureDeleted() => false;
    public Task<bool> EnsureDeletedAsync(CancellationToken ct = default) => Task.FromResult(false);

    public bool CanConnect()
    {
        try { connection.Open(); return true; }
        catch { return false; }
    }

    public Task<bool> CanConnectAsync(CancellationToken ct = default)
        => Task.FromResult(CanConnect());
}
