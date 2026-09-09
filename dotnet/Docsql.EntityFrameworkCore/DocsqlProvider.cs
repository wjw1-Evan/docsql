// 原生 DocSQL EF Core 提供程序入口:不借壳 SQLite,仅依赖
// Microsoft.EntityFrameworkCore.Relational 的通用管线。
//
// SQL 生成(Infrastructure/)、更新执行、类型映射、EnsureCreated 全部
// 由本程序集提供;连接走 Docsql.Client 的 TCP 二进制协议。

using Docsql.Client;
using Microsoft.EntityFrameworkCore;
using Docsql.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Docsql.EntityFrameworkCore;

public static class DocsqlDbContextOptionsExtensions
{
    /// <summary>连接 DocSQL:连接串 "host=..;port=..;token=..;key=.."。</summary>
    public static DbContextOptionsBuilder UseDocsql(
        this DbContextOptionsBuilder options,
        string connectionString)
    {
        var extension = (options.Options.FindExtension<DocsqlOptionsExtension>()
                ?? new DocsqlOptionsExtension())
            .WithConnectionString(connectionString);
        ((IDbContextOptionsBuilderInfrastructure)options).AddOrUpdateExtension(extension);
        return options.AddInterceptors(new DocsqlAutoCreateInterceptor());
    }

    /// <summary>用已有连接。</summary>
    public static DbContextOptionsBuilder UseDocsql(
        this DbContextOptionsBuilder options,
        DocsqlConnection connection)
    {
        var extension = (options.Options.FindExtension<DocsqlOptionsExtension>()
                ?? new DocsqlOptionsExtension())
            .WithConnection(connection);
        ((IDbContextOptionsBuilderInfrastructure)options).AddOrUpdateExtension(extension);
        return options.AddInterceptors(new DocsqlAutoCreateInterceptor());
    }
}
