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

    /// <summary>
    /// 运行期连接串工厂:<b>每次创建物理连接时</b>调用,而不是在构建 options 时定型。
    /// 适用于连接目标随上下文变化的宿主(测试夹具把每个用例路由到独立节点、读写入口
    /// 切换等)。连接信息不进入 EF 模型/服务提供程序缓存键,同一进程内模型只建一次。
    /// </summary>
    public static DbContextOptionsBuilder UseDocsql(
        this DbContextOptionsBuilder options,
        Func<string> connectionStringFactory)
    {
        var extension = (options.Options.FindExtension<DocsqlOptionsExtension>()
                ?? new DocsqlOptionsExtension())
            .WithConnectionStringFactory(connectionStringFactory);
        ((IDbContextOptionsBuilderInfrastructure)options).AddOrUpdateExtension(extension);
        return options.AddInterceptors(new DocsqlAutoCreateInterceptor());
    }
}
