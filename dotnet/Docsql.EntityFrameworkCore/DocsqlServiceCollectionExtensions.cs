using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Docsql.EntityFrameworkCore;

/// <summary>
/// 容器级(ASP.NET Core / Worker 宿主)注册入口,与 Aspire client 集成配套使用:
/// 连接串由 AppHost 经 <c>ConnectionStrings:{connectionName}</c> 注入,这里按名取出后
/// 走与 <see cref="DocsqlDbContextOptionsExtensions.UseDocsql(Microsoft.EntityFrameworkCore.DbContextOptionsBuilder,string)"/>
/// 完全相同的管线(含隐式建表拦截器)。
/// </summary>
public static class DocsqlServiceCollectionExtensions
{
    /// <summary>
    /// 注册 <typeparamref name="TContext"/>,连接串取自 <c>ConnectionStrings:{connectionName}</c>。
    /// </summary>
    /// <param name="services">服务集合。</param>
    /// <param name="connectionName">连接名(与 AppHost 的资源名/WithReference 注入名一致)。</param>
    /// <param name="optionsAction">追加的选项配置(在 UseDocsql 之后执行,可继续定制)。</param>
    public static IServiceCollection AddDocsqlDbContext<TContext>(
        this IServiceCollection services,
        string connectionName,
        Action<IServiceProvider, DbContextOptionsBuilder>? optionsAction = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(connectionName);

        services.AddDbContext<TContext>((serviceProvider, options) =>
        {
            var connectionString = serviceProvider.GetRequiredService<IConfiguration>().GetConnectionString(connectionName)
                ?? throw new InvalidOperationException($"Connection string '{connectionName}' was not found. Add the DocSQL resource to your AppHost and call WithReference to inject it as ConnectionStrings__{connectionName}.");
            options.UseDocsql(connectionString);
            optionsAction?.Invoke(serviceProvider, options);
        });
        return services;
    }
}
