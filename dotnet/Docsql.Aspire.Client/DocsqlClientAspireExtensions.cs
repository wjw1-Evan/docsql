using Docsql.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace Docsql.Aspire.Client;

/// <summary>
/// DocSQL 的 Aspire client 集成:从 <c>ConnectionStrings:{name}</c>(hosting 集成经
/// WithReference 注入)注册 <see cref="DocsqlConnection"/>(transient,连接池在
/// Docsql.Client 内部),并按需注册健康检查。
/// </summary>
public static class DocsqlClientAspireExtensions
{
    /// <summary>
    /// 注册 <see cref="DocsqlConnection"/> 连接 <c>ConnectionStrings:{name}</c> 指向的 DocSQL 节点。
    /// </summary>
    /// <param name="builder">宿主构建器。</param>
    /// <param name="name">连接名(与 AppHost 的资源名/WithReference 注入名一致)。</param>
    /// <param name="configure">可调项(健康检查开关/命名)。</param>
    public static void AddDocsqlConnection(this IHostApplicationBuilder builder, string name, Action<DocsqlAspireSettings>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(name);

        var settings = new DocsqlAspireSettings();
        configure?.Invoke(settings);

        var connectionString = builder.Configuration.GetConnectionString(name)
            ?? throw new InvalidOperationException($"Connection string '{name}' was not found. Add the DocSQL resource to your AppHost and call WithReference to inject it as ConnectionStrings__{name}.");

        builder.Services.AddTransient<DocsqlConnection>(_ => new DocsqlConnection(connectionString));

        if (!settings.DisableHealthChecks)
        {
            var healthCheckName = settings.HealthCheckName ?? $"Docsql_{name}";
            builder.Services.AddHealthChecks()
                .AddCheck(healthCheckName, new DocsqlConnectionHealthCheck(connectionString), tags: ["docsql"]);
        }
    }
}
