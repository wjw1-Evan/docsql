using Docsql.Client;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Docsql.Aspire.Client;

/// <summary>
/// 消费侧健康检查:真实打开一条连接并执行 SELECT 1(匿名/token/用户登录
/// 均由 Docsql.Client 握手层处理)。
/// </summary>
internal sealed class DocsqlConnectionHealthCheck(string connectionString) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new DocsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            await command.ExecuteScalarAsync(cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("DocSQL connection check failed.", ex);
        }
    }
}
