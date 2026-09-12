using System.Net.Sockets;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Docsql.Aspire.Hosting;

/// <summary>
/// AppHost 侧资源健康检查:对 server endpoint 做 TCP 连接探活
/// (与 deploy compose 的 healthcheck 同语义:端口能建立连接即视为就绪)。
/// </summary>
internal sealed class DocsqlServerHealthCheck(DocsqlServerResource resource) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var endpoint = resource.GetEndpoint(DocsqlServerResource.PrimaryEndpointName);
            var expression = ReferenceExpression.Create($"{endpoint.Property(EndpointProperty.HostAndPort)}");
            var hostPort = await expression.GetValueAsync(cancellationToken) ?? string.Empty;
            var separator = hostPort.LastIndexOf(':');
            if (separator <= 0)
            {
                return HealthCheckResult.Unhealthy($"DocSQL endpoint '{resource.Name}' has no allocated host:port yet.");
            }

            using var client = new TcpClient();
            await client.ConnectAsync(hostPort[..separator], int.Parse(hostPort[(separator + 1)..]), cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy($"DocSQL endpoint probe for '{resource.Name}' failed.", ex);
        }
    }
}
