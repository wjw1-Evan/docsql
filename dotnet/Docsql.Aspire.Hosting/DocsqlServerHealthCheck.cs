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
            // Host and Port separately: `HostAndPort` renders IPv6 as
            // `[::1]:port`, and passing the bracketed literal to
            // TcpClient.ConnectAsync fails DNS parsing.
            var hostExpr = ReferenceExpression.Create($"{endpoint.Property(EndpointProperty.Host)}");
            var portExpr = ReferenceExpression.Create($"{endpoint.Property(EndpointProperty.Port)}");
            var host = (await hostExpr.GetValueAsync(cancellationToken) ?? string.Empty).Trim('[', ']');
            var portText = await portExpr.GetValueAsync(cancellationToken) ?? string.Empty;
            if (host.Length == 0 || !int.TryParse(portText, out var port) || port is <= 0 or > 65535)
            {
                return HealthCheckResult.Unhealthy(
                    $"DocSQL endpoint '{resource.Name}' has no allocated host:port yet.");
            }

            using var client = new TcpClient();
            await client.ConnectAsync(host, port, cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy($"DocSQL endpoint probe for '{resource.Name}' failed.", ex);
        }
    }
}
