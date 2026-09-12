using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Docsql.Aspire.Hosting;

/// <summary>
/// DocSQL 的 Aspire hosting 集成入口。镜像 <c>ghcr.io/wjw1-evan/docsql</c> 内置
/// docsql-server / docsql-cli / docsql-web 三个二进制,server 的容器内协议端口固定 7600,
/// 控制台固定 7700;数据库文件路径是 server 的第一个位置参数(/data/docsql.db)。
/// 其余运行时变量(如 DOCSQL_ASYNC_COMMIT、DOCSQL_CATCHUP_WINDOW)用通用
/// WithEnvironment 注入即可。
/// </summary>
public static class DocsqlHostingExtensions
{
    /// <summary>DocSQL 官方多架构镜像(GHCR,amd64/arm64)。</summary>
    public const string ContainerImage = "ghcr.io/wjw1-evan/docsql";

    /// <summary>默认镜像 tag(AppHost 可用 WithImageTag 覆写)。</summary>
    public const string DefaultImageTag = "latest";

    /// <summary>协议端口:容器内固定 7600,镜像 EXPOSE 与 server 默认监听一致。</summary>
    public const int ProtocolPort = 7600;

    /// <summary>Web 控制台端口:容器内固定 7700。</summary>
    public const int WebConsolePort = 7700;

    /// <summary>
    /// 添加单节点 DocSQL 服务器容器。默认生成随机客户端 token(secret 参数,运行期保存到
    /// user secrets);<c>WithToken</c> 可替换为显式参数。数据卷用 <c>WithDataVolume</c> 按需挂载。
    /// </summary>
    public static IResourceBuilder<DocsqlServerResource> AddDocsql(this IDistributedApplicationBuilder builder, string name, int? port = null)
    {
        return AddDocsqlCore(builder, name, port, token: null);
    }

    /// <summary>集群内节点:复用共享 token 参数,避免每个节点留下孤儿生成的 token 参数。</summary>
    private static IResourceBuilder<DocsqlServerResource> AddDocsqlCore(IDistributedApplicationBuilder builder, string name, int? port, IResourceBuilder<ParameterResource>? token)
    {
        token ??= builder.AddParameter($"{name}-token", new GenerateParameterDefault(), secret: true);

        var resource = new DocsqlServerResource(name);
        var resourceBuilder = builder.AddResource(resource)
            .WithImage(ContainerImage)
            .WithImageTag(DefaultImageTag)
            .WithEndpoint(targetPort: ProtocolPort, port: port, name: DocsqlServerResource.PrimaryEndpointName)
            .WithArgs(context =>
            {
                // 镜像 ENTRYPOINT 已是 docsql-server,这里只给位置参数:<db 文件> <监听地址>。
                context.Args.Add(DocsqlServerResource.DatabasePath);
                context.Args.Add($"0.0.0.0:{ProtocolPort}");
            })
            .WithHealthCheckCore(name);

        return resourceBuilder.WithToken(token);
    }

    /// <summary>
    /// 挂载命名数据卷到 /data(容器内数据库文件所在目录),容器重建后数据保留。
    /// 未指定卷名时生成与应用路径绑定的稳定卷名。
    /// </summary>
    public static IResourceBuilder<DocsqlServerResource> WithDataVolume(this IResourceBuilder<DocsqlServerResource> builder, string? volumeName = null)
    {
        return builder.WithVolume(volumeName ?? VolumeNameGenerator.Generate(builder, "data"), "/data");
    }

    /// <summary>替换客户端 token 参数(同时更新连接串与 DOCSQL_TOKEN 注入)。</summary>
    public static IResourceBuilder<DocsqlServerResource> WithToken(this IResourceBuilder<DocsqlServerResource> builder, IResourceBuilder<ParameterResource> token)
    {
        builder.Resource.TokenParameter = token.Resource;
        return builder.WithEnvironment("DOCSQL_TOKEN", token);
    }

    /// <summary>注入节点间复制凭据 DOCSQL_CLUSTER_TOKEN(集群各节点必须同值)。</summary>
    public static IResourceBuilder<DocsqlServerResource> WithClusterToken(this IResourceBuilder<DocsqlServerResource> builder, IResourceBuilder<ParameterResource> clusterToken)
    {
        return builder.WithEnvironment("DOCSQL_CLUSTER_TOKEN", clusterToken);
    }

    /// <summary>
    /// 注入对等节点表 DOCSQL_PEERS(逗号分隔 host:port,值随各节点 endpoint 展开),
    /// 节点间互扇出写入,形成对称集群。
    /// </summary>
    public static IResourceBuilder<DocsqlServerResource> WithPeers(this IResourceBuilder<DocsqlServerResource> builder, params IReadOnlyList<IResourceBuilder<DocsqlServerResource>> peers)
    {
        if (peers.Count == 0)
        {
            return builder;
        }

        var expression = new ReferenceExpressionBuilder();
        for (var i = 0; i < peers.Count; i++)
        {
            if (i > 0)
            {
                expression.AppendLiteral(",");
            }
            var endpoint = peers[i].Resource.GetEndpoint(DocsqlServerResource.PrimaryEndpointName);
            expression.Append($"{endpoint.Property(EndpointProperty.HostAndPort)}");
        }
        return builder.WithEnvironment("DOCSQL_PEERS", expression.Build());
    }

    /// <summary>
    /// 为该节点添加 Web 控制台伴生容器(同镜像 docsql-web entrypoint,上游指向本节点),
    /// 命名 {节点名}-console;返回原 server builder 以便继续链式配置。
    /// 控制台账号门默认开启(凭据存于 /auth 卷);镜像与 server 严格同版本。
    /// </summary>
    public static IResourceBuilder<DocsqlServerResource> WithWebConsole(this IResourceBuilder<DocsqlServerResource> builder, int? port = null, string? consoleName = null)
    {
        var image = builder.Resource.Annotations.OfType<ContainerImageAnnotation>().Single();
        var console = new DocsqlWebConsoleResource(consoleName ?? $"{builder.Resource.Name}-console");
        var consoleBuilder = builder.ApplicationBuilder.AddResource(console);
        consoleBuilder = image.Registry is { } registry
            ? consoleBuilder.WithImage(registry, image.Image)
            : consoleBuilder.WithImage(image.Image);
        consoleBuilder = consoleBuilder
            .WithImageTag(image.Tag ?? DefaultImageTag)
            .WithEntrypoint("docsql-web")
            .WithEndpoint(targetPort: WebConsolePort, port: port, name: DocsqlWebConsoleResource.PrimaryEndpointName)
            .WithArgs(context =>
            {
                // docsql-web <管理节点> <监听地址>:上游即本 server 的 host:port。
                var endpoint = builder.Resource.GetEndpoint(DocsqlServerResource.PrimaryEndpointName);
                context.Args.Add(endpoint.Property(EndpointProperty.HostAndPort));
                context.Args.Add($"0.0.0.0:{WebConsolePort}");
            })
            .WithEnvironment("DOCSQL_WEB_AUTH_FILE", "/auth/console-auth.json");
        consoleBuilder.WithVolume(VolumeNameGenerator.Generate(consoleBuilder, "auth"), "/auth");

        if (builder.Resource.TokenParameter is { } token)
        {
            consoleBuilder.WithEnvironment("DOCSQL_TOKEN", token);
        }
        return builder;
    }

    /// <summary>
    /// 添加 nodeCount 个互为对等的 DocSQL 节点(对称集群:DOCSQL_PEERS 互扇出、共享
    /// 客户端 token 与节点间 token,各节点独立数据卷),返回以主节点为入口的集群句柄。
    /// </summary>
    public static DocsqlCluster AddDocsqlCluster(this IDistributedApplicationBuilder builder, string name, int nodeCount = 3, int? port = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(nodeCount, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(nodeCount, 8);

        var token = builder.AddParameter($"{name}-token", new GenerateParameterDefault(), secret: true);
        var clusterToken = builder.AddParameter($"{name}-cluster-token", new GenerateParameterDefault(), secret: true);

        var nodes = new List<IResourceBuilder<DocsqlServerResource>>();
        for (var i = 0; i < nodeCount; i++)
        {
            var nodeName = $"{name}-{(char)('a' + i)}";
            var node = AddDocsqlCore(builder, nodeName, port: null, token)
                .WithClusterToken(clusterToken)
                .WithDataVolume();
            // 指定端口只作用于主节点,其余节点走宿主随机端口。
            if (i == 0 && port is { } primaryPort)
            {
                node.WithEndpoint(targetPort: ProtocolPort, port: primaryPort, name: DocsqlServerResource.PrimaryEndpointName);
            }
            nodes.Add(node);
        }

        foreach (var node in nodes)
        {
            node.WithPeers(nodes.Where(peer => !ReferenceEquals(peer, node)).ToList());
        }
        return new DocsqlCluster(nodes[0], nodes, token, clusterToken);
    }

    /// <summary>注册 TCP 探活健康检查并关联到资源(dashboard 显示健康态,WaitFor 依赖它)。</summary>
    private static IResourceBuilder<DocsqlServerResource> WithHealthCheckCore(this IResourceBuilder<DocsqlServerResource> builder, string resourceName)
    {
        var healthCheckName = $"{resourceName}-tcp";
        builder.ApplicationBuilder.Services.AddHealthChecks()
            .AddCheck(healthCheckName, new DocsqlServerHealthCheck(builder.Resource), tags: ["docsql"]);
        return builder.WithHealthCheck(healthCheckName);
    }
}
