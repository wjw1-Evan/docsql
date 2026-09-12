using Aspire.Hosting.ApplicationModel;

namespace Docsql.Aspire.Hosting;

/// <summary>
/// <see cref="DocsqlHostingExtensions.AddDocsqlCluster"/> 的返回句柄:一组互为对等节点的
/// DocSQL 服务器(对称集群,任意节点可写,写入自动扇出到其余节点)。
/// </summary>
public sealed class DocsqlCluster(
    IResourceBuilder<DocsqlServerResource> primary,
    IReadOnlyList<IResourceBuilder<DocsqlServerResource>> nodes,
    IResourceBuilder<ParameterResource> token,
    IResourceBuilder<ParameterResource> clusterToken)
{
    /// <summary>主节点(连接串与 WithReference 的默认入口)。</summary>
    public IResourceBuilder<DocsqlServerResource> Primary { get; } = primary;

    /// <summary>全部节点(命名 {cluster}-a、{cluster}-b、…)。</summary>
    public IReadOnlyList<IResourceBuilder<DocsqlServerResource>> Nodes { get; } = nodes;

    /// <summary>共享的客户端 token 参数(注入各节点 DOCSQL_TOKEN)。</summary>
    public IResourceBuilder<ParameterResource> Token { get; } = token;

    /// <summary>共享的节点间 token 参数(注入各节点 DOCSQL_CLUSTER_TOKEN)。</summary>
    public IResourceBuilder<ParameterResource> ClusterToken { get; } = clusterToken;
}
