using Aspire.Hosting.ApplicationModel;

namespace Docsql.Aspire.Hosting;

/// <summary>
/// DocSQL Web 控制台容器资源:同镜像以 <c>docsql-web</c> 为 entrypoint 的伴生容器
/// (零存储纯管理工具,数据操作转发到上游 DocSQL 节点),HTTP 端口容器内固定 7700。
/// </summary>
public sealed class DocsqlWebConsoleResource(string name) : ContainerResource(name)
{
    /// <summary>HTTP endpoint 名称(容器内固定 7700)。</summary>
    public const string PrimaryEndpointName = "http";
}
