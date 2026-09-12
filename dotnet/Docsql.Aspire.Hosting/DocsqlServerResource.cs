using Aspire.Hosting.ApplicationModel;

namespace Docsql.Aspire.Hosting;

/// <summary>
/// DocSQL 服务器容器资源。镜像 <c>ghcr.io/wjw1-evan/docsql</c>(ENTRYPOINT 已是
/// <c>docsql-server</c>),容器内协议端口固定 7600,数据库文件固定 /data/docsql.db。
/// </summary>
public sealed class DocsqlServerResource(string name) : ContainerResource(name), IResourceWithConnectionString
{
    /// <summary>协议端口的 endpoint 名称(TCP,容器内固定 7600)。</summary>
    public const string PrimaryEndpointName = "tcp";

    /// <summary>容器内数据库文件路径(server 第一个位置参数,配 WithDataVolume 的挂载点)。</summary>
    public const string DatabasePath = "/data/docsql.db";

    /// <summary>客户端 token 参数;AddDocsql 默认生成随机值,WithToken 可替换。</summary>
    internal ParameterResource? TokenParameter { get; set; }

    /// <summary>连接串(host/port 取 endpoint 展开值,配置了 token 则追加 token 段)。</summary>
    public ReferenceExpression ConnectionStringExpression
    {
        get
        {
            var endpoint = new EndpointReference(this, PrimaryEndpointName);
            var builder = new ReferenceExpressionBuilder();
            builder.Append($"host={endpoint.Property(EndpointProperty.Host)}");
            builder.AppendLiteral(";");
            builder.Append($"port={endpoint.Property(EndpointProperty.Port)}");
            if (TokenParameter is { } token)
            {
                builder.AppendLiteral(";");
                builder.Append($"token={token}");
            }
            return builder.Build();
        }
    }
}
