using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Docsql.Aspire.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Docsql.Aspire.Hosting.Tests;

/// <summary>
/// 资源模型快照测试:内存建模(Publish 视角,不起容器、不连 docker),
/// 断言镜像/参数/端口/卷/环境变量/连接串表达式的展开形状。
/// </summary>
public class DocsqlHostingTests
{
    private static (DistributedApplication App, T Resource) Build<T>(Action<IDistributedApplicationBuilder> configure, string? resourceName = null)
        where T : class, IResource
    {
        var builder = DistributedApplication.CreateBuilder();
        configure(builder);
        var app = builder.Build();
        var resources = app.Services.GetRequiredService<DistributedApplicationModel>().Resources.OfType<T>();
        var resource = resourceName is null ? Assert.Single(resources) : Assert.Single(resources, candidate => candidate.Name == resourceName);
        return (app, resource);
    }

    private static async Task<Dictionary<string, string>> EnvAsync(IResource resource)
        => await ((IResourceWithEnvironment)resource).GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);

    private static async Task<string[]> ArgsAsync(IResource resource)
        => await ((IResourceWithArgs)resource).GetArgumentValuesAsync(DistributedApplicationOperation.Publish);

    [Fact]
    public async Task AddDocsql_configures_image_endpoint_args_token_and_health_check()
    {
        var (app, resource) = Build<DocsqlServerResource>(builder => builder.AddDocsql("docsql"));

        var image = resource.Annotations.OfType<ContainerImageAnnotation>().Single();
        Assert.Equal("ghcr.io/wjw1-evan/docsql", image.Image);
        Assert.Equal("latest", image.Tag);

        var endpoint = resource.Annotations.OfType<EndpointAnnotation>().Single(e => e.Name == DocsqlServerResource.PrimaryEndpointName);
        Assert.Equal(DocsqlHostingExtensions.ProtocolPort, endpoint.TargetPort);

        var args = await ArgsAsync(resource);
        Assert.Equal(DocsqlServerResource.DatabasePath, args[0]);
        Assert.Equal($"0.0.0.0:{DocsqlHostingExtensions.ProtocolPort}", args[1]);

        Assert.Equal("docsql-tcp", resource.Annotations.OfType<HealthCheckAnnotation>().Single().Key);
        app.Dispose();
    }

    [Fact]
    public async Task AddDocsql_generates_token_parameter_wired_into_env_and_connection_string()
    {
        var (app, resource) = Build<DocsqlServerResource>(builder => builder.AddDocsql("docsql"));

        var env = await EnvAsync(resource);
        Assert.Equal("{docsql-token.value}", env["DOCSQL_TOKEN"]);

        var connectionString = ((IResourceWithConnectionString)resource).ConnectionStringExpression.ValueExpression;
        Assert.Contains("host={docsql.bindings.tcp.host}", connectionString);
        Assert.Contains("port={docsql.bindings.tcp.port}", connectionString);
        Assert.Contains("token={docsql-token.value}", connectionString);
        app.Dispose();
    }

    [Fact]
    public async Task WithToken_replaces_generated_parameter()
    {
        var (app, resource) = Build<DocsqlServerResource>(builder =>
        {
            var token = builder.AddParameter("mytoken", secret: true);
            builder.AddDocsql("docsql").WithToken(token);
        });

        var env = await EnvAsync(resource);
        Assert.Equal("{mytoken.value}", env["DOCSQL_TOKEN"]);
        Assert.Contains("token={mytoken.value}", ((IResourceWithConnectionString)resource).ConnectionStringExpression.ValueExpression);
        app.Dispose();
    }

    [Fact]
    public void WithDataVolume_mounts_named_volume_at_data_path()
    {
        var (app, resource) = Build<DocsqlServerResource>(builder => builder.AddDocsql("docsql").WithDataVolume("mydata"));

        var mount = resource.Annotations.OfType<ContainerMountAnnotation>().Single();
        Assert.Equal(ContainerMountType.Volume, mount.Type);
        Assert.Equal("mydata", mount.Source);
        Assert.Equal("/data", mount.Target);
        Assert.False(mount.IsReadOnly);
        app.Dispose();
    }

    [Fact]
    public void WithDataVolume_generates_stable_volume_name_by_default()
    {
        var (app, resource) = Build<DocsqlServerResource>(builder => builder.AddDocsql("docsql").WithDataVolume());

        var mount = resource.Annotations.OfType<ContainerMountAnnotation>().Single();
        Assert.False(string.IsNullOrEmpty(mount.Source));
        Assert.Equal("/data", mount.Target);
        app.Dispose();
    }

    [Fact]
    public async Task WithPeers_injects_endpoint_expressions_as_comma_separated_list()
    {
        var (app, resource) = Build<DocsqlServerResource>(builder =>
        {
            var a = builder.AddDocsql("a");
            var b = builder.AddDocsql("b");
            var c = builder.AddDocsql("c");
            a.WithPeers(new[] { b, c });
        }, resourceName: "a");

        var env = await EnvAsync(resource);
        Assert.Equal("{b.bindings.tcp.host}:{b.bindings.tcp.port},{c.bindings.tcp.host}:{c.bindings.tcp.port}", env["DOCSQL_PEERS"]);
        app.Dispose();
    }

    [Fact]
    public async Task AddDocsqlCluster_wires_peers_tokens_and_volumes_for_every_node()
    {
        var builder = DistributedApplication.CreateBuilder();
        var cluster = builder.AddDocsqlCluster("docsql", nodeCount: 3);
        using var app = builder.Build();
        var model = app.Services.GetRequiredService<DistributedApplicationModel>();
        var nodes = model.Resources.OfType<DocsqlServerResource>().ToList();

        Assert.Equal(3, nodes.Count);
        Assert.Equal(["docsql-a", "docsql-b", "docsql-c"], nodes.Select(n => n.Name).ToList());

        foreach (var node in nodes)
        {
            var env = await ((IResourceWithEnvironment)node).GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
            var peers = env["DOCSQL_PEERS"].Split(',');
            Assert.Equal(2, peers.Length);
            Assert.DoesNotContain(peers, value => value.Contains($"{node.Name}.", StringComparison.Ordinal));
            Assert.Equal("{docsql-token.value}", env["DOCSQL_TOKEN"]);
            Assert.Equal("{docsql-cluster-token.value}", env["DOCSQL_CLUSTER_TOKEN"]);
            Assert.Single(node.Annotations.OfType<ContainerMountAnnotation>());
        }

        Assert.Equal(nodes[0], cluster.Primary.Resource);
        app.Dispose();
    }

    [Fact]
    public async Task WithWebConsole_adds_companion_container_pointing_at_server()
    {
        var (app, resource) = Build<DocsqlWebConsoleResource>(builder =>
        {
            var db = builder.AddDocsql("docsql");
            db.WithWebConsole();
        });

        Assert.Equal("docsql-web", ((ContainerResource)resource).Entrypoint);
        Assert.Equal("ghcr.io/wjw1-evan/docsql", resource.Annotations.OfType<ContainerImageAnnotation>().Single().Image);

        var endpoint = resource.Annotations.OfType<EndpointAnnotation>().Single(e => e.Name == DocsqlWebConsoleResource.PrimaryEndpointName);
        Assert.Equal(DocsqlHostingExtensions.WebConsolePort, endpoint.TargetPort);

        var args = await ArgsAsync(resource);
        Assert.Contains("docsql.bindings.tcp", args[0], StringComparison.Ordinal);
        Assert.Equal($"0.0.0.0:{DocsqlHostingExtensions.WebConsolePort}", args[1]);

        var env = await EnvAsync(resource);
        Assert.Equal("/auth/console-auth.json", env["DOCSQL_WEB_AUTH_FILE"]);
        Assert.Equal("{docsql-token.value}", env["DOCSQL_TOKEN"]);
        app.Dispose();
    }
}
