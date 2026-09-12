using Docsql.Aspire.Client;
using Docsql.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Docsql.Aspire.Client.Tests;

/// <summary>DI 注册面测试:不连真实服务器,只断言注册行为与配置解析。</summary>
public class DocsqlClientAspireExtensionsTests
{
    [Fact]
    public void AddDocsqlConnection_registers_transient_connection_and_health_check()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:docsql"] = "host=127.0.0.1;port=17600;token=aspire-test-token"
        });

        builder.AddDocsqlConnection("docsql");

        Assert.Contains(builder.Services, d => d.ServiceType == typeof(DocsqlConnection));

        using var host = builder.Build();
        var connection = host.Services.GetRequiredService<DocsqlConnection>();
        Assert.Contains("host=127.0.0.1", connection.ConnectionString, StringComparison.Ordinal);
        Assert.Contains("port=17600", connection.ConnectionString, StringComparison.Ordinal);

        var registrations = host.Services.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        Assert.Contains(registrations, r => r.Name == "Docsql_docsql");
    }

    [Fact]
    public void AddDocsqlConnection_with_missing_connection_string_throws()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());

        var exception = Assert.Throws<InvalidOperationException>(() => builder.AddDocsqlConnection("missing"));
        Assert.Contains("Connection string 'missing'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddDocsqlConnection_with_health_checks_disabled_skips_registration()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:docsql"] = "host=127.0.0.1;port=17600"
        });

        builder.AddDocsqlConnection("docsql", settings => settings.DisableHealthChecks = true);

        Assert.Contains(builder.Services, d => d.ServiceType == typeof(DocsqlConnection));

        using var host = builder.Build();
        var registrations = host.Services.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        Assert.DoesNotContain(registrations, r => r.Name == "Docsql_docsql");
    }

    [Fact]
    public void AddDocsqlConnection_uses_custom_health_check_name()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:docsql"] = "host=127.0.0.1;port=17600"
        });

        builder.AddDocsqlConnection("docsql", settings => settings.HealthCheckName = "custom-check");

        using var host = builder.Build();
        var registrations = host.Services.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        Assert.Contains(registrations, r => r.Name == "custom-check");
    }
}
