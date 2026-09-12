using Docsql.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Docsql.EntityFrameworkCore.Tests;

/// <summary>
/// 容器级注册(Aspire/宿主集成面)测试:连接串按名取自 IConfiguration,
/// 不打真实连接,只断言提供程序与连接串进入 DbContext 选项。
/// </summary>
public class AddDocsqlDbContextTests
{
    private sealed class SampleDbContext(DbContextOptions<SampleDbContext> options) : DbContext(options);

    [Fact]
    public async Task AddDocsqlDbContext_resolves_context_with_docsql_provider_and_connection_string()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:docsql"] = "host=127.0.0.1;port=17600"
            })
            .Build();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddDocsqlDbContext<SampleDbContext>("docsql");

        await using var provider = services.BuildServiceProvider();
        await using var context = provider.GetRequiredService<SampleDbContext>();

        Assert.Equal("Docsql.EntityFrameworkCore", context.Database.ProviderName);
        Assert.Equal("host=127.0.0.1;port=17600", context.Database.GetConnectionString());
    }

    [Fact]
    public void AddDocsqlDbContext_with_missing_connection_string_throws_on_context_creation()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddDocsqlDbContext<SampleDbContext>("missing");

        using var provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<SampleDbContext>());
    }
}
