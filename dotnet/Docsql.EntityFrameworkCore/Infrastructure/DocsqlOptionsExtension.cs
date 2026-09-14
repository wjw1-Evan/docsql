// DocSQL 的 EF Core 选项扩展:携带连接信息并注册提供程序服务。
// 不再借壳 SQLite —— 全部服务由本程序集实现。

using System.Data.Common;
using Docsql.Client;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlOptionsExtension : RelationalOptionsExtension
{
    private string? _connectionString;
    private DbConnection? _connection;
    private Func<string>? _connectionStringFactory;

    public DocsqlOptionsExtension() { }

    private DocsqlOptionsExtension(DocsqlOptionsExtension copy)
    {
        _connectionString = copy._connectionString;
        _connection = copy._connection;
        _connectionStringFactory = copy._connectionStringFactory;
    }

    public override string? ConnectionString => _connectionString;
    public override DbConnection? Connection => _connection;

    /// <summary>运行期连接串工厂(见 UseDocsql(Func&lt;string&gt;))。</summary>
    public Func<string>? ConnectionStringFactory => _connectionStringFactory;

    public override DocsqlOptionsExtension WithConnectionString(string? cs)
        => new(this) { _connectionString = cs };

    public override DocsqlOptionsExtension WithConnection(DbConnection? conn)
        => new(this) { _connection = conn };

    /// <summary>
    /// 每次创建物理连接时调用的连接串工厂。连接信息不参与 EF 模型/服务提供程序
    /// 缓存键(本扩展哈希恒为 0),同一宿主可让不同上下文连不同节点而模型只建一次。
    /// </summary>
    public DocsqlOptionsExtension WithConnectionStringFactory(Func<string> factory)
        => new(this) { _connectionStringFactory = factory };

    /// <summary>解析当前应使用的连接信息:工厂(运行期)优先于静态连接串。</summary>
    internal string? ResolveConnectionString()
        => _connectionStringFactory?.Invoke() ?? _connectionString;

    protected override RelationalOptionsExtension Clone() => new DocsqlOptionsExtension(this);

    public override void ApplyServices(IServiceCollection services)
    {
        // 先注册提供程序专属服务,再由关系层构建器补齐通用默认值
        // (Relational 版本会带上查询上下文/命令构建等关系型服务)。
        var builder = new EntityFrameworkRelationalServicesBuilder(services);
        services.AddEntityFrameworkDocsql();
        builder.TryAddCoreServices();
        // 核心默认值会抢注约定集构建器:换上关系层版本,表/列映射注解
        // 依赖它写入模型。
        services.Replace(ServiceDescriptor.Scoped<IProviderConventionSetBuilder, DocsqlConventionSetBuilder>());
        services.Replace(ServiceDescriptor.Scoped<IQuerySqlGeneratorFactory, DocsqlQuerySqlGeneratorFactory>());
        // 实体集合属性 Contains(JSON 数组)翻译:默认关系层不支持,换本提供程序实现。
        services.Replace(ServiceDescriptor.Scoped<IRelationalSqlTranslatingExpressionVisitorFactory,
            DocsqlSqlTranslatingExpressionVisitorFactory>());
    }

    public override DbContextOptionsExtensionInfo Info => new ExtensionInfo(this);

    private sealed class ExtensionInfo(DocsqlOptionsExtension extension)
        : DbContextOptionsExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => true;
        public override string LogFragment => "using DocSQL ";
        public override int GetServiceProviderHashCode() => 0;
        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;
        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["Docsql"] = "1";
    }
}

internal static class DocsqlServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkDocsql(this IServiceCollection services)
    {
        services.TryAddSingleton<IDatabaseProvider>(
            _ => new DatabaseProvider<DocsqlOptionsExtension>(new DatabaseProviderDependencies()));
        services.TryAddSingleton<LoggingDefinitions>(_ => new DocsqlLoggingDefinitions());
        // 关系层约定集(表/列名注解由这些约定写入模型)
        services.TryAddScoped<IProviderConventionSetBuilder, DocsqlConventionSetBuilder>();
        services.TryAddScoped<IRelationalConnection, DocsqlRelationalConnection>();
        services.TryAddScoped<IDatabaseCreator, DocsqlDatabaseCreator>();
        services.TryAddScoped<IRelationalDatabaseCreator, DocsqlDatabaseCreator>();
        services.TryAddSingleton<ISqlGenerationHelper, DocsqlSqlGenerationHelper>();
        services.TryAddSingleton<IRelationalTypeMappingSource, DocsqlTypeMappingSource>();
        services.TryAddScoped<IQuerySqlGeneratorFactory, DocsqlQuerySqlGeneratorFactory>();
        services.TryAddSingleton<IMethodCallTranslatorPlugin, DocsqlMethodCallTranslatorPlugin>();
        services.TryAddScoped<IUpdateSqlGenerator, DocsqlUpdateSqlGenerator>();
        services.TryAddScoped<IModificationCommandBatchFactory, DocsqlModificationCommandBatchFactory>();
        // EF Migrations 不提供:注册显式报错桩,Database.Migrate() 直接失败
        // 并指向 EnsureCreated(建表/索引自动同步),而不是产出方言外的 SQL。
        services.TryAddScoped<IMigrationsSqlGenerator, DocsqlUnsupportedMigrationsSqlGenerator>();
        services.TryAddScoped<IHistoryRepository, DocsqlUnsupportedHistoryRepository>();
        services.TryAddSingleton<IRelationalAnnotationProvider, DocsqlAnnotationProvider>();
        return services;
    }
}
