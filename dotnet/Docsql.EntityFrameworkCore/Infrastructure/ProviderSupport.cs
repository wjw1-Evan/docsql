// 提供程序支撑件:注解、日志定义、约定集、LINQ 字符串方法翻译,
// 以及"EF Migrations 不受支持"的显式报错桩(docsql 的建表走
// EnsureCreated/惰性建表拦截器与 SchemaSync,不提供迁移管线)。

using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Docsql.EntityFrameworkCore.Infrastructure;

/// <summary>
/// EF Migrations(Add-Migration / Database.Migrate)不受支持:
/// docsql 用 EnsureCreated + AutoCreate/SchemaSync 自动同步模型。
/// 显式报错,而不是让通用 ANSI 生成器产出引擎方言外的 SQL。
/// </summary>
public sealed class DocsqlUnsupportedMigrationsSqlGenerator(
    MigrationsSqlGeneratorDependencies dependencies)
    : MigrationsSqlGenerator(dependencies)
{
    public override IReadOnlyList<MigrationCommand> Generate(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model = null,
        MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
        => throw NotSupported();

    private static NotSupportedException NotSupported() => new(
        "docsql 不支持 EF Migrations;请使用 EnsureCreated(建表/索引随模型自动同步)");
}

/// <summary>
/// 迁移历史仓储:关系层没有默认实现,不注册的话 Migrator 在 DI
/// 激活阶段就死于"Unable to resolve IHistoryRepository"。每个成员都
/// 显式报错,让 Database.Migrate() 得到可读的指引。
/// </summary>
public sealed class DocsqlUnsupportedHistoryRepository : IHistoryRepository
{
    private static NotSupportedException NotSupported() => new(
        "docsql 不支持 EF Migrations;请使用 EnsureCreated(建表/索引随模型自动同步)");

    public LockReleaseBehavior LockReleaseBehavior => throw NotSupported();
    public IMigrationsDatabaseLock AcquireDatabaseLock() => throw NotSupported();
    public Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default) => throw NotSupported();
    public void Create() => throw NotSupported();
    public Task CreateAsync(CancellationToken cancellationToken = default) => throw NotSupported();
    public bool CreateIfNotExists() => throw NotSupported();
    public Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        => throw NotSupported();
    public bool Exists() => throw NotSupported();
    public Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        => throw NotSupported();
    public IReadOnlyList<HistoryRow> GetAppliedMigrations() => throw NotSupported();
    public Task<IReadOnlyList<HistoryRow>> GetAppliedMigrationsAsync(
        CancellationToken cancellationToken = default) => throw NotSupported();
    public string GetBeginIfExistsScript(string migrationId) => throw NotSupported();
    public string GetBeginIfNotExistsScript(string migrationId) => throw NotSupported();
    public string GetCreateIfNotExistsScript() => throw NotSupported();
    public string GetCreateScript() => throw NotSupported();
    public string GetDeleteScript(string migrationId) => throw NotSupported();
    public string GetEndIfScript() => throw NotSupported();
    public string GetInsertScript(HistoryRow row) => throw NotSupported();
}

public sealed class DocsqlAnnotationProvider(
    RelationalAnnotationProviderDependencies dependencies)
    : RelationalAnnotationProvider(dependencies)
{
}


public sealed class DocsqlLoggingDefinitions : RelationalLoggingDefinitions;

/// <summary>复用 EF 关系层通用约定集(表/列映射注解)。</summary>
public sealed class DocsqlConventionSetBuilder(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalConventionSetBuilder(dependencies, relationalDependencies);

/// <summary>字符串方法 LINQ 翻译(StartsWith/EndsWith/Contains → LIKE)。</summary>
public sealed class DocsqlMethodCallTranslatorPlugin : IMethodCallTranslatorPlugin
{
    public IEnumerable<IMethodCallTranslator> Translators { get; } =
        new IMethodCallTranslator[] { new DocsqlStringMethodTranslator() };
}

public sealed class DocsqlStringMethodTranslator : IMethodCallTranslator
{
    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null || arguments.Count != 1)
        {
            return null;
        }
        var (prefix, suffix) = method.Name switch
        {
            nameof(string.StartsWith) => ("", "%"),
            nameof(string.EndsWith) => ("%", ""),
            nameof(string.Contains) => ("%", "%"),
            _ => (null, null),
        };
        if (prefix is null)
        {
            return null;
        }
        var mapping = arguments[0].TypeMapping ?? instance.TypeMapping;
        // 常量模式:直接拼 LIKE 字面量(转义通配符)。
        if (arguments[0] is SqlConstantExpression { Value: string pattern })
        {
            // LIKE 通配符转义,ESCAPE 子句由 LikeExpression 生成
            var escaped = pattern
                .Replace(@"\", @"\\")
                .Replace("%", @"\%")
                .Replace("_", @"\_");
            return new LikeExpression(
                instance,
                new SqlConstantExpression(
                    System.Linq.Expressions.Expression.Constant(prefix + escaped + suffix),
                    mapping),
                new SqlConstantExpression(
                    System.Linq.Expressions.Expression.Constant(@"\"),
                    mapping),
                null);
        }
        // 变量模式(x.StartsWith(prefixVariable)):拼 CONCAT('%', @p, '%') ——
        // 引擎支持 CONCAT。返回 null 会让 EF 直接抛"翻译失败"。
        if (arguments[0] is SqlParameterExpression
            or SqlBinaryExpression
            or SqlFunctionExpression)
        {
            var pieces = new List<SqlExpression>(3);
            if (prefix.Length > 0)
            {
                pieces.Add(new SqlConstantExpression(
                    System.Linq.Expressions.Expression.Constant(prefix), mapping));
            }
            pieces.Add(arguments[0]);
            if (suffix.Length > 0)
            {
                pieces.Add(new SqlConstantExpression(
                    System.Linq.Expressions.Expression.Constant(suffix), mapping));
            }
            var concat = new SqlFunctionExpression(
                "CONCAT",
                pieces,
                nullable: true,
                argumentsPropagateNullability: pieces.Select(_ => true).ToList(),
                typeof(string),
                mapping);
            return new LikeExpression(instance, concat, new SqlConstantExpression(
                System.Linq.Expressions.Expression.Constant(@"\"), mapping), null);
        }
        return null;
    }
}
