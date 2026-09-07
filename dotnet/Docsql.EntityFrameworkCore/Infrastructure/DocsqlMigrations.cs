// 迁移 SQL 生成器与注解提供程序:最小实现(满足 DI 契约)。

using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlMigrationsSqlGenerator(
    MigrationsSqlGeneratorDependencies dependencies)
    : MigrationsSqlGenerator(dependencies)
{
    // 通用 ANSI 生成已可用;复杂迁移操作超出引擎方言时由测试暴露。
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
        // 仅处理常量模式(运行时模式返回 null 交给客户端求值)
        if (arguments[0] is not SqlConstantExpression { Value: string pattern })
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
        // LIKE 通配符转义,ESCAPE 子句由 LikeExpression 生成
        var escaped = pattern
            .Replace(@"\", @"\\")
            .Replace("%", @"\%")
            .Replace("_", @"\_");
        return new LikeExpression(
            instance,
            new SqlConstantExpression(
                System.Linq.Expressions.Expression.Constant(prefix + escaped + suffix),
                arguments[0].TypeMapping),
            new SqlConstantExpression(
                System.Linq.Expressions.Expression.Constant(@"\"),
                arguments[0].TypeMapping),
            null);
    }
}
