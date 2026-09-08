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
