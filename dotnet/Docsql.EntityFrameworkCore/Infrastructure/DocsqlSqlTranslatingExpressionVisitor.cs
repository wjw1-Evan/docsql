// SQL 翻译层扩展:实体 primitive collection 属性的 Contains 翻译。
//
// EF 把 `e.RoleIds.Contains(x)`(List<string> 等按 JSON 文本存储的集合属性)
// 表示为 `EF.Property<List<string>>(e, "RoleIds").AsQueryable().Contains(x)`,
// 默认关系层无法翻译。这里识别该形态并发射引擎的 JSON_ARRAY_CONTAINS(json, x),
// 使权限/受众等集合查询留在服务端执行(不回落客户端求值)。

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public class DocsqlSqlTranslatingExpressionVisitor(
    RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext,
    QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
    : RelationalSqlTranslatingExpressionVisitor(
        dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory = dependencies.SqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource = dependencies.TypeMappingSource;

    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;
        if (method.IsGenericMethod
            && methodCallExpression.Arguments.Count == 2
            && IsContains(method)
            && TryTranslateArrayContains(
                methodCallExpression.Arguments[0],
                methodCallExpression.Arguments[1],
                out var translated))
        {
            return translated;
        }
        return base.VisitMethodCall(methodCallExpression);
    }

    private static bool IsContains(System.Reflection.MethodInfo method)
    {
        var generic = method.GetGenericMethodDefinition();
        if (method.DeclaringType == typeof(Queryable))
        {
            return generic == QueryableMethods.Contains;
        }
        // Funcletization may reduce Queryable.Contains to Enumerable.Contains.
        return method.DeclaringType == typeof(Enumerable)
            && method.Name == nameof(Enumerable.Contains)
            && method.GetParameters().Length == 2;
    }

    private bool TryTranslateArrayContains(Expression source, Expression item, out Expression translated)
    {
        translated = QueryCompilationContext.NotTranslatedExpression;
        // Unwrap AsQueryable(EF.Property<T>(entity, name)).
        while (source is MethodCallExpression
               {
                   Method.Name: nameof(Queryable.AsQueryable),
                   Arguments.Count: 1
               } asQueryable)
        {
            source = asQueryable.Arguments[0];
        }
        if (source is not MethodCallExpression efProperty
            || efProperty.Method.DeclaringType != typeof(EF)
            || efProperty.Method.Name != nameof(EF.Property)
            || efProperty.Arguments.Count != 2)
        {
            return false;
        }
        if (Visit(efProperty) is not SqlExpression collection
            || Visit(item) is not SqlExpression needle)
        {
            return false;
        }
        translated = _sqlExpressionFactory.Function(
            "JSON_ARRAY_CONTAINS",
            [collection, needle],
            nullable: true,
            argumentsPropagateNullability: [true, true],
            typeof(bool),
            _typeMappingSource.FindMapping(typeof(bool)));
        return true;
    }
}

public class DocsqlSqlTranslatingExpressionVisitorFactory(
    RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    : IRelationalSqlTranslatingExpressionVisitorFactory
{
    protected virtual RelationalSqlTranslatingExpressionVisitorDependencies Dependencies { get; } = dependencies;

    public virtual RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        => new DocsqlSqlTranslatingExpressionVisitor(
            Dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor);
}
