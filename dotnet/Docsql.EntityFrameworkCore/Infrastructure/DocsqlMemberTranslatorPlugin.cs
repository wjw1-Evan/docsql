// 成员翻译器:把 .NET 日期时间/字符串的可翻译成员映射到引擎的 T-SQL 函数族
// (YEAR/MONTH/DAY/DAYOFYEAR/DATEPART、LENGTH/UPPER/LOWER/TRIM、GETDATE/
// GETUTCDATE/NEWID)。引擎自第四批 T-SQL 起原生支持这些函数,EF 查询因此
// 可以整体下推,不再回落客户端求值。
//
// 语义注记(与 T-SQL 对齐):
// - DateTime.Now / UtcNow 都映射 GETDATE()/GETUTCDATE()——引擎仅存 UTC,
//   本地时区不保真(与 TypeMapping 的 TIMESTAMP 文档一致);
// - AddYears/AddMonths 等 Add* 是实例方法,由方法翻译器处理(那里才有实参);
// - Math 静态成员同样在方法翻译器(DocsqlTsqlMethodTranslator)。

using System.Reflection;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlMemberTranslatorPlugin(
    ISqlExpressionFactory sqlExpressionFactory,
    IRelationalTypeMappingSource typeMappingSource)
    : IMemberTranslatorPlugin
{
    public IEnumerable<IMemberTranslator> Translators { get; } =
        new IMemberTranslator[]
        {
            new DocsqlMemberTranslator(sqlExpressionFactory, typeMappingSource),
        };
}

public sealed class DocsqlMemberTranslator(
    ISqlExpressionFactory sqlExpressionFactory,
    IRelationalTypeMappingSource typeMappingSource)
    : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sql = sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _mappings = typeMappingSource;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type runtimeType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
        {
            return TranslateStatic(member);
        }
        var declaring = member.DeclaringType;
        if (declaring == typeof(DateTime) || declaring == typeof(DateTimeOffset))
        {
            return TranslateDateTime(instance, member.Name);
        }
        if (declaring == typeof(string))
        {
            return TranslateString(instance, member.Name);
        }
        return null;
    }

    private SqlExpression? TranslateStatic(MemberInfo member)
    {
        if (member.DeclaringType is { } type
            && (type == typeof(DateTime) || type == typeof(DateTimeOffset)))
        {
            if (member.Name == nameof(DateTime.Now))
            {
                return Fn("GETDATE", typeof(DateTime));
            }
            if (member.Name == nameof(DateTime.UtcNow))
            {
                return Fn("GETUTCDATE", typeof(DateTime));
            }
        }
        if (member.DeclaringType == typeof(Guid) && member.Name == nameof(Guid.NewGuid))
        {
            return Fn("NEWID", typeof(Guid));
        }
        return null;
    }

    private SqlExpression? TranslateDateTime(SqlExpression instance, string name)
    {
        switch (name)
        {
            case nameof(DateTime.Year):
                return Fn1("YEAR", instance, typeof(int));
            case nameof(DateTime.Month):
                return Fn1("MONTH", instance, typeof(int));
            case nameof(DateTime.Day):
                return Fn1("DAY", instance, typeof(int));
            case nameof(DateTime.DayOfYear):
                return Fn1("DAYOFYEAR", instance, typeof(int));
            case nameof(DateTime.Hour):
                return DatePart("hour", instance);
            case nameof(DateTime.Minute):
                return DatePart("minute", instance);
            case nameof(DateTime.Second):
                return DatePart("second", instance);
            case nameof(DateTime.Millisecond):
                return DatePart("millisecond", instance);
            default:
                return null;
        }
    }

    private SqlExpression? TranslateString(SqlExpression instance, string name)
    {
        // Length 是属性;ToUpper/ToLower/Trim 是方法(方法翻译器负责)。
        if (name == nameof(string.Length))
        {
            return Fn1("LENGTH", instance, typeof(int));
        }
        return null;
    }

    private SqlExpression Fn(string name, Type returnType)
        => _sql.Function(name, [], nullable: false, argumentsPropagateNullability: [], returnType);

    private SqlExpression Fn1(string name, SqlExpression arg, Type returnType)
        => _sql.Function(
            name,
            [arg],
            nullable: true,
            argumentsPropagateNullability: [true],
            returnType,
            _mappings.FindMapping(returnType));

    private SqlExpression DatePart(string part, SqlExpression instance)
        => _sql.Function(
            "DATEPART",
            [_sql.Constant(part), instance],
            nullable: true,
            argumentsPropagateNullability: [false, true],
            typeof(int),
            _mappings.FindMapping(typeof(int)));
}
