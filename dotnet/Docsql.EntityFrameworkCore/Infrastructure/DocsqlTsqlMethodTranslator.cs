// T-SQL 方法翻译器:DateTime.Add*(→DATEADD)、Math 静态函数(→ABS/CEILING/
// FLOOR/POWER/ROUND/SQRT/SIGN/EXP/LOG/LOG10)、string 静态与实例方法
// (IsNullOrEmpty/Concat/Replace/Substring)、Guid.NewGuid(→NEWID),以及
// EF.Functions.DateDiff*(→DATEDIFF)。引擎自第四批 T-SQL 起原生支持该函数族。
//
// 语义注记:
// - DateTime.AddDays/Hours/Minutes/Seconds 走 DATEADD('millisecond', CAST(实参
//   × 换算 AS INT)):引擎毫秒分辨率,实参为 double 时截断到整毫秒(T-SQL 同为
//   毫秒粒度);AddYears/AddMonths 直接 INT 化,月末钳制交由引擎 DATEADD
//   (T-SQL 月末语义);
// - Math.Round 映射引擎 ROUND:半离零舍入,.NET 默认是银行家舍入,两者在有
//   .5 尾数时可能有 1 位差(与 SQL Server 行为一致,属提供程序惯例);
// - DATEDIFF 计"跨越边界"次数,与 T-SQL 相同(12-31→1-1 相差 1 年)。

using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlTsqlMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public DocsqlTsqlMethodTranslator(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sql = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
        {
            return TranslateStatic(method, arguments);
        }
        var declaring = method.DeclaringType;
        if (declaring == typeof(DateTime) || declaring == typeof(DateTimeOffset))
        {
            return TranslateDateAdd(instance, method.Name, arguments);
        }
        if (declaring == typeof(string))
        {
            return TranslateStringInstance(instance, method, arguments);
        }
        return null;
    }

    private SqlExpression? TranslateStatic(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        var declaring = method.DeclaringType;
        if (declaring == typeof(Math))
        {
            return TranslateMath(method, arguments);
        }
        if (declaring == typeof(string))
        {
            return TranslateStringStatic(method, arguments);
        }
        if (declaring == typeof(Guid) && method.Name == nameof(Guid.NewGuid))
        {
            return Fn("NEWID", typeof(Guid));
        }
        // EF.Functions.DateDiff*(start, end) → DATEDIFF('<part>', start, end)
        if (declaring == typeof(DocsqlDbFunctionsExtensions)
            && method.Name.StartsWith("DateDiff", StringComparison.Ordinal))
        {
            // EF 会把 EF.Functions 标记参数(第一个)一并传入,实际起止
            // 日期在 arguments[^2] 与 arguments[^1]。
            if (DocsqlDbFunctionsExtensions.DateDiffParts.TryGetValue(method.Name, out var part)
                && arguments.Count == 3)
            {
                return Fn(
                    "DATEDIFF", typeof(int),
                    _sql.Constant(part), arguments[^2], arguments[^1]);
            }
            return null;
        }
        return null;
    }

    private SqlExpression? TranslateMath(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        var a0 = arguments[0];
        switch (method.Name)
        {
            case nameof(Math.Abs) when arguments.Count == 1:
                return Fn("ABS", a0.Type, a0);
            case nameof(Math.Ceiling) when arguments.Count == 1:
                return Fn("CEILING", a0.Type, a0);
            case nameof(Math.Floor) when arguments.Count == 1:
                return Fn("FLOOR", a0.Type, a0);
            case nameof(Math.Sqrt) when arguments.Count == 1:
                return Fn("SQRT", typeof(double), a0);
            case nameof(Math.Sign) when arguments.Count == 1:
                return Fn("SIGN", typeof(int), a0);
            case nameof(Math.Exp) when arguments.Count == 1:
                return Fn("EXP", typeof(double), a0);
            case nameof(Math.Log10) when arguments.Count == 1:
                return Fn("LOG10", typeof(double), a0);
            case nameof(Math.Log) when arguments.Count == 1:
                return Fn("LOG", typeof(double), a0);
            case nameof(Math.Log) when arguments.Count == 2:
                return Fn("LOG", typeof(double), a0, arguments[1]);
            case nameof(Math.Pow) when arguments.Count == 2:
                return Fn("POWER", typeof(double), a0, arguments[1]);
            case nameof(Math.Round) when arguments.Count == 1:
                return Fn("ROUND", a0.Type, a0);
            case nameof(Math.Round)
                when arguments.Count == 2 && arguments[1] is SqlConstantExpression digits:
                return Fn("ROUND", a0.Type, a0, digits);
            default:
                return null;
        }
    }

    private SqlExpression? TranslateStringStatic(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        switch (method.Name)
        {
            case nameof(string.IsNullOrEmpty) when arguments.Count == 1:
                {
                    var len = Fn("LENGTH", typeof(int), arguments[0]);
                    var empty = _sql.Equal(len, _sql.Constant(0));
                    return _sql.OrElse(_sql.IsNull(arguments[0]), empty);
                }
            case nameof(string.Concat) when arguments.Count is >= 2:
                {
                    // .NET string.Concat(和 T-SQL CONCAT)把 NULL 当空串;
                    // 引擎 CONCAT 遇 NULL 返回 NULL。每个实参包
                    // COALESCE(arg, '') 对齐 .NET 语义,NULL 不再传播。
                    var coalesced = arguments
                        .Select(a => _sql.Function(
                            "COALESCE",
                            [a, _sql.Constant("")],
                            nullable: false,
                            argumentsPropagateNullability: new List<bool> { false, false },
                            typeof(string),
                            _typeMappingSource.FindMapping(typeof(string))))
                        .ToList();
                    return _sql.Function(
                        "CONCAT",
                        coalesced,
                        nullable: false,
                        argumentsPropagateNullability: coalesced.Select(_ => false).ToList(),
                        typeof(string),
                        _typeMappingSource.FindMapping(typeof(string)));
                }
            default:
                return null;
        }
    }

    private SqlExpression? TranslateStringInstance(
        SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        switch (method.Name)
        {
            // 参数零个的大小写/裁剪方法是实例方法(属性翻译器只管 Length)。
            case nameof(string.ToUpper) when arguments.Count == 0:
            case nameof(string.ToUpperInvariant) when arguments.Count == 0:
                return Fn("UPPER", typeof(string), instance);
            case nameof(string.ToLower) when arguments.Count == 0:
            case nameof(string.ToLowerInvariant) when arguments.Count == 0:
                return Fn("LOWER", typeof(string), instance);
            case nameof(string.Trim) when arguments.Count == 0:
                return Fn("TRIM", typeof(string), instance);
            case nameof(string.Replace) when arguments.Count == 2:
                return Fn("REPLACE", typeof(string), instance, arguments[0], arguments[1]);
            case nameof(string.Substring) when arguments.Count == 1:
                // .NET 0 基 → 引擎 SUBSTRING 1 基
                return Fn("SUBSTRING", typeof(string), instance, _sql.Add(arguments[0], _sql.Constant(1)));
            case nameof(string.Substring) when arguments.Count == 2:
                return Fn("SUBSTRING", typeof(string), instance, _sql.Add(arguments[0], _sql.Constant(1)), arguments[1]);
            default:
                return null;
        }
    }

    private SqlExpression? TranslateDateAdd(
        SqlExpression instance, string name, IReadOnlyList<SqlExpression> arguments)
    {
        var value = arguments[0];
        switch (name)
        {
            case nameof(DateTime.AddYears):
                return DateAdd("year", Int(value), instance, instance.Type);
            case nameof(DateTime.AddMonths):
                return DateAdd("month", Int(value), instance, instance.Type);
            case nameof(DateTime.AddDays):
                return DateAdd("millisecond", Ms(value, 86_400_000), instance, instance.Type);
            case nameof(DateTime.AddHours):
                return DateAdd("millisecond", Ms(value, 3_600_000), instance, instance.Type);
            case nameof(DateTime.AddMinutes):
                return DateAdd("millisecond", Ms(value, 60_000), instance, instance.Type);
            case nameof(DateTime.AddSeconds):
                return DateAdd("millisecond", Ms(value, 1_000), instance, instance.Type);
            case nameof(DateTime.AddMilliseconds):
                return DateAdd("millisecond", Int(value), instance, instance.Type);
            default:
                return null;
        }
    }

    private SqlExpression Int(SqlExpression value)
        => value.Type == typeof(int)
            ? value
            : _sql.Convert(value, value.Type, Map(typeof(int)));

    /// <summary>毫秒换算:CAST(value × scale AS INT)。double 实参在此截断到
    /// 整毫秒(引擎与 T-SQL 的共同分辨率)。</summary>
    private SqlExpression Ms(SqlExpression value, long scale)
    {
        var product = _sql.ApplyTypeMapping(
            _sql.Add(
                _sql.ApplyDefaultTypeMapping(_sql.Constant(0.0)),
                _sql.Multiply(
                    _sql.ApplyDefaultTypeMapping(value),
                    _sql.Constant((double)scale))),
            Map(typeof(double)));
        return _sql.Convert(product, typeof(int), Map(typeof(int)));
    }

    private SqlExpression DateAdd(string part, SqlExpression count, SqlExpression date, Type returnType)
        => Fn("DATEADD", returnType, _sql.Constant(part), count, date);

    private SqlExpression Fn(string name, Type returnType, params SqlExpression[] args)
        => _sql.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: args.Select(_ => true).ToList(),
            returnType,
            Map(returnType));

    private RelationalTypeMapping Map(Type t)
        => _typeMappingSource.FindMapping(t)
            ?? throw new InvalidOperationException($"no type mapping for {t.Name}");
}

/// <summary>
/// DocSQL 提供程序的 <c>EF.Functions</c> 扩展:把 T-SQL DATEDIFF 暴露给
/// LINQ(<c>EF.Functions.DateDiffDay(a, b)</c> 等翻译为
/// <c>DATEDIFF('day', a, b)</c>,计跨越边界次数,与 SQL Server 一致)。
/// 这些方法只在 DocSQL 提供程序内翻译;其他提供程序会抛翻译异常。
/// </summary>
public static class DocsqlDbFunctionsExtensions
{
    internal static readonly Dictionary<string, string> DateDiffParts = new()
    {
        [nameof(DateDiffYear)] = "year",
        [nameof(DateDiffQuarter)] = "quarter",
        [nameof(DateDiffMonth)] = "month",
        [nameof(DateDiffDayOfYear)] = "dayofyear",
        [nameof(DateDiffDay)] = "day",
        [nameof(DateDiffWeek)] = "week",
        [nameof(DateDiffHour)] = "hour",
        [nameof(DateDiffMinute)] = "minute",
        [nameof(DateDiffSecond)] = "second",
        [nameof(DateDiffMillisecond)] = "millisecond",
    };

    public static int DateDiffYear(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffQuarter(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffMonth(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffDayOfYear(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffDay(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffWeek(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffHour(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffMinute(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffSecond(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffMillisecond(this DbFunctions _, DateTime start, DateTime end)
        => throw new InvalidOperationException(DocsqlSqlOnly);

    public static int DateDiffYear(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffDay(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffMonth(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffHour(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffMinute(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffSecond(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffQuarter(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffDayOfYear(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffWeek(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);
    public static int DateDiffMillisecond(this DbFunctions _, DateTimeOffset start, DateTimeOffset end)
        => throw new InvalidOperationException(DocsqlSqlOnly);

    private const string DocsqlSqlOnly =
        "EF.Functions.DateDiff* 只能在 DocSQL 提供程序的 LINQ 查询内使用(翻译为 DATEDIFF),不能直接调用。";
}
