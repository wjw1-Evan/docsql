// CLR ↔ DocSQL 列类型映射:INTEGER / REAL / DECIMAL / TEXT / BLOB。
//
// DECIMAL 存精确十进制(rust_decimal,28~29 位有效数字),参数经 $dec 标记
// 以文本精确传递,聚合/比较由引擎按十进制语义执行;ByteArray 存 BLOB
// (引擎 Value::Bytes, 16MiB 文档上限),参数经 $bytes 标记。
// DateOnly/TimeOnly 沿用可排序 ISO 文本。

using Microsoft.EntityFrameworkCore.Storage;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlTypeMappingSource(
    TypeMappingSourceDependencies typeMappingDependencies,
    RelationalTypeMappingSourceDependencies relationalDependencies)
    : RelationalTypeMappingSource(typeMappingDependencies, relationalDependencies)
{
    private static readonly LongTypeMapping Integer = new("INTEGER", DbType.Int64);
    private static readonly IntTypeMapping Int32 = new("INTEGER", DbType.Int32);
    private static readonly ShortTypeMapping Int16 = new("INTEGER", DbType.Int16);
    private static readonly ByteTypeMapping Byte = new("INTEGER", DbType.Byte);
    private static readonly BoolTypeMapping Bool = new("INTEGER", DbType.Boolean);
    private static readonly DoubleTypeMapping Real = new("REAL", DbType.Double);
    private static readonly FloatTypeMapping Float = new("REAL", DbType.Single);
    private static readonly StringTypeMapping Text = new("TEXT", DbType.String);
    private static readonly GuidTypeMapping Guid = new("TEXT", DbType.Guid);
    private static readonly DocsqlBytesMapping Bytes = new();

    // Date/time literals must be plain ISO-8601 strings: the base mappings
    // render `TIMESTAMP '...'` / `TIME '...'`, which the engine rejects as
    // unsupported expressions — inline constants in LINQ would then fail
    // while closure-parameterized equivalents work. The text form matches
    // the ADO parameter path exactly, so both paths store one format.
    private static readonly IsoDateTimeMapping DateTime = new();
    private static readonly IsoDateTimeOffsetMapping DateTimeOffset = new();
    private static readonly IsoTimeSpanMapping TimeSpan = new();
    private static readonly DocsqlDateOnlyMapping DateOnly = new();
    private static readonly DocsqlTimeOnlyMapping TimeOnly = new();

    private sealed class IsoDateTimeMapping : DateTimeTypeMapping
    {
        public IsoDateTimeMapping() : base("TEXT", System.Data.DbType.DateTime) { }

        protected override string GenerateNonNullSqlLiteral(object value) =>
            $"'{((System.DateTime)value).ToString("O", CultureInfo.InvariantCulture)}'";
    }

    private sealed class IsoDateTimeOffsetMapping : DateTimeOffsetTypeMapping
    {
        public IsoDateTimeOffsetMapping() : base("TEXT", System.Data.DbType.DateTimeOffset) { }

        protected override string GenerateNonNullSqlLiteral(object value) =>
            $"'{((DateTimeOffset)value).ToString("O", CultureInfo.InvariantCulture)}'";
    }

    private sealed class IsoTimeSpanMapping : TimeSpanTypeMapping
    {
        public IsoTimeSpanMapping() : base("TEXT", System.Data.DbType.Time) { }

        protected override string GenerateNonNullSqlLiteral(object value) =>
            $"'{((System.TimeSpan)value).ToString("c", CultureInfo.InvariantCulture)}'";
    }

    /// <summary>DECIMAL:精度/小数位随模型 HasPrecision 进入列类型与 CAST 字面量。</summary>
    private sealed class DocsqlDecimalMapping : DecimalTypeMapping
    {
        public DocsqlDecimalMapping(int? precision, int? scale)
            : base("DECIMAL", System.Data.DbType.Decimal, precision, scale) { }

        private DocsqlDecimalMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
            new DocsqlDecimalMapping(parameters);

        // 裸数字字面量会被引擎解析为 Float 丢失精度;CAST 保留精确文本。
        // EF 有时用 CLR 整数承载十进制常量(如 SUM 的 COALESCE(…, 0)),
        // 统一经 Convert.ToDecimal 归一化。
        protected override string GenerateNonNullSqlLiteral(object value) =>
            "CAST('" + Convert.ToDecimal(value, CultureInfo.InvariantCulture)
                .ToString(CultureInfo.InvariantCulture) + "' AS " + StoreType + ")";
    }

    private sealed class DocsqlBytesMapping : RelationalTypeMapping
    {
        public DocsqlBytesMapping() : base("BLOB", typeof(byte[]), System.Data.DbType.Binary) { }

        private DocsqlBytesMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
            new DocsqlBytesMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var bytes = (byte[])value;
            return "x'" + Convert.ToHexString(bytes).ToLowerInvariant() + "'";
        }

        public override MethodInfo GetDataReaderMethod() =>
            GetFieldValueMethod(typeof(byte[]));
    }

    private sealed class DocsqlDateOnlyMapping : RelationalTypeMapping
    {
        public DocsqlDateOnlyMapping() : base("TEXT", typeof(DateOnly), System.Data.DbType.Date) { }

        private DocsqlDateOnlyMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
            new DocsqlDateOnlyMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value) =>
            $"'{((DateOnly)value).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}'";

        public override MethodInfo GetDataReaderMethod() =>
            GetFieldValueMethod(typeof(DateOnly));
    }

    private sealed class DocsqlTimeOnlyMapping : RelationalTypeMapping
    {
        public DocsqlTimeOnlyMapping() : base("TEXT", typeof(TimeOnly), System.Data.DbType.Time) { }

        private DocsqlTimeOnlyMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
            new DocsqlTimeOnlyMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value) =>
            $"'{((TimeOnly)value).ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture)}'";

        public override MethodInfo GetDataReaderMethod() =>
            GetFieldValueMethod(typeof(TimeOnly));
    }

    /// <summary>闭合 DbDataReader.GetFieldValue&lt;T&gt; —— DocsqlDataReader 已实现按类型读取。</summary>
    private static MethodInfo GetFieldValueMethod(Type type) =>
        typeof(DbDataReader)
            .GetMethods()
            .Single(m => m.Name == nameof(DbDataReader.GetFieldValue)
                && m.IsGenericMethodDefinition
                && m.GetParameters().Length == 1)
            .MakeGenericMethod(type);

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo info)
    {
        var clrType = info.ClrType;
        if (clrType == typeof(bool)) return Bool;
        if (clrType == typeof(byte)) return Byte;
        if (clrType == typeof(short)) return Int16;
        if (clrType == typeof(int)) return Int32;
        if (clrType == typeof(long)) return Integer;
        if (clrType == typeof(float)) return Float;
        if (clrType == typeof(double)) return Real;
        if (clrType == typeof(decimal)) return new DocsqlDecimalMapping(info.Precision, info.Scale);
        if (clrType == typeof(Guid)) return Guid;
        if (clrType == typeof(System.DateTime)) return DateTime;
        if (clrType == typeof(DateTimeOffset)) return DateTimeOffset;
        if (clrType == typeof(System.TimeSpan)) return TimeSpan;
        if (clrType == typeof(DateOnly)) return DateOnly;
        if (clrType == typeof(TimeOnly)) return TimeOnly;
        if (clrType == typeof(byte[])) return Bytes;
        if (clrType == typeof(string)) return Text;
        return base.FindMapping(in info);
    }
}
