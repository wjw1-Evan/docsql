// CLR ↔ DocSQL 列类型映射:INTEGER / REAL / TEXT。

using Microsoft.EntityFrameworkCore.Storage;
using System.Data;
using System.Globalization;

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
    private static readonly DecimalTypeMapping Decimal = new("TEXT", DbType.Decimal);

    // Date/time literals must be plain ISO-8601 strings: the base mappings
    // render `TIMESTAMP '...'` / `TIME '...'`, which the engine rejects as
    // unsupported expressions — inline constants in LINQ would then fail
    // while closure-parameterized equivalents work. The text form matches
    // the ADO parameter path exactly, so both paths store one format.
    private static readonly IsoDateTimeMapping DateTime = new();
    private static readonly IsoDateTimeOffsetMapping DateTimeOffset = new();
    private static readonly IsoTimeSpanMapping TimeSpan = new();

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
        if (clrType == typeof(decimal)) return Decimal;
        if (clrType == typeof(Guid)) return Guid;
        if (clrType == typeof(System.DateTime)) return DateTime;
        if (clrType == typeof(DateTimeOffset)) return DateTimeOffset;
        if (clrType == typeof(System.TimeSpan)) return TimeSpan;
        // byte[] intentionally unmapped: the engine has no BLOB storage, and
        // a BLOB mapping would silently round-trip garbage. No mapping makes
        // EF fail at model build with a clear error instead.
        if (clrType == typeof(string)) return Text;
        return base.FindMapping(in info);
    }
}
