// CLR ↔ docsql 列类型映射:INTEGER / REAL / TEXT。

using Microsoft.EntityFrameworkCore.Storage;
using System.Data;

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
    private static readonly DateTimeTypeMapping DateTime = new("TEXT", DbType.DateTime);
    private static readonly DateTimeOffsetTypeMapping DateTimeOffset = new("TEXT", DbType.DateTimeOffset);
    private static readonly TimeSpanTypeMapping TimeSpan = new("TEXT", DbType.Time);

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
        if (clrType == typeof(DateTime)) return DateTime;
        if (clrType == typeof(DateTimeOffset)) return DateTimeOffset;
        if (clrType == typeof(TimeSpan)) return TimeSpan;
        // byte[] intentionally unmapped: the engine has no BLOB storage, and
        // a BLOB mapping would silently round-trip garbage. No mapping makes
        // EF fail at model build with a clear error instead.
        if (clrType == typeof(string)) return Text;
        return base.FindMapping(in info);
    }
}
