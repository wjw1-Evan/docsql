// 线协议标记词汇表($dec/$ts/$bytes/$float)与 TIMESTAMP 值域常量的
// 单点定义。编码侧(JsonOf/BytesJson)与解码侧(MarkerToValue)此前各自
// 内联拼标记名与值域;.NET 侧与 python 驱动(_convert.py)各持同值常量,
// 跨栈一致性由两侧的标记编解码用例钉住。
namespace Docsql.Client;

internal static class WireMarkers
{
    internal const string Dec = "$dec";
    internal const string Ts = "$ts";
    internal const string Bytes = "$bytes";
    internal const string Float = "$float";

    // TIMESTAMP 值域(引擎构造点收口 0001..=9999,毫秒):域外值保持原样
    // 不物化(与服务端同一 fail-safe)。
    internal const long TsMinMs = -62_135_596_800_000;
    internal const long TsMaxMs = 253_402_300_799_999;
}
