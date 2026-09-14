// Dictionary<string, object> 属性映射:JSON 文本列 + 值转换/比较器。
//
// EF 默认把字典属性当作 shared-type 实体导航,模型校验直接失败
// ("must be configured ... with an explicit name for the target shared-type entity type")。
// 本约定在属性加入时把它转成 JSON 文本标量:写入序列化、读取反序列化为原生 CLR 值,
// ValueComparer 以规范 JSON(键排序)做相等/哈希/快照,变更跟踪正确。
// 说明:字典内部成员不参与 SQL 翻译(WHERE 里按键过滤仍不支持),读写与整体比较可用。

using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace Docsql.EntityFrameworkCore.Infrastructure;

/// <summary>实体/属性加入约定:Dictionary&lt;string, object&gt; → JSON 文本标量。</summary>
public sealed class DocsqlJsonDictionaryConvention : IEntityTypeAddedConvention, IPropertyAddedConvention
{
    /// <summary>
    /// 关系发现(RelationshipDiscoveryConvention)会把字典成员当 shared-type 导航;
    /// 先手把它建成标量属性,后者见已有属性即不再建导航。
    /// </summary>
    public void ProcessEntityTypeAdded(
        IConventionEntityTypeBuilder entityTypeBuilder,
        IConventionContext<IConventionEntityTypeBuilder> context)
    {
        var entityType = entityTypeBuilder.Metadata;
        if (entityType.IsOwned())
        {
            return;
        }
        foreach (var member in entityType.ClrType.GetProperties())
        {
            if (member.PropertyType != typeof(Dictionary<string, object>)
                || entityType.FindProperty(member.Name) is not null
                || entityType.FindNavigation(member.Name) is not null)
            {
                continue;
            }
            entityTypeBuilder.Property(typeof(Dictionary<string, object>), member.Name);
        }
    }

    public void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        IConventionContext<IConventionPropertyBuilder> context)
    {
        var property = propertyBuilder.Metadata;
        if (property.ClrType != typeof(Dictionary<string, object>))
        {
            return;
        }
        if (property.GetValueConverter() is not null)
        {
            return;
        }
        propertyBuilder.HasConversion(DocsqlJsonDictionary.Converter, false);
        property.SetValueComparer(DocsqlJsonDictionary.Comparer, false);
    }
}

internal static class DocsqlJsonDictionary
{
    public static readonly ValueConverter Converter =
        new ValueConverter<Dictionary<string, object>, string>(
            v => JsonSerializer.Serialize(v),
            s => Deserialize(s));

    public static readonly ValueComparer<Dictionary<string, object>> Comparer = new(
        (a, b) => Canonical(a) == Canonical(b),
        v => Canonical(v).GetHashCode(),
        v => Deserialize(JsonSerializer.Serialize(v)));

    private static Dictionary<string, object> Deserialize(string json)
    {
        if (string.IsNullOrEmpty(json))
        {
            return new Dictionary<string, object>();
        }
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.ValueKind == JsonValueKind.Object
            ? (Dictionary<string, object>)Convert(doc.RootElement)!
            : new Dictionary<string, object>();
    }

    private static object? Convert(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Object => e.EnumerateObject()
            .ToDictionary(p => p.Name, p => Convert(p.Value)!),
        JsonValueKind.Array => e.EnumerateArray().Select(Convert).ToList(),
        JsonValueKind.String => e.GetString(),
        // (object) cast keeps integers integral: without it the ternary's
        // common type is double and every number comes back as double.
        JsonValueKind.Number => e.TryGetInt64(out var l) ? (object)l : e.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        _ => null,
    };

    /// <summary>键排序后的规范 JSON——比较/哈希不受字典插入顺序影响。</summary>
    private static string Canonical(Dictionary<string, object> v)
        => JsonSerializer.Serialize(Sort(v));

    private static object? Sort(object? v) => v switch
    {
        Dictionary<string, object> d => new SortedDictionary<string, object>(
            d.ToDictionary(kv => kv.Key, kv => Sort(kv.Value)!)),
        System.Collections.IDictionary d => new SortedDictionary<string, object>(
            d.Keys.Cast<string>().ToDictionary(k => k, k => Sort(d[k])!)),
        System.Collections.IEnumerable e and not string => e.Cast<object?>().Select(Sort).ToList(),
        _ => v,
    };
}

/// <summary>把字典约定挂进提供程序约定集(优先于关系发现,避免 shared-type 判定)。</summary>
public sealed class DocsqlConventionSetBuilder(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalConventionSetBuilder(dependencies, relationalDependencies)
{
    public override ConventionSet CreateConventionSet()
    {
        var conventionSet = base.CreateConventionSet();
        var convention = new DocsqlJsonDictionaryConvention();
        // 必须先于关系发现:字典成员若被建成导航,后面就救不回来了。
        conventionSet.EntityTypeAddedConventions.Insert(0, convention);
        conventionSet.PropertyAddedConventions.Insert(0, convention);
        return conventionSet;
    }
}
