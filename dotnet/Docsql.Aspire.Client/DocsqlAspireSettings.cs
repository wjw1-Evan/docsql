namespace Docsql.Aspire.Client;

/// <summary>DocSQL client 集成的可调项。</summary>
public sealed class DocsqlAspireSettings
{
    /// <summary>不注册连接健康检查(默认注册:打开连接并执行 SELECT 1)。</summary>
    public bool DisableHealthChecks { get; set; }

    /// <summary>覆盖默认健康检查名(默认 <c>Docsql_{连接名}</c>)。</summary>
    public string? HealthCheckName { get; set; }
}
