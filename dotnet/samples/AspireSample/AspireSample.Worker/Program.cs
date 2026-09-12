// Aspire 消费侧示例:连接串由 AppHost 经 WithReference 注入为
// ConnectionStrings__docsql,AddDocsqlConnection 注册连接(并带健康检查)。
// 启动即做一轮参数化写读,结果打到控制台(AppHost 仪表盘可见)。
using Docsql.Aspire.Client;
using Docsql.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.AddDocsqlConnection("docsql");

using var host = builder.Build();
await host.StartAsync();

var connection = host.Services.GetRequiredService<DocsqlConnection>();
await connection.OpenAsync();

await using (var create = connection.CreateCommand())
{
    create.CommandText = """
        CREATE TABLE IF NOT EXISTS aspire_demo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            payload TEXT
        )
        """;
    await create.ExecuteNonQueryAsync();
}

var sampleName = $"aspire-{Environment.TickCount64:x}";
long insertedId;
await using (var insert = connection.CreateCommand())
{
    insert.CommandText = "INSERT INTO aspire_demo (name, payload) VALUES (@name, @payload) RETURNING id";
    var nameParam = insert.CreateParameter();
    nameParam.ParameterName = "@name";
    nameParam.Value = sampleName;
    var payloadParam = insert.CreateParameter();
    payloadParam.ParameterName = "@payload";
    payloadParam.Value = """{"source":"aspire","ok":true}""";
    insert.Parameters.Add(nameParam);
    insert.Parameters.Add(payloadParam);
    insertedId = (long)(await insert.ExecuteScalarAsync())!;
}

await using (var select = connection.CreateCommand())
{
    select.CommandText = "SELECT id, name, payload FROM aspire_demo WHERE name = @name";
    var nameParam = select.CreateParameter();
    nameParam.ParameterName = "@name";
    nameParam.Value = sampleName;
    select.Parameters.Add(nameParam);
    await using var reader = await select.ExecuteReaderAsync();
    if (!await reader.ReadAsync())
    {
        throw new InvalidOperationException($"round-trip failed: row '{sampleName}' not found");
    }
    Console.WriteLine($"[aspire-worker] docsql round-trip ok: id={reader.GetInt64(0)} name={reader.GetString(1)} payload={reader.GetValue(2)}");
}

await host.StopAsync();
