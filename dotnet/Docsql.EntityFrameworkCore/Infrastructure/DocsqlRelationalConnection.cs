// 把 Docsql.Client 的 TCP 连接交给 EF 的关系型基础设施。

using Docsql.Client;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlRelationalConnection(
    RelationalConnectionDependencies dependencies)
    : RelationalConnection(dependencies)
{
    protected override DbConnection CreateDbConnection()
    {
        var extension = Dependencies.ContextOptions.FindExtension<DocsqlOptionsExtension>();
        // 连接实例(UseDocsql(DocsqlConnection))优先;否则每次新建连接,
        // 连接串取运行期工厂(若有)或静态值。
        if (extension?.Connection is { } connection)
        {
            return connection;
        }
        return new DocsqlConnection(
            extension?.ResolveConnectionString() ?? "host=127.0.0.1;port=7600");
    }

    protected override bool SupportsAmbientTransactions => false;
}
