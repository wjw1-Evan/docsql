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
        var cs = Dependencies.ContextOptions.FindExtension<DocsqlOptionsExtension>()?.ConnectionString;
        return new DocsqlConnection(cs ?? "host=127.0.0.1;port=7600");
    }

    protected override bool SupportsAmbientTransactions => false;
}
