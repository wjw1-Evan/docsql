// SQL 生成三件套:标识符引用、SELECT 生成器、更新语句生成器。

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System.Text;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlSqlGenerationHelper(
    RelationalSqlGenerationHelperDependencies dependencies)
    : RelationalSqlGenerationHelper(dependencies);

public sealed class DocsqlQuerySqlGeneratorFactory(
    QuerySqlGeneratorDependencies dependencies)
    : IQuerySqlGeneratorFactory
{
    public QuerySqlGenerator Create() => new DocsqlQuerySqlGenerator(dependencies);
}

public sealed class DocsqlQuerySqlGenerator(
    QuerySqlGeneratorDependencies dependencies)
    : QuerySqlGenerator(dependencies)
{
    // 基类生成 Oracle 风格 "FETCH FIRST n ROWS ONLY";引擎方言只认
    // SQLite 风格的 LIMIT n OFFSET m。
    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Offset is not null)
        {
            if (selectExpression.Limit is null)
            {
                Sql.AppendLine().Append("LIMIT 18446744073709551615");
            }
            Sql.AppendLine().Append("OFFSET ");
            Visit(selectExpression.Offset);
        }
        if (selectExpression.Limit is not null)
        {
            if (selectExpression.Offset is null)
            {
                Sql.AppendLine();
            }
            else
            {
                Sql.AppendLine();
            }
            Sql.Append("LIMIT ");
            Visit(selectExpression.Limit);
        }
    }
}

public sealed class DocsqlUpdateSqlGenerator(
    UpdateSqlGeneratorDependencies dependencies)
    : UpdateSqlGenerator(dependencies)
{
    // SQLite 同款策略:插入走 INSERT ... RETURNING 读回生成列。
    public override ResultSetMapping AppendInsertOperation(
        StringBuilder sb, IReadOnlyModificationCommand command,
        int commandPosition, out bool requiresTransaction)
        => AppendInsertReturningOperation(sb, command, commandPosition, out requiresTransaction);

    public override ResultSetMapping AppendInsertOperation(
        StringBuilder sb, IReadOnlyModificationCommand command, int commandPosition)
    {
        bool rt;
        return AppendInsertReturningOperation(sb, command, commandPosition, out rt);
    }
}
