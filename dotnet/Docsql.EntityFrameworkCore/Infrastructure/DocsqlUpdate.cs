// 更新执行:ReaderModificationCommandBatch 基类负责命令文本构建与调度,
// Consume 消费 INSERT..RETURNING 的行;无结果集时从受影响计数校验并发。

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System.Data.Common;

namespace Docsql.EntityFrameworkCore.Infrastructure;

public sealed class DocsqlModificationCommandBatchFactory(
    ModificationCommandBatchFactoryDependencies dependencies)
    : IModificationCommandBatchFactory
{
    public ModificationCommandBatch Create()
        => new DocsqlModificationCommandBatch(dependencies);
}

public sealed class DocsqlModificationCommandBatch(
    ModificationCommandBatchFactoryDependencies dependencies)
    : ReaderModificationCommandBatch(dependencies)
{
    // 引擎一次只接受一条语句 → 每批恰好一条命令,EF 自动拆批顺序执行。
    public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        if (ModificationCommands.Count > 0)
        {
            return false;
        }
        return base.TryAddCommand(modificationCommand);
    }

    protected override void Consume(RelationalDataReader reader)
        => Consume(reader, reader.DbDataReader);

    protected override Task ConsumeAsync(
        RelationalDataReader reader, CancellationToken ct = default)
    {
        Consume(reader, reader.DbDataReader);
        return Task.CompletedTask;
    }

    private void Consume(RelationalDataReader reader, DbDataReader dbReader)
    {
        var rows = 0;
        while (dbReader.Read())
        {
            rows++;
            if (rows == 1 && ModificationCommands.Count > 0)
            {
                ModificationCommands[0].PropagateResults(reader);
            }
        }
        if (rows == 0)
        {
            rows = dbReader.RecordsAffected;
        }
        if (rows == 0)
        {
            throw new DbUpdateConcurrencyException("DocSQL: 预期影响 1 行,实际 0 行");
        }
    }
}
