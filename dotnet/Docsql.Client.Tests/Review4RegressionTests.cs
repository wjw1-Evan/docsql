// 第四轮审查缺陷回归:客户端层。
// 1) 事务代际守卫经 Proto getter 在 Close 后抛异常,Dispose/Commit/Rollback
//    从守卫里逃出而非走设计的 closed 分支;
// 2) GetInt64/GetInt32/GetDouble/GetBoolean 未传 InvariantCulture,字符串值按
//    CurrentCulture 解析(de-DE 下 "3.14"→314);
// 3) 同步命令把 CommandTimeout 预算 stamp 在物理连接上不恢复,COMMIT/ROLLBACK/
//    保存点继承上一条命令的预算;
// 4) ConnectionPool.Return 的 Closed 检查与 Enqueue 之间无锁,与 ClearAll/
//    ClearSlot 的 TOCTOU 可把连接塞进已注销槽(物理 socket 泄漏);
// 5) GetString 对 byte[] 静默返回 "System.Byte[]";
// 6) SendAsync 只捕取消/DocsqlException,IOException(对端断开)不置 Broken,
//    死连接可能回池。
// ServerFixture 在 AdoNetTests.cs(全测试项目共用的进程启动辅助类);
// 本文件只引用它,不自行启动进程。

using Docsql.Client;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Data.Common;
using Xunit;

/// <summary>1)/2)/3)/5):typed getter 契约、事务守卫分支与读预算纪律。</summary>
public sealed class ReaderAndTransactionGuardTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public ReaderAndTransactionGuardTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        return conn;
    }

    [Fact]
    public void Transaction_commit_after_close_takes_designed_branch_not_getter_throw()
    {
        var conn = Open();
        var tx = conn.BeginTransaction();
        conn.Close(); // 事务未了结:物理断开,服务端已回滚
        // 修复前:StillOwnsConnection 经 Proto getter 抛 "connection is closed",
        // 逃出到调用方;修复后:closed 分支返回 false,Commit 抛设计文案。
        var ex = Assert.Throws<InvalidOperationException>(() => tx.Commit());
        Assert.Contains("rolled the transaction back", ex.Message);
        Assert.Throws<InvalidOperationException>(() => tx.Rollback());
        tx.Dispose(); // Dispose 绝不抛
        conn.Dispose();
    }

    [Fact]
    public void Transaction_dispose_after_close_does_not_throw()
    {
        var conn = Open();
        var tx = conn.BeginTransaction();
        conn.Close();
        // 修复前 Dispose 从 Proto getter 逃出 InvalidOperationException。
        tx.Dispose();
        conn.Dispose();
    }

    [Fact]
    public void Typed_getters_parse_text_values_invariantly()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT '42', '3.14', '7', 'true'";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        // de-DE:'.' 是千分位分隔符 —— CurrentCulture 解析会把 "3.14" 读成 314。
        var original = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
        try
        {
            Assert.Equal(42L, reader.GetInt64(0));
            Assert.Equal(7, reader.GetInt32(2));
            Assert.Equal(3.14, reader.GetDouble(1));
            Assert.True(reader.GetBoolean(3));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void GetString_on_blob_throws_instead_of_returning_type_name()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT x'414243'";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // 修复前:Convert.ToString(byte[]) → "System.Byte[]" 被当列值返回。
        Assert.Throws<InvalidCastException>(() => reader.GetString(0));
        // 契约:值本身仍可按 BLOB 读出。
        Assert.Equal(new byte[] { 0x41, 0x42, 0x43 }, reader.GetFieldValue<byte[]>(0));
    }

    [Fact]
    public void Transaction_control_frames_do_not_inherit_command_read_budget()
    {
        using var conn = Open();
        using (var cmd = new DocsqlCommand
        {
            Connection = conn,
            CommandText = "SELECT 1",
            CommandTimeout = 1,
        })
        {
            cmd.ExecuteScalar();
        }
        // 同步命令确实把预算 stamp 在物理连接上(泄漏源,现状)。
        Assert.Equal(1_000, conn.Proto.ReadTimeoutMs);

        using (var tx = conn.BeginTransaction())
        {
            // BEGIN 走事务控制路径:发送前必须恢复默认预算 —— COMMIT 带着
            // 上一条命令的 1s 预算会在慢盘上被误判超时并毒化连接。
            Assert.Equal(ProtocolConnection.DefaultReadTimeoutMs, conn.Proto.ReadTimeoutMs);
            tx.Rollback();
        }
        Assert.Equal(ProtocolConnection.DefaultReadTimeoutMs, conn.Proto.ReadTimeoutMs);
    }
}

/// <summary>4):归还与注销的 TOCTOU。修复后检查+入队在池锁内原子。</summary>
public sealed class PoolReturnRaceTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public PoolReturnRaceTests(ServerFixture fx) => _fx = fx;

    [Fact]
    public async Task Return_racing_ClearAll_never_leaks_into_deregistered_slot()
    {
        ConnectionPool.ClearAll();
        var p = new DocsqlConnectionStringBuilder
        {
            ConnectionString = $"host=127.0.0.1;port={_fx.Port}",
        };
        var slot = ConnectionPool.SlotOf(p, null);
        var protos = Enumerable.Range(0, 8)
            .Select(_ => DocsqlConnection.ConnectAndAuth(p, null, 5_000))
            .ToList();
        var returns = protos.Select(proto =>
            Task.Run(() => ConnectionPool.Return(slot, proto))).ToArray();
        // 与全部归还并发注销:无论交错如何,已注销槽的 Idle 队列在尘埃落定
        // 后必须为空 —— 修复前检查与入队之间的窗口能把连接塞进没人再消费
        // (也不再排空)的队列,物理 socket 永久泄漏。
        ConnectionPool.ClearAll();
        await Task.WhenAll(returns);
        Assert.Equal(0, slot.Idle.Count);
    }
}

/// <summary>6):异步读/写路径的 IO 失败毒化纪律(本地一次性 listener,
/// 不依赖 server 进程)。</summary>
public sealed class ProtocolBreakTests
{
    [Fact]
    public async Task SendAsync_io_failure_marks_connection_broken()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            using var proto = new ProtocolConnection("127.0.0.1", port);
            using var peer = await listener.AcceptTcpClientAsync();
            peer.Close(); // 对端立即断开:写或读必然失败

            await Assert.ThrowsAnyAsync<Exception>(() => proto.SendAsync(
                new Frame(FrameType.ReqPing, 0, 0, Array.Empty<byte>())));
            // 修复前:IOException 不置 Broken,Close() 会把死连接归还池。
            Assert.True(proto.Broken, "IO 失败必须毒化连接,死连接不得回池");
        }
        finally
        {
            listener.Stop();
        }
    }
}

// 第六轮审查缺陷回归:客户端层。
// 1) Output/ReturnValue 参数被静默当 INPUT 绑定(null 进语句,读回 null);
// 2) Transaction 与连接不绑定:错配事务下语句照 autocommit 执行;
// 3) RewriteParameters 不跳 [方括号] 标识符:与服务端占位符扫描器分叉,
//    [user@domain] 内的 @ 名字命中参数时把标识符改写成 [?];
// 4) ExecuteReaderAsync 丢弃 CommandBehavior.CloseConnection(池名额泄漏)。
public sealed class Review6BindingAndScannerTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public Review6BindingAndScannerTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        return conn;
    }

    [Fact]
    public void Non_input_parameter_direction_is_rejected_loudly()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT @p";
        var p = cmd.CreateParameter();
        p.ParameterName = "@p";
        p.Value = 1;
        p.Direction = System.Data.ParameterDirection.Output;
        cmd.Parameters.Add(p);
        Assert.Throws<NotSupportedException>(() => cmd.ExecuteScalar());
        Assert.ThrowsAsync<NotSupportedException>(() => cmd.ExecuteScalarAsync()).Wait();
        Assert.Throws<NotSupportedException>(() => cmd.Prepare());
    }

    [Fact]
    public async Task Transaction_from_another_connection_is_rejected()
    {
        var a = Open();
        var b = Open();
        using (a)
        using (b)
        {
            using var tx = a.BeginTransaction();
            using var cmd = b.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.Transaction = tx;
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => cmd.ExecuteScalarAsync());
        }
    }

    [Fact]
    public async Task Bracketed_identifier_containing_at_name_is_not_rewritten()
    {
        using var conn = Open();
        await using var cmd = conn.CreateCommand();
        // [user@domain] 是标识符:p 名恰与其中的 @user 段同名时,占位符
        // 改写不得把标识符内部替换成 ?(改写后语句必然解析失败)。
        cmd.CommandText = "SELECT [user@domain]";
        var p = cmd.CreateParameter();
        p.ParameterName = "@user";
        p.Value = 5;
        cmd.Parameters.Add(p);
        // schemaless 未知列读 NULL:语句必须原样可执行且返回一行
        // (基类 ExecuteScalar 对数据库 NULL 给 DBNull)。
        var v = await cmd.ExecuteScalarAsync();
        Assert.True(v is null or DBNull, $"expected NULL, got {v?.GetType().Name}");
    }

    [Fact]
    public async Task Async_reader_with_close_connection_returns_the_slot()
    {
        var conn = Open();
        var before = DocsqlConnectionPoolInspector.IdleCount("127.0.0.1", _fx.Port);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var reader = await cmd.ExecuteReaderAsync(
            System.Data.CommandBehavior.CloseConnection);
        await reader.DisposeAsync();
        // 连接被 reader 关闭并归池:空闲数必须回到之前的水平(曾每次
        // 泄漏一个名额,直到池耗尽)。
        var after = DocsqlConnectionPoolInspector.IdleCount("127.0.0.1", _fx.Port);
        // 借出中的连接被 reader 关闭后归还:空闲数应 +1(曾经不变 =
        // 名额泄漏)。
        Assert.Equal(before + 1, after);
    }
}

// 第七轮审查缺陷回归:客户端层。
// 1) RentAsync 取消路径双重释放借出名额(每次取消净 +1,MaxPoolSize 静默失效);
// 2) Reader.Close() 不履行 CommandBehavior.CloseConnection(仅 Dispose 履行)。
public sealed class Review7PoolAndReaderTests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _fx;
    public Review7PoolAndReaderTests(ServerFixture fx) => _fx = fx;

    private DocsqlConnection Open()
    {
        var conn = new DocsqlConnection($"host=127.0.0.1;port={_fx.Port}");
        conn.Open();
        return conn;
    }

    [Fact]
    public async Task Sync_reader_close_honors_close_connection_behavior()
    {
        var conn = Open();
        var before = DocsqlConnectionPoolInspector.IdleCount("127.0.0.1", _fx.Port);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var reader = cmd.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
        reader.Close(); // NOT Dispose:Close 必须与 Dispose 等效(SqlClient 契约)
        var after = DocsqlConnectionPoolInspector.IdleCount("127.0.0.1", _fx.Port);
        Assert.Equal(before + 1, after);
    }

    [Fact]
    public async Task Cancelled_open_async_keeps_the_pool_usable()
    {
        // A pre-cancelled token fails the permit wait itself; the old outer
        // catch then released a permit that was never acquired (each cancel
        // net-inflated the semaphore). Smoke level: repeated cancels must
        // leave the pool fully usable afterwards.
        var cs = $"host=127.0.0.1;port={_fx.Port}";
        for (var i = 0; i < 3; i++)
        {
            var cts = new CancellationTokenSource();
            cts.Cancel();
            var conn = new DocsqlConnection(cs);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => conn.OpenAsync(cts.Token));
        }
        using (var ok = new DocsqlConnection(cs))
        {
            await ok.OpenAsync(CancellationToken.None);
            using var cmd = ok.CreateCommand();
            cmd.CommandText = "SELECT 42 AS v";
            Assert.Equal(42L, await cmd.ExecuteScalarAsync());
        }
    }
}

/// 池空闲计数的测试观测口(只读统计,不参与协议)。
public static class DocsqlConnectionPoolInspector
{
    public static int IdleCount(string host, int port) =>
        ConnectionPool.DebugIdleCount(host, port);
}
