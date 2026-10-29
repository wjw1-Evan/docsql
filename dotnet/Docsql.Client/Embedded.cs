// Embedded (in-process) session: `Data Source=app.db` drives the native
// DocSQL engine through the C ABI in crates/docsql-ffi — no server, no port,
// no Docker. Requests and responses are the SAME Frame values the wire
// transport uses (embedded mode synthesizes response frames from the native
// engine call), so command/reader/transaction parsing stays byte-identical
// between remote and embedded connections.
//
// Scope: full SQL engine (DDL/DML/JOIN/transactions/constraints). Server-only
// features (pub/sub, PROMOTE, backup endpoints, cluster replication, T-SQL
// batch sessions) return a loud RESP_ERROR explaining they need a server.

using System.Buffers.Binary;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Docsql.Client;

/// <summary>
/// 进程内引擎会话(SQLite 式 <c>Data Source=…</c>):实现与线协议物理连接相同的
/// <see cref="IProtocolSession"/> 表面,发送/应答都是同一套 <see cref="Frame"/>。
/// 一个数据文件在同一进程内由所有会话共享同一引擎实例(单写者纪律 + 事务守门
/// 由原生层负责),关闭会话只是释放句柄,文件/WAL 留待下次打开恢复。
/// </summary>
internal sealed class EmbeddedSession : IProtocolSession
{
    private IntPtr _handle;
    private readonly Dictionary<string, ulong> _prepared = new(StringComparer.Ordinal);
    private readonly object _sync = new();
    private ulong _nextHandle = 1;
    private int _readTimeoutMs = ProtocolConnection.DefaultReadTimeoutMs;

    public int ReadTimeoutMs
    {
        get => _readTimeoutMs;
        set => _readTimeoutMs = value;
    }

    /// <summary>嵌入式会话没有可中毒的帧流:语句失败是普通错误,连接仍可用。</summary>
    public bool Broken => false;

    public static EmbeddedSession Open(string dataSource)
    {
        try
        {
            var abi = EmbeddedNative.AbiVersion();
            if (abi != 1)
            {
                throw new NotSupportedException(
                    $"embedded native library ABI {abi} is not supported by this client (expected 1); "
                    + "update the Docsql.Client package and the native library together");
            }
            var rc = EmbeddedNative.Open(dataSource, out var handle, out var error);
            var message = EmbeddedNative.TakeString(ref error);
            if (rc != 0)
            {
                throw new DocsqlException(
                    $"embedded open failed for '{dataSource}': {message ?? $"native error {rc}"}");
            }
            return new EmbeddedSession { _handle = handle };
        }
        catch (DllNotFoundException e)
        {
            throw MissingNative(e, dataSource);
        }
        catch (BadImageFormatException e)
        {
            throw MissingNative(e, dataSource);
        }
    }

    private static DocsqlException MissingNative(Exception inner, string dataSource) =>
        new(
            $"embedded mode ('Data Source={dataSource}') requires the native library "
            + $"'{EmbeddedNative.LibraryFileName}' built for {RuntimeInformation.RuntimeIdentifier}. "
            + "Get it from the GitHub Releases binary archive (docsql-native-*.tar.gz / .zip), "
            + "place it next to the application, or point DOCSQL_EMBEDDED_NATIVE at its full path.",
            inner);

    public Frame Send(Frame request) => Send(request, -1);

    public Frame Send(Frame request, int readTimeoutMs) => Dispatch(request, readTimeoutMs);

    public Task<Frame> SendAsync(
        Frame request, CancellationToken cancellationToken = default, int readTimeoutMs = -1)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Send(request, readTimeoutMs));
    }

    public (ulong Handle, bool Cached) GetOrPrepare(string template)
    {
        lock (_sync)
        {
            if (_prepared.TryGetValue(template, out var cached))
            {
                return (cached, true);
            }
            var handle = _nextHandle++;
            _prepared[template] = handle;
            return (handle, false);
        }
    }

    public Task<(ulong Handle, bool Cached)> GetOrPrepareAsync(
        string template, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetOrPrepare(template));
    }

    private Frame Dispatch(Frame request, int readTimeoutMs)
    {
        var budget = readTimeoutMs >= 0 ? readTimeoutMs : _readTimeoutMs;
        switch (request.Type)
        {
            case FrameType.ReqSql:
                return Exec(DecodeSql(request.Payload), null, budget);

            case FrameType.ReqPrepare:
            {
                var template = DecodeSql(request.Payload);
                var (handle, _) = GetOrPrepare(template);
                var body = Encoding.UTF8.GetBytes($"{{\"handle\":{handle}}}");
                return new Frame(FrameType.RespPrepared, 0, 0, body);
            }

            case FrameType.ReqExecute:
            {
                var (handle, paramsJson) = DecodeExecuteBody(request.Payload);
                string? template;
                lock (_sync)
                {
                    template = _prepared.FirstOrDefault(kv => kv.Value == handle).Key;
                }
                if (template is null)
                {
                    return Error("execute: unknown embedded statement handle");
                }
                return Exec(template, paramsJson, budget);
            }

            case FrameType.ReqCloseStmt:
            {
                var (handle, _) = DecodeExecuteBody(request.Payload);
                lock (_sync)
                {
                    var key = _prepared.FirstOrDefault(kv => kv.Value == handle).Key;
                    if (key is not null)
                    {
                        _prepared.Remove(key);
                    }
                }
                return new Frame(FrameType.RespAffected, 0, 0, BitConverter.GetBytes(0L));
            }

            case FrameType.ReqPing:
                return new Frame(FrameType.RespPong, 0, 0, Array.Empty<byte>());

            case FrameType.ReqSessionReset:
                // Embedded sessions own no pool-shared T-SQL batch state; the
                // pooled-rent reset is a no-op here.
                return new Frame(FrameType.RespAffected, 0, 0, BitConverter.GetBytes(0L));

            default:
                return Error(
                    $"embedded mode (Data Source=…): {request.Type} is a server feature — "
                    + "pub/sub, PROMOTE, backup and cluster operations need a docsql-server "
                    + "connection (host=…;port=…)");
        }
    }

    private Frame Exec(string sql, string? paramsJson, int timeoutMs)
    {
        var rc = EmbeddedNative.Execute(_handle, sql, paramsJson, timeoutMs, out var payload);
        var text = EmbeddedNative.TakeString(ref payload);
        if (rc != 0)
        {
            return Error(text ?? $"embedded engine error {rc}");
        }
        if (text is null)
        {
            return Error("embedded engine returned no payload");
        }
        // Payload prefix discriminates the two success shapes (same prefixes
        // the native layer emits — rows JSON is handed to the reader verbatim,
        // no extra parse on the read path).
        if (text.StartsWith("{\"affected\":", StringComparison.Ordinal))
        {
            using var doc = JsonDocument.Parse(text);
            var n = doc.RootElement.GetProperty("affected").GetInt64();
            return new Frame(FrameType.RespAffected, 0, 0, BitConverter.GetBytes(n));
        }
        return new Frame(FrameType.RespRows, 0, 0, Encoding.UTF8.GetBytes(text));
    }

    private static Frame Error(string message) =>
        new(FrameType.RespError, 0, 0, Encoding.UTF8.GetBytes(message));

    /// <summary>SQL payload decode (tag 4 + LE length + UTF-8), mirroring
    /// <see cref="ProtocolConnection.EncodeSql"/>.</summary>
    private static string DecodeSql(byte[] payload)
    {
        if (payload.Length < 5 || payload[0] != 4)
        {
            throw new DocsqlException("embedded: malformed SQL payload");
        }
        var len = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(1, 4));
        if (len < 0 || 5 + len > payload.Length)
        {
            throw new DocsqlException("embedded: malformed SQL payload length");
        }
        return Encoding.UTF8.GetString(payload, 5, len);
    }

    /// <summary>REQ_EXECUTE body: <c>{"handle":n,"params":[…]}</c> — the
    /// params array is forwarded as its exact JSON text so `$dec`/`$bytes`/
    /// `$ts`/`$float` markers reach the native binder unchanged.</summary>
    private static (ulong Handle, string? Params) DecodeExecuteBody(byte[] payload)
    {
        using var doc = JsonDocument.Parse(payload);
        var handle = doc.RootElement.GetProperty("handle").GetUInt64();
        var paramsJson = doc.RootElement.TryGetProperty("params", out var p)
            ? p.GetRawText()
            : null;
        return (handle, paramsJson);
    }

    public void Dispose()
    {
        if (_handle != IntPtr.Zero)
        {
            EmbeddedNative.Close(_handle);
            _handle = IntPtr.Zero;
        }
    }
}

/// <summary>
/// P/Invoke bridge to <c>libdocsql_embedded</c> (the <c>docsql-ffi</c> crate).
/// The resolver looks for the library, in order: <c>DOCSQL_EMBEDDED_NATIVE</c>
/// (full path), <c>runtimes/&lt;rid&gt;/native/</c> next to the app, the app
/// directory itself, then <c>target/{debug,release}</c> in every ancestor
/// directory (this repository's `cargo build -p docsql-ffi` layout, which is
/// also what the dotnet test suites use).
/// </summary>
internal static class EmbeddedNative
{
    private const string Lib = "docsql_embedded";

    /// <summary>The platform's real library file name: Rust cdylibs carry the
    /// Unix "lib" prefix (libdocsql_embedded.dylib / .so) but none on Windows.
    /// DllImport probes "docsql_embedded" by name; explicit path probing must
    /// use the actual file name.</summary>
    public static readonly string LibraryFileName =
        OperatingSystem.IsWindows() ? "docsql_embedded.dll"
        : OperatingSystem.IsMacOS() ? "libdocsql_embedded.dylib"
        : "libdocsql_embedded.so";

    static EmbeddedNative()
    {
        NativeLibrary.SetDllImportResolver(typeof(EmbeddedNative).Assembly, Resolve);
    }

    private static IntPtr Resolve(string name, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (!string.Equals(name, Lib, StringComparison.Ordinal))
        {
            return IntPtr.Zero;
        }
        foreach (var candidate in Candidates())
        {
            if (NativeLibrary.TryLoad(candidate, assembly, searchPath, out var handle))
            {
                return handle;
            }
        }
        return IntPtr.Zero;
    }

    private static IEnumerable<string> Candidates()
    {
        var env = Environment.GetEnvironmentVariable("DOCSQL_EMBEDDED_NATIVE");
        if (!string.IsNullOrWhiteSpace(env))
        {
            yield return env;
        }
        var baseDir = AppContext.BaseDirectory;
        var rid = RuntimeInformation.RuntimeIdentifier;
        yield return Path.Combine(baseDir, "runtimes", rid, "native", LibraryFileName);
        yield return Path.Combine(baseDir, LibraryFileName);
        // Dev/test fallback: this repository's `cargo build -p docsql-ffi`
        // layout, found by walking up from the app/test output directory.
        for (var dir = new DirectoryInfo(baseDir); dir is not null; dir = dir.Parent)
        {
            foreach (var profile in new[] { "debug", "release" })
            {
                yield return Path.Combine(dir.FullName, "target", profile, LibraryFileName);
            }
        }
    }

    [DllImport(Lib, EntryPoint = "docsql_embedded_abi_version")]
    public static extern int AbiVersion();

    [DllImport(Lib, EntryPoint = "docsql_embedded_open")]
    public static extern int Open(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        out IntPtr handle,
        out IntPtr error);

    [DllImport(Lib, EntryPoint = "docsql_embedded_close")]
    public static extern void Close(IntPtr handle);

    [DllImport(Lib, EntryPoint = "docsql_embedded_execute")]
    public static extern int Execute(
        IntPtr handle,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string? paramsJson,
        int timeoutMs,
        out IntPtr json);

    [DllImport(Lib, EntryPoint = "docsql_embedded_free")]
    public static extern void Free(IntPtr text);

    /// <summary>Read a native-owned string and release it (null → null).</summary>
    public static string? TakeString(ref IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return null;
        }
        try
        {
            return Marshal.PtrToStringUTF8(ptr);
        }
        finally
        {
            Free(ptr);
            ptr = IntPtr.Zero;
        }
    }
}
