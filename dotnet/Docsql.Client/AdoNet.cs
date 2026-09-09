// ADO.NET provider surface for DocSQL.

using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;

namespace Docsql.Client;

public static class EndpointExtensions
{
    public static DocsqlConnectionStringBuilder ToBuilder(
        this (string host, int port, string token) ep) =>
        new() { Host = ep.host, Port = ep.port, Token = ep.token };
}

public sealed class DocsqlConnectionStringBuilder : DbConnectionStringBuilder
{
    public string Host
    {
        get => TryGetValue("host", out var v) ? (string)v : "127.0.0.1";
        set => this["host"] = value;
    }

    public int Port
    {
        get => TryGetValue("port", out var v) ? int.Parse((string)v) : 7600;
        set => this["port"] = value.ToString();
    }

    public string Token
    {
        get => TryGetValue("token", out var v) ? (string)v : "";
        set => this["token"] = value;
    }

    /// <summary>传输加密密钥(64 位 hex 字符,32 字节 AES-256-GCM)。</summary>
    public string Key
    {
        get => TryGetValue("key", out var v) ? (string)v : "";
        set => this["key"] = value;
    }
}

public sealed class DocsqlConnection : DbConnection
{
    private ConnectionState _state = ConnectionState.Closed;
    private ProtocolConnection? _proto;

    public DocsqlConnection() { }

    /// <param name="connectionString">DocSQL endpoint ("host=..;port=..;token=..") or,
    /// for EF-compatible callers, any string whose endpoint parts live in EndpointOverride.</param>
    public DocsqlConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// Optional explicit endpoint; when set it wins over ConnectionString
    /// (lets hosts like EF's SQLite layer rewrite ConnectionString freely).
    public (string host, int port, string token)? EndpointOverride { get; set; }

    /// Optional explicit transport key (hex); wins over ConnectionString's
    /// key= when EF rewrites the connection string.</summary>
    public string? KeyOverride { get; set; }

    /// <summary>hex 密钥 → 32 字节;空串返回 null(明文模式)。</summary>
    internal static byte[]? ParseKey(string hex)
    {
        hex = hex.Trim();
        if (hex.Length == 0) return null;
        if (hex.Length != 64)
            throw new ArgumentException("key 必须是 64 位 hex(32 字节)");
        return Convert.FromHexString(hex);
    }

    private DocsqlConnectionStringBuilder Parsed => new() { ConnectionString = ConnectionString };

    public override string ConnectionString { get; set; } = "";

    public override string Database => "docsql";

    public override string DataSource => $"{Parsed.Host}:{Parsed.Port}";

    public override string ServerVersion => "0.1";

    public override ConnectionState State => _state;

    internal ProtocolConnection Proto =>
        _proto ?? throw new InvalidOperationException("connection is closed");

    public override void ChangeDatabase(string databaseName) { }

    /// <summary>
    /// 故障转移提升(REQ_PROMOTE):把只读副本提升为可写节点。
    /// </summary>
    public void Promote()
    {
        var resp = Proto.Send(new Frame(FrameType.ReqPromote, 0, 0, Array.Empty<byte>()));
        if (resp.Type == FrameType.RespError)
        {
            throw new DocsqlException(Encoding.UTF8.GetString(resp.Payload));
        }
    }

    /// <summary>
    /// 发布一条持久化 pub/sub 消息(REQ_PUBLISH):服务端先落盘(WAL)再向
    /// 订阅者推送。返回 (消息 id, 实时收到的连接数);id 单调递增,可作为
    /// 断线续传的游标(DocsqlSubscriber 的 from 参数)。
    /// </summary>
    public (long Id, long Receivers) Publish(string channel, string payload)
    {
        var body = JsonSerializer.Serialize(new { channel, payload });
        var resp = Proto.Send(new Frame(FrameType.ReqPublish, 0, 0, Encoding.UTF8.GetBytes(body)));
        if (resp.Type == FrameType.RespError)
        {
            throw new DocsqlException(Encoding.UTF8.GetString(resp.Payload));
        }
        using var doc = JsonDocument.Parse(Encoding.UTF8.GetString(resp.Payload));
        var row = doc.RootElement.GetProperty("rows")[0];
        return (row[0].GetInt64(), row[1].GetInt64());
    }

    /// <summary>
    /// 保留某频道最新 keep 条消息,更早的删除(REQ_PUBSUB trim,随写复制)。
    /// 返回删除的条数。keep 必须 ≥ 1(清空会破坏 id 单调性)。
    /// </summary>
    public long PubsubTrim(string channel, long keep)
    {
        var body = JsonSerializer.Serialize(new { sub = "trim", channel, keep });
        var resp = Proto.Send(new Frame(FrameType.ReqPubsub, 0, 0, Encoding.UTF8.GetBytes(body)));
        return resp.Type switch
        {
            FrameType.RespAffected => DecodeLong(resp.Payload),
            _ => throw new DocsqlException(Encoding.UTF8.GetString(resp.Payload)),
        };
    }

    internal static long DecodeLong(byte[] payload) =>
        payload.Length >= 8 ? BitConverter.ToInt64(payload, 0) : 0;

    public override void Open()
    {
        if (_state == ConnectionState.Open)
        {
            return;
        }
        var p = EndpointOverride is { } ep ? ep.ToBuilder() : Parsed;
        try
        {
            _proto = new ProtocolConnection(p.Host, p.Port, ParseKey(KeyOverride ?? p.Key));
            // AUTH when a token is configured (REQ_AUTH carries the raw token).
            if (!string.IsNullOrEmpty(p.Token))
            {
                var payload = Encoding.UTF8.GetBytes(p.Token);
                var resp = _proto.Send(new Frame(FrameType.ReqAuth, 0, 0, payload));
                if (resp.Type == FrameType.RespError)
                {
                    throw new DocsqlException("auth failed: " + Encoding.UTF8.GetString(resp.Payload));
                }
            }
        }
        catch
        {
            // Never leak the socket on a failed open (retry would orphan it).
            _proto?.Dispose();
            _proto = null;
            _state = ConnectionState.Broken;
            throw;
        }
        _state = ConnectionState.Open;
    }

    public override void Close()
    {
        _proto?.Dispose();
        _proto = null;
        _state = ConnectionState.Closed;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        new DocsqlTransaction(this, isolationLevel);

    protected override DbCommand CreateDbCommand() =>
        new DocsqlCommand { Connection = this };

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }
}

public sealed class DocsqlParameter : DbParameter
{
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }
    public override string? ParameterName { get; set; }
    public override int Size { get; set; }
    public override string SourceColumn { get; set; } = "";
    public override bool SourceColumnNullMapping { get; set; }
    public override object? Value { get; set; }

    public override void ResetDbType() => DbType = DbType.Object;
}

public sealed class DocsqlCommand : DbCommand
{
    public DocsqlCommand() { }

    public new DocsqlConnection? Connection { get; set; }

    protected override DbConnection? DbConnection
    {
        get => Connection;
        set => Connection = value as DocsqlConnection ?? throw new ArgumentException("wrong connection type");
    }

    public override string CommandText { get; set; } = "";

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override System.Data.UpdateRowSource UpdatedRowSource { get; set; } = System.Data.UpdateRowSource.None;

    public override bool DesignTimeVisible { get; set; }

    protected override bool CanRaiseEvents => false;

    public new DocsqlParameterCollection Parameters { get; } = new();

    protected override DbParameterCollection DbParameterCollection => Parameters;

    public override void Cancel() { }

    public override int ExecuteNonQuery()
    {
        var frame = Execute();
        return frame.Type switch
        {
            FrameType.RespAffected => DecodeAffected(frame.Payload),
            FrameType.RespRows => throw new DocsqlException("statement returned rows"),
            _ => throw new DocsqlException(ErrorText(frame)),
        };
    }

    public override object? ExecuteScalar()
    {
        using var reader = ExecuteReader();
        if (!reader.Read() || reader.FieldCount == 0)
        {
            return null;
        }
        var v = reader.GetValue(0);
        if (v is double or float or decimal)
        {
            // Only narrow to long when exactly representable; out-of-range
            // doubles (1e300, SUM overflow) must not throw OverflowException.
            try
            {
                if (((IConvertible)v).ToInt64(null) == Convert.ToInt64(v))
                {
                    return Convert.ToInt64(v);
                }
            }
            catch (OverflowException)
            {
            }
        }
        return v;
    }

    public new DocsqlDataReader ExecuteReader() => (DocsqlDataReader)base.ExecuteReader();

    protected override DbTransaction? DbTransaction { get; set; }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var frame = Execute();
        return frame.Type switch
        {
            FrameType.RespRows => new DocsqlDataReader(frame.Payload),
            FrameType.RespAffected => new DocsqlDataReader(
                Array.Empty<byte>(), DecodeAffected(frame.Payload)),
            _ => throw new DocsqlException(ErrorText(frame)),
        };
    }

    private Frame Execute()
    {
        if (Connection is not { State: ConnectionState.Open } conn)
        {
            throw new InvalidOperationException("connection is not open");
        }
        var sql = BindParameters();
        return conn.Proto.Send(new Frame(FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql(sql)));
    }

    /// Substitute @name parameters (client-side v1; server-side binding is
    /// tracked for the prepared-statement milestone). A single scanner pass
    /// skips '...' string literals and matches whole identifiers, so
    /// <c>@id</c> never rewrites <c>@id2</c>, literals containing
    /// <c>@name</c> stay intact, and a parameter's own value can never be
    /// rewritten by a later parameter.
    private string BindParameters()
    {
        if (Parameters.Count == 0)
        {
            return CommandText;
        }
        var sql = CommandText;
        var sb = new StringBuilder(sql.Length);
        int i = 0;
        while (i < sql.Length)
        {
            char c = sql[i];
            if (c == '\'')
            {
                // Copy the string literal verbatim ('' is an escaped quote).
                int j = i + 1;
                while (j < sql.Length)
                {
                    if (sql[j] == '\'')
                    {
                        if (j + 1 < sql.Length && sql[j + 1] == '\'')
                        {
                            j += 2;
                            continue;
                        }
                        j++;
                        break;
                    }
                    j++;
                }
                sb.Append(sql[i..j]);
                i = j;
                continue;
            }
            if (c == '@')
            {
                int k = i + 1;
                while (k < sql.Length && (char.IsLetterOrDigit(sql[k]) || sql[k] == '_'))
                {
                    k++;
                }
                if (k > i + 1)
                {
                    var name = sql[(i + 1)..k];
                    var p = FindParameter(name);
                    if (p is not null)
                    {
                        sb.Append(LiteralOf(p));
                        i = k;
                        continue;
                    }
                }
            }
            sb.Append(c);
            i++;
        }
        return sb.ToString();
    }

    private DocsqlParameter? FindParameter(string name) =>
        Parameters.Cast<DocsqlParameter>().FirstOrDefault(
            p => (p.ParameterName?.TrimStart('@') ?? "") == name);

    private static string LiteralOf(DocsqlParameter p) => p.Value switch
    {
        null or DBNull => "NULL",
        int or long or short or byte => p.Value.ToString()!,
        double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
        float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
        // Numeric literal: the engine stores decimals as f64 — big values
        // lose precision beyond ~15-16 significant digits (no decimal type).
        decimal m => m.ToString(System.Globalization.CultureInfo.InvariantCulture),
        bool b => b ? "TRUE" : "FALSE",
        // Date/time values must round-trip in a culture-invariant,
        // lexicographically sortable text form (the engine stores TEXT):
        // a culture-dependent ToString() sorts wrongly and cannot be parsed
        // back by GetDateTime on machines with another culture.
        DateTime dt => $"'{dt.ToString("O", System.Globalization.CultureInfo.InvariantCulture)}'",
        DateTimeOffset dto => $"'{dto.ToString("O", System.Globalization.CultureInfo.InvariantCulture)}'",
        TimeSpan ts => $"'{ts.ToString("c", System.Globalization.CultureInfo.InvariantCulture)}'",
        // No BLOB storage in the engine; storing ToString() would corrupt
        // data silently — refuse loudly instead.
        byte[] => throw new NotSupportedException(
            "byte[] parameters are not supported (no BLOB storage); serialize to TEXT/Base64"),
        _ => $"'{p.Value.ToString()!.Replace("'", "''")}'",
    };

    private static string ErrorText(Frame f) => Encoding.UTF8.GetString(f.Payload);

    internal static int DecodeAffected(byte[] payload) =>
        payload.Length >= 8 ? BitConverter.ToInt32(payload, 0) : 0;

    public override void Prepare() { }

    protected override DbParameter CreateDbParameter() => new DocsqlParameter();
}

public sealed class DocsqlParameterCollection : DbParameterCollection
{
    private readonly List<DocsqlParameter> _list = new();

    public override int Count => _list.Count;
    public override object SyncRoot => _list;

    public DocsqlParameter AddWithValue(string name, object? value)
    {
        var p = new DocsqlParameter { ParameterName = name, Value = value };
        _list.Add(p);
        return p;
    }

    public override int Add(object value)
    {
        _list.Add((DocsqlParameter)value);
        return _list.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var v in values)
        {
            Add(v);
        }
    }

    public override void Clear() => _list.Clear();

    public override bool Contains(object value) => _list.Contains((DocsqlParameter)value);

    public override bool Contains(string value) => _list.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index)
    {
        for (int i = 0; i < _list.Count; i++)
        {
            array.SetValue(_list[i], index + i);
        }
    }

    public override System.Collections.IEnumerator GetEnumerator() => _list.GetEnumerator();

    public override int IndexOf(object value) => _list.IndexOf((DocsqlParameter)value);

    public override int IndexOf(string parameterName) =>
        _list.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value) => _list.Insert(index, (DocsqlParameter)value);

    public override void Remove(object value) => _list.Remove((DocsqlParameter)value);

    public override void RemoveAt(int index) => _list.RemoveAt(index);

    public override void RemoveAt(string parameterName) => _list.RemoveAt(IndexOf(parameterName));

    protected override DbParameter GetParameter(int index) => _list[index];

    protected override DbParameter GetParameter(string parameterName) => _list[IndexOf(parameterName)];

    protected override void SetParameter(int index, DbParameter value) => _list[index] = (DocsqlParameter)value;

    protected override void SetParameter(string parameterName, DbParameter value) =>
        _list[IndexOf(parameterName)] = (DocsqlParameter)value;
}

public sealed class DocsqlDataReader : DbDataReader
{
    private readonly List<string> _columns = new();
    private readonly List<object?[]> _rows = new();
    private readonly List<Type> _types = new();
    private int _pos = -1;

    private readonly int _recordsAffected;

    internal DocsqlDataReader(byte[] payload, int recordsAffected = 0)
    {
        _recordsAffected = recordsAffected;
        if (payload.Length == 0)
        {
            return;
        }
        using var doc = JsonDocument.Parse(Encoding.UTF8.GetString(payload));
        foreach (var c in doc.RootElement.GetProperty("columns").EnumerateArray())
        {
            _columns.Add(c.GetString() ?? "");
        }
        foreach (var r in doc.RootElement.GetProperty("rows").EnumerateArray())
        {
            _rows.Add(r.EnumerateArray().Select(ElemToValue).ToArray());
        }
        // Column type from the first non-null value (schemaless storage).
        for (int i = 0; i < _columns.Count; i++)
        {
            _types.Add(_rows.FirstOrDefault(r => r[i] is not null)?[i]?.GetType() ?? typeof(object));
        }
    }

    private static object? ElemToValue(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Number => e.TryGetInt64(out var l) ? l : e.GetDouble(),
        JsonValueKind.String => e.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        _ => e.GetRawText(),
    };

    public override int FieldCount => _columns.Count;
    public override bool HasRows => _rows.Count > 0;
    public override bool IsClosed => false;
    public override int RecordsAffected => _recordsAffected;

    public override bool Read()
    {
        if (_pos + 1 >= _rows.Count)
        {
            return false;
        }
        _pos++;
        return true;
    }

    private object?[] CurrentRow => _pos >= 0 && _pos < _rows.Count
        ? _rows[_pos]
        : throw new InvalidOperationException("no current row (call Read first)");

    // ADO.NET contract: NULL columns surface as DBNull.Value, not null.
    public override object GetValue(int ordinal) => CurrentRow[ordinal] ?? DBNull.Value;
    public override bool IsDBNull(int ordinal) => CurrentRow[ordinal] is null;
    public override string GetName(int ordinal) => _columns[ordinal];
    public override int GetOrdinal(string name)
    {
        var idx = _columns.IndexOf(name);
        return idx >= 0 ? idx : throw new IndexOutOfRangeException($"no column named '{name}'");
    }

    public override long GetInt64(int ordinal) => Convert.ToInt64(CurrentRow[ordinal]);
    public override int GetInt32(int ordinal) => Convert.ToInt32(CurrentRow[ordinal]);
    public override double GetDouble(int ordinal) => Convert.ToDouble(CurrentRow[ordinal]);
    public override string GetString(int ordinal) => Convert.ToString(CurrentRow[ordinal])!;
    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(CurrentRow[ordinal]);

    public override int GetValues(object[] values)
    {
        int n = Math.Min(values.Length, _columns.Count);
        for (int i = 0; i < n; i++)
        {
            // Contract: NULL must come back as DBNull.Value.
            values[i] = CurrentRow[i] ?? DBNull.Value;
        }
        return n;
    }

    public override System.Collections.IEnumerator GetEnumerator() => _rows.GetEnumerator();

    public override object this[int ordinal] => GetValue(ordinal)!;

    public override object this[string name] => GetValue(GetOrdinal(name))!;

    public override short GetInt16(int ordinal) => Convert.ToInt16(CurrentRow[ordinal]);


    public override System.Data.DataTable GetSchemaTable()
    {
        var t = new System.Data.DataTable();
        t.Columns.Add("ColumnName", typeof(string));
        t.Columns.Add("ColumnOrdinal", typeof(int));
        t.Columns.Add("DataType", typeof(Type));
        t.Columns.Add("AllowDBNull", typeof(bool));
        for (int i = 0; i < _columns.Count; i++)
        {
            int ordinal = i;
            bool nullable = _rows.Any(r => r[ordinal] is null);
            t.Rows.Add(_columns[i], ordinal, _types[i], nullable);
        }
        return t;
    }

    #region Not needed for basic flows

    public override int Depth => 0;
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    public override Type GetFieldType(int ordinal) => _types[ordinal];
    public override char GetChar(int ordinal) => Convert.ToChar(CurrentRow[ordinal]);
    public override byte GetByte(int ordinal) => Convert.ToByte(CurrentRow[ordinal]);
    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));
    public override float GetFloat(int ordinal) => Convert.ToSingle(CurrentRow[ordinal]);
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(CurrentRow[ordinal]);
    public override DateTime GetDateTime(int ordinal) =>
        Convert.ToDateTime(CurrentRow[ordinal], CultureInfo.InvariantCulture);
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
    public override bool NextResult() => false;

    #endregion
}

public sealed class DocsqlTransaction : DbTransaction
{
    private readonly DocsqlConnection _conn;
    private bool _done;

    public DocsqlTransaction(DocsqlConnection conn, IsolationLevel iso)
    {
        _conn = conn;
        IsolationLevel = iso;
        Run("BEGIN");
    }

    public override IsolationLevel IsolationLevel { get; }

    protected override DbConnection DbConnection => _conn;

    public override void Commit()
    {
        if (_done)
        {
            throw new InvalidOperationException("transaction already finished");
        }
        Run("COMMIT");
        _done = true;
    }

    public override void Rollback()
    {
        if (_done)
        {
            throw new InvalidOperationException("transaction already finished");
        }
        Run("ROLLBACK");
        _done = true;
    }

    // ADO.NET contract: disposing an unfinished transaction rolls it back —
    // otherwise `using (var tx = ...)` without Commit would leak an open
    // server-side transaction (every later BEGIN fails, writes buffer).
    protected override void Dispose(bool disposing)
    {
        if (!_done)
        {
            _done = true;
            try
            {
                _conn.Proto.Send(new Frame(
                    FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql("ROLLBACK")));
            }
            catch
            {
                // Connection already broken; nothing to roll back.
            }
        }
        base.Dispose(disposing);
    }

    private void Run(string sql)
    {
        var resp = _conn.Proto.Send(
            new Frame(FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql(sql)));
        if (resp.Type == FrameType.RespError)
        {
            // A failed BEGIN/COMMIT/ROLLBACK must surface: reporting success
            // would tell the caller data is durable when it is not.
            throw new DocsqlException(Encoding.UTF8.GetString(resp.Payload));
        }
    }
}

public sealed class DocsqlFactory : DbProviderFactory
{
    public static readonly DocsqlFactory Instance = new();

    public override DbConnection CreateConnection() => new DocsqlConnection();
    public override DbCommand CreateCommand() => new DocsqlCommand();
    public override DbParameter CreateParameter() => new DocsqlParameter();
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() =>
        new DocsqlConnectionStringBuilder();
}

/// Concrete DbException for DocSQL errors.
public sealed class DocsqlException : DbException
{
    public DocsqlException(string message) : base(message) { }
}
