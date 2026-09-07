// ADO.NET provider surface for docsql.

using System.Data;
using System.Data.Common;
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

    /// <param name="connectionString">docsql endpoint ("host=..;port=..;token=..") or,
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
    private static byte[]? ParseKey(string hex)
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
    /// 发送 KV 命令(GET/SET/PROMOTE/...),参数以 \x00 分隔编码。
    /// 返回 (响应帧类型, 文本 payload);错误帧时 payload 为错误消息。
    /// </summary>
    public (FrameType Type, string Payload) Kv(params string[] args)
    {
        var payload = Encoding.UTF8.GetBytes("\x00" + string.Join("\x00", args));
        var resp = Proto.Send(new Frame(FrameType.ReqKv, 0, 0, payload));
        return (resp.Type, Encoding.UTF8.GetString(resp.Payload));
    }

    public override void Open()
    {
        if (_state == ConnectionState.Open)
        {
            return;
        }
        var p = EndpointOverride is { } ep ? ep.ToBuilder() : Parsed;
        _proto = new ProtocolConnection(p.Host, p.Port, ParseKey(KeyOverride ?? p.Key));
        // AUTH when a token is configured.
        if (!string.IsNullOrEmpty(p.Token))
        {
            var payload = Encoding.UTF8.GetBytes($"\x00AUTH\x00{p.Token}");
            var resp = _proto.Send(new Frame(FrameType.ReqKv, 0, 0, payload));
            if (resp.Type == FrameType.RespError)
            {
                _proto.Dispose();
                _proto = null;
                throw new DocsqlException("auth failed: " + Encoding.UTF8.GetString(resp.Payload));
            }
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
        return v is double or float or decimal && ((IConvertible)v).ToInt64(null) == Convert.ToInt64(v)
            ? Convert.ToInt64(v)
            : v;
    }

    public new DocsqlDataReader ExecuteReader() => (DocsqlDataReader)base.ExecuteReader();

    protected override DbTransaction? DbTransaction { get; set; }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var frame = Execute();
        return frame.Type switch
        {
            FrameType.RespRows => new DocsqlDataReader(frame.Payload),
            FrameType.RespAffected => new DocsqlDataReader(Array.Empty<byte>()),
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
    /// tracked for the prepared-statement milestone).
    private string BindParameters()
    {
        var sql = CommandText;
        foreach (DocsqlParameter p in Parameters)
        {
            var literal = p.Value switch
            {
                null => "NULL",
                int or long or short or byte => p.Value.ToString(),
                double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
                float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
                decimal m => m.ToString(System.Globalization.CultureInfo.InvariantCulture),
                bool b => b ? "TRUE" : "FALSE",
                _ => $"'{p.Value.ToString()!.Replace("'", "''")}'",
            };
            var name = p.ParameterName?.TrimStart('@') ?? "";
            sql = sql.Replace($"@{name}", literal);
        }
        return sql;
    }

    private static string ErrorText(Frame f) => Encoding.UTF8.GetString(f.Payload);

    private static int DecodeAffected(byte[] payload) =>
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
    private int _pos = -1;

    internal DocsqlDataReader(byte[] payload)
    {
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
    public override int RecordsAffected => _rows.Count;

    public override bool Read()
    {
        if (_pos + 1 >= _rows.Count)
        {
            return false;
        }
        _pos++;
        return true;
    }

    public override object? GetValue(int ordinal) => _rows[_pos][ordinal];
    public override bool IsDBNull(int ordinal) => _rows[_pos][ordinal] is null;
    public override string GetName(int ordinal) => _columns[ordinal];
    public override int GetOrdinal(string name) => _columns.IndexOf(name);

    public override long GetInt64(int ordinal) => Convert.ToInt64(_rows[_pos][ordinal]);
    public override int GetInt32(int ordinal) => Convert.ToInt32(_rows[_pos][ordinal]);
    public override double GetDouble(int ordinal) => Convert.ToDouble(_rows[_pos][ordinal]);
    public override string GetString(int ordinal) => Convert.ToString(_rows[_pos][ordinal])!;
    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(_rows[_pos][ordinal]);

    public override int GetValues(object[] values)
    {
        int n = Math.Min(values.Length, _columns.Count);
        for (int i = 0; i < n; i++)
        {
            values[i] = _rows[_pos][i];
        }
        return n;
    }

    public override System.Collections.IEnumerator GetEnumerator() => _rows.GetEnumerator();

    public override object this[int ordinal] => GetValue(ordinal)!;

    public override object this[string name] => GetValue(GetOrdinal(name))!;

    public override short GetInt16(int ordinal) => Convert.ToInt16(_rows[_pos][ordinal]);


    public override System.Data.DataTable GetSchemaTable() => new();

    #region Not needed for basic flows

    public override int Depth => 0;
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    public override Type GetFieldType(int ordinal) => (GetValue(ordinal) ?? DBNull.Value).GetType();
    public override char GetChar(int ordinal) => Convert.ToChar(_rows[_pos][ordinal]);
    public override byte GetByte(int ordinal) => Convert.ToByte(_rows[_pos][ordinal]);
    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));
    public override float GetFloat(int ordinal) => Convert.ToSingle(_rows[_pos][ordinal]);
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(_rows[_pos][ordinal]);
    public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(_rows[_pos][ordinal]);
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

    private void Run(string sql) => _conn.Proto.Send(
        new Frame(FrameType.ReqSql, 0, 0, ProtocolConnection.EncodeSql(sql)));
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

/// Concrete DbException for docsql errors.
public sealed class DocsqlException : DbException
{
    public DocsqlException(string message) : base(message) { }
}
