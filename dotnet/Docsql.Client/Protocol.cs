// docsql wire protocol v1 client.
// Frame: magic "DSQ1" | flags:u16 | type:u16 | topology_version:u64 | len:u32 | payload

using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;

namespace Docsql.Client;

public enum FrameType : ushort
{
    ReqSql = 0x0001,
    ReqKv = 0x0002,
    ReqPing = 0x0006,
    RespRows = 0x0101,
    RespAffected = 0x0102,
    RespError = 0x0103,
    RespPong = 0x0105,
}

public readonly record struct Frame(FrameType Type, ushort Flags, ulong TopologyVersion, byte[] Payload)
{
    public byte[] Encode()
    {
        var buf = new byte[20 + Payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(0, 4), 0x31515344);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(4, 2), Flags);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(6, 2), (ushort)Type);
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(8, 8), TopologyVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(16, 4), (uint)Payload.Length);
        Payload.CopyTo(buf, 20);
        return buf;
    }
}

public sealed class ProtocolConnection : IDisposable
{
    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;
    private readonly byte[] _header = new byte[20];

    public ProtocolConnection(string host, int port)
    {
        _tcp = new TcpClient(host, port);
        _stream = _tcp.GetStream();
    }

    /// SQL text payload: length-prefixed UTF-8, tag 4 (string).
    public static byte[] EncodeSql(string sql)
    {
        var utf8 = Encoding.UTF8.GetBytes(sql);
        var buf = new byte[5 + utf8.Length];
        buf[0] = 4;
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1, 4), utf8.Length);
        utf8.CopyTo(buf, 5);
        return buf;
    }

    public Frame Send(Frame request)
    {
        _stream.Write(request.Encode());
        _stream.Flush();
        return ReadFrame();
    }

    private Frame ReadFrame()
    {
        ReadExact(_header);
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(_header.AsSpan(0, 4));
        if (magic != 0x31515344)
        {
            throw new IOException("bad frame magic");
        }
        var type = (FrameType)BinaryPrimitives.ReadUInt16LittleEndian(_header.AsSpan(6, 2));
        var flags = BinaryPrimitives.ReadUInt16LittleEndian(_header.AsSpan(4, 2));
        var topo = BinaryPrimitives.ReadUInt64LittleEndian(_header.AsSpan(8, 8));
        int len = (int)BinaryPrimitives.ReadUInt32LittleEndian(_header.AsSpan(16, 4));
        var payload = new byte[len];
        ReadExact(payload);
        return new Frame(type, flags, topo, payload);
    }

    private void ReadExact(byte[] buf)
    {
        int off = 0;
        while (off < buf.Length)
        {
            int n = _stream.Read(buf, off, buf.Length - off);
            if (n == 0)
            {
                throw new IOException("connection closed");
            }
            off += n;
        }
    }

    public void Dispose()
    {
        _stream.Dispose();
        _tcp.Dispose();
    }
}
