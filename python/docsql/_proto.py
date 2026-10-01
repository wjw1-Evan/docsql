"""DocSQL wire protocol v1 — frame codec (pure stdlib).

Mirrors `crates/docsql-core/src/proto.rs`: a 20-byte little-endian header

    magic:u32 "DSQ1" | flags:u16 | frame_type:u16 | topology_version:u64
    | payload_len:u32

followed by the payload. Only the frames a client driver speaks live here;
replication/join frames are node-internal and intentionally absent.
"""

import struct

from .errors import InterfaceError

MAGIC = 0x31515344  # "DSQ1"
# magic:u32 | flags:u16 | frame_type:u16 | topology_version:u64 | len:u32
HEADER = struct.Struct("<IHHQI")
HEADER_LEN = HEADER.size
MAX_FRAME_BYTES = 64 * 1024 * 1024

FLAG_REPLICATION = 0x0002
FLAG_ENCRYPTED = 0x0004

REQ_SQL = 0x0001
REQ_AUTH = 0x0002
REQ_AUTH_USER = 0x0018
REQ_PREPARE = 0x0003
REQ_EXECUTE = 0x0004
REQ_CLOSE_STMT = 0x0005
REQ_PING = 0x0006
REQ_STATUS = 0x0008
REQ_SUBSCRIBE = 0x0009
REQ_PSUBSCRIBE = 0x000A
REQ_UNSUBSCRIBE = 0x000B
REQ_PUNSUBSCRIBE = 0x000C
REQ_PUBLISH = 0x000D
REQ_PUBSUB = 0x000E

RESP_ROWS = 0x0101
RESP_AFFECTED = 0x0102
RESP_ERROR = 0x0103
RESP_PONG = 0x0105
RESP_STATUS = 0x0106
RESP_PUSH = 0x0107
RESP_PREPARED = 0x010E
RESP_HELLO = 0x010F


class ProtocolError(InterfaceError):
    """A framing-level failure (bad magic, oversized frame, EOF mid-frame).

    The connection is unusable after one of these; callers must discard it.
    Subclasses the DB-API InterfaceError so every ``except Error`` recovery
    path (subscriber reconnect loops, ``ping(reconnect=True)``) sees it —
    as a plain ``Exception`` it used to escape those handlers and kill the
    reader thread.
    """


def sql_payload(sql):
    """REQ_SQL / REQ_PREPARE payloads are length-prefixed UTF-8 — the
    engine's `Value::Str` encoding (tag 0x04 + u32 LE length), not raw
    text (see core/proto.rs `encode_sql`)."""
    body = sql.encode("utf-8")
    return b"\x04" + struct.pack("<I", len(body)) + body


def encode_frame(frame_type, payload, flags=0, topology_version=0):
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError(
            f"frame payload {len(payload)} bytes exceeds the {MAX_FRAME_BYTES}-byte wire cap"
        )
    return HEADER.pack(MAGIC, flags, frame_type, topology_version, len(payload)) + payload


def decode_header(buf):
    """Decode a 20-byte header; returns (flags, frame_type, payload_len)."""
    if len(buf) < HEADER_LEN:
        raise ProtocolError(f"truncated header: {len(buf)} < {HEADER_LEN} bytes")
    magic, flags, frame_type, _topology, length = HEADER.unpack_from(buf, 0)
    if magic != MAGIC:
        raise ProtocolError(f"bad frame magic {magic:#010x}")
    if length > MAX_FRAME_BYTES:
        raise ProtocolError(f"server advertised a {length}-byte frame (cap {MAX_FRAME_BYTES})")
    return flags, frame_type, length
