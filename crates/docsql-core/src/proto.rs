//! docsql wire protocol (v1) — frame definitions.
//!
//! All integers little-endian. A request frame:
//! ```text
//! magic:u32 "DSQ1" | flags:u16 | frame_type:u16 | topology_version:u64
//! | payload_len:u32 | payload:bytes
//! ```
//! - `topology_version` is reserved for the cluster milestone: 0 in
//!   single-node mode, otherwise the routing-table version the request was
//!   made against (mismatch may yield a REDIRECT response).
//! - `flags` bit 0: compressed (reserved), bits 1..15 reserved.
//!
//! Response frame uses the same header with frame_type from [resp constants];
//! a REDIRECT response carries `node_host:port` in its payload.

use crate::encode::{self, EncodeError};

pub const MAGIC: u32 = 0x31515344; // "DSQ1"
pub const HEADER_LEN: usize = 4 + 2 + 2 + 8 + 4;

// Request frame types.
pub const REQ_SQL: u16 = 0x0001;
/// Session authentication: payload = token bytes; success responds with
/// RESP_AFFECTED ("ok"), failure with RESP_ERROR.
pub const REQ_AUTH: u16 = 0x0002;
pub const REQ_PREPARE: u16 = 0x0003;
pub const REQ_EXECUTE: u16 = 0x0004;
pub const REQ_CLOSE_STMT: u16 = 0x0005;
pub const REQ_PING: u16 = 0x0006;
/// Failover promotion: clears read-only mode on this node (requires an
/// authenticated session). Replaces the former KV `PROMOTE` command.
pub const REQ_PROMOTE: u16 = 0x0007;
/// Node status report for cluster monitoring (requires an authenticated
/// session): responds with RESP_STATUS whose payload is a JSON object
/// (uptime / read-only / peers / storage / durable LSN / table totals).
pub const REQ_STATUS: u16 = 0x0008;
/// Pub/sub: subscribe to a channel (authenticated). Payload = JSON
/// `{"channel": str, "from": "latest"|"earliest"|"<id>"}`. Responds
/// RESP_AFFECTED (the connection's subscription count); when `from` asks
/// for history, RESP_PUSH replay frames follow the confirmation in id
/// order before live pushes resume.
pub const REQ_SUBSCRIBE: u16 = 0x0009;
/// Pub/sub: pattern subscribe with Redis-style glob (`*`, `?`, `[...]`).
/// Payload = JSON `{"pattern": str, "from": ...}` like REQ_SUBSCRIBE.
pub const REQ_PSUBSCRIBE: u16 = 0x000A;
/// Pub/sub: unsubscribe. Payload = JSON array of channel names; an empty
/// array unsubscribes everything on this connection. Responds
/// RESP_AFFECTED (remaining subscription count).
pub const REQ_UNSUBSCRIBE: u16 = 0x000B;
/// Pub/sub: pattern unsubscribe. Payload = JSON array of patterns; empty
/// array removes all pattern subscriptions.
pub const REQ_PUNSUBSCRIBE: u16 = 0x000C;
/// Pub/sub: publish (authenticated). Payload = JSON
/// `{"channel": str, "payload": str}`. The message is persisted in the
/// engine before any push leaves this node. Responds RESP_ROWS with
/// columns `[id, receivers]` — the persisted message id on this node and
/// the number of live connections that received the push (this node plus
/// replicated peers).
pub const REQ_PUBLISH: u16 = 0x000D;
/// Pub/sub introspection / retention (authenticated). Payload = JSON
/// `{"sub": "channels"|"numsub"|"numpat"|"trim", ...}`; the first three
/// respond RESP_ROWS, `trim` (keep the newest N messages of a channel,
/// replicated like a write) responds RESP_AFFECTED.
pub const REQ_PUBSUB: u16 = 0x000E;
/// Node logs report for the web console's logs page (requires an
/// authenticated session). Payload = optional JSON `{"limit": n}`
/// (default 200, capped at 1000); responds RESP_LOGS whose payload is
/// JSON `{"query": [...], "sync": [...]}` — recent statement audit
/// entries (the docsql_log ring) and replication/sync events, newest
/// first.
pub const REQ_LOGS: u16 = 0x000F;
/// Cluster join: a fresh node asks a peer for the cluster's current state.
/// Payload = the joiner's advertised `host:port` (empty when it does not
/// want to be registered, e.g. static-config deployments). The peer
/// quiesces the cluster (see REQ_HOLD), captures a full dump, registers
/// the joiner everywhere, then streams RESP_SYNC chunk frames terminated
/// by RESP_AFFECTED (user-table count).
pub const REQ_SYNC: u16 = 0x0010;
/// Cluster-join helper sent by the node serving REQ_SYNC to each of its
/// peers: acquire the write path (waiting out in-flight writes), register
/// the joiner from the payload, and hold until REQ_RELEASE (or an expiry
/// watchdog) so no write slips between the dump snapshot and the
/// joiner's registration. Responds RESP_AFFECTED carrying the hold id
/// (u64 LE) used by the matching REQ_RELEASE.
pub const REQ_HOLD: u16 = 0x0011;
/// Drop one hold acquired via REQ_HOLD. Payload = hold id (u64 LE).
/// The joiner stays registered as a peer.
pub const REQ_RELEASE: u16 = 0x0012;
/// Object-explorer metadata for the web console's node switching (requires
/// an authenticated session). No payload. Responds RESP_META whose payload
/// is the console `/api/meta` JSON (server/storage/totals/tables) assembled
/// by `core::meta::build_meta` — identical in shape to what the console
/// builds for its own embedded engine.
pub const REQ_META: u16 = 0x0013;
/// Per-table replication fingerprints for the cluster rejoin repair
/// (requires an authenticated session; rides FLAG_REPLICATION like every
/// node-internal frame). No payload. Responds RESP_DIGEST.
pub const REQ_DIGEST: u16 = 0x0014;
/// A replication write that carries its origin's journal position:
/// payload = [u64 seq][u32 node_id len][node_id bytes][encode_sql(sql)].
/// The receiver records (node_id, seq) after applying, so a rejoin can
/// pull exactly the ops it missed (see REQ_CATCHUP). Peers that predate
/// this frame answer RESP_ERROR; the sender then resends as plain
/// REQ_SQL, giving up incremental catch-up against old peers.
pub const REQ_SQL_SEQ: u16 = 0x0015;
/// Catch-up pull for the rejoin repair (authenticated +
/// FLAG_REPLICATION): payload = [u64 after_seq]. The origin streams its
/// journal entries with seq > after_seq as RESP_CATCHUP chunks and
/// terminates with RESP_AFFECTED carrying its journal head.
pub const REQ_CATCHUP: u16 = 0x0016;
/// Backup management (authenticated): payload = JSON
/// `{"action": "list"|"trigger"}`. `list` reports the node's backup
/// directory contents and last-attempt status; `trigger` runs one backup
/// now (rejected for read-only connections). Both respond RESP_BACKUP.
pub const REQ_BACKUP: u16 = 0x0017;

// Response frame types.
pub const RESP_ROWS: u16 = 0x0101;
pub const RESP_AFFECTED: u16 = 0x0102;
pub const RESP_ERROR: u16 = 0x0103;
/// Cluster redirect: payload = "host:port" (reserved, M12+).
pub const RESP_REDIRECT: u16 = 0x0104;
pub const RESP_PONG: u16 = 0x0105;
/// Node status report: payload = JSON object (see REQ_STATUS).
pub const RESP_STATUS: u16 = 0x0106;
/// Pub/sub push (server-initiated; only sent to connections that
/// subscribed). Payload = JSON `{"kind":"message"|"pmessage", "pattern"?,
/// "channel", "id", "ts", "payload"}` — `pmessage` carries the matched
/// pattern. `id` is the persisted message id on the delivering node
/// (monotonic; usable as a resume cursor against that node).
pub const RESP_PUSH: u16 = 0x0107;
/// Node logs report: payload = JSON object (see REQ_LOGS).
pub const RESP_LOGS: u16 = 0x0108;
/// One chunk of the cluster-join dump (see REQ_SYNC): payload is
/// length-prefixed SQL text (encode_sql). Chunks are byte slices of the
/// full script; the joiner concatenates them before replaying. RESP_SYNC
/// frames are followed by a final RESP_AFFECTED terminator.
pub const RESP_SYNC: u16 = 0x0109;
/// Console metadata report (see REQ_META): payload = JSON object built by
/// `core::meta::build_meta`.
pub const RESP_META: u16 = 0x010A;
/// Table digest report (see REQ_DIGEST): payload = JSON array of
/// `{name, rows, rows_hash, schema_hash}` objects (`core::engine::TableDigest`).
pub const RESP_DIGEST: u16 = 0x010B;
/// One chunk of the catch-up journal stream (see REQ_CATCHUP): payload is
/// a sequence of packed entries `[u64 seq][u32 sql len][sql bytes]`,
/// concatenated up to the frame-size budget. Terminated by RESP_AFFECTED
/// carrying the origin's journal head (u64 LE).
pub const RESP_CATCHUP: u16 = 0x010C;
/// Backup report (see REQ_BACKUP): payload = JSON object
/// `{dir, interval_secs, keep, count, files: [{name, bytes, ts_ms}],
/// last: {ts_ms, file, ok, error}?}`.
pub const RESP_BACKUP: u16 = 0x010D;

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub frame_type: u16,
    pub flags: u16,
    pub topology_version: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("encode error: {0}")]
    Encode(#[from] EncodeError),
    #[error("bad magic {0:#x}")]
    BadMagic(u32),
    #[error("frame truncated: need {0} bytes, have {1}")]
    Truncated(usize, usize),
}

pub type Result<T> = std::result::Result<T, ProtoError>;

impl Frame {
    pub fn new(frame_type: u16, payload: Vec<u8>) -> Frame {
        Frame {
            frame_type,
            flags: 0,
            topology_version: 0,
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.frame_type.to_le_bytes());
        out.extend_from_slice(&self.topology_version.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    /// Decode one frame; returns it and the total bytes consumed.
    pub fn decode(buf: &[u8]) -> Result<(Frame, usize)> {
        if buf.len() < HEADER_LEN {
            return Err(ProtoError::Truncated(HEADER_LEN, buf.len()));
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(ProtoError::BadMagic(magic));
        }
        let flags = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        let frame_type = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let topology_version = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let len = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
        let end = HEADER_LEN + len;
        if buf.len() < end {
            return Err(ProtoError::Truncated(end, buf.len()));
        }
        Ok((
            Frame {
                frame_type,
                flags,
                topology_version,
                payload: buf[HEADER_LEN..end].to_vec(),
            },
            end,
        ))
    }
}

/// Payload helpers: SQL text is length-prefixed UTF-8.
pub fn encode_sql(sql: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    encode::encode(&crate::Value::Str(sql.to_string()), &mut out)?;
    Ok(out)
}

pub fn decode_sql(payload: &[u8]) -> Result<String> {
    let (v, _) = encode::decode_prefix(payload)?;
    match v {
        crate::Value::Str(s) => Ok(s),
        _ => Err(ProtoError::Encode(EncodeError::UnknownTag(0))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_all_fields() {
        let f = Frame {
            frame_type: REQ_SQL,
            flags: 0b10,
            topology_version: 42,
            payload: vec![1, 2, 3, 4, 5],
        };
        let enc = f.encode().unwrap();
        let (dec, n) = Frame::decode(&enc).unwrap();
        assert_eq!(dec, f);
        assert_eq!(n, enc.len());
    }

    #[test]
    fn truncated_frames_rejected() {
        let f = Frame::new(REQ_SQL, vec![9; 100]);
        let enc = f.encode().unwrap();
        for cut in 0..enc.len() {
            assert!(Frame::decode(&enc[..cut]).is_err());
        }
    }

    #[test]
    fn bad_magic_rejected() {
        let mut enc = Frame::new(REQ_PING, vec![]).encode().unwrap();
        enc[0] ^= 0xff;
        assert!(matches!(Frame::decode(&enc), Err(ProtoError::BadMagic(_))));
    }

    #[test]
    fn sql_payload_roundtrip() {
        let p = encode_sql("SELECT * FROM t WHERE s = '中文🎉'").unwrap();
        assert_eq!(
            decode_sql(&p).unwrap(),
            "SELECT * FROM t WHERE s = '中文🎉'"
        );
    }
}
