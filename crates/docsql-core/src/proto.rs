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
/// KV command frame: payload = command name + arguments (M3+).
pub const REQ_KV: u16 = 0x0002;
pub const REQ_PREPARE: u16 = 0x0003;
pub const REQ_EXECUTE: u16 = 0x0004;
pub const REQ_CLOSE_STMT: u16 = 0x0005;
pub const REQ_PING: u16 = 0x0006;

// Response frame types.
pub const RESP_ROWS: u16 = 0x0101;
pub const RESP_AFFECTED: u16 = 0x0102;
pub const RESP_ERROR: u16 = 0x0103;
/// Cluster redirect: payload = "host:port" (reserved, M12+).
pub const RESP_REDIRECT: u16 = 0x0104;
pub const RESP_PONG: u16 = 0x0105;
pub const RESP_PUSH: u16 = 0x0106; // pub/sub delivery (M6+)

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
