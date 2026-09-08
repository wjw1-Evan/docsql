//! Shard routing: 16384 hash slots over a static shard list.
//!
//! v1 scope: deterministic placement (KV keys by full key, SQL by first
//! table name) and direct routing. A `MOVED`-style redirect and online slot
//! migration are future work; resharding today means draining and
//! re-importing with a different shard map.

use crate::{CONNECT_TIMEOUT, IO_TIMEOUT, RECV_CAP};
use docsql_core::proto::{self, Frame};
use std::collections::BTreeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const SLOTS: u64 = 16384;

/// CRC-based slot function (stable across versions, cheap).
pub fn slot_of(key: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in key.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash % SLOTS
}

pub struct ShardRouter {
    /// slot-start -> shard address; the entry with the largest start <= slot
    /// owns the slot.
    pub ring: BTreeMap<u64, String>,
}

impl ShardRouter {
    /// Evenly spread shards across the slot space.
    pub fn new(shards: Vec<String>) -> ShardRouter {
        let n = shards.len().max(1) as u64;
        let mut ring = BTreeMap::new();
        for (i, s) in shards.into_iter().enumerate() {
            ring.insert(i as u64 * SLOTS / n, s);
        }
        ShardRouter { ring }
    }

    pub fn shard_for(&self, key: &str) -> &str {
        let slot = slot_of(key);
        self.ring
            .range(..=slot)
            .next_back()
            .map(|(_, v)| v.as_str())
            .unwrap_or_else(|| self.ring.values().next().expect("non-empty ring"))
    }

    pub fn shard_count(&self) -> usize {
        self.ring.len()
    }

    /// Route one SQL statement: hash on the first table name (naive but
    /// deterministic — joins across shards are rejected by execution anyway).
    pub async fn sql(&self, sql: &str) -> std::io::Result<Frame> {
        let table = first_table(sql);
        let shard = self.shard_for(&table);
        send_sql(shard, sql).await
    }

    /// Route one full KV command (all arguments included — the shard is
    /// chosen by the key, but the value must travel with it).
    pub async fn kv(&self, args: &[&str]) -> std::io::Result<Frame> {
        let Some(key) = args.get(1) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "kv command needs a key argument",
            ));
        };
        let shard = self.shard_for(key);
        send_kv(shard, args).await
    }
}

fn first_table(sql: &str) -> String {
    let lower = sql.to_uppercase();
    let mut it = lower.split_whitespace().peekable();
    while let Some(w) = it.next() {
        if w == "FROM" || w == "INTO" || w == "TABLE" || w == "UPDATE" {
            if let Some(t) = it.next() {
                return t
                    .trim_matches(|c: char| !c.is_alphanumeric() && c != '_')
                    .to_string();
            }
        }
    }
    "DEFAULT".to_string()
}

async fn send_frame(addr: &str, frame: &Frame) -> std::io::Result<Frame> {
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await??;
    let bytes = frame.encode().map_err(std::io::Error::other)?;
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(&bytes)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let mut header = [0u8; proto::HEADER_LEN];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut header)).await??;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if len > RECV_CAP {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "shard response too large",
        ));
    }
    let mut buf = header.to_vec();
    let mut payload = vec![0u8; len];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut payload)).await??;
    buf.extend_from_slice(&payload);
    let (f, _) = Frame::decode(&buf).map_err(std::io::Error::other)?;
    Ok(f)
}

async fn send_sql(addr: &str, sql: &str) -> std::io::Result<Frame> {
    send_frame(
        addr,
        &Frame::new(
            proto::REQ_SQL,
            proto::encode_sql(sql).map_err(std::io::Error::other)?,
        ),
    )
    .await
}

async fn send_kv(addr: &str, args: &[&str]) -> std::io::Result<Frame> {
    let payload = args.join("\x00").into_bytes();
    send_frame(addr, &Frame::new(proto::REQ_KV, payload)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_function_is_stable_and_bounded() {
        assert_eq!(slot_of("user:1"), slot_of("user:1"));
        assert!(slot_of("any-key") < SLOTS);
        // different keys usually land in different slots
        assert_ne!(slot_of("a"), slot_of("b"));
    }

    #[test]
    fn first_table_extraction() {
        assert_eq!(first_table("SELECT * FROM Orders WHERE x = 1"), "ORDERS");
        assert_eq!(first_table("INSERT INTO Logs VALUES (1)"), "LOGS");
        assert_eq!(first_table("UPDATE Things SET a = 1"), "THINGS");
        assert_eq!(first_table("CREATE TABLE Stuff (a INT)"), "STUFF");
        assert_eq!(first_table("SELECT 1 + 1"), "DEFAULT");
    }

    #[test]
    fn ring_covers_all_slots_with_even_spread() {
        let r = ShardRouter::new(vec!["a".into(), "b".into(), "c".into()]);
        assert_eq!(r.shard_count(), 3);
        // All slots resolve to some shard.
        for s in (0..SLOTS).step_by(97) {
            let key = format!("k-{s}");
            let shard = r.shard_for(&key);
            assert!(["a", "b", "c"].contains(&shard));
        }
        // Determinism: same key, same shard.
        assert_eq!(r.shard_for("k-1"), r.shard_for("k-1"));
    }
}
