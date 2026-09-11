//! docsql-core: embedded multi-model database core.
//!
//! Documents (JSON-like values) are stored whole, like a document store,
//! while a full SQL layer operates on top of them.

pub mod btree;
pub mod encode;
pub mod engine;
pub mod guid;
pub mod heap;
pub mod json;
pub mod kdf;
pub mod meta;
pub mod pager;
pub mod proto;
pub mod stmt;
pub mod useradmin;
pub mod value;
pub mod wal;

pub use value::Value;

/// Milliseconds since the Unix epoch; 0 when the clock is before it.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
