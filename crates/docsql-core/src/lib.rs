//! docsql-core: embedded multi-model database core.
//!
//! Documents (JSON-like values) are stored whole, like a document store,
//! while a full SQL layer operates on top of them.

pub mod btree;
pub mod encode;
pub mod engine;
pub mod heap;
pub mod pager;
pub mod proto;
pub mod value;
pub mod wal;

pub use value::Value;
