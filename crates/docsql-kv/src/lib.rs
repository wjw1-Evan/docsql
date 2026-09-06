//! KV command layer.
//!
//! KV data lives in the `_kv` system table — the same SQL engine, pages and
//! WAL — so SQL can query KV keys and KV reads see SQL writes ("deep
//! interop"). Layout: key (TEXT, primary key), type ("string"|"counter"),
//! value (document), expire_at (ms since epoch, 0 = no TTL).
//!
//! Expiry is lazy (checked on every read) plus an explicit sweep() for
//! background cleanup; to SQL, expired keys simply do not exist.

use docsql_core::engine::{Database, ExecOutcome, QueryResult, SqlError};
use docsql_core::value::{Object, Value};
use std::time::{SystemTime, UNIX_EPOCH};

mod collections;
pub mod pubsub;

pub const KV_TABLE: &str = "_kv";

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("{0}")]
    Message(String),
    #[error("sql error: {0}")]
    Sql(#[from] SqlError),
}

pub type Result<T> = std::result::Result<T, KvError>;

fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(KvError::Message(msg.into()))
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub struct Kv {
    pub db: Database,
}

/// Options for SET.
#[derive(Debug, Clone, Default)]
pub struct SetOpts {
    /// Only set if the key does not exist.
    pub nx: bool,
    /// Only set if the key already exists.
    pub xx: bool,
    /// Time-to-live in milliseconds.
    pub ttl_ms: Option<i64>,
}

impl Kv {
    /// Wrap a Database, creating the _kv table if needed.
    pub fn new(mut db: Database) -> Result<Kv> {
        if let Err(e) = db.execute(&format!(
            "CREATE TABLE {KV_TABLE} (\"key\" TEXT, type TEXT, value TEXT, expire_at INT)"
        )) {
            if !e.to_string().contains("already exists") {
                return Err(e.into());
            }
        }
        Ok(Kv { db })
    }

    pub fn open(path: &std::path::Path) -> Result<Kv> {
        Kv::new(Database::open(path)?)
    }

    pub fn in_memory() -> Result<Kv> {
        Kv::new(Database::in_memory()?)
    }

    /// Read a live (non-expired) entry.
    pub(crate) fn raw_row(&mut self, key: &str, now: i64) -> Result<Option<Object>> {
        sweep_key(&mut self.db, key, now)?;
        let r = self.query(&format!(
            "SELECT \"key\", type, value, expire_at FROM {KV_TABLE} WHERE \"key\" = '{esc}'",
            esc = escape(key)
        ))?;
        Ok(r.rows.first().map(|row| {
            Object::from([
                ("key".into(), row[0].clone()),
                ("type".into(), row[1].clone()),
                ("value".into(), row[2].clone()),
                ("expire_at".into(), row[3].clone()),
            ])
        }))
    }

    fn query(&mut self, sql: &str) -> Result<QueryResult> {
        match self.db.execute(sql)? {
            ExecOutcome::Rows(r) => Ok(r),
            ExecOutcome::Affected(n) => err(format!("expected rows, got {n} affected")),
        }
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let now = now_ms();
        match self.raw_row(key, now)? {
            Some(o) => match o.get("value") {
                Some(Value::Str(s)) => Ok(Some(s.clone())),
                _ => err("wrong value type"),
            },
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: &str, val: &str, opts: SetOpts) -> Result<bool> {
        if opts.nx && opts.xx {
            return err("NX and XX are mutually exclusive");
        }
        let now = now_ms();
        let exists = self.raw_row(key, now)?.is_some();
        if (opts.nx && exists) || (opts.xx && !exists) {
            return Ok(false);
        }
        let expire_at = opts.ttl_ms.map(|t| now + t).unwrap_or(0);
        if exists {
            self.db.execute(&format!(
                "UPDATE {KV_TABLE} SET value = '{v}', expire_at = {e} WHERE \"key\" = '{k}'",
                v = escape(val),
                e = expire_at,
                k = escape(key)
            ))?;
        } else {
            self.db.execute(&format!(
                "INSERT INTO {KV_TABLE} (\"key\", type, value, expire_at) VALUES ('{k}', 'string', '{v}', {e})",
                k = escape(key),
                v = escape(val),
                e = expire_at
            ))?;
        }
        Ok(true)
    }

    pub fn del(&mut self, key: &str) -> Result<bool> {
        let now = now_ms();
        if self.raw_row(key, now)?.is_none() {
            return Ok(false);
        }
        self.db.execute(&format!(
            "DELETE FROM {KV_TABLE} WHERE \"key\" = '{esc}'",
            esc = escape(key)
        ))?;
        Ok(true)
    }

    /// Remaining TTL in ms; None if no TTL, Err-style None if key missing
    /// (returns Ok(None) for both missing and no-TTL callers use ttl_or).
    pub fn ttl_ms(&mut self, key: &str) -> Result<Option<i64>> {
        let now = now_ms();
        match self.raw_row(key, now)? {
            Some(o) => match o.get("expire_at") {
                Some(Value::Int(0)) | None => Ok(None),
                Some(Value::Int(at)) => Ok(Some(*at - now)),
                _ => err("bad expire_at"),
            },
            None => err("no such key"),
        }
    }

    pub fn expire(&mut self, key: &str, ttl_ms: i64) -> Result<bool> {
        let now = now_ms();
        if self.raw_row(key, now)?.is_none() {
            return Ok(false);
        }
        self.db.execute(&format!(
            "UPDATE {KV_TABLE} SET expire_at = {at} WHERE \"key\" = '{k}'",
            at = now + ttl_ms,
            k = escape(key)
        ))?;
        Ok(true)
    }

    pub fn persist(&mut self, key: &str) -> Result<bool> {
        let now = now_ms();
        if self.raw_row(key, now)?.is_none() {
            return Ok(false);
        }
        self.db.execute(&format!(
            "UPDATE {KV_TABLE} SET expire_at = 0 WHERE \"key\" = '{k}'",
            k = escape(key)
        ))?;
        Ok(true)
    }

    pub fn incr_by(&mut self, key: &str, delta: i64) -> Result<i64> {
        let now = now_ms();
        let cur = match self.raw_row(key, now)? {
            Some(o) => match o.get("value") {
                Some(Value::Str(s)) => s
                    .parse::<i64>()
                    .map_err(|_| KvError::Message("value is not an integer".into()))?,
                _ => return err("wrong value type"),
            },
            None => 0,
        };
        let next = cur + delta;
        if cur == 0 && self.raw_row(key, now)?.is_none() {
            self.db.execute(&format!(
                "INSERT INTO {KV_TABLE} (\"key\", type, value, expire_at) VALUES ('{k}', 'string', '{v}', 0)",
                k = escape(key),
                v = next
            ))?;
        } else {
            self.db.execute(&format!(
                "UPDATE {KV_TABLE} SET value = '{v}' WHERE \"key\" = '{k}'",
                v = next,
                k = escape(key)
            ))?;
        }
        Ok(next)
    }

    /// Remove every expired row (background/periodic cleanup).
    pub fn sweep(&mut self) -> Result<u64> {
        let now = now_ms();
        sweep_all(&mut self.db, now)
    }
}

/// Inline-escape a string for SQL literal embedding. Single quotes double up.
fn escape(s: &str) -> String {
    s.replace('\'', "''")
}

pub(crate) fn sweep_key(db: &mut Database, key: &str, now: i64) -> Result<()> {
    let sql = format!(
        "SELECT expire_at FROM {KV_TABLE} WHERE \"key\" = '{k}'",
        k = escape(key)
    );
    if let ExecOutcome::Rows(r) = db.execute(&sql)? {
        if let Some(row) = r.rows.first() {
            if let Value::Int(at) = row[0] {
                if at > 0 && at <= now {
                    db.execute(&format!(
                        "DELETE FROM {KV_TABLE} WHERE \"key\" = '{k}'",
                        k = escape(key)
                    ))?;
                }
            }
        }
    }
    Ok(())
}

fn sweep_all(db: &mut Database, now: i64) -> Result<u64> {
    // SELECT then DELETE (UPDATE/DELETE on the same connection is M4).
    let sql = format!("SELECT \"key\" FROM {KV_TABLE} WHERE expire_at > 0 AND expire_at <= {now}");
    let keys: Vec<String> = match db.execute(&sql)? {
        ExecOutcome::Rows(r) => r
            .rows
            .iter()
            .filter_map(|row| row[0].as_str().map(String::from))
            .collect(),
        _ => vec![],
    };
    let mut n = 0;
    for k in keys {
        let _ = db.execute(&format!(
            "DELETE FROM {KV_TABLE} WHERE \"key\" = '{esc}'",
            esc = escape(&k)
        ))?;
        n += 1;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_del_roundtrip() {
        let mut kv = Kv::in_memory().unwrap();
        assert_eq!(kv.get("hello").unwrap(), None);
        assert!(kv.set("hello", "world", SetOpts::default()).unwrap());
        assert_eq!(kv.get("hello").unwrap(), Some("world".into()));
        assert!(kv.del("hello").unwrap());
        assert_eq!(kv.get("hello").unwrap(), None);
        assert!(!kv.del("hello").unwrap());
    }

    #[test]
    fn nx_xx_semantics() {
        let mut kv = Kv::in_memory().unwrap();
        assert!(!kv
            .set(
                "k",
                "a",
                SetOpts {
                    xx: true,
                    ..Default::default()
                }
            )
            .unwrap());
        assert!(kv
            .set(
                "k",
                "a",
                SetOpts {
                    nx: true,
                    ..Default::default()
                }
            )
            .unwrap());
        assert!(!kv
            .set(
                "k",
                "b",
                SetOpts {
                    nx: true,
                    ..Default::default()
                }
            )
            .unwrap());
        assert_eq!(kv.get("k").unwrap(), Some("a".into()));
        assert!(kv
            .set(
                "k",
                "b",
                SetOpts {
                    xx: true,
                    ..Default::default()
                }
            )
            .unwrap());
        assert_eq!(kv.get("k").unwrap(), Some("b".into()));
        assert!(kv
            .set(
                "both",
                "x",
                SetOpts {
                    nx: true,
                    xx: true,
                    ..Default::default()
                }
            )
            .is_err());
    }

    #[test]
    fn incr_and_counters() {
        let mut kv = Kv::in_memory().unwrap();
        assert_eq!(kv.incr_by("hits", 1).unwrap(), 1);
        assert_eq!(kv.incr_by("hits", 5).unwrap(), 6);
        assert_eq!(kv.incr_by("hits", -2).unwrap(), 4);
        assert_eq!(kv.get("hits").unwrap(), Some("4".into()));
        kv.set("text", "abc", SetOpts::default()).unwrap();
        assert!(kv.incr_by("text", 1).is_err());
    }

    #[test]
    fn keys_with_quotes_are_safe() {
        let mut kv = Kv::in_memory().unwrap();
        assert!(kv.set("o'brien", "it's fine", SetOpts::default()).unwrap());
        assert_eq!(kv.get("o'brien").unwrap(), Some("it's fine".into()));
        assert!(kv.del("o'brien").unwrap());
    }

    #[test]
    fn sql_interop_both_directions() {
        let mut kv = Kv::in_memory().unwrap();
        // KV write visible to SQL.
        kv.set("session:1", "alice", SetOpts::default()).unwrap();
        let r = match kv
            .db
            .execute(&format!(
                "SELECT value FROM {KV_TABLE} WHERE key = 'session:1'"
            ))
            .unwrap()
        {
            ExecOutcome::Rows(r) => r,
            o => panic!("{o:?}"),
        };
        assert_eq!(r.rows[0][0], Value::Str("alice".into()));
        // SQL write visible to KV.
        kv.db.execute(&format!(
            "INSERT INTO {KV_TABLE} (\"key\", type, value, expire_at) VALUES ('from-sql', 'string', 'sql-side', 0)"
        ))
        .unwrap();
        assert_eq!(kv.get("from-sql").unwrap(), Some("sql-side".into()));
    }

    #[test]
    fn lazy_expiry_hides_key_from_kv_and_sql() {
        let mut kv = Kv::in_memory().unwrap();
        kv.set(
            "temp",
            "x",
            SetOpts {
                ttl_ms: Some(50),
                ..Default::default()
            },
        )
        .unwrap();
        // Not yet expired.
        assert_eq!(kv.get("temp").unwrap(), Some("x".into()));
        std::thread::sleep(std::time::Duration::from_millis(80));
        // Lazy expiry on read.
        assert_eq!(kv.get("temp").unwrap(), None);
        // And it is really gone from SQL too.
        let r = match kv
            .db
            .execute(&format!("SELECT key FROM {KV_TABLE} WHERE key = 'temp'"))
            .unwrap()
        {
            ExecOutcome::Rows(r) => r,
            o => panic!("{o:?}"),
        };
        assert!(r.rows.is_empty());
    }

    #[test]
    fn sweep_removes_expired_batch() {
        let mut kv = Kv::in_memory().unwrap();
        kv.set(
            "a",
            "1",
            SetOpts {
                ttl_ms: Some(30),
                ..Default::default()
            },
        )
        .unwrap();
        kv.set(
            "b",
            "2",
            SetOpts {
                ttl_ms: Some(30),
                ..Default::default()
            },
        )
        .unwrap();
        kv.set("c", "3", SetOpts::default()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert_eq!(kv.sweep().unwrap(), 2);
        assert_eq!(kv.get("c").unwrap(), Some("3".into()));
        assert_eq!(kv.sweep().unwrap(), 0);
    }

    #[test]
    fn expire_ttl_persist() {
        let mut kv = Kv::in_memory().unwrap();
        kv.set("k", "v", SetOpts::default()).unwrap();
        assert_eq!(kv.ttl_ms("k").unwrap(), None); // no TTL
        assert!(kv.expire("k", 60_000).unwrap());
        let ttl = kv.ttl_ms("k").unwrap().unwrap();
        assert!((59_000..=60_000).contains(&ttl), "ttl={ttl}");
        assert!(kv.persist("k").unwrap());
        assert_eq!(kv.ttl_ms("k").unwrap(), None);
        assert!(!kv.expire("missing", 1000).unwrap());
        assert!(kv.ttl_ms("missing").is_err());
    }

    #[test]
    fn set_replaces_ttl_when_not_keepttl() {
        let mut kv = Kv::in_memory().unwrap();
        kv.set(
            "k",
            "v",
            SetOpts {
                ttl_ms: Some(60_000),
                ..Default::default()
            },
        )
        .unwrap();
        // Plain SET clears the TTL (no KEEPTTL option given).
        kv.set("k", "v2", SetOpts::default()).unwrap();
        assert_eq!(kv.ttl_ms("k").unwrap(), None);
    }

    #[test]
    fn kv_store_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kv.db");
        {
            let mut kv = Kv::open(&path).unwrap();
            kv.set("durable", "yes", SetOpts::default()).unwrap();
        }
        let mut kv = Kv::open(&path).unwrap();
        assert_eq!(kv.get("durable").unwrap(), Some("yes".into()));
    }
}
