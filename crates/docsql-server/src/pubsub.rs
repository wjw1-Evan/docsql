//! Persistent pub/sub: live-subscriber registry and Redis-style globbing.
//!
//! Messages themselves live in the engine (`_pubsub_messages`, created at
//! server startup) so they get WAL durability for free; this module owns
//! only the live side — who listens on which channel or pattern, and how
//! push frames reach them.
//!
//! Ordering contract (no gap, no duplicate between replay and live):
//! every PUBLISH persists before it notifies, and SUBSCRIBE holds the
//! registry lock across register → snapshot watermark → replay → arm the
//! dedup filter. A publish that committed before the snapshot is covered
//! by the replay (its live push is dropped: `id <= skip_through`); one
//! that commits later passes the filter. Publishes serialize on the
//! server's `write_order`, so per-connection push frames arrive in id
//! order.
//!
//! Delivery is best-effort per message: each connection has a bounded
//! writer channel (256 frames); a connection that stops draining loses
//! live pushes but nothing else — the message stays persisted and a
//! re-subscribe from the last seen id replays it (at-least-once).

use crate::Frame;
use docsql_core::engine::{Database, ExecOutcome};
use docsql_core::value::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;

/// Backing engine table (system: hidden from SQL clients; read through
/// the `docsql_pubsub` view instead). Defined in docsql-core so the web
/// console hides it from user-facing catalogs too.
pub use docsql_core::engine::PUBSUB_TABLE;

/// Channel names and payloads are bounded so one frame cannot balloon the
/// store or the 64 MB frame cap.
pub const MAX_CHANNEL_LEN: usize = 256;
pub const MAX_PAYLOAD_LEN: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Persistence: the message store lives in the engine table so WAL fsync,
// checkpoints and restart durability come for free.
// ---------------------------------------------------------------------------

/// Create the backing table on startup (idempotent).
pub fn ensure_table(db: &mut Database) -> Result<(), String> {
    db.execute(&format!(
        "CREATE TABLE IF NOT EXISTS {PUBSUB_TABLE} \
         (id INT PRIMARY KEY AUTOINCREMENT, channel TEXT, ts_ms INT, payload TEXT)"
    ))
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// SQL string literal (single quotes doubled).
fn sql_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Append one message in the caller's engine write path; returns the
/// assigned id (AUTOINCREMENT max+1, monotonic while any row exists).
pub fn store_insert(
    db: &mut Database,
    channel: &str,
    ts: i64,
    payload: &str,
) -> Result<i64, String> {
    db.execute(&format!(
        "INSERT INTO {PUBSUB_TABLE} (channel, ts_ms, payload) VALUES ({}, {}, {})",
        sql_literal(channel),
        ts,
        sql_literal(payload)
    ))
    .map_err(|e| e.to_string())?;
    Ok(query_max_id(db))
}

/// Highest message id persisted on this node (0 on empty).
pub fn query_max_id(db: &mut Database) -> i64 {
    let sql = format!("SELECT MAX(id) AS max_id FROM {PUBSUB_TABLE}");
    match db.execute(&sql) {
        Ok(ExecOutcome::Rows(r)) => match r.rows.first().and_then(|row| row.first()) {
            Some(Value::Int(i)) => *i,
            _ => 0,
        },
        _ => 0,
    }
}

pub struct StoredMessage {
    pub id: i64,
    pub channel: String,
    pub ts: i64,
    pub payload: String,
}

/// Messages with `after_id < id <= upto_id`, in id order (all channels;
/// callers filter by channel or pattern).
pub fn query_history(
    db: &mut Database,
    after_id: i64,
    upto_id: i64,
) -> Result<Vec<StoredMessage>, String> {
    if after_id >= upto_id {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT id, channel, ts_ms, payload FROM {PUBSUB_TABLE} \
         WHERE id > {after_id} AND id <= {upto_id} ORDER BY id"
    );
    match db.execute(&sql) {
        Ok(ExecOutcome::Rows(r)) => Ok(r
            .rows
            .into_iter()
            .filter_map(|row| {
                let id = row.first()?.as_i64()?;
                let channel = row.get(1)?.as_str()?.to_string();
                let ts = row.get(2)?.as_i64()?;
                let payload = row.get(3)?.as_str()?.to_string();
                Some(StoredMessage {
                    id,
                    channel,
                    ts,
                    payload,
                })
            })
            .collect()),
        Ok(_) => Ok(Vec::new()),
        Err(e) => Err(e.to_string()),
    }
}

/// Drop everything but the newest `keep` messages of one channel. `keep`
/// must be >= 1: emptying the table entirely would reset the
/// AUTOINCREMENT watermark and reuse ids (cursor safety).
pub fn store_trim(db: &mut Database, channel: &str, keep: i64) -> Result<u64, String> {
    let sql = format!(
        "SELECT id FROM {PUBSUB_TABLE} WHERE channel = {} ORDER BY id DESC LIMIT {keep}",
        sql_literal(channel)
    );
    let keep_ids: Vec<i64> = match db.execute(&sql) {
        Ok(ExecOutcome::Rows(r)) => r
            .rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_i64()))
            .collect(),
        _ => return Ok(0),
    };
    let Some(&threshold) = keep_ids.last() else {
        return Ok(0);
    };
    let sql = format!(
        "DELETE FROM {PUBSUB_TABLE} WHERE channel = {} AND id < {threshold}",
        sql_literal(channel)
    );
    match db.execute(&sql) {
        Ok(ExecOutcome::Affected(n)) => Ok(n),
        Ok(_) => Ok(0),
        Err(e) => Err(e.to_string()),
    }
}

/// `SELECT ... FROM docsql_pubsub` view: rewrite the identifier onto the
/// backing table and ride the normal SQL path (WHERE / ORDER BY / LIMIT
/// for free). ASCII-case-insensitive, byte-length-preserving swap so the
/// original statement's indices stay valid.
pub fn try_rewrite_pubsub_view(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let trimmed = lower.trim_start();
    if !trimmed.starts_with("select") || !lower.contains("docsql_pubsub") {
        return None;
    }
    let needle = b"docsql_pubsub";
    let lb = lower.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < sql.len() {
        // String literals are data, not table references: a value like
        // 'docsql_pubsub' must survive the rewrite untouched ('' escapes
        // included).
        if lb[i] == b'\'' {
            let start = i;
            i += 1;
            while i < sql.len() {
                if lb[i] == b'\'' {
                    if lb.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(sql.get(start..i).unwrap_or(""));
            continue;
        }
        if i + needle.len() <= lb.len() && &lb[i..i + needle.len()] == needle {
            out.push_str(PUBSUB_TABLE);
            i += needle.len();
        } else {
            let step = sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            out.push_str(sql.get(i..i + step).unwrap_or(""));
            i += step;
        }
    }
    Some(out)
}

/// Parse the subscribe `from` argument: "latest" (live only), "earliest"
/// (full replay) or a numeric id to resume after.
pub fn parse_after_id(v: &serde_json::Value) -> Result<Option<i64>, String> {
    match &v["from"] {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) if s == "latest" => Ok(None),
        serde_json::Value::String(s) if s == "earliest" => Ok(Some(0)),
        serde_json::Value::String(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|_| format!("from: expected latest|earliest|<id>, got {s:?}")),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| "from: id must be an integer".to_string()),
        other => Err(format!("from: expected a string, got {other}")),
    }
}

pub type ConnId = u64;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SubKind {
    Channel,
    Pattern,
}

struct Sub {
    tx: mpsc::Sender<Frame>,
    /// Live pushes with `id <= skip_through` are duplicates of the replay
    /// and get dropped. `i64::MAX` until the replay finishes.
    skip_through: i64,
}

#[derive(Default)]
pub struct Inner {
    channels: HashMap<String, HashMap<ConnId, Sub>>,
    patterns: HashMap<String, HashMap<ConnId, Sub>>,
}

pub struct PubSub {
    next_conn: AtomicU64,
    inner: tokio::sync::Mutex<Inner>,
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    pub fn new() -> PubSub {
        PubSub {
            next_conn: AtomicU64::new(1),
            inner: tokio::sync::Mutex::new(Inner::default()),
        }
    }

    pub fn next_conn_id(&self) -> ConnId {
        self.next_conn
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Registry lock. The subscribe flow must hold it across register,
    /// watermark snapshot, replay and filter arming (see module docs).
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, Inner> {
        self.inner.lock().await
    }

    /// Deliver one persisted message to every matching live subscriber.
    /// Exact subscribers get a `message` frame, each matching pattern
    /// subscriber a `pmessage` frame (Redis shape); returns the number of
    /// distinct connections that actually received a frame.
    pub async fn notify(&self, channel: &str, id: i64, ts: i64, payload: &str) -> u64 {
        let mut inner = self.inner.lock().await;
        inner.notify(channel, id, ts, payload)
    }

    pub async fn remove_conn(&self, conn: ConnId) {
        self.inner.lock().await.remove_conn(conn);
    }
}

impl Inner {
    /// Register (or replace) one subscription; returns how many
    /// subscriptions the connection holds afterwards.
    pub fn register(
        &mut self,
        conn: ConnId,
        kind: SubKind,
        name: &str,
        tx: mpsc::Sender<Frame>,
    ) -> usize {
        let sub = Sub {
            tx,
            skip_through: i64::MAX,
        };
        let map = match kind {
            SubKind::Channel => &mut self.channels,
            SubKind::Pattern => &mut self.patterns,
        };
        map.entry(name.to_string()).or_default().insert(conn, sub);
        self.conn_count(conn)
    }

    pub fn conn_count(&self, conn: ConnId) -> usize {
        self.channels
            .values()
            .chain(self.patterns.values())
            .filter(|m| m.contains_key(&conn))
            .count()
    }

    /// Arm the replay dedup filter: live pushes with `id <= watermark`
    /// were already delivered by the replay.
    pub fn arm_filter(&mut self, conn: ConnId, kind: SubKind, name: &str, watermark: i64) {
        let map = match kind {
            SubKind::Channel => &mut self.channels,
            SubKind::Pattern => &mut self.patterns,
        };
        if let Some(sub) = map.get_mut(name).and_then(|m| m.get_mut(&conn)) {
            sub.skip_through = watermark;
        }
    }

    /// Remove subscriptions by name (empty slice = all of that kind);
    /// returns the connection's remaining subscription count.
    pub fn unregister(&mut self, conn: ConnId, kind: SubKind, names: &[String]) -> usize {
        let map = match kind {
            SubKind::Channel => &mut self.channels,
            SubKind::Pattern => &mut self.patterns,
        };
        if names.is_empty() {
            for subs in map.values_mut() {
                subs.remove(&conn);
            }
        } else {
            for name in names {
                if let Some(subs) = map.get_mut(name) {
                    subs.remove(&conn);
                }
            }
        }
        map.retain(|_, subs| !subs.is_empty());
        self.conn_count(conn)
    }

    pub fn remove_conn(&mut self, conn: ConnId) {
        for map in [&mut self.channels, &mut self.patterns] {
            for subs in map.values_mut() {
                subs.remove(&conn);
            }
            map.retain(|_, subs| !subs.is_empty());
        }
    }

    fn notify(&mut self, channel: &str, id: i64, ts: i64, payload: &str) -> u64 {
        let mut reached: HashSet<ConnId> = HashSet::new();
        let mut dead_exact: Vec<ConnId> = Vec::new();
        let mut dead_pattern: Vec<(String, ConnId)> = Vec::new();
        // Exact-channel subscribers.
        if let Some(subs) = self.channels.get(channel) {
            for (conn, sub) in subs {
                if id <= sub.skip_through {
                    continue; // covered by that subscriber's replay
                }
                let frame = push_frame(None, channel, id, ts, payload);
                match sub.tx.try_send(frame) {
                    Ok(()) => {
                        reached.insert(*conn);
                    }
                    // Stalled writer (buffer full): drop this copy, keep the
                    // subscription — the message stays persisted.
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => dead_exact.push(*conn),
                }
            }
        }
        // Pattern subscribers: one pmessage per matching pattern.
        for (pattern, subs) in &self.patterns {
            if !pattern_matches(pattern, channel) {
                continue;
            }
            for (conn, sub) in subs {
                if id <= sub.skip_through {
                    continue;
                }
                let frame = push_frame(Some(pattern), channel, id, ts, payload);
                match sub.tx.try_send(frame) {
                    Ok(()) => {
                        reached.insert(*conn);
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        dead_pattern.push((pattern.clone(), *conn))
                    }
                }
            }
        }
        // Prune closed connections (their socket is gone).
        if !dead_exact.is_empty() {
            if let Some(subs) = self.channels.get_mut(channel) {
                for conn in dead_exact {
                    subs.remove(&conn);
                }
            }
            self.channels.retain(|_, subs| !subs.is_empty());
        }
        for (name, conn) in dead_pattern {
            if let Some(subs) = self.patterns.get_mut(&name) {
                subs.remove(&conn);
            }
        }
        self.patterns.retain(|_, subs| !subs.is_empty());
        reached.len() as u64
    }

    /// Channels with at least one exact subscriber, glob-filtered and
    /// sorted (Redis PUBSUB CHANNELS).
    pub fn channels(&self, filter: Option<&str>) -> Vec<String> {
        let mut names: Vec<String> = self
            .channels
            .keys()
            .filter(|c| filter.is_none_or(|p| pattern_matches(p, c)))
            .cloned()
            .collect();
        names.sort();
        names
    }

    /// Exact-subscriber counts; every known channel when `names` is empty
    /// (Redis PUBSUB NUMSUB).
    pub fn numsub(&self, names: &[String]) -> Vec<(String, usize)> {
        let names: Vec<&String> = if names.is_empty() {
            self.channels.keys().collect()
        } else {
            names.iter().collect()
        };
        let mut out: Vec<(String, usize)> = names
            .into_iter()
            .map(|n| {
                let c = self.channels.get(n).map(|m| m.len()).unwrap_or(0);
                (n.clone(), c)
            })
            .collect();
        out.sort();
        out
    }

    pub fn numpat(&self) -> usize {
        self.patterns.values().map(|m| m.len()).sum()
    }
}

/// Build one RESP_PUSH frame (`message` or `pmessage`).
pub fn push_frame(pattern: Option<&str>, channel: &str, id: i64, ts: i64, payload: &str) -> Frame {
    let v = match pattern {
        None => serde_json::json!({
            "kind": "message",
            "channel": channel,
            "id": id,
            "ts": ts,
            "payload": payload,
        }),
        Some(p) => serde_json::json!({
            "kind": "pmessage",
            "pattern": p,
            "channel": channel,
            "id": id,
            "ts": ts,
            "payload": payload,
        }),
    };
    Frame::new(
        docsql_core::proto::RESP_PUSH,
        serde_json::to_vec(&v).unwrap_or_default(),
    )
}

/// Redis-style glob (`stringmatchlen`): `*`, `?`, `[...]` classes with
/// ranges (`a-z`) and `^` negation, `\` escapes. Iterative star
/// backtracking — no recursion, linear-ish on sane inputs.
pub fn pattern_matches(pattern: &str, s: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = s.chars().collect();
    let (mut pi, mut si) = (0usize, 0usize);
    let (mut star, mut mark) = (usize::MAX, 0usize);
    while si < t.len() {
        let matched = pi < p.len() && {
            match p[pi] {
                '*' => {
                    star = pi;
                    mark = si;
                    pi += 1;
                    true
                }
                '?' => {
                    pi += 1;
                    si += 1;
                    true
                }
                '[' => match match_class(&p, pi, t[si]) {
                    Some((hit, next_pi)) => {
                        if hit {
                            pi = next_pi;
                            si += 1;
                        }
                        hit
                    }
                    // Unterminated class: treat as a mismatching literal.
                    None => false,
                },
                '\\' if pi + 1 < p.len() => {
                    if p[pi + 1] == t[si] {
                        pi += 2;
                        si += 1;
                        true
                    } else {
                        false
                    }
                }
                c => {
                    if c == t[si] {
                        pi += 1;
                        si += 1;
                        true
                    } else {
                        false
                    }
                }
            }
        };
        if !matched {
            if star == usize::MAX {
                return false;
            }
            // Last star absorbs one more character and we retry after it.
            mark += 1;
            si = mark;
            pi = star + 1;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Match `ch` against the `[...]` class opening at `p[open]`.
/// Returns (hit, index past the closing bracket), or None when the class
/// never closes. A `]` in first position is a literal member.
fn match_class(p: &[char], open: usize, ch: char) -> Option<(bool, usize)> {
    let mut i = open + 1;
    let negate = p.get(i) == Some(&'^');
    if negate {
        i += 1;
    }
    let mut hit = false;
    let mut first = true;
    while i < p.len() {
        if p[i] == ']' && !first {
            return Some((hit != negate, i + 1));
        }
        first = false;
        if i + 2 < p.len() && p[i + 1] == '-' && p[i + 2] != ']' {
            if p[i] <= ch && ch <= p[i + 2] {
                hit = true;
            }
            i += 3;
        } else {
            if p[i] == ch {
                hit = true;
            }
            i += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_semantics() {
        assert!(pattern_matches("news.*", "news.tech"));
        assert!(pattern_matches("news.?", "news.a"));
        assert!(!pattern_matches("news.?", "news.ab"));
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("", ""));
        assert!(!pattern_matches("", "x"));
        assert!(pattern_matches("a\\*b", "a*b"));
        assert!(!pattern_matches("a\\*b", "aab"));
        // Character classes with ranges and negation.
        assert!(pattern_matches("n[0-9]", "n7"));
        assert!(!pattern_matches("n[0-9]", "nx"));
        assert!(pattern_matches("n[^0-9]", "nx"));
        assert!(!pattern_matches("n[^0-9]", "n7"));
        assert!(pattern_matches("[]x]", "]"));
        // Star backtracking: multiple stars, prefix mismatch recovery.
        assert!(pattern_matches("*c*", "abcbca"));
        assert!(!pattern_matches("*b", "aaa"));
        assert!(pattern_matches("a*b*c", "aXbYc"));
        assert!(!pattern_matches("a*b*c", "aXbY"));
        // Unterminated class: no match (never panics).
        assert!(!pattern_matches("n[0-", "n0"));
    }

    fn tx() -> (mpsc::Sender<Frame>, mpsc::Receiver<Frame>) {
        mpsc::channel(256)
    }

    #[test]
    fn store_roundtrip_and_trim() {
        let mut db = Database::in_memory().unwrap();
        ensure_table(&mut db).unwrap();
        ensure_table(&mut db).unwrap(); // idempotent
        let id1 = store_insert(&mut db, "news", 1, "a").unwrap();
        let id2 = store_insert(&mut db, "news", 2, "b").unwrap();
        let id3 = store_insert(&mut db, "other", 3, "c").unwrap();
        assert!(id1 >= 1 && id2 > id1 && id3 > id2, "monotonic ids");
        assert_eq!(query_max_id(&mut db), id3);
        let hist = query_history(&mut db, 0, id3).unwrap();
        assert_eq!(hist.len(), 3);
        assert_eq!(hist[0].channel, "news");
        assert_eq!(hist[2].payload, "c");
        // Empty range returns nothing without touching the engine.
        assert!(query_history(&mut db, id3, id3).unwrap().is_empty());
        // Trim keeps only the newest message of "news".
        assert_eq!(store_trim(&mut db, "news", 1).unwrap(), 1);
        let hist = {
            let upto = query_max_id(&mut db);
            query_history(&mut db, 0, upto).unwrap()
        };
        assert_eq!(hist.len(), 2, "other channel untouched");
        assert_eq!(hist[0].payload, "b");
    }

    #[test]
    fn view_rewrite_targets_selects_only() {
        assert_eq!(
            try_rewrite_pubsub_view("SELECT * FROM docsql_pubsub WHERE id > 3"),
            Some("SELECT * FROM _pubsub_messages WHERE id > 3".to_string())
        );
        // Case-insensitive, non-ASCII preserved.
        assert_eq!(
            try_rewrite_pubsub_view("select payload from DocSQL_PubSub where channel = '信号'"),
            Some("select payload from _pubsub_messages where channel = '信号'".to_string())
        );
        // Not a view query: untouched / None.
        assert!(try_rewrite_pubsub_view("INSERT INTO docsql_pubsub VALUES (1)").is_none());
        assert!(try_rewrite_pubsub_view("SELECT * FROM t").is_none());
    }

    #[test]
    fn parse_after_id_variants() {
        let j = |s: &str| serde_json::from_str::<serde_json::Value>(s).unwrap();
        assert_eq!(parse_after_id(&j("{}")).unwrap(), None);
        assert_eq!(parse_after_id(&j(r#"{"from":"latest"}"#)).unwrap(), None);
        assert_eq!(
            parse_after_id(&j(r#"{"from":"earliest"}"#)).unwrap(),
            Some(0)
        );
        assert_eq!(parse_after_id(&j(r#"{"from":"42"}"#)).unwrap(), Some(42));
        assert_eq!(parse_after_id(&j(r#"{"from":7}"#)).unwrap(), Some(7));
        assert!(parse_after_id(&j(r#"{"from":"soon"}"#)).is_err());
    }

    #[tokio::test]
    async fn registry_fanout_counts_distinct_connections() {
        let ps = PubSub::new();
        let c1 = ps.next_conn_id();
        let c2 = ps.next_conn_id();
        let (t1, mut r1) = tx();
        let (t2, mut r2) = tx();
        let (_t3, mut r3) = tx();
        let mut inner = ps.lock().await;
        assert_eq!(inner.register(c1, SubKind::Channel, "news", t1), 1);
        // Same connection twice on the same channel replaces, not stacks.
        let (t1b, mut r1b) = tx();
        assert_eq!(inner.register(c1, SubKind::Channel, "news", t1b), 1);
        assert_eq!(inner.register(c2, SubKind::Channel, "news", t2), 1);
        // c1 also matches via a pattern — still counted once.
        let (tp, _rp) = tx();
        assert_eq!(inner.register(c1, SubKind::Pattern, "n*", tp), 2);
        // Arm the replay filters (live-only: watermark 0 passes every id).
        for (conn, kind, name) in [
            (c1, SubKind::Channel, "news"),
            (c2, SubKind::Channel, "news"),
            (c1, SubKind::Pattern, "n*"),
        ] {
            inner.arm_filter(conn, kind, name, 0);
        }
        let n = inner.notify("news", 5, 0, "hello");
        assert_eq!(n, 2, "distinct connections, not subscriptions");
        // The replaced subscription no longer receives.
        assert!(r1.try_recv().is_err(), "replaced sub still delivered");
        for r in [&mut r1b, &mut r2] {
            let f = r.try_recv().unwrap();
            assert_eq!(f.frame_type, docsql_core::proto::RESP_PUSH);
        }
        assert!(r3.try_recv().is_err(), "non-subscriber got nothing");
        // Snapshot bookkeeping.
        assert_eq!(inner.numsub(&[]), vec![("news".to_string(), 2)]);
        assert_eq!(inner.numpat(), 1);
        assert_eq!(inner.channels(None), vec!["news".to_string()]);
        drop(inner);

        // Per-subscriber unsubscribe keeps the other subscriber alive.
        let n = ps
            .lock()
            .await
            .unregister(c1, SubKind::Channel, &["news".to_string()]);
        assert_eq!(n, 1, "c1 keeps its pattern subscription");
        // c2 via the exact channel plus c1 via its still-subscribed pattern.
        assert_eq!(ps.notify("news", 6, 0, "again").await, 2);
    }

    #[tokio::test]
    async fn skip_through_filters_replayed_ids() {
        let ps = PubSub::new();
        let c1 = ps.next_conn_id();
        let (t1, mut r1) = tx();
        let mut inner = ps.lock().await;
        inner.register(c1, SubKind::Channel, "news", t1);
        // Replay arming: everything up to the watermark is a duplicate.
        inner.arm_filter(c1, SubKind::Channel, "news", 10);
        drop(inner);
        assert_eq!(ps.notify("news", 10, 0, "old").await, 0);
        assert_eq!(ps.notify("news", 11, 0, "new").await, 1);
        let f = r1.try_recv().unwrap();
        let v: serde_json::Value = serde_json::from_slice(&f.payload).unwrap();
        assert_eq!(v["id"], 11);
        assert_eq!(v["kind"], "message");
    }

    #[tokio::test]
    async fn dead_subscribers_pruned_on_disconnect() {
        let ps = PubSub::new();
        let c1 = ps.next_conn_id();
        let c2 = ps.next_conn_id();
        let (t1, r1) = tx();
        let (t2, _r2) = tx();
        let mut inner = ps.lock().await;
        inner.register(c1, SubKind::Channel, "news", t1);
        inner.register(c2, SubKind::Channel, "news", t2);
        inner.arm_filter(c1, SubKind::Channel, "news", 0);
        inner.arm_filter(c2, SubKind::Channel, "news", 0);
        drop(inner);
        // Closed channel (receiver dropped): delivery silently skipped.
        drop(r1);
        assert_eq!(ps.notify("news", 1, 0, "x").await, 1);
        // Explicit disconnect cleanup empties the registry entry.
        ps.remove_conn(c2).await;
        assert_eq!(ps.notify("news", 2, 0, "x").await, 0);
        assert!(ps.lock().await.channels(None).is_empty());
    }
}
