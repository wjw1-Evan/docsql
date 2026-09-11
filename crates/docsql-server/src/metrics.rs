//! Node-level runtime metrics for external observability.
//!
//! Plain atomic counters (no metric-crate dependency): the frame loop, the
//! connection accept loop, the statement executor and the auth failure
//! paths bump them at the point of truth, and [`Metrics::snapshot_json`]
//! embeds the whole set into the REQ_STATUS payload. The web console's
//! `/metrics` endpoint formats the per-node snapshots into Prometheus text —
//! one scrape answers "is any node misbehaving" without a protocol client.
//!
//! Counters are process-lifetime (reset on restart) by design: uptime is
//! reported alongside, so a scraper can tell a restart from a rollover.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    /// Connections accepted and served (excludes over-limit refusals).
    pub connections_total: AtomicU64,
    /// Connections refused because DOCSQL_MAX_CONN was exhausted.
    pub connections_rejected_total: AtomicU64,
    /// Currently open connections (gauge; decremented on close).
    pub connections_active: AtomicI64,
    /// Statements executed through the SQL executor (client + replication).
    pub statements_total: AtomicU64,
    /// Statements that finished as a RESP_ERROR.
    pub statement_errors_total: AtomicU64,
    /// PUBLISH frames accepted (persistent pub/sub writes).
    pub publishes_total: AtomicU64,
    /// Failed authentication attempts (token + user login), pre-lockout.
    pub auth_failures_total: AtomicU64,
    /// Wire bytes read from / written to sockets.
    pub bytes_in_total: AtomicU64,
    pub bytes_out_total: AtomicU64,
}

impl Metrics {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::default())
    }

    fn u64(m: &AtomicU64) -> u64 {
        m.load(Ordering::Relaxed)
    }

    /// REQ_STATUS payload shape. Field names are wire-visible (the
    /// multinode deploy test pins the metrics object) — additive changes
    /// only.
    pub fn snapshot_json(&self) -> serde_json::Value {
        serde_json::json!({
            "connections_total": Self::u64(&self.connections_total),
            "connections_rejected_total": Self::u64(&self.connections_rejected_total),
            "connections_active": self.connections_active.load(Ordering::Relaxed).max(0),
            "statements_total": Self::u64(&self.statements_total),
            "statement_errors_total": Self::u64(&self.statement_errors_total),
            "publishes_total": Self::u64(&self.publishes_total),
            "auth_failures_total": Self::u64(&self.auth_failures_total),
            "bytes_in_total": Self::u64(&self.bytes_in_total),
            "bytes_out_total": Self::u64(&self.bytes_out_total),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_counters_and_clamps_negative_active() {
        let m = Metrics::new();
        m.connections_total.fetch_add(3, Ordering::Relaxed);
        m.connections_rejected_total.fetch_add(1, Ordering::Relaxed);
        m.connections_active.fetch_add(2, Ordering::Relaxed);
        m.connections_active.fetch_sub(5, Ordering::Relaxed);
        m.statements_total.fetch_add(7, Ordering::Relaxed);
        m.statement_errors_total.fetch_add(2, Ordering::Relaxed);
        m.publishes_total.fetch_add(4, Ordering::Relaxed);
        m.auth_failures_total.fetch_add(9, Ordering::Relaxed);
        m.bytes_in_total.fetch_add(11, Ordering::Relaxed);
        m.bytes_out_total.fetch_add(13, Ordering::Relaxed);
        let s = m.snapshot_json();
        assert_eq!(s["connections_total"], 3);
        assert_eq!(s["connections_rejected_total"], 1);
        // A transiently negative gauge (close racing accept at startup)
        // must never expose a negative connection count to scrapers.
        assert_eq!(s["connections_active"], 0);
        assert_eq!(s["statements_total"], 7);
        assert_eq!(s["statement_errors_total"], 2);
        assert_eq!(s["publishes_total"], 4);
        assert_eq!(s["auth_failures_total"], 9);
        assert_eq!(s["bytes_in_total"], 11);
        assert_eq!(s["bytes_out_total"], 13);
    }
}
