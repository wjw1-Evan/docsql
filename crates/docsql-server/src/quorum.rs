//! Majority-visibility write fence (design 004 §2, phase 1): a node that
//! can no longer see a majority of its voting members refuses CLIENT
//! writes — reads, subscriptions and replication apply continue — so a
//! partitioned minority stops producing the divergent writes its restart
//! repair would otherwise overwrite. The authoritative semantics live in
//! `docs/design/004-automatic-failover.md`; this module owns the
//! visibility bookkeeping, the background probe loop drives it.
//!
//! Deliberate posture (all from the design, none ad hoc here):
//! - **Local monotonic reasoning only**: visibility is "probe succeeded
//!   within the last k cycles", never a comparison of node clocks;
//! - **Debounce on the way in, lift on the way out**: a member needs k
//!   consecutive misses to count as lost (a member restarting within
//!   k×probe-ms never trips the fence), while healing lifts the fence on
//!   the FIRST majority cycle — conservatism belongs to fencing, not to
//!   staying fenced;
//! - **Default off**: without `DOCSQL_QUORUM=1` no loop runs and the
//!   write gate is inert — the historical any-node-writes semantics are
//!   exactly preserved;
//! - **Arbiters are members**: a `DOCSQL_ARBITER=1` process answers
//!   REQ_STATUS through the ordinary connection loop (everything except
//!   AUTH/PING/STATUS refuses), so probing an arbiter is byte-identical
//!   to probing a data node.

use crate::ServerState;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// One member's probe cycle budget: connect + handshake + status round
/// trip. Generous next to the cycle interval — a slow member is exactly
/// the thing being measured, and a timed-out probe simply counts as a
/// miss.
const PROBE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Visibility bookkeeping for the voting members of one node.
///
/// Members EXCLUDE this node (`member_count` = `members.len() + 1`, self
/// always visible while the process runs). `misses` counts consecutive
/// failed probe cycles; a member is lost at `>= k` (the design's
/// debounce), healed by any single success.
pub struct Quorum {
    members: Vec<String>,
    k: usize,
    misses: Mutex<HashMap<String, usize>>,
    fenced: AtomicBool,
    /// Cycles in which at least one member probe failed (observability).
    probe_failure_cycles: AtomicU64,
}

impl Quorum {
    /// `members` are the voting peers OTHER than this node, already
    /// self-filtered, trimmed and deduplicated by the caller.
    pub fn new(members: Vec<String>, k: usize) -> Self {
        Quorum {
            members,
            k: k.max(1),
            misses: Mutex::new(HashMap::new()),
            fenced: AtomicBool::new(false),
            probe_failure_cycles: AtomicU64::new(0),
        }
    }

    pub fn member_count(&self) -> usize {
        self.members.len() + 1 // + self
    }

    /// Members currently visible — self always counts while we run.
    pub fn visible(&self) -> usize {
        let misses = self.misses.lock().unwrap_or_else(|p| p.into_inner());
        1 + self
            .members
            .iter()
            .filter(|m| misses.get(*m).copied().unwrap_or(0) < self.k)
            .count()
    }

    /// Strict majority test: `visible * 2 > member_count`.
    pub fn has_majority(&self) -> bool {
        self.visible() * 2 > self.member_count()
    }

    pub fn fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }

    /// Flip the fence; returns the PREVIOUS value so the caller can audit
    /// transitions only.
    pub fn set_fenced(&self, fenced: bool) -> bool {
        self.fenced.swap(fenced, Ordering::SeqCst)
    }

    /// Record one probe cycle's outcome for a member (ok resets the miss
    /// streak, failure extends it).
    fn record(&self, member: &str, ok: bool) {
        let mut misses = self.misses.lock().unwrap_or_else(|p| p.into_inner());
        if ok {
            misses.remove(member);
        } else {
            *misses.entry(member.to_string()).or_insert(0) += 1;
        }
    }

    fn lost_members(&self) -> Vec<String> {
        let misses = self.misses.lock().unwrap_or_else(|p| p.into_inner());
        self.members
            .iter()
            .filter(|m| misses.get(*m).copied().unwrap_or(0) >= self.k)
            .cloned()
            .collect()
    }

    pub fn probe_failure_cycles(&self) -> u64 {
        self.probe_failure_cycles.load(Ordering::Relaxed)
    }
}

/// Background probe loop: one REQ_STATUS probe per member per cycle,
/// concurrently (the same shape as repair's `probe_all_digests`). Fence
/// transitions audit to the sync log — a fenced node must be LOUD: the
/// whole point is that an operator can tell "fenced on purpose" from
/// "node broken".
pub async fn quorum_task(state: Arc<ServerState>) {
    let Some(quorum) = state.quorum.clone() else {
        return;
    };
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(
        state.quorum_probe_ms.max(10),
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let mut probes = Vec::new();
        for member in quorum.members.clone() {
            let st = state.clone();
            probes.push(tokio::spawn(async move {
                let probe = tokio::time::timeout(PROBE_IO_TIMEOUT, async {
                    crate::probe_frame(&st, &member, docsql_core::proto::REQ_STATUS).await
                })
                .await;
                (member, probe.map(|r| r.is_ok()).unwrap_or(false))
            }));
        }
        let mut any_failed = false;
        for probe in probes {
            match probe.await {
                Ok((member, ok)) => {
                    if !ok {
                        any_failed = true;
                    }
                    quorum.record(&member, ok);
                }
                // A probe task dying (join error) never answered this
                // cycle: count the cycle as failed.
                Err(_) => any_failed = true,
            }
        }
        if any_failed {
            quorum.probe_failure_cycles.fetch_add(1, Ordering::Relaxed);
            state
                .metrics
                .quorum_probe_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }
        state
            .metrics
            .quorum_visible
            .store(quorum.visible() as u64, Ordering::Relaxed);
        let majority = quorum.has_majority();
        let prev = quorum.set_fenced(!majority);
        if prev != !majority {
            let lost = quorum.lost_members();
            let suffix = if lost.is_empty() {
                String::new()
            } else {
                format!("; lost: {}", lost.join(", "))
            };
            let detail = format!(
                "{}: {}/{} members visible{suffix}",
                if majority {
                    "quorum restored"
                } else {
                    "quorum lost"
                },
                quorum.visible(),
                quorum.member_count(),
            );
            eprintln!("docsql-quorum: {detail}");
            crate::querylog::sync_event(
                &state.sync_log,
                "quorum",
                "",
                None,
                majority,
                Some(detail),
            );
        }
    }
}
