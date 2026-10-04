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
/// What the last successful probe of a member revealed (parsed from its
/// REQ_STATUS payload). Missing fields degrade to defaults — older peers
/// simply never look like primaries.
#[derive(Clone, Debug)]
pub struct MemberView {
    pub epoch: u64,
    pub read_only: bool,
    pub forwarding: bool,
    pub journal_head: Option<u64>,
    /// The member's cluster node id (`cluster_id` in its status payload):
    /// the lag guard needs it to read the LOCAL applied position FOR THAT
    /// ORIGIN (`_cluster_pos`), which is what "how far behind the primary
    /// are we" actually means.
    pub node_id: Option<String>,
}

impl MemberView {
    /// An active primary: writable and not forwarding to anyone else.
    pub fn is_active_primary(&self) -> bool {
        !self.read_only && !self.forwarding
    }
}

pub struct Quorum {
    members: Vec<String>,
    k: usize,
    misses: Mutex<HashMap<String, usize>>,
    views: Mutex<HashMap<String, MemberView>>,
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
            views: Mutex::new(HashMap::new()),
            fenced: AtomicBool::new(false),
            probe_failure_cycles: AtomicU64::new(0),
        }
    }

    /// Whether `member` has missed K consecutive cycles (the design's
    /// debounce threshold) — the auto-PROMOTE trigger reads this for the
    /// primary's address.
    pub fn is_lost(&self, member: &str) -> bool {
        let misses = self.misses.lock().unwrap_or_else(|p| p.into_inner());
        misses.get(member).copied().unwrap_or(0) >= self.k
    }

    /// The journal head the member reported at its last successful probe
    /// (auto-PROMOTE's lag guard reads this for the primary).
    pub fn last_seen_head(&self, member: &str) -> Option<u64> {
        let views = self.views.lock().unwrap_or_else(|p| p.into_inner());
        views.get(member).and_then(|v| v.journal_head)
    }

    /// The member's whole last-seen view (lag guard needs the node id too).
    pub fn view_of(&self, member: &str) -> Option<MemberView> {
        let views = self.views.lock().unwrap_or_else(|p| p.into_inner());
        views.get(member).cloned()
    }

    fn record_view(&self, member: &str, view: MemberView) {
        self.views
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(member.to_string(), view);
    }

    /// Snapshot of the last-seen member views (supervision reads).
    fn member_views(&self) -> Vec<(String, MemberView)> {
        self.views
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
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
        let member_list: Vec<String> = quorum.members.clone();
        for member in member_list.clone() {
            let st = state.clone();
            probes.push(tokio::spawn(async move {
                let probe = tokio::time::timeout(PROBE_IO_TIMEOUT, async {
                    crate::probe_frame(&st, &member, docsql_core::proto::REQ_STATUS).await
                })
                .await;
                (member, probe)
            }));
        }
        let mut any_failed = false;
        for (idx, probe) in probes.into_iter().enumerate() {
            match probe.await {
                Ok((member, outcome)) => {
                    let ok = matches!(outcome, Ok(Ok(_)));
                    if !ok {
                        any_failed = true;
                    }
                    quorum.record(&member, ok);
                    if let Ok(Ok(frame)) = outcome {
                        if let Some(view) = parse_member_view(&frame) {
                            quorum.record_view(&member, view);
                        }
                    }
                }
                // A probe task dying (join error) never answered this
                // cycle: count the cycle as failed AND as a miss for the
                // member — a member whose probe task keeps panicking must
                // not stay "visible" forever, or the fence never triggers.
                Err(_) => {
                    any_failed = true;
                    quorum.record(&member_list[idx], false);
                }
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
        supervise(&state, &quorum).await;
    }
}

/// Parse the role-relevant slice of a member's REQ_STATUS payload.
fn parse_member_view(frame: &docsql_core::proto::Frame) -> Option<MemberView> {
    if frame.frame_type != docsql_core::proto::RESP_STATUS {
        return None;
    }
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).ok()?;
    Some(MemberView {
        epoch: v["primary_epoch"].as_u64().unwrap_or(0),
        read_only: v["read_only"].as_bool() == Some(true),
        forwarding: v["replicate_to"].is_string(),
        journal_head: v["journal_head"].as_u64(),
        node_id: v["cluster_id"].as_str().map(String::from),
    })
}

/// Role supervision (design 004 §4, phase 2), evaluated after every cycle:
///
/// **Auto-PROMOTE** — a read-only replica configured `DOCSQL_AUTO_PROMOTE`
/// promotes itself when its primary has missed K cycles, a majority is
/// visible, and its journal lag to the primary's last-seen head is within
/// the catch-up window (otherwise the writes the old primary confirmed
/// would be unreachable — stay read-only and warn once).
///
/// **Demotion** — an active primary that sees a HIGHER-epoch active
/// primary demotes: read-only plus re-pointed at the winner. Equal epochs
/// never demote (symmetric clusters all sit at 0 and are untouched); the
/// equal-epoch split-brain window after independent manual promotions is
/// the documented residual risk of the in-memory epoch (§4.3).
async fn supervise(state: &Arc<ServerState>, quorum: &Quorum) {
    use std::sync::atomic::Ordering;
    if state.arbiter {
        return;
    }
    let self_epoch = state.primary_epoch.load(Ordering::SeqCst);
    let read_only = state.read_only.load(Ordering::SeqCst);

    // (a) Auto-PROMOTE. Frozen after a demotion re-pointed us at a higher
    // epoch primary: the lost-primary trigger still watches the STARTUP
    // primary address, so an unfrozen loop would re-promote against the
    // dead old primary and let (b) demote it again every cycle — epoch
    // inflation and a client-visible read-only/write flap. Restart (or a
    // manual PROMOTE) is the only unfreeze.
    if state.auto_promote && read_only && !state.auto_promote_frozen.load(Ordering::SeqCst) {
        if let Some(primary) = &state.primary_addr {
            if quorum.is_lost(primary) && quorum.has_majority() {
                // Lag guard: how far behind the primary's last-seen head is
                // the APPLIED position for the primary's ORIGIN
                // (`_cluster_pos`)? The local journal head counts only this
                // node's OWN writes — a pure replica's head is always 0, so
                // a busy primary pushed the guard over the window forever
                // (never promoted), and a previously-writable replica's
                // stale head could pass the guard while it was actually
                // far behind. Zero window = unbounded (always acceptable).
                let view = quorum.view_of(primary);
                let primary_head = view.as_ref().and_then(|v| v.journal_head);
                let primary_origin = view.as_ref().and_then(|v| v.node_id.clone());
                let lag_ok = match (primary_head, primary_origin, state.catchup_window) {
                    (None, _, _) | (_, None, _) => false, // never saw the primary/its identity: cannot judge
                    // Zero window = UNBOUNDED retention: every lag is
                    // acceptable (the comment above says so; the old
                    // `ph >= applied` comparison was stricter than any
                    // finite window and locked a caught-up-but-older
                    // replica out of promotion forever).
                    (Some(_), Some(_), 0) => true,
                    (Some(ph), Some(origin), window) => {
                        ph.saturating_sub(applied_position(state, &origin).unwrap_or(0)) <= window
                    }
                };
                if lag_ok {
                    state.read_only.store(false, Ordering::SeqCst);
                    *state.replicate_to.lock().await = None;
                    let epoch = state.primary_epoch.fetch_add(1, Ordering::SeqCst) + 1;
                    let detail = format!(
                        "auto: primary {primary} lost, majority visible, lag within                          window; epoch now {epoch}"
                    );
                    eprintln!("docsql-quorum: promoted ({detail})");
                    crate::querylog::sync_event(
                        &state.sync_log,
                        "promote",
                        primary,
                        None,
                        true,
                        Some(detail),
                    );
                } else if !state.promote_lag_warned.swap(true, Ordering::SeqCst) {
                    eprintln!(
                        "docsql-quorum: primary {primary} lost but local journal is                          behind beyond DOCSQL_CATCHUP_WINDOW — staying read-only                          (promoting would orphan the writes the primary confirmed)"
                    );
                    crate::querylog::sync_event(
                        &state.sync_log,
                        "promote",
                        primary,
                        None,
                        false,
                        Some("auto: lag beyond catch-up window; staying read-only".into()),
                    );
                }
            } else if quorum.last_seen_head(primary).is_some() {
                // Primary reachable again: reset the once-per-episode warn.
                state.promote_lag_warned.store(false, Ordering::SeqCst);
            }
        }
        return; // a replica does not demote
    }

    // (b) Demotion of a stale primary.
    if !read_only {
        let forwarding = state.replicate_to.lock().await.is_some();
        if !forwarding {
            for (addr, view) in quorum.member_views() {
                if view.is_active_primary() && view.epoch > self_epoch {
                    state.read_only.store(true, Ordering::SeqCst);
                    *state.replicate_to.lock().await = Some(addr.clone());
                    // The mesh outranks us: freeze self-promotion for this
                    // process (see the (a) gate comment).
                    state.auto_promote_frozen.store(true, Ordering::SeqCst);
                    let detail = format!(
                        "higher-ranked primary visible: {addr} (epoch {} > {self_epoch}); \
                         demoted to read-only replica and re-pointed",
                        view.epoch
                    );
                    eprintln!("docsql-quorum: {detail}");
                    crate::querylog::sync_event(
                        &state.sync_log,
                        "demote",
                        &addr,
                        None,
                        true,
                        Some(detail),
                    );
                    break;
                }
            }
        }
    }
}

/// The local APPLIED position for `origin` (write-tier access: the call
/// may lazily create the cluster tables, exactly like the status payload's
/// own read). None = this node has never applied/replay-seeded anything
/// from that origin.
fn applied_position(state: &Arc<ServerState>, origin: &str) -> Option<u64> {
    let mut db = state.db.write().unwrap_or_else(|p| p.into_inner());
    db.position_get(origin).ok().flatten()
}
