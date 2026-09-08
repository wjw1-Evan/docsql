//! KV command dispatch over the wire protocol.
//!
//! Payload: NUL-separated args, first arg is the command name. Replies use
//! RESP_AFFECTED with NUL-separated values (or RESP_ERROR with a message).

use crate::ServerState;
use docsql_core::proto::{self, Frame};
use docsql_kv::SetOpts;
use std::sync::Arc;

pub fn err_payload(msg: &str) -> Vec<u8> {
    msg.as_bytes().to_vec()
}

fn ok(parts: &[&str]) -> Frame {
    Frame::new(proto::RESP_AFFECTED, parts.join("\x00").into_bytes())
}

fn nil() -> Frame {
    Frame::new(proto::RESP_AFFECTED, vec![])
}

fn int(n: u64) -> Frame {
    Frame::new(proto::RESP_AFFECTED, n.to_le_bytes().to_vec())
}

fn fail(msg: &str) -> Frame {
    Frame::new(proto::RESP_ERROR, err_payload(msg))
}

/// Connection-level side effect a KV command asks the connection loop for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnEvent {
    None,
    /// Start (or confirm) a push subscription for the channel.
    Subscribe(String),
    /// Stop the connection's subscription for the channel.
    Unsubscribe(String),
}

/// Handle one KV frame. Returns the response, the connection event (if any),
/// and the post-command auth state.
/// Commands that mutate state and must be replicated.
fn is_write_cmd(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"
            | "DEL"
            | "INCR"
            | "INCRBY"
            | "EXPIRE"
            | "PERSIST"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "HSET"
            | "SADD"
            | "ZADD"
            | "MULTI"
            | "EXEC"
            | "DISCARD"
    )
}

pub async fn handle(
    state: &Arc<ServerState>,
    frame: &Frame,
    authed: bool,
) -> (Frame, ConnEvent, bool) {
    let args = match crate::parse_kv_args(&frame.payload) {
        Ok(a) => a,
        Err(e) => return (fail(&e), ConnEvent::None, authed),
    };
    let Some(cmd) = args.first().map(|s| s.to_uppercase()) else {
        return (fail("empty command"), ConnEvent::None, authed);
    };
    let rest: Vec<String> = args[1..].to_vec();

    let is_replication = frame.flags & crate::FLAG_REPLICATION != 0;
    // Read-only gate: client writes (KV side) are rejected until PROMOTE,
    // mirroring the SQL path — without this a replica accepted durable,
    // unreplicated KV writes.
    if state.read_only.load(std::sync::atomic::Ordering::SeqCst)
        && !is_replication
        && is_write_cmd(&cmd)
    {
        return (
            fail("read-only replica; PROMOTE to accept writes"),
            ConnEvent::None,
            authed,
        );
    }
    // A client MULTI queues behind an open engine transaction instead of
    // erroring immediately — same single-writer reasoning as the SQL BEGIN
    // path in handle_sql (concurrent clients' MULTI/EXEC serialize).
    let queues = !is_replication && cmd == "MULTI";
    let deadline = tokio::time::Instant::now() + crate::BEGIN_QUEUE_WAIT;
    let (resp, sub, authed, _order_guard) = loop {
        if queues {
            crate::wait_engine_tx_free(state, deadline).await;
        }
        // Serialize write execution with its fan-out (peers see execution
        // order; buffer-vs-forward classification is race-free).
        let guard = if !is_replication && is_write_cmd(&cmd) {
            Some(state.write_order.lock().await)
        } else {
            None
        };
        let (resp, sub, authed) = handle_inner(state, authed, &cmd, &rest).await;
        // Lost the race for the engine transaction between the wait and the
        // locks: requeue until the deadline, then let the error through.
        if queues
            && resp.frame_type == docsql_core::proto::RESP_ERROR
            && String::from_utf8_lossy(&resp.payload).contains("transaction already in progress")
            && tokio::time::Instant::now() < deadline
        {
            drop(guard);
            tokio::time::sleep(crate::BEGIN_QUEUE_POLL).await;
            continue;
        }
        break (resp, sub, authed, guard);
    };

    // Replicate successful KV mutations (skips replication-internal frames).
    // Transaction control (MULTI/EXEC/DISCARD) is never forwarded — data
    // commands inside the open engine transaction buffer until EXEC commits
    // them, mirroring the SQL-side BEGIN/COMMIT/ROLLBACK timing.
    if !is_replication
        && is_write_cmd(&cmd)
        && !state.read_only.load(std::sync::atomic::Ordering::SeqCst)
        && resp.frame_type != docsql_core::proto::RESP_ERROR
    {
        match cmd.as_str() {
            "MULTI" => {}
            "EXEC" => crate::drain_tx_pending(state).await,
            "DISCARD" => state.tx_pending.lock().await.clear(),
            _ => {
                if state
                    .kv
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .db
                    .in_transaction()
                {
                    state
                        .tx_pending
                        .lock()
                        .await
                        .writes
                        .push(crate::PendingWrite::Kv(frame.clone()));
                } else {
                    crate::forward_kv_all(state, frame).await;
                }
            }
        }
    }
    (resp, sub, authed)
}

async fn handle_inner(
    state: &Arc<ServerState>,
    authed: bool,
    cmd: &str,
    rest: &[String],
) -> (Frame, ConnEvent, bool) {
    use crate::crypto::constant_time_eq;
    let a = |i: usize| -> Option<&String> { rest.get(i) };
    let none = ConnEvent::None;
    match cmd {
        "AUTH" => {
            let token = a(0).cloned().unwrap_or_default();
            match &state.auth_token {
                Some(expect) if constant_time_eq(token.as_bytes(), expect.as_bytes()) => {
                    (ok(&["ok"]), none, true)
                }
                Some(_) => (fail("bad token"), none, authed),
                None => (ok(&["ok"]), none, true), // auth disabled
            }
        }
        "PING" => (ok(&["pong"]), none, authed),
        "PUBLISH" if authed => {
            let (Some(ch), Some(payload)) = (a(0), a(1)) else {
                return (fail("PUBLISH channel payload"), none, authed);
            };
            let n = state.pubsub.publish(ch, payload);
            (int(n), none, authed)
        }
        "SUBSCRIBE" if authed => {
            let Some(ch) = a(0) else {
                return (fail("SUBSCRIBE channel"), none, authed);
            };
            (
                ok(&["subscribed", ch]),
                ConnEvent::Subscribe(ch.clone()),
                authed,
            )
        }
        "UNSUBSCRIBE" if authed => {
            let Some(ch) = a(0) else {
                return (fail("UNSUBSCRIBE channel"), none, authed);
            };
            (
                ok(&["unsubscribed", ch]),
                ConnEvent::Unsubscribe(ch.clone()),
                authed,
            )
        }
        _ if !authed => (fail("unauthorized"), none, authed),
        "GET" => {
            let key = a(0).cloned().unwrap_or_default();
            match state.kv.lock().unwrap_or_else(|p| p.into_inner()).get(&key) {
                Ok(Some(v)) => (ok(&[&v]), none, authed),
                Ok(None) => (nil(), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "SET" => {
            let key = a(0).cloned().unwrap_or_default();
            let val = a(1).cloned().unwrap_or_default();
            let mut opts = SetOpts::default();
            for flag in rest.iter().skip(2) {
                match flag.as_str() {
                    "NX" => opts.nx = true,
                    "XX" => opts.xx = true,
                    f if f.starts_with("EX=") => {
                        // A bad TTL must error, not silently mean "no TTL".
                        let secs: i64 =
                            f[3..].parse().map_err(|_| "invalid EX value").unwrap_or(-1);
                        let ms = secs.checked_mul(1000);
                        if secs < 0 || ms.is_none() {
                            return (fail("invalid or out of range EX value"), none, authed);
                        }
                        opts.ttl_ms = ms;
                    }
                    f if f.starts_with("PX=") => {
                        let ms: i64 = match f[3..].parse() {
                            Ok(v) => v,
                            Err(_) => {
                                return (fail("invalid PX value"), none, authed);
                            }
                        };
                        opts.ttl_ms = Some(ms);
                    }
                    _ => {}
                }
            }
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .set(&key, &val, opts)
            {
                Ok(done) => (ok(&[if done { "ok" } else { "skip" }]), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "EXISTS" => {
            let key = a(0).cloned().unwrap_or_default();
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .exists(&key)
            {
                Ok(true) => (int(1), none, authed),
                Ok(false) => (int(0), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "DEL" => {
            let key = a(0).cloned().unwrap_or_default();
            match state.kv.lock().unwrap_or_else(|p| p.into_inner()).del(&key) {
                Ok(done) => (ok(&[if done { "1" } else { "0" }]), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "INCR" | "INCRBY" => {
            let key = a(0).cloned().unwrap_or_default();
            let delta = if cmd == "INCR" {
                1
            } else {
                match a(1).map(|s| s.parse::<i64>()) {
                    Some(Ok(d)) => d,
                    _ => return (fail("INCRBY key delta (integer)"), none, authed),
                }
            };
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .incr_by(&key, delta)
            {
                Ok(n) => (int(n as u64), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "EXPIRE" => {
            let key = a(0).cloned().unwrap_or_default();
            // A bad TTL must error: unwrap_or(0) would delete the key.
            let ms = match a(1).map(|s| s.parse::<i64>()) {
                Some(Ok(v)) => v,
                _ => return (fail("EXPIRE key ttl-ms (integer)"), none, authed),
            };
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .expire(&key, ms)
            {
                Ok(done) => (ok(&[if done { "1" } else { "0" }]), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "PERSIST" => {
            let key = a(0).cloned().unwrap_or_default();
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .persist(&key)
            {
                Ok(done) => (ok(&[if done { "1" } else { "0" }]), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "TTL" => {
            let key = a(0).cloned().unwrap_or_default();
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .ttl_ms(&key)
            {
                Ok(Some(ms)) => (int(ms.max(0) as u64), none, authed),
                Ok(None) => (ok(&["-1"]), none, authed),
                Err(_) => (ok(&["-2"]), none, authed),
            }
        }
        "LPUSH" | "RPUSH" => {
            let key = a(0).cloned().unwrap_or_default();
            let vals: Vec<&str> = rest.iter().skip(1).map(|s| s.as_str()).collect();
            let r = if cmd == "LPUSH" {
                state
                    .kv
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .lpush(&key, &vals)
            } else {
                state
                    .kv
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .rpush(&key, &vals)
            };
            match r {
                Ok(n) => (int(n), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "LPOP" | "RPOP" => {
            let key = a(0).cloned().unwrap_or_default();
            let r = if cmd == "LPOP" {
                state
                    .kv
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .lpop(&key)
            } else {
                state
                    .kv
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .rpop(&key)
            };
            match r {
                Ok(Some(v)) => (ok(&[&v]), none, authed),
                Ok(None) => (nil(), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "LRANGE" => {
            let key = a(0).cloned().unwrap_or_default();
            let s = a(1).and_then(|x| x.parse().ok()).unwrap_or(0);
            let e = a(2).and_then(|x| x.parse().ok()).unwrap_or(-1);
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .lrange(&key, s, e)
            {
                Ok(items) => (
                    ok(&items.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
                    none,
                    authed,
                ),
                Err(er) => (fail(&er.to_string()), none, authed),
            }
        }
        "HSET" => {
            let (Some(k), Some(f), Some(v)) = (a(0), a(1), a(2)) else {
                return (fail("HSET key field value"), none, authed);
            };
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .hset(k, f, v)
            {
                Ok(n) => (int(n), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "HGET" => {
            let (Some(k), Some(f)) = (a(0), a(1)) else {
                return (fail("HGET key field"), none, authed);
            };
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .hget(k, f)
            {
                Ok(Some(v)) => (ok(&[&v]), none, authed),
                Ok(None) => (nil(), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "SADD" => {
            let key = a(0).cloned().unwrap_or_default();
            let members: Vec<&str> = rest.iter().skip(1).map(|s| s.as_str()).collect();
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .sadd(&key, &members)
            {
                Ok(n) => (int(n), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "SMEMBERS" => {
            let key = a(0).cloned().unwrap_or_default();
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .smembers(&key)
            {
                Ok(items) => (
                    ok(&items.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
                    none,
                    authed,
                ),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "ZADD" => {
            let (Some(k), Some(score), Some(member)) = (a(0), a(1), a(2)) else {
                return (fail("ZADD key score member"), none, authed);
            };
            let score: f64 = match score.parse() {
                Ok(v) => v,
                Err(_) => return (fail("ZADD score must be a number"), none, authed),
            };
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .zadd(k, score, member)
            {
                Ok(n) => (int(n), none, authed),
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "ZRANGE" => {
            let key = a(0).cloned().unwrap_or_default();
            let min = a(1)
                .and_then(|x| x.parse().ok())
                .unwrap_or(f64::NEG_INFINITY);
            let max = a(2).and_then(|x| x.parse().ok()).unwrap_or(f64::INFINITY);
            match state
                .kv
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .zrange(&key, min, max)
            {
                Ok(pairs) => {
                    let flat: Vec<String> = pairs
                        .iter()
                        .flat_map(|(m, s)| vec![m.clone(), s.to_string()])
                        .collect();
                    (
                        ok(&flat.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
                        none,
                        authed,
                    )
                }
                Err(e) => (fail(&e.to_string()), none, authed),
            }
        }
        "PROMOTE" => {
            state
                .read_only
                .store(false, std::sync::atomic::Ordering::SeqCst);
            *state.replicate_to.lock().await = None;
            (ok(&["promoted"]), none, authed)
        }
        "MULTI" => match state.kv.lock().unwrap_or_else(|p| p.into_inner()).multi() {
            Ok(()) => (ok(&["ok"]), none, authed),
            Err(e) => (fail(&e.to_string()), none, authed),
        },
        "EXEC" => match state.kv.lock().unwrap_or_else(|p| p.into_inner()).exec() {
            Ok(()) => (ok(&["ok"]), none, authed),
            Err(e) => (fail(&e.to_string()), none, authed),
        },
        "DISCARD" => match state.kv.lock().unwrap_or_else(|p| p.into_inner()).discard() {
            Ok(()) => (ok(&["ok"]), none, authed),
            Err(e) => (fail(&e.to_string()), none, authed),
        },
        other => (fail(&format!("unknown command: {other}")), none, authed),
    }
}
