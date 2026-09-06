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

/// Handle one KV frame. Returns the response and, for SUBSCRIBE, the
/// channel the connection should be pushed messages for.
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
) -> (Frame, Option<String>, bool) {
    let args = match crate::parse_kv_args(&frame.payload) {
        Ok(a) => a,
        Err(e) => return (Frame::new(proto::RESP_ERROR, err_payload(&e)), None, authed),
    };
    let Some(cmd) = args.first().map(|s| s.to_uppercase()) else {
        return (
            Frame::new(proto::RESP_ERROR, err_payload("empty command")),
            None,
            authed,
        );
    };
    let rest: Vec<String> = args[1..].to_vec();

    let (resp, sub, authed) = handle_inner(state, authed, &cmd, &rest).await;

    // Replicate successful KV mutations (skips replication-internal frames).
    let is_replication = frame.flags & crate::FLAG_REPLICATION != 0;
    // Only a writable primary replicates (replicas skip; the replication
    // flag on forwarded frames prevents loops).
    if !is_replication
        && is_write_cmd(&cmd)
        && !state.read_only.load(std::sync::atomic::Ordering::SeqCst)
        && resp.frame_type != docsql_core::proto::RESP_ERROR
    {
        if let Some(target) = state.replicate_to.lock().await.clone() {
            let mut fwd = frame.clone();
            fwd.flags = crate::FLAG_REPLICATION;
            if let Err(e) = crate::forward_frame(&target, &fwd).await {
                eprintln!("kv replication to {target} failed: {e}");
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
) -> (Frame, Option<String>, bool) {
    let a = |i: usize| -> Option<&String> { rest.get(i) };
    match cmd {
        "AUTH" => {
            let token = a(0).cloned().unwrap_or_default();
            match &state.auth_token {
                Some(expect) if token == *expect => (ok(&["ok"]), None, true),
                Some(_) => (
                    Frame::new(proto::RESP_ERROR, err_payload("bad token")),
                    None,
                    authed,
                ),
                None => (ok(&["ok"]), None, true), // auth disabled
            }
        }
        "PING" => (ok(&["pong"]), None, authed),
        "PUBLISH" if authed => {
            let (Some(ch), Some(payload)) = (a(0), a(1)) else {
                return (
                    Frame::new(proto::RESP_ERROR, err_payload("PUBLISH channel payload")),
                    None,
                    authed,
                );
            };
            let n = state.pubsub.publish(ch, payload);
            (int(n), None, authed)
        }
        "SUBSCRIBE" if authed => {
            let Some(ch) = a(0) else {
                return (
                    Frame::new(proto::RESP_ERROR, err_payload("SUBSCRIBE channel")),
                    None,
                    authed,
                );
            };
            (ok(&["subscribed", ch]), Some(ch.clone()), authed)
        }
        _ if !authed => (
            Frame::new(proto::RESP_ERROR, err_payload("unauthorized")),
            None,
            authed,
        ),
        "GET" => {
            let key = a(0).cloned().unwrap_or_default();
            match state.kv.lock().unwrap().get(&key) {
                Ok(Some(v)) => (ok(&[&v]), None, authed),
                Ok(None) => (nil(), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
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
                        opts.ttl_ms = f[3..].parse::<i64>().ok().map(|s| s * 1000)
                    }
                    f if f.starts_with("PX=") => opts.ttl_ms = f[3..].parse::<i64>().ok(),
                    _ => {}
                }
            }
            match state.kv.lock().unwrap().set(&key, &val, opts) {
                Ok(done) => (ok(&[if done { "ok" } else { "skip" }]), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "DEL" => match state
            .kv
            .lock()
            .unwrap()
            .del(&a(0).cloned().unwrap_or_default())
        {
            Ok(done) => (ok(&[if done { "1" } else { "0" }]), None, authed),
            Err(e) => (
                Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                None,
                authed,
            ),
        },
        "INCR" | "INCRBY" => {
            let key = a(0).cloned().unwrap_or_default();
            let delta = if cmd == "INCR" {
                1
            } else {
                a(1).and_then(|s| s.parse().ok()).unwrap_or(1)
            };
            match state.kv.lock().unwrap().incr_by(&key, delta) {
                Ok(n) => (int(n as u64), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "EXPIRE" => {
            let key = a(0).cloned().unwrap_or_default();
            let ms = a(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            match state.kv.lock().unwrap().expire(&key, ms) {
                Ok(done) => (ok(&[if done { "1" } else { "0" }]), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "TTL" => match state
            .kv
            .lock()
            .unwrap()
            .ttl_ms(&a(0).cloned().unwrap_or_default())
        {
            Ok(Some(ms)) => (int(ms as u64), None, authed),
            Ok(None) => (ok(&["-1"]), None, authed),
            Err(_) => (ok(&["-2"]), None, authed),
        },
        "LPUSH" | "RPUSH" => {
            let key = a(0).cloned().unwrap_or_default();
            let vals: Vec<&str> = rest.iter().skip(1).map(|s| s.as_str()).collect();
            let r = if cmd == "LPUSH" {
                state.kv.lock().unwrap().lpush(&key, &vals)
            } else {
                state.kv.lock().unwrap().rpush(&key, &vals)
            };
            match r {
                Ok(n) => (int(n), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "LPOP" | "RPOP" => {
            let key = a(0).cloned().unwrap_or_default();
            let r = if cmd == "LPOP" {
                state.kv.lock().unwrap().lpop(&key)
            } else {
                state.kv.lock().unwrap().rpop(&key)
            };
            match r {
                Ok(Some(v)) => (ok(&[&v]), None, authed),
                Ok(None) => (nil(), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "LRANGE" => {
            let key = a(0).cloned().unwrap_or_default();
            let s = a(1).and_then(|x| x.parse().ok()).unwrap_or(0);
            let e = a(2).and_then(|x| x.parse().ok()).unwrap_or(-1);
            match state.kv.lock().unwrap().lrange(&key, s, e) {
                Ok(items) => (
                    ok(&items.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
                    None,
                    authed,
                ),
                Err(er) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&er.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "HSET" => {
            let (Some(k), Some(f), Some(v)) = (a(0), a(1), a(2)) else {
                return (
                    Frame::new(proto::RESP_ERROR, err_payload("HSET key field value")),
                    None,
                    authed,
                );
            };
            match state.kv.lock().unwrap().hset(k, f, v) {
                Ok(n) => (int(n), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "HGET" => {
            let (Some(k), Some(f)) = (a(0), a(1)) else {
                return (
                    Frame::new(proto::RESP_ERROR, err_payload("HGET key field")),
                    None,
                    authed,
                );
            };
            match state.kv.lock().unwrap().hget(k, f) {
                Ok(Some(v)) => (ok(&[&v]), None, authed),
                Ok(None) => (nil(), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "SADD" => {
            let key = a(0).cloned().unwrap_or_default();
            let members: Vec<&str> = rest.iter().skip(1).map(|s| s.as_str()).collect();
            match state.kv.lock().unwrap().sadd(&key, &members) {
                Ok(n) => (int(n), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "SMEMBERS" => {
            let key = a(0).cloned().unwrap_or_default();
            match state.kv.lock().unwrap().smembers(&key) {
                Ok(items) => (
                    ok(&items.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
                    None,
                    authed,
                ),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "ZADD" => {
            let (Some(k), Some(score), Some(member)) = (a(0), a(1), a(2)) else {
                return (
                    Frame::new(proto::RESP_ERROR, err_payload("ZADD key score member")),
                    None,
                    authed,
                );
            };
            let score: f64 = score.parse().unwrap_or(0.0);
            match state.kv.lock().unwrap().zadd(k, score, member) {
                Ok(n) => (int(n), None, authed),
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "ZRANGE" => {
            let key = a(0).cloned().unwrap_or_default();
            let min = a(1)
                .and_then(|x| x.parse().ok())
                .unwrap_or(f64::NEG_INFINITY);
            let max = a(2).and_then(|x| x.parse().ok()).unwrap_or(f64::INFINITY);
            match state.kv.lock().unwrap().zrange(&key, min, max) {
                Ok(pairs) => {
                    let flat: Vec<String> = pairs
                        .iter()
                        .flat_map(|(m, s)| vec![m.clone(), s.to_string()])
                        .collect();
                    (
                        ok(&flat.iter().map(|s| s.as_str()).collect::<Vec<_>>()),
                        None,
                        authed,
                    )
                }
                Err(e) => (
                    Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                    None,
                    authed,
                ),
            }
        }
        "PROMOTE" => {
            state
                .read_only
                .store(false, std::sync::atomic::Ordering::SeqCst);
            *state.replicate_to.lock().await = None;
            (ok(&["promoted"]), None, authed)
        }
        "MULTI" => match state.kv.lock().unwrap().multi() {
            Ok(()) => (ok(&["ok"]), None, authed),
            Err(e) => (
                Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                None,
                authed,
            ),
        },
        "EXEC" => match state.kv.lock().unwrap().exec() {
            Ok(()) => (ok(&["ok"]), None, authed),
            Err(e) => (
                Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                None,
                authed,
            ),
        },
        "DISCARD" => match state.kv.lock().unwrap().discard() {
            Ok(()) => (ok(&["ok"]), None, authed),
            Err(e) => (
                Frame::new(proto::RESP_ERROR, err_payload(&e.to_string())),
                None,
                authed,
            ),
        },
        other => (
            Frame::new(
                proto::RESP_ERROR,
                err_payload(&format!("unknown command: {other}")),
            ),
            None,
            authed,
        ),
    }
}
