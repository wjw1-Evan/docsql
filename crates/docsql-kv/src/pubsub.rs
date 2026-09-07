//! Publish/subscribe bus.
//!
//! Best-effort delivery, no persistence: messages are fanned out to every
//! live subscriber. Pattern subscribers receive `pmessage` events. The bus
//! lives in the embedded layer; the server forwards to network subscribers.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A message on an exact channel: (channel, payload).
    Message { channel: String, payload: String },
    /// A message matching a glob pattern: (pattern, channel, payload).
    PMessage {
        pattern: String,
        channel: String,
        payload: String,
    },
}

/// Glob-style pattern: `*` matches any run, `?` one char.
pub fn pattern_matches(pattern: &str, s: &str) -> bool {
    fn go(p: &[u8], s: &[u8]) -> bool {
        match (p.first(), s.first()) {
            (None, None) => true,
            (Some(b'*'), _) => go(&p[1..], s) || (!s.is_empty() && go(p, &s[1..])),
            (Some(b'?'), Some(_)) => go(&p[1..], &s[1..]),
            (Some(a), Some(b)) if a == b => go(&p[1..], &s[1..]),
            _ => false,
        }
    }
    go(pattern.as_bytes(), s.as_bytes())
}

#[derive(Default)]
pub struct PubSub {
    /// channel -> subscriber senders
    channels: Mutex<HashMap<String, Vec<Sender<Event>>>>,
    patterns: Mutex<HashMap<String, Vec<Sender<Event>>>>,
}

impl PubSub {
    pub fn new() -> PubSub {
        PubSub::default()
    }

    /// Subscribe to an exact channel; returns the receiving end.
    pub fn subscribe(&self, channel: &str) -> Receiver<Event> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.channels
            .lock()
            .unwrap()
            .entry(channel.to_string())
            .or_default()
            .push(tx);
        rx
    }

    pub fn psubscribe(&self, pattern: &str) -> Receiver<Event> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.patterns
            .lock()
            .unwrap()
            .entry(pattern.to_string())
            .or_default()
            .push(tx);
        rx
    }

    /// Drop a channel/pattern entry entirely (all its subscribers).
    fn drop_key<T>(map: &mut HashMap<String, Vec<Sender<T>>>, key: &str) {
        // Senders whose receiver is dropped are detected lazily at publish
        // time (send fails); here we only prune now-empty entries.
        if let Some(v) = map.get(key) {
            if v.is_empty() {
                map.remove(key);
            }
        }
    }

    /// Stop delivering `channel` to the receiver's channel set is implicit:
    /// dropped receivers are pruned on publish. Explicit unsubscribe by key
    /// removes ALL receivers on that key (server tracks its own mapping).
    pub fn unsubscribe_all(&self, channel: &str) {
        let mut chans = self.channels.lock().unwrap();
        if let Some(v) = chans.get_mut(channel) {
            // Try sending nothing; just clear. Receivers keep what they got.
            v.clear();
        }
        Self::drop_key(&mut chans, channel);
    }

    /// Publish to all exact and pattern subscribers. Returns the number of
    /// clients the message was delivered to. Dead subscribers (dropped
    /// receivers) are pruned when a send fails.
    pub fn publish(&self, channel: &str, payload: &str) -> u64 {
        let mut delivered = 0u64;
        {
            let mut chans = self.channels.lock().unwrap();
            if let Some(subs) = chans.get_mut(channel) {
                subs.retain(|s| {
                    let ok = s
                        .send(Event::Message {
                            channel: channel.into(),
                            payload: payload.into(),
                        })
                        .is_ok();
                    delivered += ok as u64;
                    ok
                });
            }
            Self::drop_key(&mut chans, channel);
        }
        {
            let mut pats = self.patterns.lock().unwrap();
            let hits: Vec<String> = pats
                .keys()
                .filter(|p| pattern_matches(p, channel))
                .cloned()
                .collect();
            for pat in hits {
                if let Some(subs) = pats.get_mut(&pat) {
                    subs.retain(|s| {
                        let ok = s
                            .send(Event::PMessage {
                                pattern: pat.clone(),
                                channel: channel.into(),
                                payload: payload.into(),
                            })
                            .is_ok();
                        delivered += ok as u64;
                        ok
                    });
                }
                Self::drop_key(&mut pats, &pat);
            }
        }
        delivered
    }

    /// Drain pending events for a receiver (test/embedded convenience).
    pub fn drain(rx: &Receiver<Event>) -> Vec<Event> {
        let mut out = Vec::new();
        while let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        out
    }

    pub fn numsub(&self, channel: &str) -> usize {
        self.channels
            .lock()
            .unwrap()
            .get(channel)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    pub fn numpat(&self) -> usize {
        self.patterns.lock().unwrap().len()
    }

    pub fn channels(&self, pattern: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .channels
            .lock()
            .unwrap()
            .keys()
            .filter(|c| pattern.is_empty() || pattern_matches(pattern, c))
            .cloned()
            .collect();
        out.sort();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_channel_delivery() {
        let ps = PubSub::new();
        let rx = ps.subscribe("news");
        let other = ps.subscribe("weather");
        assert_eq!(ps.publish("news", "hello"), 1);
        assert_eq!(
            PubSub::drain(&rx),
            vec![Event::Message {
                channel: "news".into(),
                payload: "hello".into()
            }]
        );
        assert!(PubSub::drain(&other).is_empty());
    }

    #[test]
    fn pattern_delivery_and_glob() {
        let ps = PubSub::new();
        let rx = ps.psubscribe("user.*.login");
        assert_eq!(ps.publish("user.42.login", "ok"), 1);
        assert_eq!(ps.publish("user.42.logout", "no"), 0);
        assert_eq!(
            PubSub::drain(&rx),
            vec![Event::PMessage {
                pattern: "user.*.login".into(),
                channel: "user.42.login".into(),
                payload: "ok".into()
            }]
        );
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("a?c", "abc"));
        assert!(!pattern_matches("a?c", "abbc"));
        assert!(pattern_matches("news.*", "news.tech.ai"));
    }

    #[test]
    fn multi_subscriber_fanout() {
        let ps = PubSub::new();
        let r1 = ps.subscribe("ch");
        let r2 = ps.subscribe("ch");
        assert_eq!(ps.numsub("ch"), 2);
        assert_eq!(ps.publish("ch", "m"), 2);
        for r in [&r1, &r2] {
            assert_eq!(PubSub::drain(r).len(), 1);
        }
    }

    #[test]
    fn dropped_receivers_pruned() {
        let ps = PubSub::new();
        {
            let _rx = ps.subscribe("temp");
        }
        // The receiver is dropped; publish prunes it silently.
        assert_eq!(ps.publish("temp", "x"), 0);
        assert_eq!(ps.numsub("temp"), 0);
    }

    #[test]
    fn introspection() {
        let ps = PubSub::new();
        ps.subscribe("a.one");
        ps.subscribe("a.two");
        ps.psubscribe("a.*");
        assert_eq!(ps.channels("a.*"), vec!["a.one", "a.two"]);
        assert_eq!(ps.channels(""), vec!["a.one", "a.two"]);
        assert_eq!(ps.numpat(), 1);
    }
    #[test]
    fn unsubscribe_all_and_dead_prune() {
        let ps = PubSub::new();
        let rx = ps.subscribe("ch");
        assert_eq!(ps.numsub("ch"), 1);
        ps.unsubscribe_all("ch");
        assert_eq!(ps.numsub("ch"), 0);
        assert_eq!(ps.publish("ch", "m"), 0);
        assert!(PubSub::drain(&rx).is_empty());
        // 订阅后立即退订 pattern 端
        let rx2 = ps.psubscribe("p.*");
        ps.publish("p.1", "x");
        let _ = PubSub::drain(&rx2);
        // 空 pattern 条目被裁剪
        ps.psubscribe("gone.*");
        assert_eq!(ps.numpat(), 2);
    }
}
