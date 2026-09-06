//! Collection commands: LIST / HASH / SET / ZSET over the `_kv` table.
//!
//! Values are stored as JSON strings in the `value` column; `type` records
//! the collection kind so SQL queries can filter by it.

use crate::{escape, now_ms, Kv, KvError, Result};
use docsql_core::json;
use docsql_core::value::{Object, Value};

impl Kv {
    fn load_value(&mut self, key: &str) -> Result<Option<Value>> {
        let now = now_ms();
        match self.raw_row(key, now)? {
            Some(o) => match o.get("value") {
                Some(Value::Str(s)) => Ok(Some(
                    json::from_str(s).map_err(|e| KvError::Message(e.to_string()))?,
                )),
                _ => Err(KvError::Message("wrong value type".into())),
            },
            None => Ok(None),
        }
    }

    fn store_value(&mut self, key: &str, kind: &str, v: &Value) -> Result<()> {
        let text = json::to_string(v);
        let now = now_ms();
        if self.raw_row(key, now)?.is_some() {
            self.db
                .execute(&format!(
                    "UPDATE _kv SET value = '{v}', type = '{t}' WHERE \"key\" = '{k}'",
                    v = escape(&text),
                    t = escape(kind),
                    k = escape(key)
                ))
                .map_err(KvError::from)?;
        } else {
            self.db
                .execute(&format!(
                    "INSERT INTO _kv (\"key\", type, value, expire_at) VALUES ('{k}', '{t}', '{v}', 0)",
                    k = escape(key),
                    t = escape(kind),
                    v = escape(&text)
                ))
                .map_err(KvError::from)?;
        }
        Ok(())
    }

    // ---- LIST ----

    pub fn lpush(&mut self, key: &str, values: &[&str]) -> Result<u64> {
        let mut list = match self.load_value(key)? {
            Some(Value::Array(a)) => a,
            Some(_) => return Err(KvError::Message("wrong type for LIST".into())),
            None => vec![],
        };
        for v in values.iter().rev() {
            list.insert(0, Value::Str((*v).into()));
        }
        self.store_value(key, "list", &Value::Array(list.clone()))?;
        Ok(list.len() as u64)
    }

    pub fn rpush(&mut self, key: &str, values: &[&str]) -> Result<u64> {
        let mut list = match self.load_value(key)? {
            Some(Value::Array(a)) => a,
            Some(_) => return Err(KvError::Message("wrong type for LIST".into())),
            None => vec![],
        };
        for v in values {
            list.push(Value::Str((*v).into()));
        }
        self.store_value(key, "list", &Value::Array(list.clone()))?;
        Ok(list.len() as u64)
    }

    fn pop(&mut self, key: &str, front: bool) -> Result<Option<String>> {
        let mut list = match self.load_value(key)? {
            Some(Value::Array(a)) => a,
            Some(_) => return Err(KvError::Message("wrong type for LIST".into())),
            None => return Ok(None),
        };
        if list.is_empty() {
            return Ok(None);
        }
        let v = if front {
            list.remove(0)
        } else {
            list.pop().unwrap()
        };
        self.store_value(key, "list", &Value::Array(list))?;
        Ok(v.as_str().map(String::from))
    }

    pub fn lpop(&mut self, key: &str) -> Result<Option<String>> {
        self.pop(key, true)
    }

    pub fn rpop(&mut self, key: &str) -> Result<Option<String>> {
        self.pop(key, false)
    }

    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>> {
        let list = match self.load_value(key)? {
            Some(Value::Array(a)) => a,
            Some(_) => return Err(KvError::Message("wrong type for LIST".into())),
            None => return Ok(vec![]),
        };
        let n = list.len() as i64;
        let norm = |i: i64| -> i64 {
            if i < 0 {
                n + i
            } else {
                i
            }
        };
        let s = norm(start).max(0);
        let e = if stop == -1 {
            n - 1
        } else {
            norm(stop).min(n - 1)
        };
        if s > e || s >= n {
            return Ok(vec![]);
        }
        Ok(list[s as usize..=e as usize]
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect())
    }

    pub fn llen(&mut self, key: &str) -> Result<u64> {
        match self.load_value(key)? {
            Some(Value::Array(a)) => Ok(a.len() as u64),
            Some(_) => Err(KvError::Message("wrong type for LIST".into())),
            None => Ok(0),
        }
    }

    // ---- HASH ----

    pub fn hset(&mut self, key: &str, field: &str, value: &str) -> Result<u64> {
        let mut hash = match self.load_value(key)? {
            Some(Value::Object(o)) => o,
            Some(_) => return Err(KvError::Message("wrong type for HASH".into())),
            None => Default::default(),
        };
        let added = !hash.contains_key(field);
        hash.insert(field.into(), Value::Str(value.into()));
        self.store_value(key, "hash", &Value::Object(hash))?;
        Ok(added as u64)
    }

    pub fn hget(&mut self, key: &str, field: &str) -> Result<Option<String>> {
        match self.load_value(key)? {
            Some(Value::Object(o)) => Ok(o.get(field).and_then(|v| v.as_str()).map(String::from)),
            Some(_) => Err(KvError::Message("wrong type for HASH".into())),
            None => Ok(None),
        }
    }

    pub fn hgetall(&mut self, key: &str) -> Result<Vec<(String, String)>> {
        match self.load_value(key)? {
            Some(Value::Object(o)) => Ok(o
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()),
            Some(_) => Err(KvError::Message("wrong type for HASH".into())),
            None => Ok(vec![]),
        }
    }

    // ---- SET ----

    pub fn sadd(&mut self, key: &str, members: &[&str]) -> Result<u64> {
        let mut set = self.set_members(key)?;
        let mut added = 0;
        for m in members {
            let v = Value::Str((*m).into());
            if !set.contains(&v) {
                set.push(v);
                added += 1;
            }
        }
        self.store_value(key, "set", &Value::Array(set))?;
        Ok(added)
    }

    pub fn smembers(&mut self, key: &str) -> Result<Vec<String>> {
        Ok(self
            .set_members(key)?
            .into_iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect())
    }

    pub fn sismember(&mut self, key: &str, member: &str) -> Result<bool> {
        Ok(self.set_members(key)?.contains(&Value::Str(member.into())))
    }

    fn set_members(&mut self, key: &str) -> Result<Vec<Value>> {
        match self.load_value(key)? {
            Some(Value::Array(a)) => Ok(a),
            Some(_) => Err(KvError::Message("wrong type for SET".into())),
            None => Ok(vec![]),
        }
    }

    // ---- ZSET (sorted set) ----
    // Stored as {"member": score} object; scores are floats.

    pub fn zadd(&mut self, key: &str, score: f64, member: &str) -> Result<u64> {
        let mut z = self.zmap(key)?;
        let added = !z.contains_key(member);
        z.insert(member.into(), Value::Float(score));
        self.store_value(key, "zset", &Value::Object(z))?;
        Ok(added as u64)
    }

    pub fn zscore(&mut self, key: &str, member: &str) -> Result<Option<f64>> {
        match self.zmap(key)?.get(member) {
            Some(Value::Float(f)) => Ok(Some(*f)),
            Some(Value::Int(i)) => Ok(Some(*i as f64)),
            _ => Ok(None),
        }
    }

    /// Members with scores in [min, max], ordered by (score, member).
    pub fn zrange(&mut self, key: &str, min: f64, max: f64) -> Result<Vec<(String, f64)>> {
        let z = self.zmap(key)?;
        let mut pairs: Vec<(String, f64)> = z
            .iter()
            .filter_map(|(m, v)| match v {
                Value::Float(f) => Some((m.clone(), *f)),
                Value::Int(i) => Some((m.clone(), *i as f64)),
                _ => None,
            })
            .filter(|(_, s)| *s >= min && *s <= max)
            .collect();
        pairs.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        Ok(pairs)
    }

    pub fn zrank(&mut self, key: &str, member: &str) -> Result<Option<u64>> {
        let all = self.zrange(key, f64::NEG_INFINITY, f64::INFINITY)?;
        Ok(all.iter().position(|(m, _)| m == member).map(|i| i as u64))
    }

    fn zmap(&mut self, key: &str) -> Result<Object> {
        match self.load_value(key)? {
            Some(Value::Object(o)) => Ok(o),
            Some(_) => Err(KvError::Message("wrong type for ZSET".into())),
            None => Ok(Default::default()),
        }
    }

    pub fn type_of(&mut self, key: &str) -> Result<Option<String>> {
        let now = now_ms();
        Ok(self
            .raw_row(key, now)?
            .and_then(|o| o.get("type").and_then(|v| v.as_str()).map(String::from)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SetOpts;

    #[test]
    fn list_push_pop_range() {
        let mut kv = Kv::in_memory().unwrap();
        assert_eq!(kv.rpush("L", &["a", "b", "c"]).unwrap(), 3);
        assert_eq!(kv.lpush("L", &["z"]).unwrap(), 4);
        assert_eq!(kv.lpop("L").unwrap(), Some("z".into()));
        assert_eq!(kv.rpop("L").unwrap(), Some("c".into()));
        assert_eq!(kv.llen("L").unwrap(), 2);
        assert_eq!(kv.lrange("L", 0, -1).unwrap(), vec!["a", "b"]);
        assert_eq!(kv.lrange("L", -1, -1).unwrap(), vec!["b"]);
        assert_eq!(kv.lpop("missing").unwrap(), None);
        assert_eq!(kv.llen("missing").unwrap(), 0);
    }

    #[test]
    fn type_conflict_rejected() {
        let mut kv = Kv::in_memory().unwrap();
        kv.set("plain", "str", SetOpts::default()).unwrap();
        assert!(kv.lpush("plain", &["x"]).is_err());
        assert!(kv.hset("plain", "f", "v").is_err());
    }

    #[test]
    fn hash_operations() {
        let mut kv = Kv::in_memory().unwrap();
        assert_eq!(kv.hset("H", "a", "1").unwrap(), 1);
        assert_eq!(kv.hset("H", "a", "2").unwrap(), 0); // update not add
        assert_eq!(kv.hset("H", "b", "3").unwrap(), 1);
        assert_eq!(kv.hget("H", "a").unwrap(), Some("2".into()));
        assert_eq!(kv.hget("H", "zz").unwrap(), None);
        assert_eq!(
            kv.hgetall("H").unwrap(),
            vec![("a".into(), "2".into()), ("b".into(), "3".into())]
        );
    }

    #[test]
    fn set_operations() {
        let mut kv = Kv::in_memory().unwrap();
        assert_eq!(kv.sadd("S", &["x", "y", "x"]).unwrap(), 2);
        assert!(kv.sismember("S", "x").unwrap());
        assert!(!kv.sismember("S", "z").unwrap());
        let mut members = kv.smembers("S").unwrap();
        members.sort();
        assert_eq!(members, vec!["x", "y"]);
    }

    #[test]
    fn zset_ordered_range_and_rank() {
        let mut kv = Kv::in_memory().unwrap();
        kv.zadd("Z", 3.0, "c").unwrap();
        kv.zadd("Z", 1.0, "a").unwrap();
        kv.zadd("Z", 2.0, "b").unwrap();
        kv.zadd("Z", 2.0, "b2").unwrap();
        assert_eq!(kv.zscore("Z", "b").unwrap(), Some(2.0));
        assert_eq!(kv.zscore("Z", "nope").unwrap(), None);
        let all = kv.zrange("Z", f64::NEG_INFINITY, f64::INFINITY).unwrap();
        assert_eq!(
            all.iter().map(|(m, _)| m.as_str()).collect::<Vec<_>>(),
            vec!["a", "b", "b2", "c"]
        );
        let mid = kv.zrange("Z", 2.0, 2.9).unwrap();
        assert_eq!(mid.len(), 2);
        assert_eq!(kv.zrank("Z", "a").unwrap(), Some(0));
        assert_eq!(kv.zrank("Z", "c").unwrap(), Some(3));
        assert_eq!(kv.zrank("Z", "missing").unwrap(), None);
    }

    #[test]
    fn collections_queryable_from_sql() {
        let mut kv = Kv::in_memory().unwrap();
        kv.rpush("jobs", &["a", "b"]).unwrap();
        kv.hset("cfg", "mode", "fast").unwrap();
        let r = match kv
            .db
            .execute("SELECT type FROM _kv WHERE \"key\" = 'jobs'")
            .unwrap()
        {
            docsql_core::engine::ExecOutcome::Rows(r) => r,
            o => panic!("{o:?}"),
        };
        assert_eq!(r.rows[0][0], Value::Str("list".into()));
        let r = match kv
            .db
            .execute("SELECT COUNT(type) FROM _kv WHERE type = 'hash'")
            .unwrap()
        {
            docsql_core::engine::ExecOutcome::Rows(r) => r,
            o => panic!("{o:?}"),
        };
        assert_eq!(r.rows[0][0], Value::Int(1));
    }

    #[test]
    fn type_of_reports_kind() {
        let mut kv = Kv::in_memory().unwrap();
        kv.set("s", "v", SetOpts::default()).unwrap();
        kv.rpush("l", &["x"]).unwrap();
        assert_eq!(kv.type_of("s").unwrap().as_deref(), Some("string"));
        assert_eq!(kv.type_of("l").unwrap().as_deref(), Some("list"));
        assert_eq!(kv.type_of("nope").unwrap(), None);
    }
}
