//! Document value type system.
//!
//! Every stored record is a `Value::Object` whose fields may be arbitrarily
//! nested — the storage engine never splits documents into fixed columns.

use std::collections::BTreeMap;
use std::fmt;

/// Field ordering in objects is deterministic (BTreeMap) so encodings are
/// stable across processes and replays.
pub type Object = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(Object),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Str(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Total ordering used by indexes and ORDER BY. Null < Bool < numbers
    /// (int/float compared numerically) < Str < Bytes < Array < Object.
    pub fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Int(_) | Value::Float(_) => 2,
                Value::Str(_) => 3,
                Value::Bytes(_) => 4,
                Value::Array(_) => 5,
                Value::Object(_) => 6,
            }
        }
        let (ra, rb) = (rank(a), rank(b));
        if ra != rb {
            return ra.cmp(&rb);
        }
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            (Value::Int(_), Value::Int(_))
            | (Value::Float(_), Value::Float(_))
            | (Value::Int(_), Value::Float(_))
            | (Value::Float(_), Value::Int(_)) => {
                let (x, y) = (num_as_f64(a), num_as_f64(b));
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            }
            (Value::Str(x), Value::Str(y)) => x.cmp(y),
            (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y),
            (Value::Array(x), Value::Array(y)) => {
                for (xa, ya) in x.iter().zip(y.iter()) {
                    let o = Value::cmp_values(xa, ya);
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                x.len().cmp(&y.len())
            }
            (Value::Object(x), Value::Object(y)) => {
                let mut ix = x.iter();
                let mut iy = y.iter();
                loop {
                    match (ix.next(), iy.next()) {
                        (None, None) => return Ordering::Equal,
                        (None, Some(_)) => return Ordering::Less,
                        (Some(_), None) => return Ordering::Greater,
                        (Some((ka, va)), Some((kb, vb))) => {
                            let o = ka.cmp(kb);
                            if o != Ordering::Equal {
                                return o;
                            }
                            let o = Value::cmp_values(va, vb);
                            if o != Ordering::Equal {
                                return o;
                            }
                        }
                    }
                }
            }
            // Ranks are equal, so both sides are the same variant.
            _ => unreachable!("equal rank implies same variant"),
        }
    }
}

fn num_as_f64(v: &Value) -> f64 {
    match v {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => 0.0,
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Str(s) => write!(f, "{s}"),
            Value::Bytes(b) => write!(
                f,
                "x'{}'",
                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
            ),
            Value::Array(a) => {
                write!(f, "[")?;
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Value::Object(o) => {
                write!(f, "{{")?;
                for (i, (k, v)) in o.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{k}:{v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_total_and_type_stable() {
        use std::cmp::Ordering::*;
        assert_eq!(Value::cmp_values(&Value::Null, &Value::Bool(false)), Less);
        assert_eq!(Value::cmp_values(&Value::Bool(true), &Value::Int(9),), Less);
        assert_eq!(
            Value::cmp_values(&Value::Int(3), &Value::Float(2.5)),
            Greater
        );
        assert_eq!(Value::cmp_values(&Value::Int(3), &Value::Float(3.0)), Equal);
        assert_eq!(
            Value::cmp_values(&Value::Str("a".into()), &Value::Str("b".into())),
            Less
        );
        assert_eq!(
            Value::cmp_values(
                &Value::Array(vec![Value::Int(1), Value::Int(2)]),
                &Value::Array(vec![Value::Int(1)]),
            ),
            Greater
        );
        // Same value compares equal (reflexivity, required by B-tree keys).
        let v = Value::Object(Object::from([
            ("a".into(), Value::Int(1)),
            ("b".into(), Value::Array(vec![Value::Str("x".into())])),
        ]));
        assert_eq!(Value::cmp_values(&v, &v.clone()), Equal);
    }
}
