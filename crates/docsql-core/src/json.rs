//! JSON serialization for document values.
//!
//! Objects serialize with their (BTreeMap) insertion-sorted key order, so
//! output is deterministic. Numbers: integers stay integers; floats print
//! via Rust's shortest round-trip format.

use crate::value::{Object, Value};
use std::fmt::Write as _;

#[derive(Debug, thiserror::Error)]
pub enum JsonError {
    #[error("unexpected end of input")]
    Eof,
    #[error("unexpected character {0:?} at position {1}")]
    Unexpected(char, usize),
    #[error("invalid number at position {0}")]
    BadNumber(usize),
    #[error("invalid escape at position {0}")]
    BadEscape(usize),
    #[error("trailing input at position {0}")]
    Trailing(usize),
    #[error("nesting too deep (max {0})")]
    Depth(usize),
}

pub type Result<T> = std::result::Result<T, JsonError>;

const MAX_DEPTH: usize = 128;

pub fn to_string(v: &Value) -> String {
    let mut s = String::new();
    write_value(&mut s, v);
    s
}

/// Stable text for a non-finite float inside the `$float` marker.
pub(crate) fn float_marker_text(f: f64) -> &'static str {
    if f.is_nan() {
        "NaN"
    } else if f > 0.0 {
        "inf"
    } else {
        "-inf"
    }
}

/// Parse a `$float` marker payload back into a non-finite float.
pub(crate) fn parse_float_marker(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "inf" | "+inf" | "Infinity" => Some(f64::INFINITY),
        "-inf" | "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => {
            let _ = write!(out, "{i}");
        }
        Value::Float(f) => {
            if f.is_finite() {
                let _ = write!(out, "{f}");
            } else {
                // JSON has no NaN/Inf: a marker object keeps the exact value
                // losslessly (mirrors `$dec`/`$bytes`; `from_str` decodes it).
                // Rendering `null` used to mutate stored data on every wire
                // round-trip and made backups of such rows unreplayable.
                out.push_str("{\"$float\":\"");
                out.push_str(float_marker_text(*f));
                out.push_str("\"}");
            }
        }
        Value::Decimal(d) => {
            // JSON numbers are IEEE doubles on the consuming side; a marker
            // object keeps the exact decimal text (like `$bytes` below).
            out.push_str("{\"$dec\":\"");
            let _ = write!(out, "{d}");
            out.push_str("\"}");
        }
        Value::Timestamp(ms) => {
            // A bare JSON number would decay into a double on JS consumers
            // (> 2^53 is not the issue here — the issue is the TYPE: a wall
            // clock must survive as a timestamp, not as a generic number).
            // {"$ts": <millis>} round-trips exactly, mirroring $dec.
            out.push_str("{\"$ts\":");
            let _ = write!(out, "{ms}");
            out.push('}');
        }
        Value::Str(s) => write_json_string(out, s),
        Value::Bytes(b) => {
            // Encode as {"$bytes": [ints]} — non-standard but lossless.
            out.push_str("{\"$bytes\":[");
            for (i, x) in b.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let _ = write!(out, "{x}");
            }
            out.push_str("]}");
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(obj) => {
            out.push('{');
            for (i, (k, val)) in obj.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(out, k);
                out.push(':');
                write_value(out, val);
            }
            out.push('}');
        }
    }
}

/// Append the JSON-escaped body of `s` (no surrounding quotes) to `out`.
/// The clean prefix up to the first escapable char goes in as one slice;
/// escape_str used to build a fresh `String` per value, which put an
/// allocation plus full copy on every string cell of every response row.
fn push_escaped(out: &mut String, s: &str) {
    let Some(first) = s.find(|c| matches!(c, '"' | '\\' | '\n' | '\r' | '\t') || (c as u32) < 0x20)
    else {
        out.push_str(s);
        return;
    };
    out.push_str(&s[..first]);
    for c in s[first..].chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Escape `s` for embedding inside a JSON string literal — the escaped
/// body only, no surrounding quotes (for callers assembling JSON by hand).
pub fn escape_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    push_escaped(&mut out, s);
    out
}

fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    push_escaped(out, s);
    out.push('"');
}

/// Wire markers decode back to their exact scalar types: `{"$dec":"..."}` is
/// the lossless DECIMAL carrier, `{"$bytes":[...]}` the BLOB carrier and
/// `{"$ts":<millis>}` the TIMESTAMP carrier. Shapes that do not qualify stay
/// plain objects.
fn decode_marker(obj: Object) -> Value {
    if obj.len() == 1 {
        if let Some(Value::Str(s)) = obj.get("$dec") {
            if let Ok(d) = s.parse::<crate::value::Decimal>() {
                return Value::Decimal(d);
            }
        }
        if let Some(Value::Str(s)) = obj.get("$float") {
            if let Some(f) = parse_float_marker(s) {
                return Value::Float(f);
            }
        }
        if let Some(Value::Int(ms)) = obj.get("$ts") {
            // Out-of-domain instants stay a plain object instead of becoming
            // Timestamps the canonical text form (and value_literal replay)
            // cannot represent.
            if crate::value::is_valid_timestamp_ms(*ms) {
                return Value::Timestamp(*ms);
            }
        }
        if let Some(Value::Array(items)) = obj.get("$bytes") {
            let mut bytes = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    Value::Int(i) if (0..=255).contains(i) => bytes.push(*i as u8),
                    _ => return Value::Object(obj),
                }
            }
            return Value::Bytes(bytes);
        }
    }
    Value::Object(obj)
}

pub fn from_str(s: &str) -> Result<Value> {
    let mut p = Parser {
        buf: s.as_bytes(),
        pos: 0,
    };
    p.skip_ws();
    let v = p.value(0)?;
    p.skip_ws();
    if p.pos != p.buf.len() {
        return Err(JsonError::Trailing(p.pos));
    }
    Ok(v)
}

struct Parser<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, b: u8) -> Result<()> {
        if self.bump() == Some(b) {
            Ok(())
        } else {
            Err(JsonError::Unexpected(b as char, self.pos))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth > MAX_DEPTH {
            return Err(JsonError::Depth(MAX_DEPTH));
        }
        self.skip_ws();
        match self.peek() {
            Some(b'n') => self.lit("null", Value::Null),
            Some(b't') => self.lit("true", Value::Bool(true)),
            Some(b'f') => self.lit("false", Value::Bool(false)),
            Some(b'"') => Ok(Value::Str(self.string()?)),
            Some(b'[') => {
                self.bump();
                let mut items = Vec::new();
                self.skip_ws();
                if self.peek() == Some(b']') {
                    self.bump();
                    return Ok(Value::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    self.skip_ws();
                    match self.bump() {
                        Some(b',') => continue,
                        Some(b']') => break,
                        _ => return Err(JsonError::Unexpected(',', self.pos)),
                    }
                }
                Ok(Value::Array(items))
            }
            Some(b'{') => {
                self.bump();
                let mut obj = Object::new();
                self.skip_ws();
                if self.peek() == Some(b'}') {
                    self.bump();
                    return Ok(Value::Object(obj));
                }
                loop {
                    self.skip_ws();
                    let k = self.string()?;
                    self.skip_ws();
                    self.expect(b':')?;
                    let v = self.value(depth + 1)?;
                    obj.insert(k, v);
                    self.skip_ws();
                    match self.bump() {
                        Some(b',') => continue,
                        Some(b'}') => break,
                        _ => return Err(JsonError::Unexpected('}', self.pos)),
                    }
                }
                Ok(decode_marker(obj))
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            other => Err(JsonError::Unexpected(
                other.map(|b| b as char).unwrap_or('?'),
                self.pos,
            )),
        }
    }

    fn lit(&mut self, word: &str, v: Value) -> Result<Value> {
        if self.buf[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(v)
        } else {
            Err(JsonError::Unexpected(
                self.peek().map(|b| b as char).unwrap_or('?'),
                self.pos,
            ))
        }
    }

    fn number(&mut self) -> Result<Value> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.bump();
        }
        let mut is_float = false;
        while let Some(b) = self.peek() {
            match b {
                b'0'..=b'9' => {
                    self.bump();
                }
                b'.' | b'e' | b'E' | b'+' | b'-' => {
                    is_float = true;
                    self.bump();
                }
                _ => break,
            }
        }
        let text = std::str::from_utf8(&self.buf[start..self.pos])
            .map_err(|_| JsonError::BadNumber(start))?;
        if text.is_empty() || text == "-" {
            return Err(JsonError::BadNumber(start));
        }
        if !is_float {
            if let Ok(i) = text.parse::<i64>() {
                return Ok(Value::Int(i));
            }
        }
        text.parse::<f64>()
            .map(Value::Float)
            .map_err(|_| JsonError::BadNumber(start))
    }

    fn string(&mut self) -> Result<String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            match self.bump() {
                None => return Err(JsonError::Eof),
                Some(b'"') => return Ok(out),
                Some(b'\\') => match self.bump() {
                    Some(b'"') => out.push('"'),
                    Some(b'\\') => out.push('\\'),
                    Some(b'/') => out.push('/'),
                    Some(b'n') => out.push('\n'),
                    Some(b'r') => out.push('\r'),
                    Some(b't') => out.push('\t'),
                    Some(b'b') => out.push('\u{8}'),
                    Some(b'f') => out.push('\u{c}'),
                    Some(b'u') => {
                        let cp = self.hex4()?;
                        // Surrogate pair handling.
                        let ch = if (0xD800..0xDC00).contains(&cp) {
                            if self.bump() == Some(b'\\') && self.bump() == Some(b'u') {
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    // Not a low surrogate: the pair arithmetic
                                    // would underflow — substitute instead.
                                    '\u{FFFD}'
                                } else {
                                    let combined = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                    char::from_u32(combined).unwrap_or('\u{FFFD}')
                                }
                            } else {
                                '\u{FFFD}'
                            }
                        } else if (0xDC00..0xE000).contains(&cp) {
                            // Lone low surrogate is not a scalar value.
                            '\u{FFFD}'
                        } else {
                            char::from_u32(cp).unwrap_or('\u{FFFD}')
                        };
                        out.push(ch);
                    }
                    _ => return Err(JsonError::BadEscape(self.pos)),
                },
                Some(b) if b < 0x80 => out.push(b as char),
                Some(b) => {
                    // Multi-byte UTF-8: collect continuation bytes.
                    let len = if b >= 0xF0 {
                        4
                    } else if b >= 0xE0 {
                        3
                    } else {
                        2
                    };
                    let start = self.pos - 1;
                    for _ in 1..len {
                        self.bump();
                    }
                    let s = std::str::from_utf8(&self.buf[start..self.pos])
                        .map_err(|_| JsonError::Unexpected('?', self.pos))?;
                    out.push_str(s);
                }
            }
        }
    }

    fn hex4(&mut self) -> Result<u32> {
        let mut v = 0u32;
        for _ in 0..4 {
            let b = self.bump().ok_or(JsonError::Eof)?;
            let d = (b as char)
                .to_digit(16)
                .ok_or(JsonError::BadEscape(self.pos))?;
            v = v * 16 + d;
        }
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_scalars() {
        for v in [
            Value::Null,
            Value::Bool(true),
            Value::Int(-42),
            Value::Float(3.5),
            Value::Decimal("3.14159265358979323846".parse().unwrap()),
            Value::Str("hello 世界 🎉 \"quoted\" \\".into()),
        ] {
            let s = to_string(&v);
            assert_eq!(from_str(&s).unwrap(), v, "via {s}");
        }
    }

    #[test]
    fn roundtrip_nested() {
        let doc = Value::Object(Object::from([
            (
                "a".into(),
                Value::Array(vec![Value::Int(1), Value::Null, Value::Bool(false)]),
            ),
            (
                "b".into(),
                Value::Object(Object::from([("c".into(), Value::Str("x".into()))])),
            ),
        ]));
        let s = to_string(&doc);
        assert_eq!(from_str(&s).unwrap(), doc);
        assert_eq!(s, r#"{"a":[1,null,false],"b":{"c":"x"}}"#);
    }

    #[test]
    fn escapes_and_unicode() {
        assert_eq!(
            to_string(&Value::Str("a\nb\tc\"d".into())),
            r#""a\nb\tc\"d""#
        );
        assert_eq!(from_str(r#""😀""#).unwrap(), Value::Str("😀".into()));
        assert_eq!(from_str(r#""A""#).unwrap(), Value::Str("A".into()));
    }

    #[test]
    fn integers_preferred_over_floats() {
        assert_eq!(from_str("42").unwrap(), Value::Int(42));
        assert_eq!(from_str("-7").unwrap(), Value::Int(-7));
        assert!(matches!(from_str("4.5"), Ok(Value::Float(_))));
        assert!(matches!(from_str("1e3"), Ok(Value::Float(_))));
    }

    #[test]
    fn nan_inf_round_trip_via_float_marker() {
        // JSON has no NaN/Inf, but silently writing `null` mutated stored
        // data on every response and broke dump round-trips. The `$float`
        // marker keeps them losslessly (symmetric with `$dec`/`$bytes`).
        assert_eq!(to_string(&Value::Float(f64::NAN)), r#"{"$float":"NaN"}"#);
        assert_eq!(
            to_string(&Value::Float(f64::INFINITY)),
            r#"{"$float":"inf"}"#
        );
        assert_eq!(
            to_string(&Value::Float(f64::NEG_INFINITY)),
            r#"{"$float":"-inf"}"#
        );
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            match from_str(&to_string(&Value::Float(v))).unwrap() {
                Value::Float(f) => {
                    if v.is_nan() {
                        assert!(f.is_nan());
                    } else {
                        assert_eq!(f, v);
                    }
                }
                other => panic!("expected float, got {other:?}"),
            }
        }
        // Nested values keep the marker through arrays/objects.
        let arr = Value::Array(vec![Value::Float(f64::INFINITY), Value::Int(1)]);
        assert_eq!(from_str(&to_string(&arr)).unwrap(), arr);
        // A user object that merely looks like a marker for an unknown type
        // stays an object.
        assert_eq!(
            from_str(r#"{"$float":"bogus"}"#).unwrap(),
            Value::Object(Object::from([(
                "$float".into(),
                Value::Str("bogus".into())
            )]))
        );
    }

    #[test]
    fn errors_on_garbage() {
        assert!(from_str("").is_err());
        assert!(from_str("{").is_err());
        assert!(from_str("[1,]").is_err());
        assert!(from_str("tru").is_err());
        assert!(from_str("1 2").is_err());
        assert!(from_str(r#""\q""#).is_err());
    }

    #[test]
    fn deep_nesting_bounded() {
        let s = "[".repeat(200) + &"]".repeat(200);
        assert!(matches!(from_str(&s), Err(JsonError::Depth(_))));
    }

    #[test]
    fn truncated_never_panics() {
        let s = to_string(&Value::Object(Object::from([(
            "k".into(),
            Value::Array(vec![Value::Float(1.5), Value::Str("v".into())]),
        )])));
        for cut in 0..s.len() {
            let _ = from_str(&s[..cut]);
        }
    }

    #[test]
    fn bytes_serialize_lossless() {
        let v = Value::Bytes(vec![0, 1, 255]);
        let s = to_string(&v);
        assert_eq!(s, r#"{"$bytes":[0,1,255]}"#);
        // The marker round-trips back to the exact byte string.
        assert_eq!(from_str(&s).unwrap(), v);
        assert_eq!(to_string(&Value::Bytes(vec![])), r#"{"$bytes":[]}"#);
        // Out-of-range elements fall back to a plain object.
        assert!(matches!(
            from_str(r#"{"$bytes":[256]}"#),
            Ok(Value::Object(_))
        ));
    }

    #[test]
    fn decimals_serialize_as_exact_marker() {
        let v = Value::Decimal("0.10000000000000000555".parse().unwrap());
        assert_eq!(to_string(&v), r#"{"$dec":"0.10000000000000000555"}"#);
        assert_eq!(from_str(&to_string(&v)).unwrap(), v);
        let v = Value::Decimal("12345678901234567890.12".parse().unwrap());
        assert_eq!(to_string(&v), r#"{"$dec":"12345678901234567890.12"}"#);
        assert_eq!(from_str(&to_string(&v)).unwrap(), v);
        // Non-decimal text stays an object.
        assert!(matches!(
            from_str(r#"{"$dec":"nope"}"#),
            Ok(Value::Object(_))
        ));
    }

    #[test]
    fn control_characters_escaped() {
        assert_eq!(
            to_string(&Value::Str("\u{1}\u{1f}".into())),
            "\"\\u0001\\u001f\""
        );
        assert_eq!(from_str(r#""\u0001""#).unwrap(), Value::Str("\u{1}".into()));
        assert_eq!(to_string(&Value::Str("\r".into())), r#""\r""#);
    }

    #[test]
    fn empty_containers_and_whitespace() {
        assert_eq!(from_str(" [] ").unwrap(), Value::Array(vec![]));
        assert_eq!(from_str(" { } ").unwrap(), Value::Object(Object::new()));
        assert_eq!(
            from_str("\t[1]\n").unwrap(),
            Value::Array(vec![Value::Int(1)])
        );
        assert_eq!(to_string(&Value::Array(vec![])), "[]");
        assert_eq!(to_string(&Value::Object(Object::new())), "{}");
    }

    #[test]
    fn bad_numbers_rejected() {
        assert!(matches!(from_str("-"), Err(JsonError::BadNumber(_))));
        assert!(from_str("1.2.3").is_err());
        // huge integer overflows i64 → falls back to float
        assert!(matches!(
            from_str("99999999999999999999999"),
            Ok(Value::Float(_))
        ));
        assert!(matches!(from_str("-1e-5"), Ok(Value::Float(_))));
    }

    #[test]
    fn string_escapes_full_set() {
        assert_eq!(
            from_str(r#""\" \\ \/ \b \f \n \r \t""#).unwrap(),
            Value::Str("\" \\ / \u{8} \u{c} \n \r \t".into())
        );
        // bad \u hex digits
        assert!(matches!(
            from_str(r#""\uZZZZ""#),
            Err(JsonError::BadEscape(_))
        ));
        // \u with fewer than 4 digits / eof
        assert!(from_str(r#""\u12""#).is_err());
        assert!(from_str(r#""\u""#).is_err());
        // lone high surrogate without a following pair degrades to U+FFFD
        // (the closing quote is consumed by the failed pair lookahead, so a
        // trailing char keeps the parse alive)
        assert_eq!(
            from_str(r#""\ud83d x""#).unwrap(),
            Value::Str("\u{FFFD}x".into())
        );
        // proper surrogate pair decodes to the emoji
        assert_eq!(
            from_str("\"\\ud83d\\ude00\"").unwrap(),
            Value::Str("😀".into())
        );
        // invalid code point degrades to U+FFFD
        assert_eq!(
            from_str(r#""\udfff""#).unwrap(),
            Value::Str("\u{FFFD}".into())
        );
    }

    #[test]
    fn structural_errors_are_precise() {
        assert!(matches!(
            from_str("{1:2}"),
            Err(JsonError::Unexpected(_, _))
        ));
        assert!(matches!(
            from_str("[1 2]"),
            Err(JsonError::Unexpected(_, _))
        ));
        assert!(matches!(
            from_str(r#"{"a" 1}"#),
            Err(JsonError::Unexpected(_, _))
        ));
        assert!(matches!(from_str(""), Err(JsonError::Unexpected(_, _))));
        assert!(matches!(from_str("@"), Err(JsonError::Unexpected('@', _))));
        assert!(matches!(from_str("nulls"), Err(JsonError::Trailing(_))));
        assert!(matches!(from_str("truex"), Err(JsonError::Trailing(_))));
        assert!(matches!(from_str("[1]]"), Err(JsonError::Trailing(_))));
        assert!(matches!(from_str(r#""abc"#), Err(JsonError::Eof)));
        assert!(matches!(from_str(r#""auncfé""#), Ok(Value::Str(_))));
    }
}
