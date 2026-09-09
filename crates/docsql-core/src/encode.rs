//! Compact binary encoding for document values (BSON-like).
//!
//! Layout (all integers little-endian):
//! ```text
//! value := tag payload
//! tag   := 0 null | 1 bool | 2 int | 3 float | 4 str | 5 bytes
//!       | 6 array | 7 object
//! str   := len:u32 bytes:utf8
//! bytes := len:u32 bytes
//! array := len:u32 value*
//! object:= len:u32 (str_key value)*
//! ```
//! Trailing garbage after one value is rejected by `decode` to catch
//! corruption early; `decode_prefix` allows framed streams.

use crate::value::{Object, Value};

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("string exceeds 4 GiB limit")]
    StringTooLarge,
    #[error("collection exceeds 4 GiB limit")]
    TooManyItems,
    #[error("unexpected end of input")]
    Eof,
    #[error("unknown value tag {0}")]
    UnknownTag(u8),
    #[error("invalid bool byte {0:#x}")]
    BadBool(u8),
    #[error("nesting exceeds {MAX_DEPTH} levels")]
    TooDeep,
}

/// Matches json.rs — the decoder runs on network payloads, so recursion
/// must be bounded to keep hostile nesting from overflowing the stack.
const MAX_DEPTH: usize = 128;

#[derive(Debug)]
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Decoder { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], EncodeError> {
        if self.remaining() < n {
            return Err(EncodeError::Eof);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, EncodeError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, EncodeError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i64v(&mut self) -> Result<i64, EncodeError> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn f64v(&mut self) -> Result<f64, EncodeError> {
        let b = self.take(8)?;
        Ok(f64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, EncodeError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String, EncodeError> {
        let raw = self.bytes()?;
        // Invalid UTF-8 is corruption; replace would silently alter data.
        String::from_utf8(raw).map_err(|_| EncodeError::Eof)
    }

    fn value(&mut self) -> Result<Value, EncodeError> {
        self.value_at(0)
    }

    fn value_at(&mut self, depth: usize) -> Result<Value, EncodeError> {
        if depth > MAX_DEPTH {
            return Err(EncodeError::TooDeep);
        }
        Ok(match self.u8()? {
            0 => Value::Null,
            1 => Value::Bool(match self.u8()? {
                0 => false,
                1 => true,
                b => return Err(EncodeError::BadBool(b)),
            }),
            2 => Value::Int(self.i64v()?),
            3 => Value::Float(self.f64v()?),
            4 => Value::Str(self.string()?),
            5 => Value::Bytes(self.bytes()?),
            6 => {
                let len = self.u32()? as usize;
                let mut items = Vec::with_capacity(len.min(1024));
                for _ in 0..len {
                    items.push(self.value_at(depth + 1)?);
                }
                Value::Array(items)
            }
            7 => {
                let len = self.u32()? as usize;
                let mut obj = Object::new();
                for _ in 0..len {
                    let k = self.string()?;
                    let v = self.value_at(depth + 1)?;
                    obj.insert(k, v);
                }
                Value::Object(obj)
            }
            t => return Err(EncodeError::UnknownTag(t)),
        })
    }
}

pub fn encode(value: &Value, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    fn put_u32(out: &mut Vec<u8>, v: usize) -> Result<(), EncodeError> {
        let v = u32::try_from(v).map_err(|_| EncodeError::TooManyItems)?;
        out.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    match value {
        Value::Null => out.push(0),
        Value::Bool(b) => {
            out.push(1);
            out.push(*b as u8);
        }
        Value::Int(i) => {
            out.push(2);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            out.push(3);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::Str(s) => {
            out.push(4);
            put_u32(out, s.len())?;
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            out.push(5);
            put_u32(out, b.len())?;
            out.extend_from_slice(b);
        }
        Value::Array(items) => {
            out.push(6);
            put_u32(out, items.len())?;
            for v in items {
                encode(v, out)?;
            }
        }
        Value::Object(obj) => {
            out.push(7);
            put_u32(out, obj.len())?;
            for (k, v) in obj {
                put_u32(out, k.len())?;
                out.extend_from_slice(k.as_bytes());
                encode(v, out)?;
            }
        }
    }
    Ok(())
}

pub fn encode_to_vec(value: &Value) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::new();
    encode(value, &mut out)?;
    Ok(out)
}

/// Decode exactly one value; trailing bytes are an error.
pub fn decode(buf: &[u8]) -> Result<Value, EncodeError> {
    let mut d = Decoder::new(buf);
    let v = d.value()?;
    if d.remaining() != 0 {
        return Err(EncodeError::UnknownTag(0xff));
    }
    Ok(v)
}

/// Decode one value and return it with the number of bytes consumed.
pub fn decode_prefix(buf: &[u8]) -> Result<(Value, usize), EncodeError> {
    let mut d = Decoder::new(buf);
    let v = d.value()?;
    Ok((v, d.pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: Value) {
        let enc = encode_to_vec(&v).unwrap();
        let dec = decode(&enc).unwrap();
        assert_eq!(dec, v, "roundtrip mismatch for {v:?}");
    }

    #[test]
    fn roundtrip_scalars() {
        roundtrip(Value::Null);
        roundtrip(Value::Bool(true));
        roundtrip(Value::Int(i64::MIN));
        roundtrip(Value::Int(i64::MAX));
        roundtrip(Value::Float(3.25));
        roundtrip(Value::Float(-0.0));
        roundtrip(Value::Str("hello 世界 🎉".into()));
        roundtrip(Value::Bytes(vec![0, 1, 255, 128]));
    }

    #[test]
    fn roundtrip_nested() {
        let doc = Value::Object(Object::from([
            ("_id".into(), Value::Int(1)),
            (
                "profile".into(),
                Value::Object(Object::from([
                    ("name".into(), Value::Str("alice".into())),
                    (
                        "tags".into(),
                        Value::Array(vec![Value::Str("a".into()), Value::Null]),
                    ),
                    ("score".into(), Value::Float(9.5)),
                ])),
            ),
            ("empty_obj".into(), Value::Object(Object::new())),
            ("empty_arr".into(), Value::Array(vec![])),
        ]));
        roundtrip(doc);
    }

    #[test]
    fn rejects_truncated_input() {
        let enc = encode_to_vec(&Value::Str("hello".into())).unwrap();
        for cut in 0..enc.len() {
            assert!(decode(&enc[..cut]).is_err(), "should fail at cut={cut}");
        }
    }

    #[test]
    fn rejects_trailing_garbage_and_bad_tags() {
        let mut enc = encode_to_vec(&Value::Int(1)).unwrap();
        enc.push(9);
        assert!(decode(&enc).is_err());

        assert!(decode(&[0xaa]).is_err());
        // bool payload must be 0 or 1
        assert!(decode(&[1, 7]).is_err());
        // invalid utf-8 in string
        assert!(decode(&[4, 2, 0, 0, 0, 0xff, 0xfe]).is_err());
    }

    #[test]
    fn decode_prefix_reports_consumed_len() {
        let mut buf = encode_to_vec(&Value::Str("abcd".into())).unwrap();
        buf.extend_from_slice(&[1, 2, 3]);
        let (v, n) = decode_prefix(&buf).unwrap();
        assert_eq!(v, Value::Str("abcd".into()));
        assert_eq!(n, buf.len() - 3);
    }

    #[test]
    fn deep_nesting_rejected_instead_of_stack_overflow() {
        // Each array level costs 7 bytes; a few hundred KB of nested arrays
        // used to recurse unboundedly and abort the process.
        let mut buf = Vec::new();
        for _ in 0..10_000 {
            buf.extend_from_slice(&[6, 1, 0, 0, 0]);
        }
        buf.push(0);
        let err = decode(&buf).unwrap_err();
        assert!(matches!(err, EncodeError::TooDeep), "{err:?}");
    }

    /// Cheap fuzz-ish sweep: truncated encodings of nested docs must never
    /// panic, only error. Seed space is small but exercises the decoder paths.
    #[test]
    fn truncated_never_panics() {
        let doc = Value::Object(Object::from([
            (
                "a".into(),
                Value::Array(vec![Value::Int(1), Value::Float(2.0)]),
            ),
            ("b".into(), Value::Bytes(vec![9; 20])),
        ]));
        let enc = encode_to_vec(&doc).unwrap();
        for cut in 0..enc.len() {
            let _ = decode(&enc[..cut]);
        }
    }
}
