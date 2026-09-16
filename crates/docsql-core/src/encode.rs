//! Compact binary encoding for document values (BSON-like).
//!
//! Layout (all integers little-endian):
//! ```text
//! value := tag payload
//! tag   := 0 null | 1 bool | 2 int | 3 float | 4 str | 5 bytes
//!       | 6 array | 7 object | 8 decimal
//! str   := len:u32 bytes:utf8
//! bytes := len:u32 bytes
//! array := len:u32 value*
//! object:= len:u32 (str_key value)*
//! decimal := 16 bytes (rust_decimal serialized form)
//! ```
//! Trailing garbage after one value is rejected by `decode` to catch
//! corruption early; `decode_prefix` allows framed streams.

use crate::value::{Decimal, Object, Value};

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

    /// Advance past one encoded value without materializing it.
    fn skip_value(&mut self) -> Result<(), EncodeError> {
        self.skip_at(0)
    }

    fn skip_at(&mut self, depth: usize) -> Result<(), EncodeError> {
        if depth > MAX_DEPTH {
            return Err(EncodeError::TooDeep);
        }
        match self.u8()? {
            0 => Ok(()),
            1 => {
                self.u8()?;
                Ok(())
            }
            2 | 3 | 9 => {
                self.take(8)?;
                Ok(())
            }
            8 => {
                self.take(16)?;
                Ok(())
            }
            4 | 5 => {
                let len = self.u32()? as usize;
                self.take(len)?;
                Ok(())
            }
            6 => {
                let len = self.u32()? as usize;
                for _ in 0..len {
                    self.skip_at(depth + 1)?;
                }
                Ok(())
            }
            7 => {
                let len = self.u32()? as usize;
                for _ in 0..len {
                    let klen = self.u32()? as usize;
                    self.take(klen)?;
                    self.skip_at(depth + 1)?;
                }
                Ok(())
            }
            t => Err(EncodeError::UnknownTag(t)),
        }
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
            8 => {
                let b = self.take(16)?;
                let mut raw = [0u8; 16];
                raw.copy_from_slice(b);
                Value::Decimal(Decimal::deserialize(raw))
            }
            9 => {
                let b = self.take(8)?;
                let mut raw = [0u8; 8];
                raw.copy_from_slice(b);
                Value::Timestamp(i64::from_le_bytes(raw))
            }
            t => return Err(EncodeError::UnknownTag(t)),
        })
    }
}

fn put_u32(out: &mut Vec<u8>, v: usize) -> Result<(), EncodeError> {
    let v = u32::try_from(v).map_err(|_| EncodeError::TooManyItems)?;
    out.extend_from_slice(&v.to_le_bytes());
    Ok(())
}

pub fn encode(value: &Value, out: &mut Vec<u8>) -> Result<(), EncodeError> {
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
        Value::Decimal(d) => {
            out.push(8);
            out.extend_from_slice(&d.serialize());
        }
        Value::Timestamp(ms) => {
            out.push(9);
            out.extend_from_slice(&ms.to_le_bytes());
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

/// Encode an object document directly — byte-identical to encoding
/// `Value::Object(doc)` — without cloning the document into a `Value`
/// first (`Heap::insert` used to deep-copy every field of every row here).
pub fn encode_object(doc: &Object, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    out.push(7);
    put_u32(out, doc.len())?;
    for (k, v) in doc {
        put_u32(out, k.len())?;
        out.extend_from_slice(k.as_bytes());
        encode(v, out)?;
    }
    Ok(())
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

/// One top-level object field, without materializing the rest of the
/// document: entries are walked in order, every other value is skipped by
/// its encoded size. `None` when the root is not an object or the field is
/// absent (the caller maps that to SQL NULL, like `eval_expr` does).
/// Used by the unindexed ORDER BY window to sort on raw heap bytes.
pub fn extract_field(buf: &[u8], field: &str) -> Result<Option<Value>, EncodeError> {
    let mut d = Decoder::new(buf);
    if d.u8()? != 7 {
        return Ok(None);
    }
    let len = d.u32()? as usize;
    for _ in 0..len {
        let key = d.string()?;
        if key == field {
            return Ok(Some(d.value()?));
        }
        d.skip_value()?;
    }
    Ok(None)
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
        roundtrip(Value::Decimal(
            "12345678901234567890.123456".parse().unwrap(),
        ));
        roundtrip(Value::Decimal(Decimal::new(-1, 28)));
        roundtrip(Value::Timestamp(0));
        roundtrip(Value::Timestamp(-1));
        roundtrip(Value::Timestamp(1_789_430_400_123));
        roundtrip(Value::Timestamp(253_402_300_799_999));
        roundtrip(Value::Str("hello 世界 🎉".into()));
        roundtrip(Value::Bytes(vec![0, 1, 255, 128]));
    }

    #[test]
    fn decimal_scale_is_part_of_the_encoding() {
        // Same numeric value, different scale: distinct byte keys (DISTINCT/
        // GROUP BY semantics documented in value.rs), text preserved.
        let a = encode_to_vec(&Value::Decimal("1.5".parse().unwrap())).unwrap();
        let b = encode_to_vec(&Value::Decimal("1.50".parse().unwrap())).unwrap();
        assert_ne!(a, b);
        match decode(&b).unwrap() {
            Value::Decimal(d) => assert_eq!(d.to_string(), "1.50"),
            other => panic!("expected decimal, got {other:?}"),
        }
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
    fn extract_field_matches_decode_without_materializing() {
        let obj = Object::from([
            ("a".into(), Value::Int(1)),
            ("name".into(), Value::Str("bob".into())),
            (
                "nested".into(),
                Value::Object(Object::from([("name".into(), Value::Int(9))])),
            ),
            (
                "arr".into(),
                Value::Array(vec![Value::Null, Value::Str("x".into())]),
            ),
            ("dec".into(), Value::Decimal("1.50".parse().unwrap())),
            ("bin".into(), Value::Bytes(vec![0, 255])),
        ]);
        let enc = encode_to_vec(&Value::Object(obj.clone())).unwrap();
        assert_eq!(
            extract_field(&enc, "name").unwrap(),
            Some(Value::Str("bob".into()))
        );
        assert_eq!(extract_field(&enc, "a").unwrap(), Some(Value::Int(1)));
        assert_eq!(extract_field(&enc, "dec").unwrap(), obj.get("dec").cloned());
        assert_eq!(
            extract_field(&enc, "bin").unwrap(),
            Some(Value::Bytes(vec![0, 255]))
        );
        assert_eq!(
            extract_field(&enc, "nested").unwrap(),
            obj.get("nested").cloned()
        );
        assert_eq!(extract_field(&enc, "missing").unwrap(), None);
        // Non-object roots have no fields.
        assert_eq!(
            extract_field(&encode_to_vec(&Value::Array(vec![])).unwrap(), "x").unwrap(),
            None
        );
    }

    #[test]
    fn extract_field_truncated_never_panics() {
        let doc = Value::Object(Object::from([
            (
                "a".into(),
                Value::Array(vec![Value::Int(1), Value::Object(Object::new())]),
            ),
            ("b".into(), Value::Str("tail".into())),
        ]));
        let enc = encode_to_vec(&doc).unwrap();
        for cut in 0..enc.len() {
            let _ = extract_field(&enc[..cut], "b");
            let _ = extract_field(&enc[..cut], "a");
        }
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
