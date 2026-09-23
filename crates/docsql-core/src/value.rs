//! Document value type system.
//!
//! Every stored record is a `Value::Object` whose fields may be arbitrarily
//! nested — the storage engine never splits documents into fixed columns.

use std::collections::BTreeMap;
use std::fmt;

pub use rust_decimal::Decimal;

/// Field ordering in objects is deterministic (BTreeMap) so encodings are
/// stable across processes and replays.
pub type Object = BTreeMap<String, Value>;

/// Days since epoch ← days-from-civil (Howard Hinnant). Inverse of the
/// engine's `civil_from_days`; both are pinned by known-answer tests.
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// Format UTC milliseconds as fixed-width RFC 3339 text
/// (`YYYY-MM-DDTHH:MM:SS.mmmZ`). Fixed width + zero padding + UTC make the
/// text form lexicographically ordered exactly like the numeric form.
pub fn format_timestamp_ms(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // civil_from_days lives in the engine (shared with SYSDATE/backup
    // stamping); re-derive the calendar here to keep value.rs leaf-level.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    let h = rem / 3600;
    let mi = (rem % 3600) / 60;
    let se = rem % 60;
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{se:02}.{millis:03}Z")
}

/// Smallest UTC millisecond instant the canonical 4-digit-year text form
/// can carry (0001-01-01T00:00:00.000Z). Values outside this range have no
/// round-trippable text, so every construction site must reject them —
/// otherwise `value_literal` emits a CAST the replica cannot re-parse.
pub const TIMESTAMP_MIN_MS: i64 = -62_135_596_800_000;
/// Largest such instant (9999-12-31T23:59:59.999Z).
pub const TIMESTAMP_MAX_MS: i64 = 253_402_300_799_999;

/// True when `ms` lies inside the canonical TIMESTAMP domain
/// (see [`TIMESTAMP_MIN_MS`]).
pub fn is_valid_timestamp_ms(ms: i64) -> bool {
    (TIMESTAMP_MIN_MS..=TIMESTAMP_MAX_MS).contains(&ms)
}

/// Parse a timestamp in any of the accepted forms into UTC milliseconds:
/// `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM`, `...:SS`, `...SS.mmm[...]`, separator
/// `T` or space, optional trailing `Z`/`±HH:MM`/`±HHMM` offset. Returns
/// None on anything malformed — callers turn that into NULL (predicates)
/// or an error (CAST), never a wrong time.
pub fn parse_timestamp_ms(text: &str) -> Option<i64> {
    let s = text.trim();
    let b = s.as_bytes();
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let num = |from: usize, to: usize| -> Option<i64> {
        let sub = s.get(from..to)?;
        if sub.is_empty() || !sub.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        sub.parse::<i64>().ok()
    };
    let y = num(0, 4)?;
    let mo = num(5, 7)?;
    let d = num(8, 10)?;
    // Year range 0001..=9999: the canonical text form has a 4-digit year,
    // and instants outside it could not round-trip through that text.
    if !(1..=9999).contains(&y) || !(1..=12).contains(&mo) {
        return None;
    }
    // Full calendar validation (leap years included): a malformed date must
    // not parse into a wrong instant.
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ][(mo - 1) as usize];
    if d < 1 || d > dim as i64 {
        return None;
    }
    // Split off the optional time part and the optional offset.
    let (rest, offset_min) = match s.get(10..) {
        Some(rest) if rest.starts_with('T') || rest.starts_with('t') || rest.starts_with(' ') => {
            let time = &rest[1..];
            // Offset: trailing Z / ±HH:MM / ±HHMM.
            let bytes = time.as_bytes();
            let mut offset_min = 0i64;
            let mut time = time;
            if let Some(last) = bytes.last() {
                let cut = if *last == b'Z' || *last == b'z' {
                    Some(time.len() - 1)
                } else {
                    match time.rfind(['+', '-']) {
                        Some(pos) if pos > 0 => {
                            let off = &time[pos..];
                            let digits: String = off[1..].chars().filter(|c| *c != ':').collect();
                            // `len()` counts bytes: reject before slicing so
                            // a multibyte char can never straddle `digits[..2]`
                            // (`+1中` is four bytes but not four digits).
                            if digits.len() != 4 || !digits.bytes().all(|c| c.is_ascii_digit()) {
                                return None;
                            }
                            let oh: i64 = digits[..2].parse().ok()?;
                            let om: i64 = digits[2..].parse().ok()?;
                            if oh > 23 || om > 59 {
                                return None;
                            }
                            let mag = oh * 60 + om;
                            offset_min = if off.starts_with('-') { -mag } else { mag };
                            Some(pos)
                        }
                        _ => None,
                    }
                };
                if let Some(cut) = cut {
                    time = &time[..cut];
                }
            }
            (time, offset_min)
        }
        Some(rest) if rest.trim().is_empty() => ("", 0),
        _ => return None,
    };
    let tb = rest.as_bytes();
    let (mut hh, mut mm, mut ss, mut ms) = (0i64, 0i64, 0i64, 0i64);
    match tb.len() {
        0 => {}
        5 => {
            if tb[2] != b':' {
                return None;
            }
            hh = num(11, 13)?;
            mm = num(14, 16)?;
        }
        8 => {
            if tb[2] != b':' || tb[5] != b':' {
                return None;
            }
            hh = num(11, 13)?;
            mm = num(14, 16)?;
            ss = num(17, 19)?;
        }
        n if n > 9 => {
            if tb[2] != b':' || tb[5] != b':' || tb[8] != b'.' {
                return None;
            }
            hh = num(11, 13)?;
            mm = num(14, 16)?;
            ss = num(17, 19)?;
            let frac = &rest[9..];
            if frac.is_empty() || !frac.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            // Millisecond precision; extra digits truncate (never round a
            // stored instant away from its parsed text).
            let mut scaled = String::from(frac);
            while scaled.len() < 3 {
                scaled.push('0');
            }
            ms = scaled[..3].parse().ok()?;
        }
        _ => return None,
    }
    if hh > 23 || mm > 59 || ss > 59 {
        return None;
    }
    let days = days_from_civil(y, mo as u32, d as u32);
    let secs = days * 86_400 + hh * 3600 + mm * 60 + ss;
    let ms_total = secs * 1000 + ms - offset_min * 60_000;
    // The calendar fields are each in range, but a UTC offset can still push
    // the instant outside 0001..=9999 (e.g. 0001-01-01T00:00+23:59 lands
    // before the domain floor). Such a value has no canonical text form —
    // `value_literal` cannot replay it, so the whole dump/backup would fail.
    // Reject here, at the single string→instant exit every caller shares.
    if !is_valid_timestamp_ms(ms_total) {
        return None;
    }
    Some(ms_total)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// Exact decimal (SQL DECIMAL/NUMERIC). Compared numerically against
    /// Int/Float; arithmetic stays exact while no Float is involved.
    Decimal(Decimal),
    /// Point in time: UTC milliseconds since the Unix epoch (SQL TIMESTAMP).
    /// Displayed as fixed-width RFC 3339 UTC text (year 0001..=9999, the
    /// range the canonical text round-trips); its own comparison band
    /// (never mixed with Str — a parseable string inserted into the time
    /// order would break transitivity, the exact class of the old
    /// Int/Float comparison bug).
    Timestamp(i64),
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
            Value::Decimal(_) => "decimal",
            Value::Timestamp(_) => "timestamp",
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

    pub fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Value::Decimal(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Total ordering used by indexes and ORDER BY. Null < Bool < numbers
    /// (int/float/decimal compared numerically) < Timestamp < Str < Bytes
    /// < Array < Object. Timestamp never mixes with Str: the band keeps the
    /// order total (see the variant doc).
    pub fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null => 0,
                Value::Bool(_) => 1,
                Value::Int(_) | Value::Float(_) | Value::Decimal(_) => 2,
                Value::Timestamp(_) => 3,
                Value::Str(_) => 4,
                Value::Bytes(_) => 5,
                Value::Array(_) => 6,
                Value::Object(_) => 7,
            }
        }
        let (ra, rb) = (rank(a), rank(b));
        if ra != rb {
            return ra.cmp(&rb);
        }
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::Decimal(x), Value::Decimal(y)) => x.cmp(y),
            (Value::Decimal(x), Value::Int(y)) => x.cmp(&Decimal::from(*y)),
            (Value::Int(x), Value::Decimal(y)) => Decimal::from(*x).cmp(y),
            (Value::Decimal(x), Value::Float(y)) => cmp_decimal_f64(x, *y),
            (Value::Float(x), Value::Decimal(y)) => cmp_decimal_f64(y, *x).reverse(),
            (Value::Int(i), Value::Float(f)) => cmp_int_f64(*i, *f),
            (Value::Float(f), Value::Int(i)) => cmp_int_f64(*i, *f).reverse(),
            (Value::Float(x), Value::Float(y)) => {
                // NaN needs a deterministic rank (above all finite numbers)
                // or cmp_values would not be a total order — sort and unique
                // checks rely on that.
                match (x.is_nan(), y.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
                }
            }
            (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
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

/// Decimal ↔ f64 order: NaN ranks above everything (matching the Float/Float
/// rule), ±infinity sits beyond the finite decimal range.
fn cmp_decimal_f64(d: &Decimal, f: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if f.is_nan() {
        return Ordering::Less; // d < NaN
    }
    match Decimal::from_f64_retain(f) {
        Some(fd) => {
            if fd.is_zero() && f != 0.0 {
                // 下溢:非零浮点小到 Decimal 装不下时 from_f64_retain 给
                // Some(0),让 Decimal(0) 与 1e-30 判等 —— 序失去传递性,
                // 唯一判定/树内定位全部失真。拆开按量级比:
                // · d == 0:零小于任何正次正规、大于任何负次正规;
                // · 非零 d:量级 ≥ 1e-28 严格大于 |f|,正 d 恒大、负 d 恒小。
                if d.is_zero() {
                    return if f > 0.0 {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                return if d.is_sign_negative() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            d.cmp(&fd)
        }
        None if f > 0.0 => Ordering::Less, // d < +inf
        None => Ordering::Greater,         // d > -inf
    }
}

/// Integer ↔ f64 order without the lossy `as f64` round-trip: above 2^53 the
/// cast maps distinct integers onto the same float and equality stops being
/// transitive across Int/Float/Decimal, which breaks every binary search and
/// sort that relies on `cmp_values` being a total order. NaN ranks above all
/// finite numbers (matching Float/Float).
fn cmp_int_f64(i: i64, f: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if f.is_nan() {
        return Ordering::Less; // i < NaN
    }
    // 2^63 exactly; i64::MAX as f64 rounds up to this, so compare against the
    // exact power of two instead.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if f >= TWO_POW_63 {
        return Ordering::Less;
    }
    if f < -TWO_POW_63 {
        return Ordering::Greater;
    }
    let t = f.trunc();
    let ti = t as i64; // exact: |t| < 2^63
    match i.cmp(&ti) {
        Ordering::Equal => {
            let frac = f - t;
            if frac > 0.0 {
                Ordering::Less
            } else if frac < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        other => other,
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::Timestamp(ms) => write!(f, "{}", format_timestamp_ms(*ms)),
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

    /// Decimal ↔ 次正规 Float(如 1e-30)的下溢:from_f64_retain 会给
    /// Some(0),曾让 Decimal(0) 与非零 Float 判等,序失去传递性。
    #[test]
    fn decimal_vs_subnormal_float_keeps_total_order() {
        use std::cmp::Ordering::*;
        let d0 = Decimal::ZERO;
        let tiny_pos = 1e-30f64;
        let tiny_neg = -1e-30f64;
        assert_eq!(
            Value::cmp_values(&Value::Decimal(d0), &Value::Float(tiny_pos)),
            Less
        );
        assert_eq!(
            Value::cmp_values(&Value::Decimal(d0), &Value::Float(tiny_neg)),
            Greater
        );
        let dpos: Decimal = "1".parse().unwrap();
        let dneg: Decimal = "-1".parse().unwrap();
        assert_eq!(
            Value::cmp_values(&Value::Decimal(dpos), &Value::Float(tiny_pos)),
            Greater
        );
        assert_eq!(
            Value::cmp_values(&Value::Decimal(dneg), &Value::Float(tiny_neg)),
            Less
        );
        // 传递性守卫:Decimal(0) < Float(1e-30) < Float(1e-29)。
        assert_eq!(
            Value::cmp_values(&Value::Float(tiny_pos), &Value::Float(1e-29)),
            Less
        );
    }

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
        // Decimal participates in the numeric rank and compares exactly.
        assert_eq!(
            Value::cmp_values(&Value::Int(3), &Value::Decimal(Decimal::new(30, 1))),
            Equal
        );
        assert_eq!(
            Value::cmp_values(&Value::Decimal(Decimal::new(15, 1)), &Value::Float(1.5)),
            Equal
        );
        assert_eq!(
            Value::cmp_values(&Value::Decimal(Decimal::new(2, 0)), &Value::Float(1.5)),
            Greater
        );
        assert_eq!(
            Value::cmp_values(&Value::Decimal(Decimal::new(2, 0)), &Value::Float(f64::NAN)),
            Less
        );
        assert_eq!(
            Value::cmp_values(
                &Value::Decimal(Decimal::new(2, 0)),
                &Value::Float(f64::INFINITY)
            ),
            Less
        );
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

    #[test]
    fn mixed_numeric_ordering_is_transitive_above_2_53() {
        use std::cmp::Ordering::*;
        // The lossy `as f64` cast used to map 2^53+1 and 2^53+2 onto the same
        // float, so Int/Float/Decimal equality was not transitive and B-tree
        // searches could miss. Both directions must be exact now.
        let i1 = Value::Int(9_007_199_254_740_993); // 2^53 + 1
        let i2 = Value::Int(9_007_199_254_740_994); // 2^53 + 2
        let f = Value::Float(9_007_199_254_740_992.0); // 2^53
        assert_eq!(Value::cmp_values(&i1, &f), Greater);
        assert_eq!(Value::cmp_values(&i2, &f), Greater);
        assert_eq!(Value::cmp_values(&i1, &i1.clone()), Equal);
        assert_eq!(Value::cmp_values(&f, &i1), Less);
        // Equality still holds for exactly representable values.
        assert_eq!(
            Value::cmp_values(&Value::Int(9_007_199_254_740_992), &f),
            Equal
        );
        // Boundary: 2^63 as f64 is i64::MAX + 1.
        assert_eq!(
            Value::cmp_values(
                &Value::Int(i64::MAX),
                &Value::Float(9_223_372_036_854_775_808.0)
            ),
            Less
        );
        assert_eq!(
            Value::cmp_values(
                &Value::Int(i64::MIN),
                &Value::Float(-9_223_372_036_854_775_808.0)
            ),
            Equal
        );
        assert_eq!(
            Value::cmp_values(&Value::Int(1), &Value::Float(f64::NAN)),
            Less
        );
        assert_eq!(
            Value::cmp_values(&Value::Int(1), &Value::Float(f64::NEG_INFINITY)),
            Greater
        );
        // Transitivity over the triple that used to disagree.
        let d = Value::Decimal(Decimal::new(9_007_199_254_740_992, 0));
        assert_eq!(Value::cmp_values(&f, &d), Equal);
        assert_eq!(Value::cmp_values(&i1, &d), Greater);
    }

    #[test]
    fn accessors_and_type_names() {
        let s = Value::Str("hi".into());
        let dec = Decimal::new(12345, 2);
        assert_eq!(s.as_str(), Some("hi"));
        assert_eq!(Value::Int(7).as_i64(), Some(7));
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Decimal(dec).as_decimal(), Some(dec));
        // wrong-type accessors yield None
        assert_eq!(s.as_i64(), None);
        assert_eq!(s.as_bool(), None);
        assert_eq!(Value::Int(1).as_str(), None);
        assert_eq!(Value::Bool(false).as_i64(), None);
        assert_eq!(Value::Null.as_bool(), None);
        assert_eq!(Value::Int(1).as_decimal(), None);
        for (v, name) in [
            (&Value::Null, "null"),
            (&Value::Bool(false), "bool"),
            (&Value::Int(0), "int"),
            (&Value::Float(0.0), "float"),
            (&Value::Decimal(dec), "decimal"),
            (&s, "string"),
            (&Value::Bytes(vec![]), "bytes"),
            (&Value::Array(vec![]), "array"),
            (&Value::Object(Object::new()), "object"),
        ] {
            assert_eq!(v.type_name(), name);
        }
    }

    #[test]
    fn display_covers_all_variants() {
        assert_eq!(Value::Null.to_string(), "null");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::Int(-5).to_string(), "-5");
        assert_eq!(Value::Float(1.5).to_string(), "1.5");
        assert_eq!(Value::Decimal(Decimal::new(12345, 2)).to_string(), "123.45");
        assert_eq!(Value::Str("s".into()).to_string(), "s");
        assert_eq!(
            Value::Bytes(vec![0xde, 0xad, 0x01]).to_string(),
            "x'dead01'"
        );
        assert_eq!(
            Value::Array(vec![Value::Int(1), Value::Str("b".into())]).to_string(),
            "[1,b]"
        );
        assert_eq!(
            Value::Object(Object::from([
                ("a".into(), Value::Int(1)),
                ("b".into(), Value::Null),
            ]))
            .to_string(),
            "{a:1,b:null}"
        );
    }

    #[test]
    fn ordering_object_key_and_prefix_cases() {
        use std::cmp::Ordering::*;
        let mk = |pairs: &[(&str, i64)]| {
            Value::Object(
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), Value::Int(*v)))
                    .collect(),
            )
        };
        // key decides before value
        assert_eq!(Value::cmp_values(&mk(&[("a", 9)]), &mk(&[("b", 1)])), Less);
        // equal keys compare values
        assert_eq!(Value::cmp_values(&mk(&[("a", 1)]), &mk(&[("a", 2)])), Less);
        // prefix object is smaller
        assert_eq!(
            Value::cmp_values(&mk(&[("a", 1)]), &mk(&[("a", 1), ("b", 1)])),
            Less
        );
        assert_eq!(
            Value::cmp_values(&mk(&[("a", 1), ("b", 1)]), &mk(&[("a", 1)])),
            Greater
        );
        // bytes compare
        assert_eq!(
            Value::cmp_values(&Value::Bytes(vec![1]), &Value::Bytes(vec![2])),
            Less
        );
        // rank separation: str < bytes < array < object
        assert_eq!(
            Value::cmp_values(&Value::Str("z".into()), &Value::Bytes(vec![])),
            Less
        );
        assert_eq!(
            Value::cmp_values(&Value::Bytes(vec![]), &Value::Array(vec![])),
            Less
        );
        assert_eq!(
            Value::cmp_values(&Value::Array(vec![]), &Value::Object(Object::new())),
            Less
        );
        // nested array element decides before length
        assert_eq!(
            Value::cmp_values(
                &Value::Array(vec![Value::Int(1)]),
                &Value::Array(vec![Value::Int(2), Value::Int(3)])
            ),
            Less
        );
        // equal-length arrays with equal elements
        assert_eq!(
            Value::cmp_values(
                &Value::Array(vec![Value::Null]),
                &Value::Array(vec![Value::Null])
            ),
            Equal
        );
        // float NaN has a deterministic rank (above all finite numbers) so
        // the total order stays usable for unique checks and sorting
        assert_eq!(
            Value::cmp_values(&Value::Float(f64::NAN), &Value::Float(1.0)),
            Greater
        );
        assert_eq!(
            Value::cmp_values(&Value::Float(1.0), &Value::Float(f64::NAN)),
            Less
        );
        assert_eq!(
            Value::cmp_values(&Value::Float(f64::NAN), &Value::Float(f64::NAN)),
            Equal
        );
    }

    #[test]
    fn timestamp_parse_and_format_kat() {
        use super::{format_timestamp_ms, parse_timestamp_ms};
        // Known answers.
        assert_eq!(format_timestamp_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_timestamp_ms(1_789_430_400_123),
            "2026-09-15T00:00:00.123Z"
        );
        assert_eq!(parse_timestamp_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_timestamp_ms("2026-09-15T00:00:00.123Z"),
            Some(1_789_430_400_123)
        );
        // Accepted forms.
        assert_eq!(
            parse_timestamp_ms("2026-09-15"),
            Some(1_789_430_400_000),
            "date-only = midnight UTC"
        );
        assert_eq!(
            parse_timestamp_ms("2026-09-16"),
            Some(1_789_516_800_000),
            "any date-only form is midnight UTC"
        );
        // Leap-year calendar: 2024-02-29 exists, 2023-02-29 does not.
        assert_eq!(parse_timestamp_ms("2024-02-29"), Some(1_709_164_800_000));
        assert_eq!(parse_timestamp_ms("2023-02-29"), None);
        assert_eq!(
            parse_timestamp_ms("2026-09-15 12:30"),
            parse_timestamp_ms("2026-09-15T12:30:00.000Z")
        );
        assert_eq!(
            parse_timestamp_ms("2026-09-15T12:30:45.5"),
            Some(parse_timestamp_ms("2026-09-15T12:30:45.500Z").unwrap())
        );
        // Offset handling: +08:00 subtracts the offset from the wall clock.
        assert_eq!(
            parse_timestamp_ms("2026-09-15T08:00:00+08:00"),
            parse_timestamp_ms("2026-09-15T00:00:00Z")
        );
        assert_eq!(
            parse_timestamp_ms("2026-09-15T00:00:00-0530"),
            parse_timestamp_ms("2026-09-15T05:30:00Z")
        );
        // Malformed forms never parse to a wrong time.
        for bad in [
            "",
            "garbage",
            "2026-13-01",
            "2026-09-31T00:00:00Z",
            "2023-02-29T00:00:00Z",
            "2026-09-15T25:00:00Z",
            "2026-09-15T12:60:00Z",
            "2026-9-15",
            "2026-09-15T12:30:00+99:00",
            "2026-09-15T12:30:00Zx",
        ] {
            assert_eq!(parse_timestamp_ms(bad), None, "{bad:?} must not parse");
        }
        // Format ∘ parse = identity across a spread of instants (negative,
        // epoch, far future — the fixed-width text form is total).
        for ms in [
            -86_400_000_001i64,
            -1,
            0,
            1,
            999,
            1_789_430_400_123,
            253_402_300_799_999,
        ] {
            assert_eq!(
                parse_timestamp_ms(&format_timestamp_ms(ms)),
                Some(ms),
                "{ms}"
            );
        }
    }

    #[test]
    fn timestamp_has_its_own_total_order_band() {
        use super::Value;
        use std::cmp::Ordering::*;
        let ts = |ms| Value::Timestamp(ms);
        assert_eq!(Value::cmp_values(&ts(1), &ts(2)), Less);
        assert_eq!(Value::cmp_values(&ts(2), &ts(1)), Greater);
        // The band sits between numbers and strings and never mixes: a
        // parseable string compared with a Timestamp keeps the string rank
        // (the predicate layer owns the coercion — cmp_values must stay a
        // total order, and a parseable string woven into the time order
        // would break transitivity).
        assert_eq!(Value::cmp_values(&Value::Int(9), &ts(-9_000_000_000)), Less);
        assert_eq!(
            Value::cmp_values(&ts(-9_000_000_000), &Value::Int(9)),
            Greater
        );
        assert_eq!(
            Value::cmp_values(&ts(0), &Value::Str("1970-01-01T00:00:00.000Z".into())),
            Less
        );
        assert_eq!(
            Value::cmp_values(
                &Value::Str("1970-01-01T00:00:00.000Z".into()),
                &ts(86_400_000)
            ),
            // The Str band ranks ABOVE the whole Timestamp band regardless
            // of the instants involved.
            Greater
        );
    }
}
