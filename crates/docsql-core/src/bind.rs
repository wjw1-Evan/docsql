//! Parameter binding: wire JSON params → engine values → typed SQL literals.
//!
//! This is the **single** injection-sensitive rendering surface in the
//! project. Both consumers share it: the server's REQ_EXECUTE path
//! (`crates/docsql-server/src/conn.rs`, which re-exports these functions
//! through `server/params.rs`) and the in-process FFI embedding path
//! (`crates/docsql-ffi`, .NET `Data Source=…` mode). Do not fork a second
//! renderer — the escaping rules below are the contract that keeps bound
//! values data-only.

use crate::value::Value;

/// Wire param (JSON) → engine value. Nested objects/arrays keep their JSON
/// text shape — `render_param` quotes them as text literals. The `$dec` and
/// `$bytes` marker objects (emitted by the .NET client) decode back to the
/// exact scalar types; unknown objects remain text.
pub fn json_param_to_value(p: &serde_json::Value) -> Value {
    match p {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Value::Int)
            .unwrap_or_else(|| Value::Float(n.as_f64().unwrap_or(0.0))),
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Object(o) => {
            if let Some(serde_json::Value::String(s)) = o.get("$dec") {
                if let Ok(d) = s.parse::<crate::value::Decimal>() {
                    return Value::Decimal(d);
                }
            }
            // TIMESTAMP: UTC milliseconds (the ADO.NET driver's $ts marker).
            // Domain-checked against the canonical 0001..=9999 window: an
            // out-of-range value has no round-trippable text form, so
            // value_literal/dump could not replay it — fall back to text,
            // the same policy as an unparsable $dec.
            if let Some(serde_json::Value::Number(n)) = o.get("$ts") {
                if let Some(ms) = n
                    .as_i64()
                    .filter(|ms| crate::value::is_valid_timestamp_ms(*ms))
                {
                    return Value::Timestamp(ms);
                }
            }
            // Non-finite float (the ADO.NET driver's $float marker): the
            // read direction emits it, so the bind direction must decode it
            // — without this arm NaN/±Infinity parameters bound as the
            // literal marker TEXT and silently polluted the column.
            if let Some(serde_json::Value::String(s)) = o.get("$float") {
                if let Some(f) = crate::json::parse_float_marker(s) {
                    return Value::Float(f);
                }
            }
            if let Some(serde_json::Value::Array(a)) = o.get("$bytes") {
                let mut bytes = Vec::with_capacity(a.len());
                for v in a {
                    let Some(b) = v.as_u64().filter(|b| *b <= 0xff) else {
                        return Value::Str(p.to_string());
                    };
                    bytes.push(b as u8);
                }
                return Value::Bytes(bytes);
            }
            Value::Str(p.to_string())
        }
        other => Value::Str(other.to_string()),
    }
}

/// Server-side parameter binding for REQ_EXECUTE (and the FFI execute
/// surface): substitute `?` placeholders with typed literals rendered from
/// the param array.
/// The scan is quote-aware — a `?` inside a string literal (or a doubled
/// `''` escape) is data, not a placeholder. String params escape single
/// quotes by doubling, so a value can never terminate the literal early:
/// that closes the injection surface client-side binding leaves open.
/// The count is checked in both directions (too few / too many parameters).
pub fn bind_params(sql: &str, params: &[Value]) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len() + 16 * params.len());
    let mut next = 0usize;
    let mut i = 0usize;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        match bytes[i] {
            // Literals/identifiers/comments are copied verbatim: a `?` inside
            // one is data, not a placeholder. Skipping only single quotes
            // made `SELECT "ready?"` or `-- ?` misbind.
            b'\'' => {
                let (end, _) = crate::stmt::sql_literal_end(sql, i);
                out.push_str(&sql[i..end]);
                i = end;
            }
            b'"' | b'`' => {
                let quote = bytes[i];
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            // T-SQL [bracket] identifiers are data to the preprocessor
            // (`[x]` becomes `"x"` before parsing): a `?` inside one is
            // part of the identifier, not a placeholder.
            b'[' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b']' {
                        if bytes.get(i + 1) == Some(&b']') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let start = i;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            b'?' => {
                let Some(p) = params.get(next) else {
                    return Err(format!(
                        "statement has more ? placeholders than the {} parameter(s) supplied",
                        params.len()
                    ));
                };
                next += 1;
                out.push_str(&render_param(p));
                i += 1;
            }
            _ => {
                // Advance by one CHARACTER (multibyte safety).
                let ch_len = sql[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
                out.push_str(&sql[i..i + ch_len]);
                i += ch_len;
            }
        }
    }
    if next < params.len() {
        return Err(format!(
            "{} parameter(s) supplied but the statement has only {next} ? placeholder(s)",
            params.len()
        ));
    }
    Ok(out)
}

/// Typed SQL literal for one bound parameter.
pub fn render_param(p: &Value) -> String {
    match p {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Int(i) => i.to_string(),
        // Debug formatting keeps integral floats distinguishable from Int
        // (Display prints 3.0 as "3", which the engine would re-parse and
        // store as an Int) — same rationale as value_literal. Non-finite
        // floats have no bare literal (NaN/inf would parse as identifiers
        // and bind NULL or error); use the same CAST form the dump /
        // replication rewrite round-trips.
        Value::Float(f) if !f.is_finite() => format!(
            "CAST({} AS REAL)",
            crate::stmt::sql_string_literal(crate::json::float_marker_text(*f))
        ),
        Value::Float(f) => format!("{f:?}"),
        Value::Decimal(d) => {
            // Exactness survives the text round-trip: an unquoted number
            // would re-parse as Float on the engine side.
            format!(
                "CAST({} AS DECIMAL)",
                crate::stmt::sql_string_literal(&d.to_string())
            )
        }
        Value::Str(s) => crate::stmt::sql_string_literal(s),
        Value::Timestamp(ms) => format!(
            "CAST({} AS TIMESTAMP)",
            crate::stmt::sql_string_literal(&crate::value::format_timestamp_ms(*ms))
        ),
        Value::Bytes(b) => {
            let mut hex = String::with_capacity(b.len() * 2 + 3);
            hex.push_str("x'");
            for byte in b {
                hex.push_str(&format!("{byte:02x}"));
            }
            hex.push('\'');
            hex
        }
        // Nested documents/arrays travel as JSON text literals.
        other => crate::stmt::sql_string_literal(&crate::json::to_string(other)),
    }
}

#[cfg(test)]
mod tests {
    // The binding rules are exercised through the server's REQ_EXECUTE tests
    // (crates/docsql-server/src/lib.rs::tests) and the FFI crate's embedded
    // suite; the quote-aware scan itself is pinned here so a regression in
    // the moved code cannot hide behind a server-level test rewrite.
    use super::*;

    #[test]
    fn placeholder_scan_skips_literals_identifiers_and_comments() {
        assert_eq!(
            bind_params("SELECT 'a?b''c?' , ? FROM t", &[Value::Int(1)]).unwrap(),
            "SELECT 'a?b''c?' , 1 FROM t"
        );
        assert_eq!(
            bind_params("SELECT \"ready?\" FROM t WHERE id = ?", &[Value::Int(2)]).unwrap(),
            "SELECT \"ready?\" FROM t WHERE id = 2"
        );
        assert_eq!(
            bind_params("SELECT ? -- set?\n+ ?", &[Value::Int(1), Value::Int(2)]).unwrap(),
            "SELECT 1 -- set?\n+ 2"
        );
        assert_eq!(
            bind_params("SELECT /* ? */ ?", &[Value::Int(3)]).unwrap(),
            "SELECT /* ? */ 3"
        );
        assert_eq!(
            bind_params("SELECT [col?], name FROM t WHERE id = ?", &[Value::Int(7)]).unwrap(),
            "SELECT [col?], name FROM t WHERE id = 7"
        );
    }

    #[test]
    fn parameter_count_is_checked_both_ways() {
        assert!(bind_params("SELECT ?", &[]).is_err());
        assert!(bind_params("SELECT 1", &[Value::Int(1)]).is_err());
    }

    #[test]
    fn string_values_are_data_only() {
        let out = bind_params(
            "SELECT * FROM t WHERE x = ?",
            &[Value::Str("a'; DROP TABLE t; --".into())],
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE x = 'a''; DROP TABLE t; --'");
    }
}
