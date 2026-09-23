//! T-SQL (SQL Server) compatibility layer: statement preprocessing and the
//! T-SQL scalar/table function families.
//!
//! DocSQL's kernel is standard SQL; this module carries the deliberate
//! T-SQL surface on top of it in three pieces:
//!
//! * [`preprocess`] — a string/comment-aware text pass that runs before the
//!   generic parser: `[bracket]` identifiers become quoted identifiers,
//!   `CONVERT(type, value[, style])` / `PARSE(expr AS type)` (whose argument
//!   order the generic dialect cannot express) become marker function calls,
//!   and the session shims `SET <option> ON|OFF`, `USE <db>` and `PRINT`
//!   become `PRAGMA` statements (the engine's accepted-and-ignored compat
//!   channel, same as SQLite PRAGMA probing from ORMs).
//! * [`scalar`] — every T-SQL expression-level function the value model can
//!   honor (date/time, string, math, conversion, logical, metadata, hash).
//!   The engine's scalar dispatcher calls this before giving up.
//! * [`table_function`] — the `FROM` table-valued functions
//!   (`STRING_SPLIT`, `GENERATE_SERIES`, `OPENJSON`).
//!
//! Boundaries are loud, never silent: session-identity functions
//! (`SUSER_SNAME`, `HOST_NAME`, …) and `RAND()` refuse with explicit
//! messages instead of returning plausible-looking lies.

use crate::engine::{civil_from_days, err, SqlError};
use crate::json;
use crate::stmt;
use crate::value::{days_from_civil, parse_timestamp_ms, Object, Value};
use rust_decimal::prelude::ToPrimitive as _;

type Res<T> = crate::engine::Result<T>;

// ---------------------------------------------------------------------------
// Statement preprocessing
// ---------------------------------------------------------------------------

/// T-SQL type names accepted as the first argument of `CONVERT`. The
/// generic dialect parses `CONVERT(x, y)` in MySQL order, so the rewriter
/// only fires when the first comma-separated segment is one of these —
/// anything else stays verbatim and fails loudly downstream.
fn is_tsql_type_name(word: &str) -> bool {
    let w = word.trim();
    // The CONVERT spec form is `data_type [(length)]` — `VARCHAR(10)`,
    // `DECIMAL(10,2)`, `NVARCHAR(MAX)` are the canonical spellings, the
    // bare name is the special case. The suffix is validated but kept in
    // the type text: `cast_value` understands the sized forms.
    let (base, suffix) = match w.find('(') {
        Some(idx) if w.ends_with(')') => (&w[..idx], Some(&w[idx + 1..w.len() - 1])),
        Some(_) => return false,
        None => (w, None),
    };
    if let Some(s) = suffix {
        let s = s.trim();
        let parts: Vec<&str> = s.split(',').collect();
        let ok = s.eq_ignore_ascii_case("max")
            || (parts.len() <= 2
                && parts
                    .iter()
                    .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit())));
        if !ok {
            return false;
        }
    }
    matches!(
        base.to_ascii_uppercase().as_str(),
        "INT"
            | "INTEGER"
            | "BIGINT"
            | "SMALLINT"
            | "TINYINT"
            | "BIT"
            | "BOOL"
            | "BOOLEAN"
            | "CHAR"
            | "VARCHAR"
            | "NCHAR"
            | "NVARCHAR"
            | "TEXT"
            | "NTEXT"
            | "STRING"
            | "REAL"
            | "FLOAT"
            | "DOUBLE"
            | "DECIMAL"
            | "NUMERIC"
            | "MONEY"
            | "SMALLMONEY"
            | "BINARY"
            | "VARBINARY"
            | "BLOB"
            | "BYTES"
            | "DATETIME"
            | "DATETIME2"
            | "DATE"
            | "SMALLDATETIME"
            | "DATETIMEOFFSET"
            | "TIMESTAMP"
            | "GUID"
            | "UNIQUEIDENTIFIER"
            | "UUID"
            | "UUIDV7"
    )
}

/// Session `SET` options accepted as no-op shims (the PRAGMA channel).
/// DocSQL has no session-tunable behavior for any of them; accepting the
/// statement lets stock SQL Server scripts run instead of dying on their
/// prologue. They are documented as accepted-and-ignored — the engine's
/// NULL/identifier semantics are fixed.
fn is_set_option(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "NOCOUNT"
            | "ANSI_NULLS"
            | "ANSI_PADDING"
            | "ANSI_WARNINGS"
            | "ARITHABORT"
            | "ANSI_NULL_DFLT_ON"
            | "ANSI_NULL_DFLT_OFF"
            | "CONCAT_NULL_YIELDS_NULL"
            | "NUMERIC_ROUNDABORT"
            | "QUOTED_IDENTIFIER"
            | "XACT_ABORT"
            | "LOCK_TIMEOUT"
            | "ROWCOUNT"
            | "TEXTSIZE"
            | "DEADLOCK_PRIORITY"
            | "LANGUAGE"
            | "DATEFORMAT"
            | "DATEFIRST"
            | "FMTONLY"
    )
}

/// Rewrite T-SQL notations the generic SQL parser cannot express into forms
/// it can. Runs once per statement text before parsing; idempotent (the
/// emitted markers are distinct words that never re-trigger).
///
/// Everything happens outside string literals, quoted identifiers and
/// comments — the same scan discipline as [`stmt::fold_wall_clocks`]. Any
/// shape the rewriter does not fully recognize is left verbatim so the
/// parser reports it loudly.
pub fn preprocess(sql: &str) -> String {
    // Fast path: nothing to rewrite. An ASCII-lowercase copy is
    // byte-index compatible with the original (see the Unicode red line
    // on case mapping in AGENTS.md).
    let lower = sql.to_ascii_lowercase();
    let may_rewrite = sql.contains('[')
        || lower.contains("convert")
        || lower.contains("parse")
        || contains_word(&lower, "set")
        || contains_word(&lower, "use")
        || contains_word(&lower, "print");
    if !may_rewrite {
        return sql.to_string();
    }

    let b = sql.as_bytes();
    let lb = lower.as_bytes();
    let mut out = String::with_capacity(sql.len() + 64);
    let mut i = 0usize;
    // Statement-start tracking: the shims only apply to a leading
    // SET/USE/PRINT (T-SQL never uses them mid-statement).
    let mut at_start = true;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(sql, i);
                out.push_str(&sql[i..end]);
                i = end;
                at_start = false;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                let start = i;
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let start = i;
                let mut j = i + 2;
                while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                    j += 1;
                }
                j = if j + 1 < b.len() { j + 2 } else { b.len() };
                out.push_str(&sql[start..j]);
                i = j;
            }
            b'"' | b'`' => {
                let quote = b[i];
                out.push(b[i] as char);
                i += 1;
                while i < b.len() {
                    let ch_len = utf8_len(b[i]);
                    out.push_str(&sql[i..i + ch_len]);
                    if b[i] == quote {
                        if b.get(i + 1) == Some(&quote) {
                            out.push_str(&sql[i + 1..i + 2]);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += ch_len;
                }
                at_start = false;
            }
            b'[' => {
                // [bracket identifier] → "quoted identifier"; the T-SQL ]]
                // escape doubles to SQL "".
                let mut j = i + 1;
                let mut ident = String::new();
                let mut closed = false;
                while j < b.len() {
                    match b[j] {
                        b']' => {
                            if b.get(j + 1) == Some(&b']') {
                                ident.push(']');
                                j += 2;
                                continue;
                            }
                            j += 1;
                            closed = true;
                            break;
                        }
                        _ => {
                            let ch_len = utf8_len(b[j]);
                            ident.push_str(&sql[j..j + ch_len]);
                            j += ch_len;
                        }
                    }
                }
                if closed {
                    out.push('"');
                    out.push_str(&ident.replace('"', "\"\""));
                    out.push('"');
                } else {
                    // Unterminated: copy verbatim, let the parser complain.
                    out.push_str(&sql[i..j]);
                }
                i = j;
                at_start = false;
            }
            b';' => {
                out.push(';');
                i += 1;
                at_start = true;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let word = &lb[start..i];
                if at_start && matches!(word, b"set" | b"use" | b"print") {
                    if let Some(next) = shim_statement(sql, i, word, &mut out) {
                        i = next;
                        at_start = false;
                        continue;
                    }
                }
                if matches!(word, b"convert" | b"try_convert" | b"parse" | b"try_parse") {
                    let marker = match word {
                        b"convert" => "__TSQL_CONVERT__",
                        b"try_convert" => "__TSQL_TRY_CONVERT__",
                        b"parse" => "__TSQL_PARSE__",
                        _ => "__TSQL_TRY_PARSE__",
                    };
                    match rewrite_call(sql, i, word, marker, &mut out) {
                        Some(next) => {
                            i = next;
                            at_start = false;
                        }
                        None => {
                            out.push_str(&sql[start..i]);
                            at_start = false;
                        }
                    }
                } else {
                    out.push_str(&sql[start..i]);
                    at_start = false;
                }
            }
            c if c.is_ascii_whitespace() => {
                out.push(c as char);
                i += 1;
            }
            c if c < 0x80 => {
                out.push(c as char);
                i += 1;
                at_start = false;
            }
            _ => {
                let ch_len = utf8_len(b[i]);
                out.push_str(&sql[i..i + ch_len]);
                i += ch_len;
                at_start = false;
            }
        }
    }
    out
}

/// Word-boundary containment on an ASCII-lowercased copy — the preprocess
/// fast path uses it to decide whether the full scanner is worth running.
/// A false positive only costs the scan (the scanner itself decides); a
/// false negative is impossible for shim keywords, which always stand as
/// their own word.
fn contains_word(lower: &str, word: &str) -> bool {
    let b = lower.as_bytes();
    let w = word.as_bytes();
    debug_assert!(!w.is_empty());
    let mut i = 0;
    while i + w.len() <= b.len() {
        if &b[i..i + w.len()] == w {
            let before_ok = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
            let after = i + w.len();
            let after_ok =
                after >= b.len() || !(b[after].is_ascii_alphanumeric() || b[after] == b'_');
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Try the leading SET/USE/PRINT shim. On success appends the PRAGMA
/// replacement to `out` and returns the index after the consumed span;
/// `None` leaves the word verbatim (the caller re-copies it).
fn shim_statement(sql: &str, after_word: usize, word: &[u8], out: &mut String) -> Option<usize> {
    let b = sql.as_bytes();
    let mut i = after_word;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    match word {
        b"use" => {
            // USE <name> — single-database engine: accept any one name.
            let name = read_shim_ident(sql, &mut i)?;
            if !at_statement_end(sql, i) {
                return None;
            }
            out.push_str("PRAGMA tsql_use = ");
            out.push_str(&stmt::sql_string_literal(&name));
            Some(i)
        }
        b"set" => {
            let opt_start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            if opt_start == i || !is_set_option(&sql[opt_start..i]) {
                return None;
            }
            let opt = sql[opt_start..i].to_ascii_uppercase();
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            let val_start = i;
            // ON / OFF / integer / 'string' / "quoted" are all accepted.
            if b.get(i) == Some(&b'\'') {
                let (end, closed) = stmt::sql_literal_end(sql, i);
                if !closed {
                    return None; // unterminated value: leave verbatim
                }
                i = end;
            } else if b.get(i) == Some(&b'"') {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    i += 1;
                }
                if i >= b.len() {
                    return None;
                }
                i += 1;
            } else {
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric() || matches!(b[i], b'_' | b'-' | b'.'))
                {
                    i += 1;
                }
                if val_start == i {
                    return None;
                }
            }
            let val = sql[val_start..i].trim();
            if !at_statement_end(sql, i) {
                return None;
            }
            out.push_str("PRAGMA tsql_set = ");
            out.push_str(&stmt::sql_string_literal(&format!("{opt} {val}")));
            Some(i)
        }
        b"print" => {
            // PRINT <literal-or-number> — the engine outcome has no message
            // channel, so the statement becomes a no-op PRAGMA (the CLI
            // prints it locally before sending). The PRAGMA value grammar
            // only accepts a single literal, so anything else stays
            // verbatim and the parser reports it loudly.
            let start = i;
            let end = statement_end(sql, start);
            let expr = sql[start..end].trim();
            if expr.is_empty() {
                return None;
            }
            let single_literal = if expr.starts_with('\'') {
                match sql[start..].find('\'') {
                    Some(off) => {
                        let open = start + off;
                        let (lit_end, closed) = stmt::sql_literal_end(sql, open);
                        closed && sql[lit_end..end].trim().is_empty()
                    }
                    None => false,
                }
            } else {
                expr.chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, '-' | '.'))
                    && expr.chars().any(|c| c.is_ascii_digit())
            };
            if !single_literal {
                return None;
            }
            out.push_str(&format!("PRAGMA tsql_print = {expr}"));
            Some(end)
        }
        _ => None,
    }
}

/// Read one identifier (bare word or "quoted") starting at `*i`, advancing
/// past it.
fn read_shim_ident(sql: &str, i: &mut usize) -> Option<String> {
    let b = sql.as_bytes();
    if *i >= b.len() {
        return None;
    }
    if b[*i] == b'"' {
        let start = *i;
        *i += 1;
        while *i < b.len() && b[*i] != b'"' {
            *i += 1;
        }
        if *i >= b.len() {
            return None; // unterminated quote
        }
        *i += 1;
        return Some(sql[start + 1..*i - 1].replace("\"\"", "\""));
    }
    if b[*i].is_ascii_alphabetic() || b[*i] == b'_' {
        let start = *i;
        while *i < b.len() && (b[*i].is_ascii_alphanumeric() || b[*i] == b'_') {
            *i += 1;
        }
        return Some(sql[start..*i].to_string());
    }
    None
}

/// Index of the top-level `;` (or end of input) starting from `from`,
/// skipping literals and comments.
fn statement_end(sql: &str, from: usize) -> usize {
    let b = sql.as_bytes();
    let mut i = from;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(sql, i);
                i = end;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(b.len());
            }
            b';' => return i,
            _ => i += 1,
        }
    }
    b.len()
}

/// True when only whitespace (and an optional trailing `;`) remains before
/// the statement end.
fn at_statement_end(sql: &str, from: usize) -> bool {
    let rest = &sql[from..statement_end(sql, from)];
    // Comments between the value and the statement end are transparent.
    let stripped = strip_leading_trivia(rest);
    let trimmed = stripped.trim();
    trimmed.is_empty() || trimmed == ";"
}

/// Drop `--`/`/* */` comment runs from a small statement tail.
fn strip_leading_trivia(s: &str) -> String {
    let b = s.as_bytes();
    let mut i = 0usize;
    let mut out = String::new();
    while i < b.len() {
        if b[i] == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else {
            let ch_len = utf8_len(b[i]);
            out.push_str(&s[i..i + ch_len]);
            i += ch_len;
        }
    }
    out
}

/// Rewrite `CONVERT(type, value[, style])` / `PARSE(value AS type [USING
/// 'culture'])` into the marker call. Returns the index after the closing
/// paren, or None to leave the call verbatim.
/// Trim only spaces/tabs: newlines must survive — they terminate `--` line
/// comments, so a full `trim()` could pull the closing paren of a rewritten
/// call into a trailing comment and break the statement.
fn trim_h(s: &str) -> &str {
    let t = |c: char| c == ' ' || c == '\t';
    s.trim_matches(t)
}

fn rewrite_call(
    sql: &str,
    word_end: usize,
    word: &[u8],
    marker: &str,
    out: &mut String,
) -> Option<usize> {
    let b = sql.as_bytes();
    let mut i = word_end;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if b.get(i) != Some(&b'(') {
        return None;
    }
    let close = balanced_close(sql, i)?;
    let inner = trim_h(&sql[i + 1..close]);
    let is_parse = word == b"parse" || word == b"try_parse";
    let (type_text, value_text, tail) = if is_parse {
        split_parse_args(inner)?
    } else {
        let args = split_top_level(inner, b',');
        match args.len() {
            2 => (args[0], args[1], None),
            3 if is_tsql_type_name(args[0]) => (args[0], args[1], Some(args[2])),
            // MySQL order, missing/malformed style, or a non-type first
            // segment: leave verbatim for the parser to judge loudly.
            _ => return None,
        }
    };
    if !is_parse && !is_tsql_type_name(type_text) {
        return None;
    }
    out.push_str(&format!(
        "{marker}({}, {}",
        stmt::sql_string_literal(trim_h(type_text)),
        trim_h(value_text)
    ));
    if let Some(tail) = tail {
        out.push_str(&format!(", {}", trim_h(tail)));
    }
    out.push(')');
    Some(close + 1)
}

/// Split `value AS type [USING 'culture']` into (type, value, culture).
fn split_parse_args(inner: &str) -> Option<(&str, &str, Option<&str>)> {
    let b = inner.as_bytes();
    let lower = inner.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let mut using_idx = None;
    let mut as_idx = None;
    let mut depth = 0i32;
    let mut i = 0;
    let mut word_start: Option<usize> = None;
    // Track word boundaries: a word ends at the first byte that is not a
    // word character; `as`/`using` only count at paren depth 0 outside
    // string literals.
    let mut check_word = |ws: usize, we: usize, depth: i32| {
        if depth != 0 {
            return;
        }
        let word = &lb[ws..we];
        if word == b"as" && as_idx.is_none() {
            as_idx = Some((ws, we));
        } else if word == b"using" && using_idx.is_none() {
            using_idx = Some((ws, we));
        }
    };
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(inner, i);
                if let Some(ws) = word_start.take() {
                    check_word(ws, i, depth);
                }
                i = end;
                continue;
            }
            b'(' => {
                if let Some(ws) = word_start.take() {
                    check_word(ws, i, depth);
                }
                depth += 1;
            }
            b')' => {
                if let Some(ws) = word_start.take() {
                    check_word(ws, i, depth);
                }
                depth -= 1;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                if word_start.is_none() {
                    word_start = Some(i);
                }
            }
            _ => {
                if let Some(ws) = word_start.take() {
                    check_word(ws, i, depth);
                }
            }
        }
        i += 1;
    }
    if let Some(ws) = word_start.take() {
        check_word(ws, b.len(), depth);
    }
    let (as_ws, as_we) = as_idx?;
    // trim_h (not trim): newlines terminate `--` comments and must survive
    // into the rewritten statement.
    let value = trim_h(&inner[..as_ws]);
    let ty_end = using_idx.map(|(ws, _)| ws).unwrap_or(b.len());
    let ty = trim_h(&inner[as_we..ty_end]);
    if value.is_empty() || ty.is_empty() {
        return None;
    }
    let culture = using_idx.map(|(_, we)| trim_h(&inner[we..]));
    if culture.is_some_and(|c| c.is_empty()) {
        return None;
    }
    Some((ty, value, culture))
}

/// Index of the `)` matching the `(` at `open`, skipping literals and
/// comments; None when unbalanced. (The index points AT the paren so the
/// caller slices the interior as `sql[open+1..close]`.)
fn balanced_close(sql: &str, open: usize) -> Option<usize> {
    let b = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(sql, i);
                i = end;
                continue;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(b.len());
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split on top-level commas (paren-depth 0, outside literals/comments).
fn split_top_level(s: &str, sep: u8) -> Vec<&str> {
    let b = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = stmt::sql_literal_end(s, i);
                i = end;
                continue;
            }
            // Comments are opaque: a comma inside one must not split the
            // argument list (a mis-split leaves the call verbatim, which
            // then fails loudly downstream).
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut j = i + 2;
                while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                    j += 1;
                }
                i = if j + 1 < b.len() { j + 2 } else { b.len() };
                continue;
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            c if c == sep && depth == 0 => {
                parts.push(trim_h(&s[start..i]));
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(trim_h(&s[start..]));
    parts
}

fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

// ---------------------------------------------------------------------------
// LIKE with T-SQL character classes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum LikeTok {
    Literal(char),
    AnyOne,
    AnyRun,
    Class {
        neg: bool,
        ranges: Vec<(char, char)>,
    },
}

/// SQL LIKE with `%`, `_`, `ESCAPE` — and the T-SQL `[abc]` / `[^a-z]`
/// character classes. The pattern is tokenized once (escape resolved),
/// then matched with the same backtracking loop the old `%`/`_`-only
/// matcher used. A `[` without a closing `]` is a literal bracket; a `]`
/// before any class member is a literal member; ranges are inclusive; `^`
/// negates only at the very start of the class; `%`/`_` inside a class are
/// literal characters.
pub fn like_match(s: &str, pat: &str, esc: Option<char>) -> bool {
    let toks = tokenize_like(pat, esc);
    let s: Vec<char> = s.chars().collect();
    let p: &[LikeTok] = &toks;
    let (mut si, mut pi) = (0usize, 0usize);
    let (mut star, mut mark) = (usize::MAX, 0usize);
    while si < s.len() {
        if pi < p.len() {
            let hit = match &p[pi] {
                LikeTok::Literal(c) => s[si] == *c,
                LikeTok::AnyOne => true,
                LikeTok::Class { neg, ranges } => {
                    let in_class = ranges.iter().any(|(lo, hi)| s[si] >= *lo && s[si] <= *hi);
                    in_class != *neg
                }
                LikeTok::AnyRun => false,
            };
            if hit {
                si += 1;
                pi += 1;
                continue;
            }
            if let LikeTok::AnyRun = p[pi] {
                star = pi;
                mark = si;
                pi += 1;
                continue;
            }
        }
        if star != usize::MAX {
            pi = star + 1;
            mark += 1;
            si = mark;
            continue;
        }
        return false;
    }
    while pi < p.len() && matches!(p[pi], LikeTok::AnyRun) {
        pi += 1;
    }
    pi == p.len()
}

fn tokenize_like(pat: &str, esc: Option<char>) -> Vec<LikeTok> {
    let chars: Vec<char> = pat.chars().collect();
    let mut toks = Vec::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if Some(c) == esc {
            if i + 1 < chars.len() {
                toks.push(LikeTok::Literal(chars[i + 1]));
                i += 2;
            } else {
                // Trailing escape char: match it literally.
                toks.push(LikeTok::Literal(c));
                i += 1;
            }
        } else if c == '%' {
            toks.push(LikeTok::AnyRun);
            i += 1;
        } else if c == '_' {
            toks.push(LikeTok::AnyOne);
            i += 1;
        } else if c == '[' && i + 1 < chars.len() {
            // Class parse: `^` negates only at the start; a `]` before any
            // member is a literal member. ESCAPE does not apply inside
            // classes (T-SQL semantics).
            let mut j = i + 1;
            let mut neg = false;
            if chars[j] == '^' {
                neg = true;
                j += 1;
            }
            let mut ranges: Vec<(char, char)> = Vec::new();
            let mut first = true;
            let mut closed = false;
            while j < chars.len() {
                if chars[j] == ']' && !first {
                    closed = true;
                    j += 1;
                    break;
                }
                first = false;
                if j + 2 < chars.len() && chars[j + 1] == '-' && chars[j + 2] != ']' {
                    ranges.push((chars[j], chars[j + 2]));
                    j += 3;
                } else {
                    ranges.push((chars[j], chars[j]));
                    j += 1;
                }
            }
            if closed && !ranges.is_empty() {
                toks.push(LikeTok::Class { neg, ranges });
                i = j;
            } else {
                toks.push(LikeTok::Literal('['));
                i += 1;
            }
        } else {
            toks.push(LikeTok::Literal(c));
            i += 1;
        }
    }
    toks
}

// ---------------------------------------------------------------------------
// Date/time core (UTC milliseconds, T-SQL dateparts)
// ---------------------------------------------------------------------------

const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

#[derive(Debug, Clone, Copy, PartialEq)]
enum DatePart {
    Year,
    Quarter,
    Month,
    DayOfYear,
    Day,
    Week,
    Weekday,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// Resolve a T-SQL datepart name (all documented abbreviations).
fn datepart_code(name: &str) -> Res<DatePart> {
    use DatePart::*;
    let p = match name.trim().to_ascii_lowercase().as_str() {
        "year" | "yy" | "yyyy" => Year,
        "quarter" | "qq" | "q" => Quarter,
        "month" | "mm" | "m" => Month,
        "dayofyear" | "dy" | "y" => DayOfYear,
        "day" | "dd" | "d" => Day,
        "week" | "wk" | "ww" => Week,
        "weekday" | "dw" => Weekday,
        "hour" | "hh" => Hour,
        "minute" | "mi" | "n" => Minute,
        "second" | "ss" | "s" => Second,
        "millisecond" | "ms" => Millisecond,
        "microsecond" | "mcs" => Microsecond,
        "nanosecond" | "ns" => Nanosecond,
        other => return err(format!("unknown datepart {other:?}")),
    };
    Ok(p)
}

/// Calendar breakdown of a UTC-millisecond instant.
struct TsParts {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
    /// 1-based day of the year.
    doy: i64,
    /// T-SQL weekday: Sunday = 1 … Saturday = 7.
    weekday: i64,
    /// Days since the Unix epoch.
    days: i64,
}

fn ts_parts(ms: i64) -> TsParts {
    let secs = ms.div_euclid(1000);
    let millisecond = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let doy = days - days_from_civil(year, 1, 1) + 1;
    let weekday = (days + 4).rem_euclid(7) + 1; // 1970-01-01 was a Thursday
    TsParts {
        year,
        month: month as i64,
        day: day as i64,
        hour: rem / 3600,
        minute: (rem % 3600) / 60,
        second: rem % 60,
        millisecond,
        doy,
        weekday,
        days,
    }
}

fn date_ovf() -> SqlError {
    SqlError::Message("date arithmetic overflow".into())
}

fn add64(a: i64, b: i64) -> Res<i64> {
    a.checked_add(b).ok_or_else(date_ovf)
}

fn mul64(a: i64, b: i64) -> Res<i64> {
    a.checked_mul(b).ok_or_else(date_ovf)
}

/// Build a UTC-millisecond instant from calendar parts, validating ranges
/// and the engine's timestamp domain (out-of-domain instants have no
/// canonical text form and cannot be journaled or replayed).
fn ms_from_parts(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
) -> Res<i64> {
    if !(1..=12).contains(&month) {
        return err(format!("month {month} out of range"));
    }
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return err("time component out of range");
    }
    if !(0..=999).contains(&millisecond) {
        return err("millisecond component out of range");
    }
    if !(1..=31).contains(&day) {
        return err(format!("day {day} out of range"));
    }
    let days = days_from_civil(year, month as u32, day as u32);
    // Round-trip rejects nonexistent calendar days (Feb 30 etc.).
    if civil_from_days(days) != (year, month as u32, day as u32) {
        return err(format!("day {day} does not exist in month {month}"));
    }
    let secs = add64(mul64(days, 86_400)?, hour * 3600 + minute * 60 + second)?;
    let ms = add64(mul64(secs, 1000)?, millisecond)?;
    if !crate::value::is_valid_timestamp_ms(ms) {
        return err("date out of the supported range (0001-01-01..9999-12-31)");
    }
    Ok(ms)
}

/// Coerce a function argument to a timestamp (T-SQL date functions accept
/// text dates). `Ok(None)` = NULL propagation.
fn ts_arg(v: &Value, fn_name: &str) -> Res<Option<i64>> {
    match v {
        Value::Null => Ok(None),
        Value::Timestamp(ms) => Ok(Some(*ms)),
        Value::Int(ms) => {
            if crate::value::is_valid_timestamp_ms(*ms) {
                Ok(Some(*ms))
            } else {
                err(format!(
                    "cannot interpret {ms} as a timestamp in {fn_name}: out of range"
                ))
            }
        }
        Value::Str(s) => match parse_timestamp_ms(s) {
            Some(ms) => Ok(Some(ms)),
            None => err(format!(
                "cannot convert string {s:?} to a date/time value in {fn_name}"
            )),
        },
        other => err(format!(
            "{fn_name} requires a date/time value, got {}",
            other.type_name()
        )),
    }
}

/// Integer argument (`Ok(None)` = NULL propagation).
fn int_arg(v: &Value, fn_name: &str, arg: usize) -> Res<Option<i64>> {
    match v {
        Value::Null => Ok(None),
        Value::Int(i) => Ok(Some(*i)),
        other => err(format!(
            "function {fn_name}: argument {} must be an integer, got {}",
            arg + 1,
            other.type_name()
        )),
    }
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap {
                29
            } else {
                28
            }
        }
    }
}

fn dateadd(part: DatePart, n: i64, ms: i64) -> Res<i64> {
    use DatePart::*;
    let p = ts_parts(ms);
    let out = match part {
        Year | Quarter | Month => {
            let step = if part == Year {
                12
            } else if part == Quarter {
                3
            } else {
                1
            };
            let total = add64(add64(mul64(p.year, 12)?, p.month - 1)?, mul64(n, step)?)?;
            let (y, m) = (total.div_euclid(12), total.rem_euclid(12) + 1);
            // Clamp the day: Jan 31 + 1 month lands on Feb 28/29 (T-SQL).
            let d = p.day.min(days_in_month(y, m));
            ms_from_parts(y, m, d, p.hour, p.minute, p.second, p.millisecond)?
        }
        Day | DayOfYear | Week | Weekday => {
            let step = if part == Week { 7 } else { 1 };
            add64(ms, mul64(mul64(n, step)?, 86_400_000)?)?
        }
        Hour => add64(ms, mul64(n, 3_600_000)?)?,
        Minute => add64(ms, mul64(n, 60_000)?)?,
        Second => add64(ms, mul64(n, 1_000)?)?,
        Millisecond => add64(ms, n)?,
        // Sub-millisecond precision: apply whole milliseconds only.
        Microsecond => add64(ms, n.div_euclid(1000))?,
        Nanosecond => add64(ms, n.div_euclid(1_000_000))?,
    };
    if !crate::value::is_valid_timestamp_ms(out) {
        return err("DATEADD result out of the supported timestamp range");
    }
    Ok(out)
}

fn datepart_value(part: DatePart, ms: i64) -> i64 {
    use DatePart::*;
    let p = ts_parts(ms);
    match part {
        Year => p.year,
        Quarter => (p.month - 1) / 3 + 1,
        Month => p.month,
        DayOfYear => p.doy,
        Day => p.day,
        Week => {
            // T-SQL wk: week 1 contains Jan 1, a new week starts on Sunday.
            let jan1_wd = (p.days - (p.doy - 1) + 4).rem_euclid(7);
            (p.doy - 1 + jan1_wd) / 7 + 1
        }
        Weekday => p.weekday,
        Hour => p.hour,
        Minute => p.minute,
        Second => p.second,
        Millisecond => p.millisecond,
        Microsecond => p.millisecond * 1000,
        Nanosecond => p.millisecond * 1_000_000,
    }
}

fn datediff(part: DatePart, a: i64, b: i64) -> Res<i64> {
    use DatePart::*;
    // T-SQL DATEDIFF counts crossed *boundaries*, not elapsed units:
    // DATEDIFF(year, '2025-12-31', '2026-01-01') = 1.
    let (pa, pb) = (ts_parts(a), ts_parts(b));
    let diff = match part {
        Year => pb.year - pa.year,
        Quarter => (pb.year * 4 + (pb.month - 1) / 3) - (pa.year * 4 + (pa.month - 1) / 3),
        Month => (pb.year * 12 + pb.month) - (pa.year * 12 + pa.month),
        Day | DayOfYear => pb.days - pa.days,
        // Week boundaries are Sundays; weekday diffs count the same.
        Week | Weekday => {
            let sun = |p: &TsParts| p.days - (p.weekday - 1);
            (sun(&pb) - sun(&pa)) / 7
        }
        Hour => b.div_euclid(3_600_000) - a.div_euclid(3_600_000),
        Minute => b.div_euclid(60_000) - a.div_euclid(60_000),
        Second => b.div_euclid(1_000) - a.div_euclid(1_000),
        Millisecond => b - a,
        Microsecond => mul64(b - a, 1000)?,
        Nanosecond => mul64(b - a, 1_000_000)?,
    };
    Ok(diff)
}

// ---------------------------------------------------------------------------
// Scalar function families
// ---------------------------------------------------------------------------

fn text_arg(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        other => Some(crate::engine::value_to_text(other)),
    }
}

/// A large but finite cap for string builders driven by an integer length
/// argument (REPLICATE/SPACE): engine documents cap out at 16 MiB, so a
/// 4 MiB character budget sits far above every legitimate use while
/// keeping a hostile length from aborting the process on allocation.
const MAX_BUILD_CHARS: usize = 4 * 1024 * 1024;

/// Dispatch a T-SQL scalar function. `None` = not a T-SQL function (the
/// engine falls through to its unknown-function error); `Some(Err)` is a
/// loud T-SQL boundary (session functions, RAND, unsupported styles…).
#[allow(clippy::too_many_lines)]
pub fn scalar(name: &str, args: &[Value]) -> Option<Res<Value>> {
    if !is_tsql_scalar_name(name) {
        return None;
    }
    Some(scalar_impl(name, args))
}

/// The T-SQL function surface `scalar_impl` carries; everything else falls
/// through to the engine's unknown-function error.
fn is_tsql_scalar_name(name: &str) -> bool {
    matches!(
        name,
        "__TSQL_CONVERT__"
            | "__TSQL_TRY_CONVERT__"
            | "__TSQL_PARSE__"
            | "__TSQL_TRY_PARSE__"
            | "GETDATE"
            | "GETUTCDATE"
            | "SYSDATETIME"
            | "SYSUTCDATETIME"
            | "SYSDATETIMEOFFSET"
            | "DATEADD"
            | "DATEDIFF"
            | "DATEDIFF_BIG"
            | "DATEPART"
            | "DATENAME"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "DAYOFYEAR"
            | "EOMONTH"
            | "DATEFROMPARTS"
            | "DATETIMEFROMPARTS"
            | "SMALLDATETIMEFROMPARTS"
            | "DATETIME2FROMPARTS"
            | "DATETIMEOFFSETFROMPARTS"
            | "TIMEFROMPARTS"
            | "ISDATE"
            | "ISNUMERIC"
            | "LEFT"
            | "RIGHT"
            | "CHARINDEX"
            | "REPLACE"
            | "REPLICATE"
            | "REVERSE"
            | "SPACE"
            | "STR"
            | "QUOTENAME"
            | "ASCII"
            | "CHAR"
            | "NCHAR"
            | "UNICODE"
            | "CONCAT_WS"
            | "TRANSLATE"
            | "STUFF"
            | "STRING_ESCAPE"
            | "FORMAT"
            | "FLOOR"
            | "CEILING"
            | "POWER"
            | "SQRT"
            | "SQUARE"
            | "EXP"
            | "SIN"
            | "COS"
            | "TAN"
            | "COT"
            | "ASIN"
            | "ACOS"
            | "ATAN"
            | "LOG10"
            | "DEGREES"
            | "RADIANS"
            | "LOG"
            | "ATN2"
            | "PI"
            | "SIGN"
            | "RAND"
            | "IIF"
            | "CHOOSE"
            | "NEWID"
            | "NEWSEQUENTIALID"
            | "DB_NAME"
            | "DB_ID"
            | "SERVERPROPERTY"
            | "CHECKSUM"
            | "BINARY_CHECKSUM"
            | "HASHBYTES"
            | "HOST_NAME"
            | "SUSER_SNAME"
            | "SUSER_SID"
            | "APP_NAME"
            | "USER_NAME"
            | "SESSION_USER"
            | "SYSTEM_USER"
            | "ORIGINAL_LOGIN"
            | "SCOPE_IDENTITY"
            | "CURRENT_USER"
    )
}

fn scalar_impl(name: &str, args: &[Value]) -> Res<Value> {
    match name {
        // ---- marker functions emitted by `preprocess` ----
        "__TSQL_CONVERT__" => convert_call(args, false),
        "__TSQL_TRY_CONVERT__" => convert_call(args, true),
        "__TSQL_PARSE__" => parse_call(args, false),
        "__TSQL_TRY_PARSE__" => parse_call(args, true),

        // ---- date/time ----
        "GETDATE" | "GETUTCDATE" | "SYSDATETIME" | "SYSUTCDATETIME" | "SYSDATETIMEOFFSET" => {
            if !args.is_empty() {
                return err(format!("function {name} takes no arguments"));
            }
            // DocSQL keeps UTC milliseconds only; GETDATE/SYSDATETIME are
            // documented as UTC here (no local-time zone machinery).
            Ok(Value::Timestamp(crate::now_ms() as i64))
        }
        "DATEADD" => three_arg_date(args, name, |p, n, ms| {
            dateadd(p, n, ms).map(Value::Timestamp)
        }),
        "DATEDIFF" | "DATEDIFF_BIG" => match args {
            [p, a, b] => {
                // (datepart, startdate, enddate) — both dates, unlike
                // DATEADD's (datepart, number, date).
                let Some(pname) = text_arg(p) else {
                    return Ok(Value::Null);
                };
                let part = datepart_code(&pname)?;
                let (Some(a), Some(b)) = (ts_arg(a, name)?, ts_arg(b, name)?) else {
                    return Ok(Value::Null);
                };
                datediff(part, a, b).map(Value::Int)
            }
            _ => err(format!(
                "function {name} takes 3 arguments (datepart, startdate, enddate)"
            )),
        },
        "DATEPART" => two_arg_datepart(args, name, |p, ms| Ok(Value::Int(datepart_value(p, ms)))),
        "DATENAME" => two_arg_datepart(args, name, |p, ms| {
            Ok(match p {
                DatePart::Month => Value::Str(
                    MONTH_NAMES[(datepart_value(DatePart::Month, ms) - 1) as usize].into(),
                ),
                DatePart::Weekday => Value::Str(
                    DAY_NAMES[(datepart_value(DatePart::Weekday, ms) - 1) as usize].into(),
                ),
                other => Value::Str(datepart_value(other, ms).to_string()),
            })
        }),
        "YEAR" | "MONTH" | "DAY" | "DAYOFYEAR" => {
            let part = match name {
                "YEAR" => DatePart::Year,
                "MONTH" => DatePart::Month,
                "DAYOFYEAR" => DatePart::DayOfYear,
                _ => DatePart::Day,
            };
            match args {
                [v] => match ts_arg(v, name)? {
                    Some(ms) => Ok(Value::Int(datepart_value(part, ms))),
                    None => Ok(Value::Null),
                },
                _ => err(format!("function {name} takes 1 argument")),
            }
        }
        "EOMONTH" => match args {
            [v] => eomonth(v, 0),
            [v, o] => match int_arg(o, name, 1)? {
                Some(n) => eomonth(v, n),
                None => Ok(Value::Null),
            },
            _ => err("function EOMONTH takes 1 or 2 arguments"),
        },
        "DATEFROMPARTS" => from_parts_call(name, args, [None, None, None, None]),
        "DATETIMEFROMPARTS" => from_parts_call(name, args, [Some(3), Some(4), Some(5), Some(6)]),
        "SMALLDATETIMEFROMPARTS" => from_parts_call(name, args, [Some(3), Some(4), None, None]),
        "DATETIME2FROMPARTS" | "DATETIMEOFFSETFROMPARTS" => {
            // (y, m, d, h, mi, s, fractions[, precision]) — the offset
            // variant carries two offset args before precision; both are
            // ignored (UTC-only engine, documented).
            let min_args = if name == "DATETIME2FROMPARTS" { 7 } else { 9 };
            let prec_idx = if name == "DATETIME2FROMPARTS" { 7 } else { 9 };
            if args.len() < min_args {
                return err(format!(
                    "function {name} takes at least {min_args} arguments"
                ));
            }
            let (frac_ms, ok) = frac_to_ms(name, args, prec_idx)?;
            if !ok {
                return Ok(Value::Null);
            }
            from_parts_call(name, &args[..6], [Some(3), Some(4), Some(5), None]).and_then(|v| {
                match v {
                    Value::Timestamp(ms) => Ok(Value::Timestamp(add64(ms, frac_ms)?)),
                    other => Ok(other),
                }
            })
        }
        "TIMEFROMPARTS" => {
            err("TIMEFROMPARTS is not supported: the engine has no TIME-of-day type")
        }
        "ISDATE" => match args {
            [v] => Ok(Value::Int(match v {
                Value::Timestamp(_) => 1,
                Value::Str(s) => parse_timestamp_ms(s).map(|_| 1).unwrap_or(0),
                // Integers are epoch milliseconds elsewhere in the date
                // family (YEAR(0) works) — ISDATE must agree.
                Value::Int(ms) => i64::from(crate::value::is_valid_timestamp_ms(*ms)),
                _ => 0,
            })),
            _ => err("ISDATE takes 1 argument"),
        },
        "ISNUMERIC" => match args {
            [v] => Ok(Value::Int(is_numeric(v))),
            _ => err("ISNUMERIC takes 1 argument"),
        },

        // ---- string ----
        "LEFT" | "RIGHT" => match args {
            [v, n] => {
                let (Some(s), Some(n)) = (text_arg(v), int_arg(n, name, 1)?) else {
                    return Ok(Value::Null);
                };
                if n < 0 {
                    return err(format!("Invalid length parameter in {name}: {n}"));
                }
                let chars: Vec<char> = s.chars().collect();
                let take = (n as usize).min(chars.len());
                let out = if name == "LEFT" {
                    chars[..take].iter().collect::<String>()
                } else {
                    chars[chars.len() - take..].iter().collect::<String>()
                };
                Ok(Value::Str(out))
            }
            _ => err(format!("function {name} takes 2 arguments")),
        },
        "CHARINDEX" => match args {
            [needle, hay] => charindex(needle, hay, None),
            [needle, hay, start] => charindex(needle, hay, int_arg(start, name, 2)?),
            _ => err("CHARINDEX takes 2 or 3 arguments"),
        },
        "REPLACE" => match args {
            [s, from, to] => {
                let (Some(s), Some(from), Some(to)) = (text_arg(s), text_arg(from), text_arg(to))
                else {
                    return Ok(Value::Null);
                };
                if from.is_empty() {
                    return Ok(Value::Str(s));
                }
                Ok(Value::Str(s.replace(&from, &to)))
            }
            _ => err("REPLACE takes 3 arguments"),
        },
        "REPLICATE" => match args {
            [s, n] => {
                let (Some(s), Some(n)) = (text_arg(s), int_arg(n, name, 1)?) else {
                    return Ok(Value::Null);
                };
                if n < 0 {
                    return err(format!("Invalid length parameter in REPLICATE: {n}"));
                }
                let total = s.chars().count().saturating_mul(n as usize);
                if total > MAX_BUILD_CHARS {
                    return err("REPLICATE result exceeds the supported maximum length");
                }
                Ok(Value::Str(s.repeat(n as usize)))
            }
            _ => err("REPLICATE takes 2 arguments"),
        },
        "REVERSE" => match args {
            [v] => Ok(match text_arg(v) {
                Some(s) => Value::Str(s.chars().rev().collect()),
                None => Value::Null,
            }),
            _ => err("REVERSE takes 1 argument"),
        },
        "SPACE" => match args {
            [n] => {
                let Some(n) = int_arg(n, name, 0)? else {
                    return Ok(Value::Null);
                };
                if n < 0 {
                    return err(format!("Invalid length parameter in SPACE: {n}"));
                }
                if n as usize > MAX_BUILD_CHARS {
                    return err("SPACE result exceeds the supported maximum length");
                }
                Ok(Value::Str(" ".repeat(n as usize)))
            }
            _ => err("SPACE takes 1 argument"),
        },
        "STR" => str_fn(args),
        "QUOTENAME" => match args {
            [v] => quotename(v, None),
            [v, d] => quotename(v, text_arg(d).as_deref()),
            _ => err("QUOTENAME takes 1 or 2 arguments"),
        },
        "ASCII" => match args {
            [v] => Ok(match text_arg(v) {
                // First byte of the first UTF-8 character (T-SQL truncates
                // non-ASCII input to its leading byte).
                Some(s) => Value::Int(s.as_bytes().first().map(|b| *b as i64).unwrap_or(0)),
                None => Value::Null,
            }),
            _ => err("ASCII takes 1 argument"),
        },
        "CHAR" => match args {
            [n] => {
                let Some(n) = int_arg(n, name, 0)? else {
                    return Ok(Value::Null);
                };
                Ok(match u8::try_from(n) {
                    Ok(b) => Value::Str((b as char).to_string()), // Latin-1, like T-SQL
                    Err(_) => Value::Null,
                })
            }
            _ => err("CHAR takes 1 argument"),
        },
        "NCHAR" => match args {
            [n] => {
                let Some(n) = int_arg(n, name, 0)? else {
                    return Ok(Value::Null);
                };
                Ok(match u32::try_from(n).ok().and_then(char::from_u32) {
                    Some(c) => Value::Str(c.to_string()),
                    None => Value::Null,
                })
            }
            _ => err("NCHAR takes 1 argument"),
        },
        "UNICODE" => match args {
            [v] => Ok(match text_arg(v).and_then(|s| s.chars().next()) {
                Some(c) => Value::Int(c as u32 as i64),
                None => Value::Null,
            }),
            _ => err("UNICODE takes 1 argument"),
        },
        "CONCAT_WS" => {
            if args.len() < 2 {
                return err("CONCAT_WS takes at least 2 arguments");
            }
            // T-SQL: NULL separator → NULL; NULL values are skipped.
            let Some(sep) = text_arg(&args[0]) else {
                return Ok(Value::Null);
            };
            let parts: Vec<String> = args[1..].iter().filter_map(text_arg).collect();
            Ok(Value::Str(parts.join(&sep)))
        }
        "TRANSLATE" => match args {
            [s, from, to] => {
                let (Some(s), Some(from), Some(to)) = (text_arg(s), text_arg(from), text_arg(to))
                else {
                    return Ok(Value::Null);
                };
                if from.chars().count() != to.chars().count() {
                    return err("TRANSLATE requires matching from/to lengths");
                }
                let map: std::collections::BTreeMap<char, char> =
                    from.chars().zip(to.chars()).collect();
                Ok(Value::Str(
                    s.chars().map(|c| *map.get(&c).unwrap_or(&c)).collect(),
                ))
            }
            _ => err("TRANSLATE takes 3 arguments"),
        },
        "STUFF" => match args {
            [s, start, len, ins] => {
                let (Some(s), Some(start), Some(len), Some(ins)) = (
                    text_arg(s),
                    int_arg(start, name, 1)?,
                    int_arg(len, name, 2)?,
                    text_arg(ins),
                ) else {
                    return Ok(Value::Null);
                };
                let chars: Vec<char> = s.chars().collect();
                // Out-of-range parameters yield NULL (T-SQL).
                if start < 1 || len < 0 || (start as usize) > chars.len() + 1 {
                    return Ok(Value::Null);
                }
                let start = (start - 1) as usize;
                let end = (start + len as usize).min(chars.len());
                let mut out: String = chars[..start].iter().collect();
                out.push_str(&ins);
                out.extend(&chars[end..]);
                Ok(Value::Str(out))
            }
            _ => err("STUFF takes 4 arguments"),
        },
        "STRING_ESCAPE" => match args {
            [v, kind] => {
                let (Some(s), Some(kind)) = (text_arg(v), text_arg(kind)) else {
                    return Ok(Value::Null);
                };
                if !kind.eq_ignore_ascii_case("json") {
                    return err("STRING_ESCAPE supports only the 'json' escaping type");
                }
                Ok(Value::Str(string_escape_json(&s)))
            }
            _ => err("STRING_ESCAPE takes 2 arguments"),
        },
        "FORMAT" => format_fn(args),

        // ---- math ----
        "FLOOR" | "CEILING" => match args {
            [v] => Ok(match v {
                Value::Null => Value::Null,
                Value::Int(i) => Value::Int(*i),
                Value::Float(f) => Value::Float(if name == "FLOOR" { f.floor() } else { f.ceil() }),
                Value::Decimal(d) => {
                    Value::Decimal(if name == "FLOOR" { d.floor() } else { d.ceil() })
                }
                other => return err(format!("{name} of non-numeric: {}", other.type_name())),
            }),
            _ => err(format!("function {name} takes 1 argument")),
        },
        "POWER" => match args {
            [a, b] => {
                let (Some(a), Some(b)) = (num_arg(a), num_arg(b)) else {
                    return Ok(Value::Null);
                };
                match (a, b) {
                    (Num::Int(a), Num::Int(b)) if (0..=63).contains(&b) => {
                        match a.checked_pow(b as u32) {
                            Some(v) => Ok(Value::Int(v)),
                            None => err("POWER overflow"),
                        }
                    }
                    (a, b) => Ok(Value::Float(a.as_f64().powf(b.as_f64()))),
                }
            }
            _ => err("POWER takes 2 arguments"),
        },
        "SQRT" | "SQUARE" | "EXP" | "SIN" | "COS" | "TAN" | "COT" | "ASIN" | "ACOS" | "ATAN"
        | "LOG10" | "DEGREES" | "RADIANS" => match args {
            [v] => {
                let Some(x) = num_arg(v) else {
                    return Ok(Value::Null);
                };
                let x = x.as_f64();
                let out = match name {
                    "SQRT" => {
                        if x < 0.0 {
                            return Ok(Value::Null); // T-SQL returns NULL
                        }
                        x.sqrt()
                    }
                    "SQUARE" => x * x,
                    "EXP" => x.exp(),
                    "SIN" => x.sin(),
                    "COS" => x.cos(),
                    "TAN" => x.tan(),
                    "COT" => {
                        let t = 1.0 / x.tan();
                        if t.is_finite() {
                            t
                        } else {
                            return Ok(Value::Null);
                        }
                    }
                    "ASIN" => {
                        if !(-1.0..=1.0).contains(&x) {
                            return Ok(Value::Null);
                        }
                        x.asin()
                    }
                    "ACOS" => {
                        if !(-1.0..=1.0).contains(&x) {
                            return Ok(Value::Null);
                        }
                        x.acos()
                    }
                    "ATAN" => x.atan(),
                    "LOG10" => {
                        if x <= 0.0 {
                            return Ok(Value::Null);
                        }
                        x.log10()
                    }
                    "DEGREES" => x.to_degrees(),
                    _ => x.to_radians(),
                };
                Ok(Value::Float(out))
            }
            _ => err(format!("function {name} takes 1 argument")),
        },
        "LOG" => match args {
            [v] => log_fn(v, None),
            [v, base] => log_fn(v, Some(base)),
            _ => err("LOG takes 1 or 2 arguments"),
        },
        "ATN2" => match args {
            [y, x] => {
                let (Some(y), Some(x)) = (num_arg(y), num_arg(x)) else {
                    return Ok(Value::Null);
                };
                Ok(Value::Float(y.as_f64().atan2(x.as_f64())))
            }
            _ => err("ATN2 takes 2 arguments"),
        },
        "PI" => {
            if !args.is_empty() {
                return err("PI takes no arguments");
            }
            Ok(Value::Float(std::f64::consts::PI))
        }
        "SIGN" => match args {
            [v] => Ok(match v {
                Value::Null => Value::Null,
                Value::Int(i) => Value::Int(i.signum()),
                Value::Float(f) => Value::Int(if *f > 0.0 {
                    1
                } else if *f < 0.0 {
                    -1
                } else {
                    0
                }),
                Value::Decimal(d) => Value::Int(if d.is_zero() {
                    0
                } else if d.is_sign_negative() {
                    -1
                } else {
                    1
                }),
                other => return err(format!("SIGN of non-numeric: {}", other.type_name())),
            }),
            _ => err("SIGN takes 1 argument"),
        },
        "RAND" => {
            if !args.is_empty() {
                return err("RAND() takes no arguments");
            }
            // Read-side RAND is safe (reads never replicate); the WRITE
            // path literalizes it like NEWID (exec_insert) and UPDATE/
            // DELETE/MERGE refuse it (stmt_calls_newid covers RAND).
            Ok(Value::Float(rand_unit()))
        }

        // ---- logical ----
        "IIF" => match args {
            [c, a, b] => Ok(match c {
                Value::Bool(true) => a.clone(),
                // T-SQL treats a NULL condition as false…
                Value::Null => b.clone(),
                // …but a non-boolean condition is a type error, not a
                // silent else-branch.
                other => {
                    return err(format!(
                        "IIF condition must be a boolean, got {}",
                        other.type_name()
                    ))
                }
            }),
            _ => err("IIF takes 3 arguments"),
        },
        "CHOOSE" => {
            if args.len() < 2 {
                return err("CHOOSE takes at least 2 arguments");
            }
            match int_arg(&args[0], name, 0)? {
                Some(i) if i >= 1 && (i as usize) < args.len() => Ok(args[i as usize].clone()),
                _ => Ok(Value::Null),
            }
        }

        // ---- identifiers / metadata ----
        "NEWID" | "NEWSEQUENTIALID" => {
            if !args.is_empty() {
                return err(format!("function {name} takes no arguments"));
            }
            // Time-ordered UUIDv7 for both names — the engine's native id
            // shape (NEWSEQUENTIALID's "sequential" promise included).
            Ok(Value::Str(crate::guid::uuidv7()))
        }
        "DB_NAME" => match args {
            [] => Ok(Value::Str("docsql".into())),
            [Value::Null] => Ok(Value::Null),
            [v] => Ok(match text_arg(v).as_deref() {
                Some(n) if n.eq_ignore_ascii_case("docsql") => Value::Str("docsql".into()),
                _ => Value::Null,
            }),
            _ => err("DB_NAME takes at most 1 argument"),
        },
        "DB_ID" => match args {
            [] => Ok(Value::Int(1)),
            [Value::Null] => Ok(Value::Null),
            [v] => Ok(match text_arg(v).as_deref() {
                Some(n) if n.eq_ignore_ascii_case("docsql") => Value::Int(1),
                _ => Value::Null,
            }),
            _ => err("DB_ID takes at most 1 argument"),
        },
        "SERVERPROPERTY" => match args {
            [v] => Ok(
                match text_arg(v).as_deref().map(|s| s.to_ascii_lowercase()) {
                    Some(ref k) if k == "edition" => Value::Str("DocSQL Engine".into()),
                    Some(ref k) if k == "engineedition" => Value::Int(5),
                    Some(ref k) if k == "productversion" => {
                        Value::Str(env!("CARGO_PKG_VERSION").into())
                    }
                    Some(ref k) if k == "productlevel" => Value::Str("RTM".into()),
                    // Unknown properties return NULL, like SQL Server.
                    _ => Value::Null,
                },
            ),
            _ => err("SERVERPROPERTY takes 1 argument"),
        },

        // ---- hashes ----
        "CHECKSUM" | "BINARY_CHECKSUM" => {
            if args.is_empty() {
                return err(format!("function {name} requires at least 1 argument"));
            }
            // FNV-1a over the canonical value renderings — a stable content
            // hash for equality checks inside this engine (documented:
            // values differ from SQL Server's CHECKSUM).
            let mut h: u64 = 0xcbf2_9ce4_8422_2325;
            for v in args {
                let text = crate::engine::value_literal(v)?;
                for byte in text.as_bytes() {
                    h ^= *byte as u64;
                    h = h.wrapping_mul(0x0000_0100_0000_01b3);
                }
                h ^= 0x1f;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            Ok(Value::Int(h as i64))
        }
        "HASHBYTES" => match args {
            [algo, v] => {
                let (Some(algo), Some(data)) = (
                    text_arg(algo),
                    match v {
                        Value::Null => None,
                        Value::Bytes(b) => Some(b.clone()),
                        other => Some(crate::engine::value_to_text(other).into_bytes()),
                    },
                ) else {
                    return Ok(Value::Null);
                };
                let bytes = match algo.to_ascii_uppercase().as_str() {
                    "MD5" => md5(&data),
                    "SHA" | "SHA1" => sha1(&data),
                    "SHA2_256" | "SHA256" => sha256(&data),
                    "SHA2_512" | "SHA512" => {
                        return err("HASHBYTES SHA2_512 is not supported (use SHA2_256)")
                    }
                    other => {
                        return err(format!("HASHBYTES algorithm {other:?} is not supported"));
                    }
                };
                Ok(Value::Bytes(bytes))
            }
            _ => err("HASHBYTES takes 2 arguments"),
        },

        // ---- loud session-context boundaries ----
        "HOST_NAME" | "SUSER_SNAME" | "SUSER_SID" | "APP_NAME" | "USER_NAME" | "SESSION_USER"
        | "SYSTEM_USER" | "ORIGINAL_LOGIN" | "SCOPE_IDENTITY" | "CURRENT_USER" => err(format!(
            "session-context function {name}() is not supported: the engine has no \
                 per-connection identity state",
        )),

        _ => unreachable!("filtered by is_tsql_scalar_name: {name}"),
    }
}

fn string_escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn is_numeric(v: &Value) -> i64 {
    match v {
        Value::Int(_) | Value::Float(_) | Value::Decimal(_) | Value::Bool(_) => 1,
        Value::Str(s) => {
            // T-SQL accepts digits, one decimal point, sign, $ , + - and
            // scientific notation.
            let t = s.trim();
            let body = t
                .trim_start_matches(['$', '+', '-', ','])
                .trim_end_matches([',', '+', '-']);
            if body.is_empty() {
                return 0;
            }
            let mut seen_digit = false;
            let mut seen_dot = false;
            for (i, c) in body.char_indices() {
                match c {
                    '0'..='9' => seen_digit = true,
                    '.' if !seen_dot => seen_dot = true,
                    'e' | 'E' if i > 0 => {}
                    ',' => {}
                    '+' | '-'
                        if i > 0 && matches!(body.as_bytes().get(i - 1), Some(b'e' | b'E')) => {}
                    _ => return 0,
                }
            }
            i64::from(seen_digit)
        }
        _ => 0,
    }
}

enum Num {
    Int(i64),
    Float(f64),
}

impl Num {
    fn as_f64(&self) -> f64 {
        match self {
            Num::Int(i) => *i as f64,
            Num::Float(f) => *f,
        }
    }
}

fn num_arg(v: &Value) -> Option<Num> {
    match v {
        Value::Null => None,
        Value::Int(i) => Some(Num::Int(*i)),
        Value::Float(f) => Some(Num::Float(*f)),
        Value::Decimal(d) => Some(Num::Float(d.to_f64().unwrap_or(f64::NAN))),
        Value::Str(s) => s.trim().parse::<f64>().ok().map(Num::Float),
        _ => None,
    }
}

fn log_fn(v: &Value, base: Option<&Value>) -> Res<Value> {
    let (Some(x), base) = (num_arg(v), base.and_then(num_arg).map(|b| b.as_f64())) else {
        return Ok(Value::Null);
    };
    let x = x.as_f64();
    if x <= 0.0 {
        return Ok(Value::Null);
    }
    Ok(Value::Float(match base {
        None => x.ln(),
        Some(b) if b <= 0.0 || b == 1.0 => return err("LOG base must be positive and not 1"),
        Some(b) => x.ln() / b.ln(),
    }))
}

fn charindex(needle: &Value, hay: &Value, start: Option<i64>) -> Res<Value> {
    let (Some(needle), Some(hay)) = (text_arg(needle), text_arg(hay)) else {
        return Ok(Value::Null);
    };
    let start = start.unwrap_or(1);
    if start < 1 {
        return Ok(Value::Int(0));
    }
    let hay_chars: Vec<char> = hay.chars().collect();
    if (start as usize) > hay_chars.len() {
        return Ok(Value::Int(0));
    }
    let from: String = hay_chars[(start - 1) as usize..].iter().collect();
    Ok(Value::Int(
        from.find(&needle)
            .map(|byte_idx| from[..byte_idx].chars().count() as i64 + start)
            .unwrap_or(0),
    ))
}

fn str_fn(args: &[Value]) -> Res<Value> {
    if args.is_empty() || args.len() > 3 {
        return err("STR takes 1 to 3 arguments");
    }
    let x = match num_arg(&args[0]) {
        Some(n) => n.as_f64(),
        None => return Ok(Value::Null),
    };
    let len = match args.get(1) {
        Some(v) => match int_arg(v, "STR", 1)? {
            Some(n) if n >= 0 => n as usize,
            Some(_) => return err("Invalid length parameter in STR"),
            None => return Ok(Value::Null),
        },
        None => 10,
    };
    // Rust's format width only holds u16 and `"*"..repeat` would happily
    // allocate gigabytes — a hostile length must fail loudly, same rule as
    // REPLICATE/SPACE's MAX_BUILD_CHARS.
    if len > 65535 {
        return err("Invalid length parameter in STR (max 65535)");
    }
    let dec = match args.get(2) {
        Some(v) => match int_arg(v, "STR", 2)? {
            Some(n) if (0..=16).contains(&n) => n as usize,
            Some(_) => return err("Invalid decimal parameter in STR"),
            None => return Ok(Value::Null),
        },
        None => 0,
    };
    let formatted = format!("{:.*}", dec, x);
    let out = if formatted.chars().count() > len {
        "*".repeat(len)
    } else {
        format!("{formatted:>len$}")
    };
    Ok(Value::Str(out))
}

fn quotename(v: &Value, delim: Option<&str>) -> Res<Value> {
    let Some(s) = text_arg(v) else {
        return Ok(Value::Null);
    };
    if s.chars().count() > 128 {
        return Ok(Value::Null); // T-SQL: NULL when the input exceeds 128 chars
    }
    let delim = delim.unwrap_or("[");
    let out = match delim {
        "[" => format!("[{}]", s.replace(']', "]]")),
        "'" => format!("'{}'", s.replace('\'', "''")),
        "\"" => format!("\"{}\"", s.replace('"', "\"\"")),
        "(" => format!("({s})"),
        ")" => format!("({s})"), // ')' pairs with '(' per the T-SQL doc table
        _ => return Ok(Value::Null),
    };
    Ok(Value::Str(out))
}

fn eomonth(v: &Value, months: i64) -> Res<Value> {
    let Some(ms) = ts_arg(v, "EOMONTH")? else {
        return Ok(Value::Null);
    };
    let p = ts_parts(ms);
    let total = add64(add64(mul64(p.year, 12)?, p.month - 1)?, months)?;
    let (y, m) = (total.div_euclid(12), total.rem_euclid(12) + 1);
    let last = days_in_month(y, m);
    Ok(Value::Timestamp(ms_from_parts(y, m, last, 0, 0, 0, 0)?))
}

/// Shared *FROMPARTS body: slots 0..3 are always y/m/d; the optional
/// entries name the argument slots holding hour/minute/second/millisecond
/// (`None` = that component is zero). The declared arity is 3 plus the
/// present slots; any NULL part yields NULL (T-SQL).
fn from_parts_call(name: &str, args: &[Value], slots: [Option<usize>; 4]) -> Res<Value> {
    let want = 3 + slots.iter().flatten().count();
    if args.len() < want {
        return err(format!(
            "function {name} takes {want} arguments, got {}",
            args.len()
        ));
    }
    let mut parts = [0i64; 7];
    let all: [Option<usize>; 7] = [
        Some(0),
        Some(1),
        Some(2),
        slots[0],
        slots[1],
        slots[2],
        slots[3],
    ];
    for (out_i, slot) in all.iter().enumerate() {
        let Some(slot) = *slot else {
            continue;
        };
        match args.get(slot) {
            Some(Value::Int(n)) => parts[out_i] = *n,
            None | Some(Value::Null) => return Ok(Value::Null),
            Some(other) => {
                return err(format!(
                    "function {name}: part {} must be an integer, got {}",
                    slot + 1,
                    other.type_name()
                ))
            }
        }
    }
    Ok(Value::Timestamp(ms_from_parts(
        parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6],
    )?))
}

/// Convert DATETIME2FROMPARTS' fractional-seconds argument (scaled by its
/// precision) into whole milliseconds.
fn frac_to_ms(name: &str, args: &[Value], prec_idx: usize) -> Res<(i64, bool)> {
    let frac = match args.get(6) {
        None | Some(Value::Null) => return Ok((0, false)),
        Some(v) => match int_arg(v, name, 6)? {
            Some(n) => n,
            None => return Ok((0, false)),
        },
    };
    let prec = match args.get(prec_idx) {
        Some(Value::Int(p)) if (0..=7).contains(p) => *p,
        _ => 7,
    };
    let scale = 10i64.checked_pow(prec as u32).unwrap_or(10_000_000);
    Ok((frac.saturating_mul(1000) / scale, true))
}

fn three_arg_date(
    args: &[Value],
    name: &str,
    f: impl Fn(DatePart, i64, i64) -> Res<Value>,
) -> Res<Value> {
    match args {
        [p, a, b] => {
            let Some(pname) = text_arg(p) else {
                return Ok(Value::Null);
            };
            let part = datepart_code(&pname)?;
            let (Some(a), Some(b)) = (
                int_arg(a, name, 1)?,
                match b {
                    Value::Null => None,
                    v => ts_arg(v, name)?,
                },
            ) else {
                return Ok(Value::Null);
            };
            f(part, a, b)
        }
        _ => err(format!(
            "function {name} takes 3 arguments (datepart, number, date)"
        )),
    }
}

fn two_arg_datepart(
    args: &[Value],
    name: &str,
    f: impl Fn(DatePart, i64) -> Res<Value>,
) -> Res<Value> {
    match args {
        [p, v] => {
            let Some(pname) = text_arg(p) else {
                return Ok(Value::Null);
            };
            let part = datepart_code(&pname)?;
            match ts_arg(v, name)? {
                Some(ms) => f(part, ms),
                None => Ok(Value::Null),
            }
        }
        _ => err(format!(
            "function {name} takes 2 arguments (datepart, date)"
        )),
    }
}

// ---------------------------------------------------------------------------
// CONVERT / PARSE
// ---------------------------------------------------------------------------

/// Styles the CONVERT implementation knows (output rendering plus the input
/// restyle table). Anything else is a parameter error even under TRY_CONVERT.
const KNOWN_CONVERT_STYLES: &[i64] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 20, 21, 23, 25, 101, 102, 103, 104, 105, 106,
    107, 108, 110, 111, 112, 113, 114, 120, 121, 126, 127,
];

fn convert_call(args: &[Value], try_cast: bool) -> Res<Value> {
    // (type, value[, style])
    if args.len() < 2 || args.len() > 3 {
        return err("CONVERT takes 2 or 3 arguments");
    }
    let Value::Str(ty) = &args[0] else {
        return err("CONVERT target type must be a type name");
    };
    let style = match args.get(2) {
        Some(Value::Null) | None => None,
        Some(Value::Int(n)) => Some(*n),
        Some(_) => return err("CONVERT style must be an integer"),
    };
    // Style validity is a parameter error, not a conversion failure: T-SQL
    // rejects an unknown style even under TRY_CONVERT, and a value that
    // never parses as a date must not silently dodge the validation.
    if let Some(n) = style {
        if !KNOWN_CONVERT_STYLES.contains(&n) {
            return err(format!("CONVERT style {n} is not supported"));
        }
    }
    let res = convert_with_style(ty, &args[1], style);
    if try_cast {
        Ok(res.unwrap_or(Value::Null))
    } else {
        res
    }
}

fn parse_call(args: &[Value], try_parse: bool) -> Res<Value> {
    if args.len() < 2 || args.len() > 3 {
        return err("PARSE takes 2 or 3 arguments");
    }
    let Value::Str(ty) = &args[0] else {
        return err("PARSE target type must be a type name");
    };
    if let Some(culture) = args.get(2) {
        match culture {
            Value::Null => {}
            Value::Str(c) if c.eq_ignore_ascii_case("en-us") => {}
            other => {
                return err(format!(
                    "PARSE culture must be 'en-US' (got {})",
                    other.type_name()
                ))
            }
        }
    }
    // PARSE only converts text; failures raise (TRY_PARSE → NULL).
    let res = match &args[1] {
        Value::Null => Ok(Value::Null),
        Value::Str(s) => crate::engine::cast_value(Value::Str(s.clone()), ty),
        other => err(format!(
            "PARSE requires a text argument, got {}",
            other.type_name()
        )),
    };
    if try_parse {
        Ok(res.unwrap_or(Value::Null))
    } else {
        res
    }
}

/// CONVERT with a T-SQL style: the supported style table covers the common
/// date formats and numeric 0/1; anything else errors loudly rather than
/// quietly returning an unstyled conversion.
fn convert_with_style(ty: &str, v: &Value, style: Option<i64>) -> Res<Value> {
    let t = ty.trim().to_ascii_uppercase();
    let is_text = t.contains("CHAR") || t.contains("TEXT") || t.contains("STRING");
    let is_date = t.contains("DATETIME")
        || t.contains("TIMESTAMP")
        || t == "DATE"
        || t == "SMALLDATETIME"
        || t == "DATETIMEOFFSET";

    match (is_text, is_date, style) {
        // → string: the style shapes date rendering.
        (true, _, Some(style)) => {
            let ms = match v {
                Value::Null => return Ok(Value::Null),
                Value::Timestamp(ms) => *ms,
                Value::Str(s) => match parse_timestamp_ms(s) {
                    Some(ms) => ms,
                    None => return crate::engine::cast_value(v.clone(), ty),
                },
                other => return crate::engine::cast_value(other.clone(), ty),
            };
            format_style(ms, style)
        }
        // → date: the style describes the *input* text layout.
        (false, true, style) => match v {
            Value::Null => Ok(Value::Null),
            Value::Str(s) => {
                let iso = match style {
                    None | Some(0) | Some(20) | Some(21) | Some(23) | Some(25) | Some(100)
                    | Some(120) | Some(121) | Some(126) | Some(127) => s.clone(),
                    Some(n) => restyle_to_iso(s, n)?,
                };
                crate::engine::cast_value(Value::Str(iso), ty)
            }
            other => crate::engine::cast_value(other.clone(), ty),
        },
        // Numeric/bit targets: only styles 0/1 are meaningful in T-SQL and
        // only for float→string; for everything else the style is ignored.
        (false, false, Some(0 | 1)) => crate::engine::cast_value(v.clone(), ty),
        (false, false, Some(style)) => err(format!(
            "CONVERT style {style} is not supported for target {t}"
        )),
        _ => crate::engine::cast_value(v.clone(), ty),
    }
}

/// Fractional-seconds suffix that disappears when the milliseconds are zero
/// (styles 126/127 per the CONVERT table footnote).
fn frac_part(mill: i64) -> String {
    if mill == 0 {
        String::new()
    } else {
        format!(".{mill:03}")
    }
}

/// Render an instant with a T-SQL date→string style.
fn format_style(ms: i64, style: i64) -> Res<Value> {
    let secs = ms.div_euclid(1000);
    let mill = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, mo, d) = civil_from_days(days);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let mon3 = &MONTH_NAMES[(mo - 1) as usize][..3];
    let _h12 = if h % 12 == 0 { 12 } else { h % 12 };
    let (yy, y4) = ((y % 100).abs(), format!("{y:04}"));
    let (d2, m2) = (format!("{d:02}"), format!("{mo:02}"));
    let text = match style {
        1 => format!("{m2}/{d2}/{yy:02}"),
        101 => format!("{m2}/{d2}/{y4}"),
        2 => format!("{yy:02}.{m2}.{d2}"),
        102 => format!("{y4}.{m2}.{d2}"),
        3 => format!("{d2}/{m2}/{yy:02}"),
        103 => format!("{d2}/{m2}/{y4}"),
        4 => format!("{d2}.{m2}.{yy:02}"),
        104 => format!("{d2}.{m2}.{y4}"),
        5 => format!("{d2}-{m2}-{yy:02}"),
        105 => format!("{d2}-{m2}-{y4}"),
        6 => format!("{d} {mon3} {yy:02}"),
        106 => format!("{d} {mon3} {y4}"),
        7 => format!("{mon3} {d:02}, {yy:02}"),
        107 => format!("{mon3} {d:02}, {y4}"),
        8 | 108 => format!("{h:02}:{mi:02}:{s:02}"),
        // 14/114 = hh:mi:ss:mmm (24h) — same as 13/113's time part; folding
        // it into 8/108 silently dropped the milliseconds.
        14 | 114 => format!("{h:02}:{mi:02}:{s:02}:{mill:03}"),
        10 => format!("{m2}-{d2}-{yy:02}"),
        110 => format!("{m2}-{d2}-{y4}"),
        11 => format!("{yy:02}/{m2}/{d2}"),
        111 => format!("{y4}/{m2}/{d2}"),
        12 => format!("{yy:02}{m2}{d2}"),
        112 => format!("{y4}{m2}{d2}"),
        13 | 113 => format!("{d:02} {mon3} {y4} {h:02}:{mi:02}:{s:02}:{mill:03}"),
        20 | 120 => format!("{y4}-{m2}-{d2} {h:02}:{mi:02}:{s:02}"),
        21 | 25 | 121 => format!("{y4}-{m2}-{d2} {h:02}:{mi:02}:{s:02}.{mill:03}"),
        23 => format!("{y4}-{m2}-{d2}"),
        // ODBC/ISO forms omit the fractional part when it is zero (style
        // footnote 6 in the CONVERT table).
        126 => format!("{y4}-{m2}-{d2}T{h:02}:{mi:02}:{s:02}{}", frac_part(mill)),
        127 => format!("{y4}-{m2}-{d2}T{h:02}:{mi:02}:{s:02}{}Z", frac_part(mill)),
        other => return err(format!("CONVERT style {other} is not supported")),
    };
    Ok(Value::Str(text))
}

/// Rearrange a date string written in a supported input style into the
/// ISO layout the engine's timestamp parser accepts.
fn restyle_to_iso(s: &str, style: i64) -> Res<String> {
    let t = s.trim();
    let bad = || SqlError::Message(format!("cannot convert {t:?} with CONVERT style {style}"));
    let split3 = |sep: char| -> Option<Vec<String>> {
        let parts: Vec<&str> = t.split(sep).collect();
        (parts.len() == 3).then(|| parts.into_iter().map(str::to_string).collect())
    };
    let century = |yy: i64| if yy < 50 { 2000 + yy } else { 1900 + yy };
    let (y, m, d) = match style {
        1 | 101 => {
            let p = split3('/').ok_or_else(bad)?;
            match (p[2].parse(), p[0].parse(), p[1].parse()) {
                (Ok(y), Ok(m), Ok(d)) => (y, m, d),
                _ => return Err(bad()),
            }
        }
        2 | 102 => {
            let p = split3('.').ok_or_else(bad)?;
            match (p[0].parse(), p[1].parse(), p[2].parse()) {
                (Ok(y), Ok(m), Ok(d)) => (y, m, d),
                _ => return Err(bad()),
            }
        }
        3 | 103 | 4 | 104 => {
            let p = split3(if style == 3 || style == 103 { '/' } else { '.' }).ok_or_else(bad)?;
            match (p[2].parse(), p[1].parse(), p[0].parse()) {
                (Ok(y), Ok(m), Ok(d)) => (y, m, d),
                _ => return Err(bad()),
            }
        }
        5 | 105 | 10 | 110 => {
            let p = split3('-').ok_or_else(bad)?;
            // 5/105: dd-mm-(yy)yyyy; 10/110: mm-dd-(yy)yyyy.
            let (mi, di) = if style == 5 || style == 105 {
                (1, 0)
            } else {
                (0, 1)
            };
            match (p[2].parse(), p[mi].parse(), p[di].parse()) {
                (Ok(y), Ok(m), Ok(d)) => (y, m, d),
                _ => return Err(bad()),
            }
        }
        11 | 111 => {
            let p = split3('/').ok_or_else(bad)?;
            match (p[0].parse(), p[1].parse(), p[2].parse()) {
                (Ok(y), Ok(m), Ok(d)) => (y, m, d),
                _ => return Err(bad()),
            }
        }
        12 | 112 => {
            // ISO 紧凑形:原文必须恰好是 6 或 8 个 ASCII 数字 —— 预过滤
            // 数字会静默接受 '2026-09-01' 乃至混入字母后凑足位数的输入,
            // 得到完全不同的日期(静默错误数据)。
            if !t.chars().all(|c| c.is_ascii_digit()) || (t.len() != 6 && t.len() != 8) {
                return Err(bad());
            }
            let (ys, rest) = if t.len() == 8 {
                (&t[..4], &t[4..])
            } else {
                (&t[..2], &t[2..])
            };
            let y: i64 = ys.parse().map_err(|_| bad())?;
            let y = if t.len() == 6 { century(y) } else { y };
            let m: i64 = rest[..2].parse().map_err(|_| bad())?;
            let d: i64 = rest[2..].parse().map_err(|_| bad())?;
            (y, m, d)
        }
        _ => return Err(bad()),
    };
    let y = if (0..=99).contains(&y) { century(y) } else { y };
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}

// ---------------------------------------------------------------------------
// FORMAT
// ---------------------------------------------------------------------------

fn format_fn(args: &[Value]) -> Res<Value> {
    if args.len() < 2 || args.len() > 3 {
        return err("FORMAT takes 2 or 3 arguments");
    }
    if let Some(culture) = args.get(2) {
        match culture {
            Value::Null => {}
            Value::Str(c) if c.eq_ignore_ascii_case("en-us") => {}
            _ => return err("FORMAT supports only the 'en-US' culture"),
        }
    }
    let (Some(fmt), v) = (text_arg(&args[1]), &args[0]) else {
        return Ok(Value::Null);
    };
    if matches!(v, Value::Null) {
        return Ok(Value::Null);
    }
    // Standard numeric specifier: one letter + optional precision digits.
    // A BARE letter is ambiguous ('d' day vs 'D' decimal): date tokens win
    // when the value is date-like, numeric letters when it is not.
    let mut chars = fmt.chars();
    if let Some(c) = chars.next().map(|c| c.to_ascii_uppercase()) {
        let rest: String = chars.collect();
        let numeric_shape = c.is_ascii_alphabetic() && rest.chars().all(|d| d.is_ascii_digit());
        if numeric_shape && !rest.is_empty() {
            let prec: usize = rest.parse().unwrap_or(2);
            return format_numeric(v, c, prec);
        }
        if numeric_shape {
            let date_like = match v {
                Value::Timestamp(_) => true,
                Value::Str(s) => parse_timestamp_ms(s).is_some(),
                _ => false,
            };
            let date_token = fmt.chars().next().is_some_and(|ch| {
                matches!(
                    ch.to_ascii_lowercase(),
                    'y' | 'm' | 'd' | 'h' | 's' | 'f' | 't' | 'z' | 'k'
                )
            });
            if !(date_like && date_token)
                && matches!(c, 'C' | 'D' | 'F' | 'G' | 'N' | 'P' | 'X' | 'E')
            {
                // Bare D/X mean minimum digits in .NET (no zero padding);
                // the other numeric specifiers default to 2.
                return format_numeric(v, c, if c == 'D' || c == 'X' { 0 } else { 2 });
            }
        }
    }
    // Date/time custom pattern (yyyy-MM-dd HH:mm:ss style tokens).
    if let Some(ms) = ts_arg(v, "FORMAT")? {
        return Ok(Value::Str(format_date_pattern(ms, &fmt)?));
    }
    err("FORMAT pattern is neither a numeric specifier nor a date pattern")
}

fn format_numeric(v: &Value, c: char, prec: usize) -> Res<Value> {
    // Rust's format width/precision only hold u16 — a hostile `.D1000000`
    // pattern must fail loudly instead of panicking the process (same rule
    // as STR's length cap).
    if prec > 65535 {
        return err("FORMAT precision exceeds 65535");
    }
    let out = match c {
        'D' => match v {
            Value::Int(i) => format!("{i:0width$}", width = prec.max(1)),
            _ => return err("FORMAT 'D' requires an integer"),
        },
        'X' => match v {
            Value::Int(i) => format!("{i:0width$X}", width = prec),
            _ => return err("FORMAT 'X' requires an integer"),
        },
        'F' => format!("{:.*}", prec, as_fmt_f64(v)?),
        'N' => with_thousands(as_fmt_f64(v)?, prec),
        'P' => format!("{}%", with_thousands(as_fmt_f64(v)? * 100.0, prec)),
        'C' => format!("${}", with_thousands(as_fmt_f64(v)?, prec)),
        'E' => format!("{:.prec$e}", as_fmt_f64(v)?, prec = prec).to_uppercase(),
        'G' => {
            let x = as_fmt_f64(v)?;
            let s = format!("{x}");
            if s.contains('e') {
                format!("{x:e}")
            } else {
                s
            }
        }
        other => {
            return err(format!(
                "FORMAT standard specifier {other:?} is not supported"
            ))
        }
    };
    Ok(Value::Str(out))
}

fn as_fmt_f64(v: &Value) -> Res<f64> {
    match num_arg(v) {
        Some(n) => Ok(n.as_f64()),
        None => err("FORMAT requires a numeric or date value"),
    }
}

fn with_thousands(x: f64, prec: usize) -> String {
    let s = format!("{:.*}", prec, x);
    let (int_part, frac) = match s.split_once('.') {
        Some((i, f)) => (i.to_string(), Some(f)),
        None => (s, None),
    };
    let neg = int_part.starts_with('-');
    let digits = int_part.trim_start_matches('-');
    let mut grouped = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    out.push_str(&grouped);
    if let Some(f) = frac {
        out.push('.');
        out.push_str(f);
    }
    out
}

/// .NET-style custom date pattern with the common tokens. Unsupported
/// tokens error loudly instead of being dropped (a dropped token silently
/// changes the output shape).
fn format_date_pattern(ms: i64, fmt: &str) -> Res<String> {
    let p = ts_parts(ms);
    let chars: Vec<char> = fmt.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    // Whether the previous token was an hour (disambiguates m/M).
    let mut after_hour = false;
    while i < chars.len() {
        let c = chars[i];
        let mut run = 1;
        while i + run < chars.len() && chars[i + run] == c {
            run += 1;
        }
        match c {
            'y' => match run {
                1 | 2 => out.push_str(&format!("{:02}", (p.year % 100).abs())),
                3..=65535 => out.push_str(&format!("{:0width$}", p.year, width = run)),
                _ => return err("FORMAT date pattern 'y' run exceeds 65535"),
            },
            'M' => match run {
                1 => out.push_str(&p.month.to_string()),
                2 => out.push_str(&format!("{:02}", p.month)),
                3 => out.push_str(&MONTH_NAMES[(p.month - 1) as usize][..3]),
                _ => out.push_str(MONTH_NAMES[(p.month - 1) as usize]),
            },
            'd' => match run {
                1 => out.push_str(&p.day.to_string()),
                2 => out.push_str(&format!("{:02}", p.day)),
                3 => out.push_str(&DAY_NAMES[(p.weekday - 1) as usize][..3]),
                _ => out.push_str(DAY_NAMES[(p.weekday - 1) as usize]),
            },
            'H' => match run {
                1 => out.push_str(&p.hour.to_string()),
                _ => out.push_str(&format!("{:02}", p.hour)),
            },
            'h' => {
                let h12 = if p.hour % 12 == 0 { 12 } else { p.hour % 12 };
                match run {
                    1 => out.push_str(&h12.to_string()),
                    _ => out.push_str(&format!("{:02}", h12)),
                }
            }
            'm' if after_hour => match run {
                1 => out.push_str(&p.minute.to_string()),
                _ => out.push_str(&format!("{:02}", p.minute)),
            },
            'm' => return err(
                "FORMAT token 'm' (minute) is only valid after an hour token; use 'M' for months",
            ),
            's' => match run {
                1 => out.push_str(&p.second.to_string()),
                _ => out.push_str(&format!("{:02}", p.second)),
            },
            'f' => {
                // Engine precision is milliseconds; digits beyond three
                // render as zeros (documented).
                let frac = format!("{:03}", p.millisecond);
                for k in 0..run.min(7) {
                    out.push(frac.as_bytes().get(k).map(|b| *b as char).unwrap_or('0'));
                }
            }
            'F' => {
                let frac = format!("{:03}", p.millisecond);
                for k in 0..run.min(7) {
                    if let Some(b) = frac.as_bytes().get(k) {
                        out.push(*b as char);
                    }
                }
            }
            't' => {
                let ap = if p.hour < 12 { "A" } else { "P" };
                if run >= 2 {
                    out.push_str(ap);
                    out.push('M');
                } else {
                    out.push_str(ap);
                }
            }
            'z' | 'K' => out.push('Z'), // UTC-only engine
            ':' | '.' | '-' | '/' | ',' | ' ' => out.push(c),
            'Y' | 'W' | 'g' | 'k' | 'u' | 'U' => {
                return err(format!("FORMAT token {c:?} is not supported"))
            }
            other => out.push(other),
        }
        // Literal separators (":", "-", spaces) do not change what a
        // following 'm' means — only alphabetic tokens do.
        if c.is_ascii_alphabetic() {
            after_hour = matches!(c, 'H' | 'h');
        }
        i += run;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Table-valued functions
// ---------------------------------------------------------------------------

/// Evaluate a `FROM` table function by name. `None` = not one of ours (the
/// engine reports its usual table-function error).
pub fn table_function(name: &str, args: &[Value]) -> Option<Res<Vec<Object>>> {
    let n = name.to_ascii_uppercase();
    match n.as_str() {
        "STRING_SPLIT" => Some(string_split(args)),
        "GENERATE_SERIES" => Some(generate_series(args)),
        "OPENJSON" => Some(openjson(args)),
        _ => None,
    }
}

fn string_split(args: &[Value]) -> Res<Vec<Object>> {
    if args.len() < 2 || args.len() > 3 {
        return err("STRING_SPLIT takes 2 or 3 arguments");
    }
    // NULL input yields an empty rowset (subquery-existence semantics).
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Vec::new());
    }
    let (Value::Str(s), Value::Str(sep)) = (&args[0], &args[1]) else {
        return err("STRING_SPLIT requires text arguments");
    };
    if sep.is_empty() {
        return err("STRING_SPLIT separator cannot be empty");
    }
    let ordinal = matches!(args.get(2), Some(Value::Int(1)) | Some(Value::Bool(true)));
    let mut rows = Vec::new();
    for (i, part) in s.split(sep.as_str()).enumerate() {
        let mut doc = Object::new();
        doc.insert("value".into(), Value::Str(part.into()));
        if ordinal {
            doc.insert("ordinal".into(), Value::Int(i as i64 + 1));
        }
        rows.push(doc);
    }
    Ok(rows)
}

fn generate_series(args: &[Value]) -> Res<Vec<Object>> {
    if args.len() < 2 || args.len() > 3 {
        return err("GENERATE_SERIES takes 2 or 3 arguments");
    }
    let mut nums = [0i64; 3];
    for (i, v) in args.iter().take(3).enumerate() {
        match int_arg(v, "GENERATE_SERIES", i)? {
            Some(n) => nums[i] = n,
            None => return Ok(Vec::new()),
        }
    }
    let (a, b) = (nums[0], nums[1]);
    let step = if args.len() == 3 { nums[2] } else { 1 };
    if step == 0 {
        return err("GENERATE_SERIES step cannot be 0");
    }
    // 跨度/步长取负都可能溢出(异号极值、step = i64::MIN),溢出时行数必然远超上限,
    // 与超限统一报错,绝不带符号回绕绕过行数门禁。
    let count = if step > 0 {
        b.checked_sub(a).and_then(|span| {
            span.checked_div(step)
                .and_then(|q| q.checked_add(i64::from(b >= a)))
        })
    } else {
        step.checked_neg().and_then(|step_abs| {
            a.checked_sub(b).and_then(|span| {
                span.checked_div(step_abs)
                    .and_then(|q| q.checked_add(i64::from(a >= b)))
            })
        })
    };
    let count = match count {
        Some(c) => c,
        None => return err("GENERATE_SERIES range is too large"),
    };
    if count > 1_000_000 {
        return err("GENERATE_SERIES result exceeds 1,000,000 rows");
    }
    let mut rows = Vec::new();
    let mut v = a;
    while (step > 0 && v <= b) || (step < 0 && v >= b) {
        if rows.len() > 1_000_000 {
            return err("GENERATE_SERIES result exceeds 1,000,000 rows");
        }
        let mut doc = Object::new();
        doc.insert("value".into(), Value::Int(v));
        rows.push(doc);
        v = match v.checked_add(step) {
            Some(nv) => nv,
            None => break,
        };
    }
    Ok(rows)
}

fn openjson(args: &[Value]) -> Res<Vec<Object>> {
    if args.len() != 1 {
        return err("OPENJSON takes 1 argument (the WITH shape is not supported)");
    }
    let Value::Str(text) = &args[0] else {
        return err("OPENJSON requires a text argument");
    };
    let parsed = match json::from_str(text) {
        Ok(v) => v,
        Err(_) => return err("OPENJSON: argument is not valid JSON"),
    };
    let mut rows = Vec::new();
    let items: Vec<(Value, Value)> = match parsed {
        Value::Array(items) => items
            .into_iter()
            .enumerate()
            .map(|(i, v)| (Value::Int(i as i64), v))
            .collect(),
        Value::Object(fields) => fields
            .into_iter()
            .map(|(k, v)| (Value::Str(k), v))
            .collect(),
        _ => {
            return err("OPENJSON requires a JSON array or object at the top level");
        }
    };
    for (key, item) in items {
        let mut doc = Object::new();
        doc.insert("key".into(), key);
        doc.insert("value".into(), item.clone());
        doc.insert("type".into(), Value::Int(json_type(&item)));
        rows.push(doc);
    }
    Ok(rows)
}

fn json_type(v: &Value) -> i64 {
    match v {
        Value::Null => 0,
        Value::Str(_) | Value::Timestamp(_) | Value::Bytes(_) => 1,
        Value::Int(_) | Value::Float(_) | Value::Decimal(_) => 2,
        Value::Bool(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

// ---------------------------------------------------------------------------
// Hash primitives (MD5 / SHA-1 / SHA-256)
// ---------------------------------------------------------------------------

/// MD5 (RFC 1321). The per-round table is `floor(2^32 * |sin(i)|)`
/// computed rather than transcribed — no integer-radian sine lands near a
/// table boundary at f64 precision, so the floor is exact here.
pub fn md5(data: &[u8]) -> Vec<u8> {
    let mut a: u32 = 0x6745_2301;
    let mut b: u32 = 0xefcd_ab89;
    let mut c: u32 = 0x98ba_dcfe;
    let mut d: u32 = 0x1032_5476;

    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    let shifts = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];

    for chunk in msg.as_chunks::<64>().0 {
        let mut m = [0u32; 16];
        for (j, w) in m.iter_mut().enumerate() {
            *w = u32::from_le_bytes(chunk[j * 4..j * 4 + 4].try_into().unwrap());
        }
        let (mut aa, mut bb, mut cc, mut dd) = (a, b, c, d);
        for (i, &shift) in shifts.iter().enumerate() {
            let (f, g) = match i {
                0..=15 => ((bb & cc) | (!bb & dd), i),
                16..=31 => ((dd & bb) | (!dd & cc), (5 * i + 1) % 16),
                32..=47 => (bb ^ cc ^ dd, (3 * i + 5) % 16),
                _ => (cc ^ (bb | !dd), (7 * i) % 16),
            };
            let k = ((i as f64 + 1.0).sin().abs() * 4_294_967_296.0) as u32;
            let tmp = bb.wrapping_add(
                (aa.wrapping_add(f).wrapping_add(k).wrapping_add(m[g])).rotate_left(shift),
            );
            aa = dd;
            dd = cc;
            cc = bb;
            bb = tmp;
        }
        a = a.wrapping_add(aa);
        b = b.wrapping_add(bb);
        c = c.wrapping_add(cc);
        d = d.wrapping_add(dd);
    }
    let mut out = Vec::with_capacity(16);
    for w in [a, b, c, d] {
        out.extend_from_slice(&w.to_le_bytes());
    }
    out
}

/// SHA-1 (FIPS 180-4).
pub fn sha1(data: &[u8]) -> Vec<u8> {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xefcd_ab89,
        0x98ba_dcfe,
        0x1032_5476,
        0xc3d2_e1f0,
    ];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for j in 0..16 {
            w[j] = u32::from_be_bytes(chunk[j * 4..j * 4 + 4].try_into().unwrap());
        }
        for j in 16..80 {
            w[j] = (w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5a82_7999u32),
                20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
                _ => (b ^ c ^ d, 0xca62_c1d6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = Vec::with_capacity(20);
    for w in h {
        out.extend_from_slice(&w.to_be_bytes());
    }
    out
}

/// SHA-256 (FIPS 180-4). The K table (fractional bits of the cube roots of
/// the first 64 primes) is pinned by the known-answer tests below.
pub fn sha256(data: &[u8]) -> Vec<u8> {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for j in 0..16 {
            w[j] = u32::from_be_bytes(chunk[j * 4..j * 4 + 4].try_into().unwrap());
        }
        for j in 16..64 {
            let s0 = w[j - 15].rotate_right(7) ^ w[j - 15].rotate_right(18) ^ (w[j - 15] >> 3);
            let s1 = w[j - 2].rotate_right(17) ^ w[j - 2].rotate_right(19) ^ (w[j - 2] >> 10);
            w[j] = w[j - 16]
                .wrapping_add(s0)
                .wrapping_add(w[j - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for j in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[j])
                .wrapping_add(w[j]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = Vec::with_capacity(32);
    for w in h {
        out.extend_from_slice(&w.to_be_bytes());
    }
    out
}

/// Per-process PRNG state for RAND(): xorshift64, seeded from the wall
/// clock so successive calls differ across statements. Statistical
/// quality is irrelevant — RAND feeds feature code, not cryptography.
/// Statement-level semantics (one value per statement) come from the
/// read-path fold in [`crate::stmt::fold_rand_calls`], not from here.
pub(crate) fn rand_unit() -> f64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = crate::now_ms() | 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    STATE.store(x, Ordering::Relaxed);
    // Map to [0, 1): 53 mantissa bits.
    (x >> 11) as f64 / (1u64 << 53) as f64
}

/// Hex encode (debugging/testing helper).
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::parse_timestamp_ms;

    fn v_str(s: &str) -> Value {
        Value::Str(s.into())
    }

    fn ts(text: &str) -> Value {
        Value::Timestamp(parse_timestamp_ms(text).unwrap())
    }

    // ---- preprocess ----

    #[test]
    fn preprocess_brackets() {
        assert_eq!(
            preprocess("SELECT [a b], [c]]d] FROM [t]"),
            "SELECT \"a b\", \"c]d\" FROM \"t\""
        );
        // Inside strings untouched.
        assert_eq!(preprocess("SELECT '[not an id]'"), "SELECT '[not an id]'");
        // Unterminated bracket passes through for the parser to reject.
        assert_eq!(preprocess("SELECT [abc"), "SELECT [abc");
        // Idempotent.
        let once = preprocess("SELECT [x] FROM [t]");
        assert_eq!(preprocess(&once), once);
    }

    #[test]
    fn preprocess_convert() {
        assert_eq!(
            preprocess("SELECT CONVERT(VARCHAR, d, 23) FROM t"),
            "SELECT __TSQL_CONVERT__('VARCHAR', d, 23) FROM t"
        );
        assert_eq!(
            preprocess("SELECT CONVERT(DATETIME, '2026-01-02', 101)"),
            "SELECT __TSQL_CONVERT__('DATETIME', '2026-01-02', 101)"
        );
        assert_eq!(
            preprocess("SELECT TRY_CONVERT(INT, x)"),
            "SELECT __TSQL_TRY_CONVERT__('INT', x)"
        );
        assert_eq!(
            preprocess("SELECT PARSE('2026-01-02' AS DATETIME)"),
            "SELECT __TSQL_PARSE__('DATETIME', '2026-01-02')"
        );
        assert_eq!(
            preprocess("SELECT PARSE('1.5' AS DECIMAL USING 'en-US')"),
            "SELECT __TSQL_PARSE__('DECIMAL', '1.5', 'en-US')"
        );
        // MySQL order / non-type first arg stays verbatim (loud downstream).
        assert_eq!(
            preprocess("SELECT CONVERT(x, VARCHAR)"),
            "SELECT CONVERT(x, VARCHAR)"
        );
        assert_eq!(
            preprocess("SELECT CONVERT(x USING utf8)"),
            "SELECT CONVERT(x USING utf8)"
        );
        // Nested parens + strings in the value expression survive.
        assert_eq!(
            preprocess("SELECT CONVERT(INT, CASE WHEN a IN ('1)', 2) THEN 1 ELSE 0 END)"),
            "SELECT __TSQL_CONVERT__('INT', CASE WHEN a IN ('1)', 2) THEN 1 ELSE 0 END)"
        );
        // Marker names never re-trigger (idempotency).
        let once = preprocess("SELECT CONVERT(INT, x)");
        assert_eq!(preprocess(&once), once);
    }

    #[test]
    fn preprocess_shims() {
        assert_eq!(
            preprocess("SET NOCOUNT ON"),
            "PRAGMA tsql_set = 'NOCOUNT ON'"
        );
        assert_eq!(
            preprocess("SET QUOTED_IDENTIFIER OFF;"),
            "PRAGMA tsql_set = 'QUOTED_IDENTIFIER OFF';"
        );
        assert_eq!(preprocess("USE master"), "PRAGMA tsql_use = 'master'");
        assert_eq!(preprocess("USE \"my db\""), "PRAGMA tsql_use = 'my db'");
        assert_eq!(preprocess("PRINT 'hello'"), "PRAGMA tsql_print = 'hello'");
        assert_eq!(preprocess("PRINT 42"), "PRAGMA tsql_print = 42");
        // Non-literal PRINT messages stay verbatim (the PRAGMA value
        // grammar cannot carry expressions; the parser reports it).
        assert_eq!(
            preprocess("PRINT 'hello ' + 'world'"),
            "PRINT 'hello ' + 'world'"
        );
        // SET @var must NOT be shimmed (variables stay a loud error).
        assert_eq!(preprocess("SET @x = 1"), "SET @x = 1");
        // Unknown options stay verbatim (loud downstream).
        assert_eq!(preprocess("SET FOO ON"), "SET FOO ON");
        // Mid-statement words are columns, not shims.
        assert_eq!(
            preprocess("SELECT set, use FROM t"),
            "SELECT set, use FROM t"
        );
        // Shim applies per statement in a batch.
        assert_eq!(
            preprocess("SELECT 1; SET ANSI_NULLS ON"),
            "SELECT 1; PRAGMA tsql_set = 'ANSI_NULLS ON'"
        );
        // A leading comment does not hide the statement start.
        assert_eq!(
            preprocess("-- setup\nSET NOCOUNT ON"),
            "-- setup\nPRAGMA tsql_set = 'NOCOUNT ON'"
        );
    }

    // ---- LIKE ----

    #[test]
    fn like_classes() {
        assert!(like_match("abc", "[abc]bc", None));
        assert!(!like_match("xbc", "[abc]bc", None));
        assert!(like_match("b", "[a-c]", None));
        assert!(!like_match("d", "[a-c]", None));
        assert!(like_match("a", "[^b-z]", None));
        assert!(!like_match("b", "[^b-z]", None));
        // `]` first inside a class is a literal member.
        assert!(like_match("]", "[]]", None));
        assert!(like_match("]x", "[]]x", None));
        // Unterminated class = literal bracket run.
        assert!(like_match("[abc", "[abc", None));
        // A terminated class matches exactly one character from the set.
        assert!(like_match("a", "[abc]", None));
        // Dash at the class edges is a literal.
        assert!(like_match("-", "[-a]", None));
        // ESCAPE outside classes still applies.
        assert!(like_match("a%c", "a!%c", Some('!')));
        assert!(!like_match("abc", "a!%c", Some('!')));
        // % and _ inside classes are literals.
        assert!(like_match("%", "[%]", None));
        assert!(like_match("_x", "[_]x", None));
        // Class sequences with backtracking.
        assert!(like_match("a1b2", "[ab][12][ab][12]", None));
        assert!(!like_match("a1b3", "[ab][12][ab][12]", None));
        // Plain %/_ behavior unchanged.
        assert!(like_match("hello", "h%o", None));
        assert!(like_match("hi", "h_", None));
        assert!(!like_match("hi", "h__", None));
    }

    // ---- date core ----

    #[test]
    fn dateparts_and_boundary_semantics() {
        let ny = parse_timestamp_ms("2026-01-01T00:00:00Z").unwrap();
        let ny_eve = parse_timestamp_ms("2025-12-31T23:59:59Z").unwrap();
        // Boundary crossing, not elapsed time.
        assert_eq!(datediff(DatePart::Year, ny_eve, ny).unwrap(), 1);
        assert_eq!(datediff(DatePart::Day, ny_eve, ny).unwrap(), 1);
        assert_eq!(datediff(DatePart::Hour, ny_eve, ny).unwrap(), 1);

        let d = parse_timestamp_ms("2026-09-21T13:45:06.250Z").unwrap();
        assert_eq!(datepart_value(DatePart::Year, d), 2026);
        assert_eq!(datepart_value(DatePart::Month, d), 9);
        assert_eq!(datepart_value(DatePart::Day, d), 21);
        assert_eq!(datepart_value(DatePart::Hour, d), 13);
        assert_eq!(datepart_value(DatePart::Minute, d), 45);
        assert_eq!(datepart_value(DatePart::Second, d), 6);
        assert_eq!(datepart_value(DatePart::Millisecond, d), 250);
        assert_eq!(datepart_value(DatePart::Quarter, d), 3);
        assert_eq!(datepart_value(DatePart::Weekday, d), 2); // Monday
                                                             // Week: 2026-01-01 is a Thursday → Jan 4 (Sunday) starts week 2.
        let jan4 = parse_timestamp_ms("2026-01-04T00:00:00Z").unwrap();
        assert_eq!(datepart_value(DatePart::Week, jan4), 2);
        assert_eq!(datepart_value(DatePart::Week, d), 39);
    }

    #[test]
    fn dateadd_clamps_month_ends() {
        let jan31 = parse_timestamp_ms("2026-01-31T00:00:00Z").unwrap();
        let feb = dateadd(DatePart::Month, 1, jan31).unwrap();
        assert_eq!(
            crate::value::format_timestamp_ms(feb),
            "2026-02-28T00:00:00.000Z"
        );
        // Leap year.
        let jan31_24 = parse_timestamp_ms("2024-01-31T00:00:00Z").unwrap();
        let feb24 = dateadd(DatePart::Month, 1, jan31_24).unwrap();
        assert_eq!(
            crate::value::format_timestamp_ms(feb24),
            "2024-02-29T00:00:00.000Z"
        );
        let plus_year = dateadd(DatePart::Year, 1, jan31).unwrap();
        assert_eq!(
            crate::value::format_timestamp_ms(plus_year),
            "2027-01-31T00:00:00.000Z"
        );
        // Out-of-domain result is a loud error.
        let max = crate::value::TIMESTAMP_MAX_MS - 1;
        assert!(dateadd(DatePart::Year, 10, max).is_err());
    }

    #[test]
    fn ms_from_parts_validates() {
        assert!(ms_from_parts(2026, 2, 29, 0, 0, 0, 0).is_err());
        assert!(ms_from_parts(2024, 2, 29, 0, 0, 0, 0).is_ok());
        assert!(ms_from_parts(2026, 13, 1, 0, 0, 0, 0).is_err());
        assert!(ms_from_parts(2026, 1, 1, 25, 0, 0, 0).is_err());
        assert_eq!(
            crate::value::format_timestamp_ms(ms_from_parts(2026, 9, 21, 1, 2, 3, 4).unwrap()),
            "2026-09-21T01:02:03.004Z"
        );
    }

    // ---- scalar functions ----

    #[test]
    fn string_family() {
        let f = |name: &str, args: &[Value]| scalar(name, args).unwrap().unwrap();
        assert_eq!(f("LEFT", &[v_str("hello"), Value::Int(2)]), v_str("he"));
        assert_eq!(f("RIGHT", &[v_str("hello"), Value::Int(2)]), v_str("lo"));
        assert_eq!(f("CHARINDEX", &[v_str("l"), v_str("hello")]), Value::Int(3));
        assert_eq!(
            f("CHARINDEX", &[v_str("l"), v_str("hello"), Value::Int(4)]),
            Value::Int(4)
        );
        assert_eq!(
            f("CHARINDEX", &[v_str("z"), v_str("hello"), Value::Int(0)]),
            Value::Int(0)
        );
        assert_eq!(
            f("REPLACE", &[v_str("aXbX"), v_str("X"), v_str("-")]),
            v_str("a-b-")
        );
        assert_eq!(f("REPLICATE", &[v_str("ab"), Value::Int(2)]), v_str("abab"));
        assert_eq!(f("REVERSE", &[v_str("abc")]), v_str("cba"));
        assert_eq!(f("SPACE", &[Value::Int(3)]), v_str("   "));
        assert_eq!(f("STR", &[Value::Float(123.456)]), v_str("       123"));
        assert_eq!(
            f(
                "STR",
                &[Value::Float(123.456), Value::Int(8), Value::Int(2)]
            ),
            v_str("  123.46")
        );
        assert_eq!(
            f("STR", &[Value::Float(123.456), Value::Int(2)]),
            v_str("**")
        );
        assert_eq!(f("QUOTENAME", &[v_str("a]b")]), v_str("[a]]b]"));
        assert_eq!(f("QUOTENAME", &[v_str("ab"), v_str("'")]), v_str("'ab'"));
        assert_eq!(f("ASCII", &[v_str("A")]), Value::Int(65));
        assert_eq!(f("CHAR", &[Value::Int(65)]), v_str("A"));
        assert_eq!(f("CHAR", &[Value::Int(999)]), Value::Null);
        assert_eq!(f("NCHAR", &[Value::Int(8364)]), v_str("€"));
        assert_eq!(f("UNICODE", &[v_str("€")]), Value::Int(8364));
        assert_eq!(
            f(
                "CONCAT_WS",
                &[v_str(","), v_str("a"), Value::Null, v_str("b")]
            ),
            v_str("a,b")
        );
        assert_eq!(f("CONCAT_WS", &[Value::Null, v_str("a")]), Value::Null);
        assert_eq!(
            f("TRANSLATE", &[v_str("abc"), v_str("ab"), v_str("xy")]),
            v_str("xyc")
        );
        assert_eq!(
            f(
                "STUFF",
                &[v_str("abcdef"), Value::Int(2), Value::Int(3), v_str("XY")]
            ),
            v_str("aXYef")
        );
        assert_eq!(
            f(
                "STUFF",
                &[v_str("abc"), Value::Int(0), Value::Int(1), v_str("x")]
            ),
            Value::Null
        );
        assert_eq!(
            f("STRING_ESCAPE", &[v_str("a\"b\nc"), v_str("json")]),
            v_str("a\\\"b\\nc")
        );
        // NULL propagation.
        assert_eq!(f("LEFT", &[Value::Null, Value::Int(2)]), Value::Null);
        assert_eq!(
            f("REPLACE", &[Value::Null, v_str("a"), v_str("b")]),
            Value::Null
        );
        // Errors.
        assert!(scalar("LEFT", &[v_str("x"), Value::Int(-1)])
            .unwrap()
            .is_err());
        assert!(scalar("SPACE", &[Value::Int(-1)]).unwrap().is_err());
        assert!(scalar("STRING_ESCAPE", &[v_str("x"), v_str("xml")])
            .unwrap()
            .is_err());
    }

    #[test]
    fn math_family() {
        let f = |name: &str, args: &[Value]| scalar(name, args).unwrap().unwrap();
        assert_eq!(f("FLOOR", &[Value::Float(1.7)]), Value::Float(1.0));
        assert_eq!(f("CEILING", &[Value::Float(1.2)]), Value::Float(2.0));
        assert_eq!(
            f("POWER", &[Value::Int(2), Value::Int(10)]),
            Value::Int(1024)
        );
        match f("SQRT", &[Value::Int(9)]) {
            Value::Float(x) => assert!((x - 3.0).abs() < 1e-12),
            other => panic!("{other:?}"),
        }
        assert_eq!(f("SQRT", &[Value::Int(-1)]), Value::Null);
        assert_eq!(f("SIGN", &[Value::Int(-5)]), Value::Int(-1));
        match f("LOG", &[Value::Float(100.0), Value::Float(10.0)]) {
            Value::Float(x) => assert!((x - 2.0).abs() < 1e-12),
            other => panic!("{other:?}"),
        }
        match f("ATN2", &[Value::Float(1.0), Value::Float(1.0)]) {
            Value::Float(x) => assert!((x - std::f64::consts::FRAC_PI_4).abs() < 1e-12),
            other => panic!("{other:?}"),
        }
        // POWER overflow is loud.
        assert!(scalar("POWER", &[Value::Int(2), Value::Int(63)])
            .unwrap()
            .is_err());
        // RAND refuses.
        match scalar("RAND", &[]).unwrap().unwrap() {
            Value::Float(x) => assert!((0.0..1.0).contains(&x)),
            other => panic!("{other:?}"),
        }
        assert!(scalar("RAND", &[Value::Int(1)]).unwrap().is_err());
    }

    #[test]
    fn logical_and_metadata() {
        let f = |name: &str, args: &[Value]| scalar(name, args).unwrap().unwrap();
        assert_eq!(
            f("IIF", &[Value::Bool(true), v_str("y"), v_str("n")]),
            v_str("y")
        );
        assert_eq!(f("IIF", &[Value::Null, v_str("y"), v_str("n")]), v_str("n"));
        assert_eq!(
            f("CHOOSE", &[Value::Int(2), v_str("a"), v_str("b")]),
            v_str("b")
        );
        assert_eq!(
            f("CHOOSE", &[Value::Int(9), v_str("a"), v_str("b")]),
            Value::Null
        );
        assert_eq!(f("DB_NAME", &[]), v_str("docsql"));
        assert_eq!(f("DB_NAME", &[v_str("other")]), Value::Null);
        assert_eq!(f("SERVERPROPERTY", &[v_str("ProductLevel")]), v_str("RTM"));
        assert_eq!(f("SERVERPROPERTY", &[v_str("NoSuchProp")]), Value::Null);
        // Session boundary is loud.
        assert!(scalar("SUSER_SNAME", &[]).unwrap().is_err());
        // NEWID shape: a canonical UUIDv7 string.
        match f("NEWID", &[]) {
            Value::Str(s) => {
                assert_eq!(s.len(), 36);
                assert_eq!(s.as_bytes()[14], b'7'); // version nibble
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn convert_styles() {
        let d = ts("2026-09-21T13:45:06.250Z");
        let f = |args: &[Value]| scalar("__TSQL_CONVERT__", args).unwrap().unwrap();
        assert_eq!(
            f(&[v_str("VARCHAR"), d.clone(), Value::Int(23)]),
            v_str("2026-09-21")
        );
        assert_eq!(
            f(&[v_str("VARCHAR"), d.clone(), Value::Int(120)]),
            v_str("2026-09-21 13:45:06")
        );
        assert_eq!(
            f(&[v_str("VARCHAR"), d.clone(), Value::Int(121)]),
            v_str("2026-09-21 13:45:06.250")
        );
        assert_eq!(
            f(&[v_str("VARCHAR"), d.clone(), Value::Int(101)]),
            v_str("09/21/2026")
        );
        assert_eq!(
            f(&[v_str("VARCHAR"), d.clone(), Value::Int(8)]),
            v_str("13:45:06")
        );
        assert_eq!(
            f(&[v_str("VARCHAR"), d.clone(), Value::Int(106)]),
            v_str("21 Sep 2026")
        );
        assert_eq!(
            f(&[v_str("VARCHAR"), d.clone()]),
            v_str("2026-09-21T13:45:06.250Z")
        );
        assert_eq!(
            f(&[v_str("INT"), v_str("42"), Value::Int(0)]),
            Value::Int(42)
        );
        // Unknown style errors.
        assert!(scalar(
            "__TSQL_CONVERT__",
            &[v_str("VARCHAR"), d.clone(), Value::Int(77)]
        )
        .unwrap()
        .is_err());
        // Numeric target with non-0/1 style errors.
        assert!(scalar(
            "__TSQL_CONVERT__",
            &[v_str("INT"), v_str("42"), Value::Int(23)]
        )
        .unwrap()
        .is_err());
        // Input styles reorder to ISO.
        assert_eq!(
            f(&[v_str("DATETIME"), v_str("09/21/2026"), Value::Int(101)]),
            ts("2026-09-21T00:00:00Z")
        );
        assert_eq!(
            f(&[v_str("DATETIME"), v_str("20260921"), Value::Int(112)]),
            ts("2026-09-21T00:00:00Z")
        );
        // Two-digit-year century cutoff (2049).
        assert_eq!(
            f(&[v_str("DATETIME"), v_str("01/02/49"), Value::Int(101)]),
            ts("2049-01-02T00:00:00Z")
        );
        assert_eq!(
            f(&[v_str("DATETIME"), v_str("01/02/50"), Value::Int(101)]),
            ts("1950-01-02T00:00:00Z")
        );
        // TRY_CONVERT swallows the error.
        let t = scalar("__TSQL_TRY_CONVERT__", &[v_str("INT"), v_str("nope")])
            .unwrap()
            .unwrap();
        assert_eq!(t, Value::Null);
    }

    #[test]
    fn parse_calls() {
        let f = |args: &[Value]| scalar("__TSQL_PARSE__", args).unwrap().unwrap();
        assert_eq!(f(&[v_str("INT"), v_str("42")]), Value::Int(42));
        assert!(scalar("__TSQL_PARSE__", &[v_str("INT"), v_str("x")])
            .unwrap()
            .is_err());
        assert_eq!(
            scalar("__TSQL_TRY_PARSE__", &[v_str("INT"), v_str("x")])
                .unwrap()
                .unwrap(),
            Value::Null
        );
        // Culture must be en-US.
        assert!(scalar(
            "__TSQL_PARSE__",
            &[v_str("INT"), v_str("1"), v_str("fr-FR")]
        )
        .unwrap()
        .is_err());
    }

    #[test]
    fn date_functions() {
        let d = ts("2026-09-21T13:45:06.250Z");
        let f = |name: &str, args: &[Value]| scalar(name, args).unwrap().unwrap();
        assert_eq!(f("YEAR", std::slice::from_ref(&d)), Value::Int(2026));
        assert_eq!(f("MONTH", std::slice::from_ref(&d)), Value::Int(9));
        assert_eq!(f("DAY", std::slice::from_ref(&d)), Value::Int(21));
        assert_eq!(f("DATEPART", &[v_str("qq"), d.clone()]), Value::Int(3));
        assert_eq!(
            f("DATENAME", &[v_str("month"), d.clone()]),
            v_str("September")
        );
        assert_eq!(f("DATENAME", &[v_str("dw"), d.clone()]), v_str("Monday"));
        assert_eq!(
            f("DATEADD", &[v_str("day"), Value::Int(1), d.clone()]),
            ts("2026-09-22T13:45:06.250Z")
        );
        // Text dates are accepted like T-SQL.
        assert_eq!(
            f(
                "DATEADD",
                &[v_str("day"), Value::Int(1), v_str("2026-09-21")]
            ),
            ts("2026-09-22T00:00:00Z")
        );
        assert_eq!(
            f("EOMONTH", std::slice::from_ref(&d)),
            ts("2026-09-30T00:00:00Z")
        );
        assert_eq!(
            f("EOMONTH", &[d.clone(), Value::Int(2)]),
            ts("2026-11-30T00:00:00Z")
        );
        assert_eq!(
            f(
                "DATEDIFF",
                &[v_str("day"), v_str("2026-09-20"), v_str("2026-09-21")]
            ),
            Value::Int(1)
        );
        assert_eq!(
            f(
                "DATEDIFF",
                &[v_str("yy"), v_str("2025-12-31"), v_str("2026-01-01")]
            ),
            Value::Int(1)
        );
        assert_eq!(
            f(
                "DATEFROMPARTS",
                &[Value::Int(2026), Value::Int(9), Value::Int(21)]
            ),
            ts("2026-09-21T00:00:00Z")
        );
        assert_eq!(
            f(
                "DATETIMEFROMPARTS",
                &[
                    Value::Int(2026),
                    Value::Int(9),
                    Value::Int(21),
                    Value::Int(1),
                    Value::Int(2),
                    Value::Int(3),
                    Value::Int(4)
                ]
            ),
            ts("2026-09-21T01:02:03.004Z")
        );
        assert_eq!(f("ISDATE", &[v_str("2026-09-21")]), Value::Int(1));
        assert_eq!(f("ISDATE", &[v_str("nope")]), Value::Int(0));
        assert_eq!(f("ISNUMERIC", &[v_str("$1,234.50")]), Value::Int(1));
        assert_eq!(f("ISNUMERIC", &[v_str("12ab")]), Value::Int(0));
        assert!(scalar("DATEADD", &[v_str("bogus"), Value::Int(1), d])
            .unwrap()
            .is_err());
    }

    #[test]
    fn format_fn_dates_and_numbers() {
        let d = ts("2026-09-21T13:45:06.250Z");
        let f = |args: &[Value]| scalar("FORMAT", args).unwrap().unwrap();
        assert_eq!(f(&[d.clone(), v_str("yyyy-MM-dd")]), v_str("2026-09-21"));
        assert_eq!(f(&[d.clone(), v_str("HH:mm:ss")]), v_str("13:45:06"));
        assert_eq!(
            f(&[d.clone(), v_str("MMMM d, yyyy")]),
            v_str("September 21, 2026")
        );
        assert_eq!(f(&[d.clone(), v_str("hh:mm tt")]), v_str("01:45 PM"));
        assert_eq!(f(&[Value::Float(1234.5), v_str("N2")]), v_str("1,234.50"));
        assert_eq!(f(&[Value::Float(0.25), v_str("P0")]), v_str("25%"));
        assert_eq!(f(&[Value::Int(42), v_str("D5")]), v_str("00042"));
        assert_eq!(f(&[Value::Int(255), v_str("X")]), v_str("FF"));
        // 'z' tokens render the fixed UTC designator (no offset machinery).
        assert_eq!(f(&[d.clone(), v_str("zzz")]), v_str("Z"));
        // Unsupported tokens are loud.
        assert!(scalar("FORMAT", &[d, v_str("gg")]).unwrap().is_err());
    }

    #[test]
    fn hashes_known_answers() {
        // MD5 (RFC 1321 test suite)
        assert_eq!(hex(&md5(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(hex(&md5(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(
            hex(&md5(
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            )),
            "d174ab98d277d9f5a5611c2c9f419d9f"
        );
        // SHA-1 (FIPS 180-4 example)
        assert_eq!(
            hex(&sha1(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        // SHA-256 (FIPS 180-4 examples)
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // Long input crossing many block boundaries.
        let million_a = vec![b'a'; 1_000_000];
        assert_eq!(
            hex(&sha256(&million_a)),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
        // HASHBYTES wrapper.
        match scalar("HASHBYTES", &[v_str("MD5"), v_str("abc")])
            .unwrap()
            .unwrap()
        {
            Value::Bytes(b) => assert_eq!(hex(&b), "900150983cd24fb0d6963f7d28e17f72"),
            other => panic!("{other:?}"),
        }
        assert!(scalar("HASHBYTES", &[v_str("SHA2_512"), v_str("x")])
            .unwrap()
            .is_err());
        // CHECKSUM: deterministic.
        let a = scalar("CHECKSUM", &[v_str("a"), v_str("b")])
            .unwrap()
            .unwrap();
        let b = scalar("CHECKSUM", &[v_str("a"), v_str("b")])
            .unwrap()
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn table_functions_eval() {
        let rows = table_function("STRING_SPLIT", &[v_str("a,b,c"), v_str(",")])
            .unwrap()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("value"), Some(&v_str("a")));

        let rows = table_function("STRING_SPLIT", &[v_str("a,b"), v_str(","), Value::Int(1)])
            .unwrap()
            .unwrap();
        assert_eq!(rows[1].get("ordinal"), Some(&Value::Int(2)));

        assert!(table_function("STRING_SPLIT", &[v_str("a"), v_str("")])
            .unwrap()
            .is_err());

        let rows = table_function("GENERATE_SERIES", &[Value::Int(1), Value::Int(3)])
            .unwrap()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2].get("value"), Some(&Value::Int(3)));

        // 异号极值的跨度溢出:必须报错,不得绕过行数门禁(debug 下曾是
        // 减法溢出 panic,release 下 wrapping 成负数后无限循环 OOM)。
        assert!(table_function(
            "GENERATE_SERIES",
            &[Value::Int(i64::MIN + 1), Value::Int(i64::MAX)]
        )
        .unwrap()
        .is_err());
        // step = i64::MIN:取负溢出同样报错
        assert!(table_function(
            "GENERATE_SERIES",
            &[Value::Int(0), Value::Int(i64::MAX), Value::Int(i64::MIN)]
        )
        .unwrap()
        .is_err());
        // 大跨度但步长同步放大:行数在限内,正常出数
        let rows = table_function(
            "GENERATE_SERIES",
            &[
                Value::Int(0),
                Value::Int(i64::MAX),
                Value::Int(i64::MAX / 2),
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(rows.len(), 3);

        let rows = table_function("OPENJSON", &[v_str(r#"[1,"a",null]"#)])
            .unwrap()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].get("type"), Some(&Value::Int(1)));
        assert_eq!(rows[0].get("type"), Some(&Value::Int(2)));
        assert_eq!(rows[2].get("type"), Some(&Value::Int(0)));
    }

    #[test]
    fn not_a_tsql_function_falls_through() {
        assert!(scalar("NOT_A_TSQL_FN", &[]).is_none());
        assert!(table_function("NOT_A_TVF", &[]).is_none());
    }

    /// Error/NULL branches of every family — each line pins a loud-boundary
    /// or NULL-propagation rule that the happy-path tests do not touch.
    #[test]
    fn scalar_edges_and_boundaries() {
        use Value::*;
        // Arity errors.
        for (name, args) in [
            ("GETDATE", vec![Int(1)] as Vec<Value>),
            ("YEAR", vec![]),
            ("EOMONTH", vec![]),
            ("ISDATE", vec![]),
            ("ISNUMERIC", vec![]),
            ("LEFT", vec![v_str("x")]),
            ("REPLACE", vec![]),
            ("REPLICATE", vec![]),
            ("REVERSE", vec![]),
            ("SPACE", vec![]),
            ("STR", vec![]),
            ("QUOTENAME", vec![]),
            ("ASCII", vec![]),
            ("CHAR", vec![]),
            ("NCHAR", vec![]),
            ("UNICODE", vec![]),
            ("CONCAT_WS", vec![v_str(",")]),
            ("TRANSLATE", vec![]),
            ("STUFF", vec![]),
            ("STRING_ESCAPE", vec![]),
            ("FORMAT", vec![Int(1)]),
            ("FLOOR", vec![]),
            ("POWER", vec![]),
            ("SQRT", vec![]),
            ("LOG", vec![]),
            ("ATN2", vec![]),
            ("PI", vec![Int(1)]),
            ("SIGN", vec![]),
            ("IIF", vec![]),
            ("CHOOSE", vec![Int(1)]),
            ("NEWID", vec![Int(1)]),
            ("DB_NAME", vec![Int(1), Int(2)]),
            ("DB_ID", vec![Int(1), Int(2)]),
            ("SERVERPROPERTY", vec![]),
            ("CHECKSUM", vec![]),
            ("HASHBYTES", vec![]),
            ("DATEADD", vec![]),
            ("DATEDIFF", vec![]),
            ("DATEPART", vec![]),
            ("DATENAME", vec![]),
            ("DATEFROMPARTS", vec![]),
            ("DATETIMEFROMPARTS", vec![]),
            ("SMALLDATETIMEFROMPARTS", vec![]),
            ("DATETIME2FROMPARTS", vec![Int(1)]),
            ("__TSQL_CONVERT__", vec![]),
            ("__TSQL_PARSE__", vec![]),
        ] {
            assert!(
                scalar(name, &args).unwrap().is_err(),
                "{name} should reject {args:?}"
            );
        }
        // NULL propagation across the families.
        for (name, args) in [
            ("YEAR", vec![Null]),
            ("EOMONTH", vec![Null]),
            ("LEFT", vec![Null, Int(1)]),
            ("RIGHT", vec![v_str("x"), Null]),
            ("CHARINDEX", vec![Null, v_str("x")]),
            ("REPLACE", vec![v_str("a"), Null, v_str("b")]),
            ("REPLICATE", vec![Null, Int(1)]),
            ("REVERSE", vec![Null]),
            ("SPACE", vec![Null]),
            ("STR", vec![Null]),
            ("QUOTENAME", vec![Null]),
            ("ASCII", vec![Null]),
            ("UNICODE", vec![Null]),
            ("TRANSLATE", vec![Null, v_str("a"), v_str("b")]),
            ("STUFF", vec![Null, Int(1), Int(1), v_str("x")]),
            ("STRING_ESCAPE", vec![Null, v_str("json")]),
            ("FORMAT", vec![Null, v_str("D")]),
            ("FLOOR", vec![Null]),
            ("POWER", vec![Null, Int(1)]),
            ("SQRT", vec![Null]),
            ("LOG", vec![Null]),
            ("ATN2", vec![Null, Int(1)]),
            ("SIGN", vec![Null]),
            ("DATEADD", vec![v_str("day"), Null, v_str("2026-01-01")]),
            ("DATEPART", vec![Null, v_str("2026-01-01")]),
            ("DATENAME", vec![v_str("month"), Null]),
            ("HASHBYTES", vec![v_str("MD5"), Null]),
            ("__TSQL_CONVERT__", vec![v_str("INT"), Null]),
            ("__TSQL_PARSE__", vec![v_str("INT"), Null]),
        ] {
            assert_eq!(
                scalar(name, &args).unwrap().unwrap(),
                Null,
                "{name} should NULL-propagate over {args:?}"
            );
        }
        // NULL datepart first arg propagates too.
        assert_eq!(
            scalar("DATEPART", &[Null, v_str("2026-01-01")])
                .unwrap()
                .unwrap(),
            Null
        );
        // Typed-input errors.
        assert!(scalar("YEAR", &[Bool(true)]).unwrap().is_err());
        assert!(scalar("DATEADD", &[Int(1), Int(1), v_str("2026-01-01")])
            .unwrap()
            .is_err());
        assert!(
            scalar("DATEADD", &[v_str("day"), v_str("x"), v_str("2026-01-01")])
                .unwrap()
                .is_err()
        );
        assert!(
            scalar("DATEDIFF", &[v_str("dy"), v_str("x"), v_str("2026-01-01")])
                .unwrap()
                .is_err()
        );
        assert!(scalar("__TSQL_CONVERT__", &[Int(1), Int(2)])
            .unwrap()
            .is_err());
        assert!(
            scalar("__TSQL_CONVERT__", &[v_str("INT"), Int(1), v_str("x")])
                .unwrap()
                .is_err()
        );
        assert!(scalar("__TSQL_PARSE__", &[Int(1), v_str("1")])
            .unwrap()
            .is_err());
        assert!(scalar("__TSQL_PARSE__", &[v_str("INT"), Int(1)])
            .unwrap()
            .is_err());
        // Length caps.
        assert!(scalar("REPLICATE", &[v_str("ab"), Int(3_000_000)])
            .unwrap()
            .is_err());
        assert!(scalar("SPACE", &[Int(5_000_000)]).unwrap().is_err());
        assert!(scalar("TRANSLATE", &[v_str("ab"), v_str("a"), v_str("xy")])
            .unwrap()
            .is_err());
        // QUOTENAME boundaries.
        assert_eq!(
            scalar("QUOTENAME", &[v_str(&"x".repeat(129))])
                .unwrap()
                .unwrap(),
            Null
        );
        assert_eq!(
            scalar("QUOTENAME", &[v_str("x"), v_str("|")])
                .unwrap()
                .unwrap(),
            Null
        );
        // Math domain edges.
        assert_eq!(scalar("ASIN", &[Int(5)]).unwrap().unwrap(), Null);
        assert_eq!(scalar("ACOS", &[Int(-5)]).unwrap().unwrap(), Null);
        assert_eq!(scalar("LOG", &[Int(-1)]).unwrap().unwrap(), Null);
        assert_eq!(scalar("LOG10", &[Int(0)]).unwrap().unwrap(), Null);
        assert!(scalar("LOG", &[Int(10), Int(1)]).unwrap().is_err());
        assert!(scalar("FLOOR", &[v_str("x")]).unwrap().is_err());
        assert!(scalar("SIGN", &[v_str("x")]).unwrap().is_err());
        // String edges.
        assert_eq!(scalar("ASCII", &[v_str("")]).unwrap().unwrap(), Int(0));
        assert_eq!(scalar("UNICODE", &[v_str("")]).unwrap().unwrap(), Null);
        assert_eq!(
            scalar("LEFT", &[v_str("x"), Int(9)]).unwrap().unwrap(),
            v_str("x")
        );
        // DATETIME2FROMPARTS fractions/precision.
        let d2 = |args: &[Value]| scalar("DATETIME2FROMPARTS", args).unwrap().unwrap();
        assert_eq!(
            d2(&[
                Int(2026),
                Int(1),
                Int(2),
                Int(3),
                Int(4),
                Int(5),
                Int(500000),
                Int(6)
            ]),
            Timestamp(parse_timestamp_ms("2026-01-02T03:04:05.500Z").unwrap())
        );
        // SMALLDATETIMEFROMPARTS ignores seconds.
        assert_eq!(
            scalar(
                "SMALLDATETIMEFROMPARTS",
                &[Int(2026), Int(1), Int(2), Int(3), Int(4)]
            )
            .unwrap()
            .unwrap(),
            Timestamp(parse_timestamp_ms("2026-01-02T03:04:00Z").unwrap())
        );
        // Out-of-domain FROMPARTS is loud.
        assert!(scalar(
            "DATETIMEFROMPARTS",
            &[Int(10000), Int(1), Int(1), Int(0), Int(0), Int(0), Int(0)]
        )
        .unwrap()
        .is_err());
        // EOMONTH clamps through month arithmetic.
        assert_eq!(
            scalar("EOMONTH", &[v_str("2026-01-31"), Int(1)])
                .unwrap()
                .unwrap(),
            Timestamp(parse_timestamp_ms("2026-02-28T00:00:00Z").unwrap())
        );
    }

    #[test]
    fn convert_style_table_edges() {
        use Value::Int;
        let d = ts("2026-09-21T13:45:06.250Z");
        let f = |args: &[Value]| scalar("__TSQL_CONVERT__", args).unwrap().unwrap();
        // Every supported date→string style renders.
        for (style, want) in [
            (1, "09/21/26"),
            (2, "26.09.21"),
            (3, "21/09/26"),
            (4, "21.09.26"),
            (5, "21-09-26"),
            (6, "21 Sep 26"),
            (7, "Sep 21, 26"),
            (8, "13:45:06"),
            (10, "09-21-26"),
            (11, "26/09/21"),
            (12, "260921"),
            (13, "21 Sep 2026 13:45:06:250"),
            (20, "2026-09-21 13:45:06"),
            (21, "2026-09-21 13:45:06.250"),
            (23, "2026-09-21"),
            (25, "2026-09-21 13:45:06.250"),
            (101, "09/21/2026"),
            (102, "2026.09.21"),
            (103, "21/09/2026"),
            (104, "21.09.2026"),
            (105, "21-09-2026"),
            (106, "21 Sep 2026"),
            (107, "Sep 21, 2026"),
            (108, "13:45:06"),
            (110, "09-21-2026"),
            (111, "2026/09/21"),
            (112, "20260921"),
            (113, "21 Sep 2026 13:45:06:250"),
            (114, "13:45:06:250"),
            (120, "2026-09-21 13:45:06"),
            (121, "2026-09-21 13:45:06.250"),
            (126, "2026-09-21T13:45:06.250"),
            (127, "2026-09-21T13:45:06.250Z"),
        ] {
            assert_eq!(
                f(&[v_str("VARCHAR"), d.clone(), Int(style)]),
                v_str(want),
                "style {style}"
            );
        }
        // Input styles reorder to ISO.
        for (style, text) in [
            (2, "26.09.21"),
            (3, "21/09/26"),
            (4, "21.09.26"),
            (5, "21-09-26"),
            (10, "09-21-26"),
            (11, "26/09/21"),
            (102, "2026.09.21"),
            (103, "21/09/2026"),
            (104, "21.09.2026"),
            (105, "21-09-2026"),
            (110, "09-21-2026"),
            (111, "2026/09/21"),
        ] {
            assert_eq!(
                f(&[v_str("DATETIME"), v_str(text), Int(style)]),
                ts("2026-09-21T00:00:00Z"),
                "input style {style}"
            );
        }
        // Unstyled/unlisted styles pass through to the standard forms.
        assert_eq!(
            f(&[v_str("DATETIME"), v_str("2026-09-21T00:00:00Z"), Int(0)]),
            ts("2026-09-21T00:00:00Z")
        );
        assert_eq!(
            f(&[v_str("DATETIME"), v_str("2026-09-21T00:00:00Z"), Int(126)]),
            ts("2026-09-21T00:00:00Z")
        );
        // Malformed input for a style is loud.
        assert!(scalar(
            "__TSQL_CONVERT__",
            &[v_str("DATETIME"), v_str("xx/yy"), Int(101)]
        )
        .unwrap()
        .is_err());
        assert!(scalar(
            "__TSQL_CONVERT__",
            &[v_str("DATETIME"), v_str("9"), Int(112)]
        )
        .unwrap()
        .is_err());
        // A text value that is not a date passes through the text cast.
        assert_eq!(
            f(&[v_str("VARCHAR"), v_str("plain"), Int(23)]),
            v_str("plain")
        );
        assert_eq!(f(&[v_str("VARCHAR"), Int(7), Int(23)]), v_str("7"));
        // Numeric target with styles 0/1 ignores the style.
        assert_eq!(f(&[v_str("INT"), v_str("7"), Int(1)]), Int(7));
    }

    #[test]
    fn format_edges() {
        use Value::Int;
        let d = ts("2026-09-21T13:45:06.250Z");
        let f = |args: &[Value]| scalar("FORMAT", args).unwrap().unwrap();
        assert_eq!(f(&[d.clone(), v_str("yy")]), v_str("26"));
        assert_eq!(f(&[d.clone(), v_str("y")]), v_str("26"));
        assert_eq!(f(&[d.clone(), v_str("yyyyy")]), v_str("02026"));
        assert_eq!(f(&[d.clone(), v_str("M")]), v_str("9"));
        assert_eq!(f(&[d.clone(), v_str("MM")]), v_str("09"));
        assert_eq!(f(&[d.clone(), v_str("MMM")]), v_str("Sep"));
        assert_eq!(f(&[d.clone(), v_str("d")]), v_str("21"));
        assert_eq!(f(&[d.clone(), v_str("ddd")]), v_str("Mon"));
        assert_eq!(f(&[d.clone(), v_str("dddd")]), v_str("Monday"));
        assert_eq!(f(&[d.clone(), v_str("H")]), v_str("13"));
        assert_eq!(f(&[d.clone(), v_str("h")]), v_str("1"));
        assert_eq!(f(&[d.clone(), v_str("hh")]), v_str("01"));
        assert_eq!(f(&[d.clone(), v_str("HH:mm")]), v_str("13:45"));
        assert_eq!(f(&[d.clone(), v_str("s")]), v_str("6"));
        assert_eq!(f(&[d.clone(), v_str("fff")]), v_str("250"));
        assert_eq!(f(&[d.clone(), v_str("ffffff")]), v_str("250000"));
        assert_eq!(f(&[d.clone(), v_str("FFF")]), v_str("250"));
        assert_eq!(f(&[d.clone(), v_str("t")]), v_str("P"));
        assert_eq!(f(&[d.clone(), v_str("K")]), v_str("Z"));
        assert_eq!(
            f(&[d.clone(), v_str("yyyy-MM-ddTHH:mm:ss")]),
            v_str("2026-09-21T13:45:06")
        );
        assert_eq!(f(&[Value::Float(1.5), v_str("G")]), v_str("1.5"));
        match f(&[Value::Float(12345.0), v_str("E2")]) {
            Value::Str(s) => assert!(s.contains("E"), "{s}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(f(&[Value::Float(-1234.5), v_str("N1")]), v_str("-1,234.5"));
        // Numeric-with-culture and error branches.
        assert!(scalar("FORMAT", &[Int(1), v_str("D"), v_str("fr-FR")])
            .unwrap()
            .is_err());
        assert!(scalar("FORMAT", &[v_str("x"), v_str("N2")])
            .unwrap()
            .is_err());
        assert!(scalar("FORMAT", &[Int(1), v_str("D"), v_str("fr-FR")])
            .unwrap()
            .is_err());
        assert!(scalar("FORMAT", &[Value::Float(1.5), v_str("D2")])
            .unwrap()
            .is_err());
        assert!(scalar("FORMAT", &[Value::Float(1.5), v_str("X2")])
            .unwrap()
            .is_err());
        assert!(scalar("FORMAT", &[Int(1), v_str("Q1")]).unwrap().is_err());
        assert!(
            scalar("FORMAT", &[Int(1), v_str("dd-mm")]).is_none()
                || scalar("FORMAT", &[Int(1), v_str("dd-mm")])
                    .unwrap()
                    .is_err()
        );
    }

    #[test]
    fn preprocess_edge_paths() {
        // Comments and strings inside CONVERT argument spans.
        assert_eq!(
            preprocess("SELECT CONVERT(INT, x /* keep */ + y)"),
            "SELECT __TSQL_CONVERT__('INT', x /* keep */ + y)"
        );
        // GO-style keywords never appear mid-statement.
        assert_eq!(
            preprocess("SELECT use, [set] FROM t"),
            "SELECT use, \"set\" FROM t"
        );
        // USE with a trailing clause stays verbatim.
        assert_eq!(preprocess("USE db extra"), "USE db extra");
        // Unterminated quoted db name stays verbatim.
        assert_eq!(preprocess("USE \"db"), "USE \"db");
        // SET with no value / partial value stays verbatim.
        assert_eq!(preprocess("SET NOCOUNT"), "SET NOCOUNT");
        assert_eq!(preprocess("SET NOCOUNT ON extra"), "SET NOCOUNT ON extra");
        // SET with a quoted value re-escapes into the shim literal.
        assert_eq!(
            preprocess("SET LANGUAGE 'us-english'"),
            "PRAGMA tsql_set = 'LANGUAGE ''us-english'''"
        );
        assert_eq!(
            preprocess("SET LOCK_TIMEOUT 5000"),
            "PRAGMA tsql_set = 'LOCK_TIMEOUT 5000'"
        );
        // PRINT with an unterminated literal stays verbatim.
        assert_eq!(preprocess("PRINT 'oops"), "PRINT 'oops");
        // CONVERT with four arguments stays verbatim.
        assert_eq!(
            preprocess("SELECT CONVERT(INT, a, 0, 9)"),
            "SELECT CONVERT(INT, a, 0, 9)"
        );
        // PARSE with an empty culture stays verbatim.
        assert_eq!(
            preprocess("SELECT PARSE('1' AS INT USING)"),
            "SELECT PARSE('1' AS INT USING)"
        );
        // try_parse marker name.
        assert_eq!(
            preprocess("SELECT TRY_PARSE('1' AS INT)"),
            "SELECT __TSQL_TRY_PARSE__('INT', '1')"
        );
        // LIKE-class style brackets in a bare ident position pass through
        // the fast path untouched when nothing else triggers a rewrite.
        assert_eq!(preprocess("SELECT 1"), "SELECT 1");
    }

    /// Second edge sweep: date-part arithmetic corners, FORMAT token runs,
    /// table-function guards and the preprocess scanner's rare branches.
    #[test]
    fn edge_sweep_two() {
        use Value::*;
        let d = ts("2026-09-21T13:45:06.250Z");
        let f = |name: &str, args: &[Value]| scalar(name, args).unwrap().unwrap();

        // Sub-ms dateparts derive from milliseconds only.
        assert_eq!(f("DATEPART", &[v_str("mcs"), d.clone()]), Int(250_000));
        assert_eq!(f("DATEPART", &[v_str("ns"), d.clone()]), Int(250_000_000));
        assert_eq!(
            f(
                "DATEDIFF",
                &[v_str("mcs"), v_str("2026-09-21"), v_str("2026-09-22")]
            ),
            Int(86_400_000_000)
        );
        // DATEADD through weekday/micro/nano dateparts.
        assert_eq!(
            f("DATEADD", &[v_str("dw"), Int(1), d.clone()]),
            f("DATEADD", &[v_str("dd"), Int(1), d.clone()])
        );
        assert_eq!(
            f("DATEADD", &[v_str("mcs"), Int(1500), d.clone()]),
            f("DATEADD", &[v_str("ms"), Int(1), d.clone()])
        );
        assert_eq!(
            f("DATEADD", &[v_str("ns"), Int(2_000_000), d.clone()]),
            f("DATEADD", &[v_str("ms"), Int(2), d.clone()])
        );
        // Integer dates are UTC milliseconds.
        assert_eq!(f("YEAR", &[Int(0)]), Int(1970));
        // Calendar validation branches.
        assert!(scalar("DATEFROMPARTS", &[Int(2026), Int(0), Int(1)])
            .unwrap()
            .is_err());
        assert!(scalar("DATEFROMPARTS", &[Int(2026), Int(1), Int(0)])
            .unwrap()
            .is_err());
        assert!(scalar("DATEFROMPARTS", &[Int(2026), Int(1), Int(32)])
            .unwrap()
            .is_err());
        assert!(scalar(
            "DATETIMEFROMPARTS",
            &[Int(2026), Int(1), Int(1), Int(0), Int(0), Int(0), Int(1000)]
        )
        .unwrap()
        .is_err());
        assert_eq!(
            scalar("DATEFROMPARTS", &[Int(2026), Null, Int(1)])
                .unwrap()
                .unwrap(),
            Null
        );
        assert!(scalar("DATEFROMPARTS", &[v_str("x"), Int(1), Int(1)])
            .unwrap()
            .is_err());

        // STR parameter typing.
        assert!(scalar("STR", &[Float(1.5), v_str("x")]).unwrap().is_err());
        assert!(scalar("STR", &[Float(1.5), Int(8), v_str("x")])
            .unwrap()
            .is_err());
        assert_eq!(
            scalar("STR", &[Float(1.5), Null, Int(2)]).unwrap().unwrap(),
            Null
        );
        assert_eq!(
            scalar("QUOTENAME", &[v_str("x"), Int(5)]).unwrap().unwrap(),
            Null
        );
        // EOMONTH over a NULL offset.
        assert_eq!(f("EOMONTH", &[d.clone(), Null]), Null);

        // FORMAT token runs and literal separators.
        let fmt = |args: &[Value]| f("FORMAT", args);
        assert_eq!(fmt(&[d.clone(), v_str("yyyyyy")]), v_str("002026"));
        assert_eq!(fmt(&[d.clone(), v_str("MMMM")]), v_str("September"));
        assert_eq!(fmt(&[d.clone(), v_str("H:m")]), v_str("13:45"));
        assert_eq!(fmt(&[d.clone(), v_str("s")]), v_str("6"));
        assert_eq!(fmt(&[d.clone(), v_str("tt")]), v_str("PM"));
        assert_eq!(fmt(&[d.clone(), v_str("yyyy/MM/dd")]), v_str("2026/09/21"));
        assert_eq!(fmt(&[d.clone(), v_str("d, yyyy")]), v_str("21, 2026"));
        assert_eq!(fmt(&[d.clone(), v_str("hh:mm.ss")]), v_str("01:45.06"));
        assert_eq!(fmt(&[Value::Int(255), v_str("X4")]), v_str("00FF"));
        assert_eq!(fmt(&[Value::Float(1.5), v_str("C0")]), v_str("$2"));
        assert_eq!(fmt(&[Value::Float(0.5), v_str("P2")]), v_str("50.00%"));
        assert_eq!(
            fmt(&[Value::Float(1e20), v_str("G")]),
            v_str("100000000000000000000")
        );
        assert!(scalar("FORMAT", &[d.clone(), v_str("W")]).unwrap().is_err());

        // HASHBYTES over a binary value.
        match f("HASHBYTES", &[v_str("MD5"), Bytes(b"abc".to_vec())]) {
            Bytes(b) => assert_eq!(hex(&b), "900150983cd24fb0d6963f7d28e17f72"),
            other => panic!("{other:?}"),
        }

        // Table-function guards.
        assert!(table_function("STRING_SPLIT", &[v_str("a")])
            .unwrap()
            .is_err());
        assert!(table_function("STRING_SPLIT", &[Int(1), v_str(",")])
            .unwrap()
            .is_err());
        assert!(table_function("STRING_SPLIT", &[v_str("a"), Int(1)])
            .unwrap()
            .is_err());
        assert_eq!(
            table_function("STRING_SPLIT", &[Null, v_str(",")])
                .unwrap()
                .unwrap()
                .len(),
            0
        );
        assert!(
            table_function("STRING_SPLIT", &[v_str("a,b"), v_str(","), Bool(false)])
                .unwrap()
                .unwrap()
                .iter()
                .all(|d| !d.contains_key("ordinal"))
        );
        assert!(
            table_function("STRING_SPLIT", &[v_str("a,b"), v_str(","), Bool(true)])
                .unwrap()
                .unwrap()[0]
                .contains_key("ordinal")
        );
        assert_eq!(
            table_function("GENERATE_SERIES", &[Null, Int(3)])
                .unwrap()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            table_function("GENERATE_SERIES", &[Int(5), Int(1), Int(-2)])
                .unwrap()
                .unwrap()
                .len(),
            3
        );
        assert!(table_function("GENERATE_SERIES", &[Int(0), Int(2_000_000)])
            .unwrap()
            .is_err());
        assert!(table_function("OPENJSON", &[]).unwrap().is_err());
        assert!(table_function("OPENJSON", &[v_str("not json")])
            .unwrap()
            .is_err());
        assert!(table_function("OPENJSON", &[v_str("42")]).unwrap().is_err());
        assert!(table_function("OPENJSON", &[Int(42)]).unwrap().is_err());

        // LIKE with a trailing escape character.
        assert!(like_match("a!", "a!", Some('!')));
        // split_parse_args with a function-call value (AS inside parens).
        assert_eq!(
            preprocess("SELECT PARSE(LOWER('A') AS VARCHAR)"),
            "SELECT __TSQL_PARSE__('VARCHAR', LOWER('A'))"
        );

        // Preprocess scanner corners: comments around shims, unterminated
        // SET string, non-identifier USE target.
        assert_eq!(
            preprocess("SET NOCOUNT ON -- trailing"),
            "PRAGMA tsql_set = 'NOCOUNT ON' -- trailing"
        );
        assert_eq!(preprocess("USE 123"), "USE 123");
        assert_eq!(preprocess("SET NOCOUNT 'oops"), "SET NOCOUNT 'oops");
        assert_eq!(
            preprocess("SELECT CONVERT(INT, x -- c\n)"),
            "SELECT __TSQL_CONVERT__('INT', x -- c\n)"
        );
    }
    /// Final sweep over the scanner/date/catalogue branches the earlier
    /// tests did not reach.
    #[test]
    fn edge_sweep_three() {
        use Value::*;
        let d = ts("2026-09-21T13:45:06.250Z");
        let f = |name: &str, args: &[Value]| scalar(name, args).unwrap().unwrap();

        // Scanner corners.
        assert_eq!(
            preprocess("/* c */ SET NOCOUNT ON"),
            "/* c */ PRAGMA tsql_set = 'NOCOUNT ON'"
        );
        assert_eq!(
            preprocess("SET NOCOUNT ON /* tail */"),
            "PRAGMA tsql_set = 'NOCOUNT ON' /* tail */"
        );
        assert_eq!(
            preprocess("SELECT CONVERT(INT, x /* keep */)"),
            "SELECT __TSQL_CONVERT__('INT', x /* keep */)"
        );
        assert_eq!(preprocess("SELECT CONVERT"), "SELECT CONVERT");
        assert_eq!(preprocess("USE"), "USE");
        assert_eq!(preprocess("SET NOCOUNT \"x"), "SET NOCOUNT \"x");
        // PARSE with a paren-wrapped value.
        assert_eq!(
            preprocess("SELECT PARSE(('a') AS VARCHAR)"),
            "SELECT __TSQL_PARSE__('VARCHAR', ('a'))"
        );

        // Date helpers.
        assert!(
            scalar("DATEADD", &[v_str("yy"), Int(9_000_000_000), d.clone()])
                .unwrap()
                .is_err()
        );
        assert!(scalar("YEAR", &[Int(253_402_300_800_000)])
            .unwrap()
            .is_err());
        assert_eq!(
            f("DATEADD", &[v_str("hh"), Int(1), d.clone()]),
            f("DATEADD", &[v_str("mi"), Int(60), d.clone()])
        );
        assert_eq!(
            f(
                "DATEDIFF",
                &[v_str("qq"), v_str("2025-12-31"), v_str("2026-01-01")]
            ),
            Int(1)
        );
        assert_eq!(f("DATENAME", &[v_str("day"), d.clone()]), Str("21".into()));
        assert_eq!(f("DATENAME", &[v_str("dy"), d.clone()]), Str("264".into()));

        // Function branches.
        assert!(scalar("CHARINDEX", &[v_str("a")]).unwrap().is_err());
        assert_eq!(f("STUFF", &[v_str("abc"), Int(1), Int(1), Null]), Null);
        assert_eq!(f("FLOOR", &[Int(5)]), Int(5));
        match f("COT", &[Float(1.0)]) {
            Float(x) => assert!((x - 1.0 / 1.0_f64.tan()).abs() < 1e-12),
            other => panic!("{other:?}"),
        }
        assert_eq!(f("DB_NAME", &[v_str("DocSQL")]), Str("docsql".into()));
        assert_eq!(f("DB_ID", &[v_str("docsql")]), Int(1));
        assert_eq!(
            f("__TSQL_CONVERT__", &[v_str("VARCHAR"), Null, Int(23)]),
            Null
        );
        assert_eq!(
            f("__TSQL_CONVERT__", &[v_str("DATETIME"), Null, Int(101)]),
            Null
        );
        assert!(scalar(
            "__TSQL_CONVERT__",
            &[v_str("DATETIME"), v_str("xx"), Int(2)]
        )
        .unwrap()
        .is_err());
        assert_eq!(
            f("FORMAT", &[d.clone(), v_str("yyyyyyy")]),
            Str("0002026".into())
        );
        assert_eq!(
            f("FORMAT", &[d.clone(), v_str("yyyyMMMM")]),
            Str("2026September".into())
        );

        // Table functions.
        assert!(table_function("GENERATE_SERIES", &[Int(1)])
            .unwrap()
            .is_err());
        let rows = table_function("OPENJSON", &[v_str("{\"a\":1,\"b\":true}")])
            .unwrap()
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].get("type"), Some(&Int(3)));
    }
}
