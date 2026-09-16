//! Statement splitting for batch-over-the-wire execution. The wire protocol
//! executes one statement per REQ_SQL frame, while the console's SQL box runs
//! batches; this module re-splits a batch into per-statement texts on the
//! sending side (see the web console's remote node switching).

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// SQL string literal with `'` doubled — the single escaping rule this
/// engine accepts. Every site that embeds a value into SQL text goes
/// through here (or [`sql_quote_ident`]); drifting hand-rolled copies are
/// how injection holes appear.
pub fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Double-quoted SQL identifier with `"` doubled.
pub fn sql_quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Byte offset just past the SQL string literal whose opening quote sits at
/// `open` (`sql.as_bytes()[open] == b'\''`), plus whether the closing quote
/// was found. `''` inside the literal is an escaped quote, never the end.
/// An unterminated literal runs to the end of the input — callers that must
/// not leak a tail (password redaction) rely on that, and callers that must
/// not mis-read data (placeholder binding, view rewriting) must not treat
/// the literal's contents as SQL.
pub fn sql_literal_end(sql: &str, open: usize) -> (usize, bool) {
    let b = sql.as_bytes();
    debug_assert_eq!(b.get(open), Some(&b'\''), "not a literal start");
    let mut i = open + 1;
    while i < b.len() {
        if b[i] == b'\'' {
            if b.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return (i + 1, true);
        }
        i += 1;
    }
    (b.len(), false)
}

/// Cheap pre-filter for [`fold_wall_clocks`]: true when the text contains
/// any wall-clock function token at all. ASCII-lowercase is byte-length
/// preserving, so a byte scan over the lowercase copy indexes the original
/// safely (the Unicode `to_lowercase` panic class is off the table).
pub fn mentions_wall_clock(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    lower.contains("now(") || lower.contains("sysdate(") || lower.contains("current_timestamp")
}

/// Fold wall-clock functions in a WRITE statement into literals stamped with
/// `now_ms`, returning the rewritten SQL (None when nothing folded).
///
/// Red line (non-deterministic generated values): a journaled/fanned-out
/// write carrying `NOW()`/`SYSDATE()`/`CURRENT_TIMESTAMP` would let every
/// peer stamp its own clock and silently diverge by the replica lag. The
/// writing node resolves the instant ONCE and ships the literal. Replacement
/// happens on the SQL TEXT — outside string literals, quoted identifiers and
/// comments — so a value like `'call now()'` is never touched. The result
/// re-parses identically in every other respect.
pub fn fold_wall_clocks(sql: &str, now_ms: i64) -> Option<String> {
    if !mentions_wall_clock(sql) {
        return None;
    }
    let b = sql.as_bytes();
    let lower = sql.to_ascii_lowercase();
    let ts_lit = format!(
        "CAST({} AS TIMESTAMP)",
        sql_string_literal(&crate::value::format_timestamp_ms(now_ms))
    );
    let str_lit = sql_string_literal(&crate::value::format_timestamp_ms(now_ms));
    let mut out = String::with_capacity(sql.len() + 64);
    let mut folded = false;
    let lower_bytes = lower.as_bytes();
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                let (end, _) = sql_literal_end(sql, i);
                out.push_str(&sql[i..end]);
                i = end;
            }
            b'"' | b'`' => {
                // Quoted identifier run with doubling escapes.
                let quote = b[i];
                out.push_str(&sql[i..i + 1]);
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
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let word = &lower_bytes[start..i];
                let is_call = (word == b"now" || word == b"sysdate")
                    && lower_bytes.get(i) == Some(&b'(')
                    && lower_bytes[i + 1..]
                        .iter()
                        .find(|&&x| x != b' ' && x != b'\t' && x != b'\r' && x != b'\n')
                        == Some(&b')');
                if is_call {
                    let mut j = i + 1;
                    while j < b.len() && b[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    out.push_str(if word == b"now" { &ts_lit } else { &str_lit });
                    folded = true;
                    i = j + 1;
                } else if word == b"current_timestamp" {
                    out.push_str(&ts_lit);
                    folded = true;
                } else {
                    out.push_str(&sql[start..i]);
                }
            }
            c if c < 0x80 => {
                out.push(c as char);
                i += 1;
            }
            _ => {
                // Non-ASCII UTF-8 sequence: copy it whole (char boundaries
                // are guaranteed by the input being a valid &str).
                let ch_len = utf8_len(b[i]);
                out.push_str(&sql[i..i + ch_len]);
                i += ch_len;
            }
        }
    }
    folded.then_some(out)
}

/// Length in bytes of the UTF-8 sequence starting with byte `b` (1 for ASCII;
/// the input being a valid `&str` guarantees the continuation bytes exist).
fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

/// Quote/comment-aware split on top-level semicolons: string literals,
/// quoted identifiers, `--` line comments and `/* */` block comments never
/// split. Needed because user-management statements are hand-parsed and
/// cannot ride the sqlparser AST (their grammar is not accepted).
fn text_chunks(sql: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' || c == '"' {
            // quoted run with doubling escapes; always stays inside the
            // current chunk
            let quote = c;
            cur.push(c);
            i += 1;
            while i < chars.len() {
                cur.push(chars[i]);
                if chars[i] == quote {
                    if chars.get(i + 1) == Some(&quote) {
                        cur.push(*chars.get(i + 1).unwrap());
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                cur.push(chars[i]);
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            cur.push('/');
            cur.push('*');
            i += 2;
            while i < chars.len() {
                cur.push(chars[i]);
                if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    cur.push('*');
                    cur.push('/');
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == ';' {
            chunks.push(std::mem::take(&mut cur));
            i += 1;
            continue;
        }
        cur.push(c);
        i += 1;
    }
    chunks.push(cur);
    chunks
}

/// Split `sql` into individual statement texts.
///
/// A single-statement input is returned verbatim (trimmed) so the common
/// case never depends on AST rendering. Multi-statement batches are
/// re-rendered from the parsed AST — the parser is shared with the engine,
/// so rendered text parses to the same statement. User-management
/// statements (hand-parsed; see `useradmin`) pass through verbatim.
/// Parse failures and empty input surface as `Err` with the parser's
/// message.
pub fn split_statements(sql: &str) -> Result<Vec<String>, String> {
    let chunks: Vec<String> = text_chunks(sql)
        .into_iter()
        .filter(|c| !c.trim().is_empty())
        .collect();
    if chunks.is_empty() {
        return Err("empty statement".into());
    }
    if chunks.len() == 1 {
        // Single statement: validate parseability (malformed input must
        // error like before), then return the original text verbatim so
        // execution never depends on AST rendering.
        if crate::useradmin::parse(&chunks[0]).is_some() {
            return Ok(vec![sql.trim().to_string()]);
        }
        let stmts = Parser::parse_sql(&GenericDialect {}, &chunks[0]).map_err(|e| e.to_string())?;
        if stmts.is_empty() {
            return Err("empty statement".into());
        }
        return Ok(vec![sql.trim().to_string()]);
    }
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if crate::useradmin::parse(&chunk).is_some() {
            out.push(chunk.trim().to_string());
            continue;
        }
        let stmts = Parser::parse_sql(&GenericDialect {}, &chunk).map_err(|e| e.to_string())?;
        for s in stmts {
            out.push(format!("{s};"));
        }
    }
    if out.is_empty() {
        return Err("empty statement".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_statement_returns_original_text() {
        let sql = "SELECT *\n  FROM t\n WHERE id = 1";
        assert_eq!(split_statements(sql).unwrap(), vec![sql]);
    }

    #[test]
    fn multi_statement_batch_splits_in_order() {
        let v = split_statements(
            "CREATE TABLE s (id INT PRIMARY KEY); INSERT INTO s VALUES (1), (2); SELECT id FROM s",
        )
        .unwrap();
        assert_eq!(v.len(), 3);
        assert!(v[0].to_uppercase().contains("CREATE TABLE"));
        assert!(v[1].to_uppercase().contains("INSERT INTO"));
        assert!(v[2].to_uppercase().contains("SELECT"));
    }

    #[test]
    fn semicolons_inside_literals_do_not_split() {
        let v = split_statements("INSERT INTO s VALUES ('a;b;c', 'x'); INSERT INTO s VALUES (';')")
            .unwrap();
        assert_eq!(v.len(), 2);
        // The string literal must survive the AST round-trip intact.
        assert!(v[0].contains("'a;b;c'"));
        assert!(v[1].contains("';'"));
    }

    #[test]
    fn quoted_identifiers_and_unicode_round_trip() {
        let v =
            split_statements("INSERT INTO \"my table\" (\"列名\") VALUES ('中文🎉;OK'); SELECT 1")
                .unwrap();
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("中文🎉"));
    }

    #[test]
    fn parse_error_and_empty_rejected() {
        // NB: "SELECT FROM WHERE" is not a guaranteed parse error — some
        // dialects read it as bare-identifier projections — so the test
        // uses unambiguously malformed input.
        assert!(split_statements("(((((").is_err());
        assert!(split_statements("").is_err());
        assert!(split_statements("   ;  ").is_err());
    }

    #[test]
    fn split_statements_execute_equivalently() {
        // The rendered statements must run through the engine unchanged.
        let mut db = crate::engine::Database::in_memory().unwrap();
        let batch = "CREATE TABLE eq (id INT PRIMARY KEY, v TEXT);\
                     INSERT INTO eq VALUES (1, 'a;b');\
                     INSERT INTO eq VALUES (2, 'x''y');\
                     SELECT COUNT(*) FROM eq";
        for stmt in split_statements(batch).unwrap() {
            db.execute(&stmt).unwrap();
        }
        let r = db.execute("SELECT COUNT(*) FROM eq").unwrap();
        match r {
            crate::engine::ExecOutcome::Rows(q) => {
                assert_eq!(q.rows[0][0].as_i64(), Some(2));
            }
            _ => panic!("expected rows"),
        }
        let r = db.execute("SELECT v FROM eq WHERE id = 2").unwrap();
        match r {
            crate::engine::ExecOutcome::Rows(q) => {
                assert_eq!(q.rows[0][0].to_string(), "x'y");
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_literal_end_covers_escapes_and_unterminated_tails() {
        let sql = "'ab' tail";
        assert_eq!(sql_literal_end(sql, 0), (4, true));
        // '' is an escaped quote, not the end.
        let sql = "'a''b' tail";
        assert_eq!(sql_literal_end(sql, 0), (6, true));
        // Closing quote as the last byte.
        let sql = "'ab'";
        assert_eq!(sql_literal_end(sql, 0), (4, true));
        // Unterminated: runs to the end (the trailing '' is an escaped quote).
        let sql = "'ab";
        assert_eq!(sql_literal_end(sql, 0), (3, false));
        let sql = "'ab''";
        assert_eq!(sql_literal_end(sql, 0), (5, false));
        // Empty literal.
        let sql = "'' x";
        assert_eq!(sql_literal_end(sql, 0), (2, true));
    }

    #[test]
    fn literal_and_identifier_quoting_doubles_embedded_quotes() {
        assert_eq!(sql_string_literal("O'Brien"), "'O''Brien'");
        // A value that would otherwise close the literal stays data.
        assert_eq!(
            sql_string_literal("'; DROP TABLE t; --"),
            "'''; DROP TABLE t; --'"
        );
        assert_eq!(sql_quote_ident(r#"we"ird"#), r#""we""ird""#);
    }

    #[test]
    fn comments_never_split_statements() {
        // `;` inside line and block comments stays inside its statement.
        let v = split_statements("SELECT 1 -- one; two\n; SELECT 2").unwrap();
        assert_eq!(v.len(), 2);
        assert!(v[0].to_uppercase().contains("SELECT 1"));
        assert!(v[1].to_uppercase().contains("SELECT 2"));
        let v = split_statements("SELECT 1 /* a; b */; SELECT 2").unwrap();
        assert_eq!(v.len(), 2);
        // A single user-management statement (hand-parsed, not sqlparser
        // grammar) passes through verbatim.
        assert_eq!(
            split_statements("CREATE USER u PASSWORD 'p'").unwrap(),
            vec!["CREATE USER u PASSWORD 'p'"]
        );
        // Comment-only input parses to zero statements: explicit error, not
        // an empty batch.
        assert!(split_statements("/* only a comment */").is_err());
        assert!(split_statements("/* a */; /* b */").is_err());
    }

    #[test]
    fn fold_wall_clocks_respects_literals_and_idents() {
        use super::{fold_wall_clocks, mentions_wall_clock};
        assert!(!mentions_wall_clock("SELECT 1"));
        assert!(mentions_wall_clock("INSERT INTO t VALUES (now())"));
        // Plain fold.
        let out = fold_wall_clocks("INSERT INTO t VALUES (NOW(), sysdate( ))", 1_000).unwrap();
        assert!(
            out.contains("CAST('1970-01-01T00:00:01.000Z' AS TIMESTAMP)"),
            "{out}"
        );
        assert!(out.contains("'1970-01-01T00:00:01.000Z'"), "{out}");
        assert!(!out.to_lowercase().contains("now("));
        // String literal contents are never touched: with nothing outside
        // the literal to fold, there is nothing to do (None), and a mixed
        // statement keeps the literal verbatim while folding the call.
        assert!(fold_wall_clocks("INSERT INTO t VALUES ('call now() later')", 1_000).is_none());
        let out =
            fold_wall_clocks("INSERT INTO t VALUES ('call now() later', now())", 1_000).unwrap();
        assert!(out.contains("'call now() later'"), "{out}");
        assert!(out.contains("AS TIMESTAMP"), "{out}");
        // Quoted identifiers are untouched: a column literally named NOW()
        // is not a call, and with nothing else to fold the result is None.
        assert!(fold_wall_clocks("SELECT \"NOW()\" FROM t", 1_000).is_none());
        // Mixed: the quoted identifier stays, the bare call folds.
        let out = fold_wall_clocks("SELECT \"NOW()\", now() FROM t", 1_000).unwrap();
        assert!(out.contains("\"NOW()\""), "{out}");
        assert!(out.contains("AS TIMESTAMP"), "{out}");
        // CURRENT_TIMESTAMP folds without parentheses.
        let out = fold_wall_clocks("INSERT INTO t VALUES (CURRENT_TIMESTAMP)", 1_000).unwrap();
        assert!(out.contains("AS TIMESTAMP"), "{out}");
        // Nothing to fold → None.
        assert!(fold_wall_clocks("INSERT INTO t VALUES (1)", 1_000).is_none());
    }
}
