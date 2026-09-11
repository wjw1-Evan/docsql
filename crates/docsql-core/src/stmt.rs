//! Statement splitting for batch-over-the-wire execution. The wire protocol
//! executes one statement per REQ_SQL frame, while the console's SQL box runs
//! batches; this module re-splits a batch into per-statement texts on the
//! sending side (see the web console's remote node switching).

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
}
