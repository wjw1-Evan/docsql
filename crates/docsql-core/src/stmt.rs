//! Statement splitting for batch-over-the-wire execution. The wire protocol
//! executes one statement per REQ_SQL frame, while the console's SQL box runs
//! batches; this module re-splits a batch into per-statement texts on the
//! sending side (see the web console's remote node switching).

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Split `sql` into individual statement texts.
///
/// A single-statement input is returned verbatim (trimmed) so the common
/// case never depends on AST rendering. Multi-statement batches are
/// re-rendered from the parsed AST — the parser is shared with the engine,
/// so rendered text parses to the same statement. Parse failures and empty
/// input surface as `Err` with the parser's message.
pub fn split_statements(sql: &str) -> Result<Vec<String>, String> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| e.to_string())?;
    if stmts.is_empty() {
        return Err("empty statement".into());
    }
    if stmts.len() == 1 {
        return Ok(vec![sql.trim().to_string()]);
    }
    Ok(stmts
        .into_iter()
        .map(|s: Statement| format!("{s};"))
        .collect())
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
