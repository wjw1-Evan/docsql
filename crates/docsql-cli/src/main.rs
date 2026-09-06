//! docsql shell — embedded mode: `docsql <file.db>`, reads SQL from stdin.

use docsql_core::engine::{Database, ExecOutcome};
use std::io::BufRead;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| ":memory:".to_string());
    let mut db = if path == ":memory:" {
        Database::in_memory()
    } else {
        Database::open(std::path::Path::new(&path))
    }
    .unwrap_or_else(|e| {
        eprintln!("docsql: cannot open {path}: {e}");
        std::process::exit(1);
    });
    println!("docsql — type SQL statements ending with ';', quit with exit;");

    let mut stmt = String::new();
    let stdin = std::io::BufReader::new(std::io::stdin().lock());
    for line in stdin.lines() {
        let line = line.unwrap_or_else(|e| {
            eprintln!("read error: {e}");
            std::process::exit(1);
        });
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "exit;" || trimmed == "exit" {
            break;
        }
        stmt.push_str(&line);
        stmt.push('\n');
        if !trimmed.ends_with(';') {
            continue;
        }
        match db.execute(stmt.trim()) {
            Ok(ExecOutcome::Rows(r)) => print_rows(&r),
            Ok(ExecOutcome::Affected(n)) => println!("({n} rows affected)"),
            Err(e) => eprintln!("error: {e}"),
        }
        stmt.clear();
    }
}

fn print_rows(r: &docsql_core::engine::QueryResult) {
    if r.rows.is_empty() {
        println!("(no rows)");
        return;
    }
    let mut widths: Vec<usize> = r.columns.iter().map(|c| c.len()).collect();
    let cells: Vec<Vec<String>> = r
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    docsql_core::Value::Null => "NULL".to_string(),
                    other => other.to_string(),
                })
                .collect()
        })
        .collect();
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            widths[i] = widths[i].max(c.len());
        }
    }
    let sep: String = widths
        .iter()
        .map(|w| "-".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("+");
    let header: String = r
        .columns
        .iter()
        .zip(&widths)
        .map(|(c, w)| format!(" {c:<w$}"))
        .collect::<Vec<_>>()
        .join("|");
    println!("{header}");
    println!("{sep}");
    for row in &cells {
        let line: String = row
            .iter()
            .zip(&widths)
            .map(|(c, w)| format!(" {c:<w$}"))
            .collect::<Vec<_>>()
            .join("|");
        println!("{line}");
    }
    println!("({} rows)", r.rows.len());
}
