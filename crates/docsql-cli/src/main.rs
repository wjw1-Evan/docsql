//! docsql shell.
//!
//! - `docsql <file.db>`      embedded mode (SQL from stdin)
//! - `docsql :memory:`       embedded in-memory
//! - `docsql connect <addr>` remote mode over the v1 protocol

use docsql_core::engine::{Database, ExecOutcome, QueryResult};
use docsql_core::proto::{self, Frame};
use docsql_core::value::Value;
use std::io::{BufRead, Read, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("connect") {
        let addr = args
            .get(2)
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:7600".into());
        remote_shell(&addr);
        return;
    }
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

fn remote_shell(addr: &str) {
    let mut stream = match std::net::TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("docsql: cannot connect {addr}: {e}");
            std::process::exit(1);
        }
    };
    println!("docsql → {addr} — SQL over the wire, quit with exit;");
    let mut stmt = String::new();
    let stdin = std::io::BufReader::new(std::io::stdin().lock());
    for line in stdin.lines() {
        let line = line.unwrap_or_default();
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "exit" || trimmed == "exit;" {
            break;
        }
        stmt.push_str(&line);
        stmt.push('\n');
        if !trimmed.ends_with(';') {
            continue;
        }
        let frame = Frame::new(proto::REQ_SQL, proto::encode_sql(stmt.trim()).unwrap());
        let bytes = frame.encode().unwrap();
        if let Err(e) = stream.write_all(&bytes).and_then(|_| stream.flush()) {
            eprintln!("write error: {e}");
            break;
        }
        let mut buf = Vec::new();
        let mut header = [0u8; proto::HEADER_LEN];
        if stream.read_exact(&mut header).is_err() {
            eprintln!("connection closed");
            break;
        }
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        buf.extend_from_slice(&header);
        let mut payload = vec![0u8; len];
        if stream.read_exact(&mut payload).is_err() {
            eprintln!("connection closed");
            break;
        }
        buf.extend_from_slice(&payload);
        let (f, _) = match Frame::decode(&buf) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("protocol error: {e}");
                break;
            }
        };
        match f.frame_type {
            proto::RESP_ROWS => {
                if let Ok(Value::Object(o)) =
                    docsql_core::json::from_str(&String::from_utf8_lossy(&f.payload))
                {
                    let columns = match o.get("columns") {
                        Some(Value::Array(a)) => a
                            .iter()
                            .filter_map(|x| x.as_str().map(String::from))
                            .collect(),
                        _ => vec![],
                    };
                    let rows: Vec<Vec<Value>> = match o.get("rows") {
                        Some(Value::Array(a)) => a
                            .iter()
                            .map(|r| match r {
                                Value::Array(items) => items.clone(),
                                _ => vec![],
                            })
                            .collect(),
                        _ => vec![],
                    };
                    print_rows(&QueryResult { columns, rows });
                }
            }
            proto::RESP_AFFECTED => {
                let n = u64::from_le_bytes(f.payload[..8].try_into().unwrap_or([0; 8]));
                println!("({n} rows affected)");
            }
            _ => println!("error: {}", String::from_utf8_lossy(&f.payload)),
        }
        stmt.clear();
    }
}

fn print_rows(r: &QueryResult) {
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
                    Value::Null => "NULL".to_string(),
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
