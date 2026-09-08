//! docsql shell.
//!
//! - `docsql <file.db>`             embedded mode (SQL from stdin)
//! - `docsql :memory:`              embedded in-memory
//! - `docsql connect <addr> [token]` remote mode over the v1 protocol
//!   (`auth <token>;` also works mid-session)

use docsql_core::engine::{Database, ExecOutcome, QueryResult};
use docsql_core::proto::{self, Frame};
use docsql_core::value::Value;
use std::io::{BufRead, Read, Write};

/// Cap on server-advertised frame sizes (mirrors the server's inbound cap).
const RECV_CAP: usize = 64 * 1024 * 1024;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("connect") {
        let addr = args
            .get(2)
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:7600".into());
        let token = args.get(3).cloned();
        remote_shell(&addr, token.as_deref());
        return;
    }
    if args.get(1).map(String::as_str) == Some("kv") {
        // `docsql-cli kv host:port "SET key value ..." | "PROMOTE"`
        let addr = args
            .get(2)
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:7600".into());
        let rest: Vec<String> = args.get(3..).map(|s| s.to_vec()).unwrap_or_default();
        kv_one(&addr, &rest);
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

/// Send one frame and read one response frame. Both directions are checked:
/// a decode failure or an oversized advertised length is an error, never a
/// panic or a multi-GB allocation.
fn round_trip(stream: &mut std::net::TcpStream, frame: &Frame) -> Result<Frame, String> {
    let bytes = frame.encode().map_err(|e| e.to_string())?;
    stream
        .write_all(&bytes)
        .and_then(|_| stream.flush())
        .map_err(|e| e.to_string())?;
    let mut header = [0u8; proto::HEADER_LEN];
    stream
        .read_exact(&mut header)
        .map_err(|_| "connection closed".to_string())?;
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if len > RECV_CAP {
        return Err("server advertised an oversized frame".into());
    }
    let mut buf = header.to_vec();
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .map_err(|_| "connection closed".to_string())?;
    buf.extend_from_slice(&payload);
    Frame::decode(&buf)
        .map(|(f, _)| f)
        .map_err(|e| format!("protocol error: {e}"))
}

/// Send one KV command; print the NUL-joined reply payload.
fn kv_one(addr: &str, args: &[String]) {
    let Ok(mut stream) = std::net::TcpStream::connect(addr) else {
        eprintln!("docsql: cannot connect {addr}");
        std::process::exit(1);
    };
    let payload = args.join("\x00").into_bytes();
    let frame = Frame::new(proto::REQ_KV, payload);
    let f = match round_trip(&mut stream, &frame) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    if f.frame_type == proto::RESP_ERROR {
        eprintln!("error: {}", String::from_utf8_lossy(&f.payload));
        std::process::exit(2);
    }
    // Integer replies (counts, TTLs): small LE u64s have zero high bytes,
    // which an 8-character ASCII string never does.
    println!("{}", decode_kv_payload(&f.payload));
}

/// AUTH against a token-protected server. Returns success.
fn kv_auth(stream: &mut std::net::TcpStream, token: &str) -> bool {
    let payload = format!("AUTH\x00{token}").into_bytes();
    match round_trip(stream, &Frame::new(proto::REQ_KV, payload)) {
        Ok(f) if f.frame_type != proto::RESP_ERROR => true,
        Ok(f) => {
            eprintln!("auth failed: {}", String::from_utf8_lossy(&f.payload));
            false
        }
        Err(e) => {
            eprintln!("{e}");
            false
        }
    }
}

fn remote_shell(addr: &str, token: Option<&str>) {
    let mut stream = match std::net::TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("docsql: cannot connect {addr}: {e}");
            std::process::exit(1);
        }
    };
    if let Some(t) = token {
        if !kv_auth(&mut stream, t) {
            std::process::exit(2);
        }
    }
    println!(
        "docsql → {addr} — SQL over the wire, quit with exit; (`auth <token>;` to authenticate)"
    );
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
        if trimmed == "exit" || trimmed == "exit;" {
            break;
        }
        // Inline AUTH: forward as a KV command instead of SQL.
        if let Some(tok) = trimmed
            .strip_prefix("auth ")
            .or_else(|| trimmed.strip_prefix("AUTH "))
            .map(|t| t.trim().trim_end_matches(';').trim())
            .filter(|t| !t.is_empty())
        {
            if kv_auth(&mut stream, tok) {
                println!("ok");
            }
            continue;
        }
        stmt.push_str(&line);
        stmt.push('\n');
        if !trimmed.ends_with(';') {
            continue;
        }
        let frame = Frame::new(proto::REQ_SQL, proto::encode_sql(stmt.trim()).unwrap());
        let f = match round_trip(&mut stream, &frame) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("{e}");
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
                } else {
                    eprintln!("protocol error: undecodable rows payload");
                }
            }
            proto::RESP_AFFECTED => {
                let n = f
                    .payload
                    .get(..8)
                    .and_then(|s| s.try_into().ok())
                    .map_or(0, u64::from_le_bytes);
                println!("({n} rows affected)");
            }
            _ => println!("error: {}", String::from_utf8_lossy(&f.payload)),
        }
        stmt.clear();
    }
}

fn print_rows(r: &QueryResult) {
    print!("{render}", render = render_rows(r));
}

/// Format a result table exactly as the shell prints it (pure, testable).
pub fn render_rows(r: &QueryResult) -> String {
    if r.rows.is_empty() {
        return "(no rows)\n".to_string();
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
            // Rows wider than the column list must not panic the client.
            if let Some(w) = widths.get_mut(i) {
                *w = (*w).max(c.len());
            }
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
    let mut out = String::new();
    out.push_str(&header);
    out.push('\n');
    out.push_str(&sep);
    out.push('\n');
    for row in &cells {
        let line: String = row
            .iter()
            .zip(&widths)
            .map(|(c, w)| format!(" {c:<w$}"))
            .collect::<Vec<_>>()
            .join("|");
        out.push_str(&line);
        out.push('\n');
    }
    out.push_str(&format!("({} rows)\n", r.rows.len()));
    out
}

/// Decode one KV reply payload the way `kv_one` prints it: small LE u64
/// integers print numerically, everything else NUL-joins non-empty parts.
pub fn decode_kv_payload(payload: &[u8]) -> String {
    if payload.len() == 8 && payload[4..8] == [0u8; 4] {
        return u64::from_le_bytes(payload[..8].try_into().unwrap()).to_string();
    }
    String::from_utf8_lossy(payload)
        .split('\x00')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use docsql_core::engine::Database;

    #[test]
    fn render_rows_empty_and_populated() {
        let empty = QueryResult {
            columns: vec!["a".into()],
            rows: vec![],
        };
        assert_eq!(render_rows(&empty), "(no rows)\n");

        let r = QueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Value::Int(1), Value::Str("ann".into())],
                vec![Value::Null, Value::Str("bo".into())],
            ],
        };
        let out = render_rows(&r);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 5);
        // NULL 渲染与行计数(lines[0] 表头、[1] 分隔线、[2..] 数据)
        assert!(lines[3].contains("NULL"));
        assert!(lines[3].contains("bo"));
        assert_eq!(lines[4], "(2 rows)");
        // 数据行之间等宽(按列宽渲染)
        assert_eq!(lines[2].chars().count(), lines[3].chars().count());
        // 分隔线只含 - 与 +
        assert!(lines[1].chars().all(|c| c == '-' || c == '+'));
    }

    #[test]
    fn decode_kv_payload_int_and_text() {
        assert_eq!(decode_kv_payload(&7u64.to_le_bytes()), "7");
        assert_eq!(decode_kv_payload(&0u64.to_le_bytes()), "0");
        // 8 字节非整数(ASCII 文本)不误判
        assert_eq!(decode_kv_payload(b"12345678"), "12345678");
        assert_eq!(decode_kv_payload(b"subscribed\x00ch"), "subscribed ch");
        assert_eq!(decode_kv_payload(b"a\x00\x00b"), "a b");
    }

    #[test]
    fn embedded_end_to_end_via_helpers() {
        // 完整链路:执行 SQL 后按 shell 规则渲染
        let mut db = Database::in_memory().unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
            .unwrap();
        let out = match db.execute("SELECT id, v FROM t ORDER BY id").unwrap() {
            ExecOutcome::Rows(r) => render_rows(&r),
            other => panic!("{other:?}"),
        };
        assert!(out.contains("id") && out.contains("(2 rows)"));
        let err = db.execute("SELECT * FROM nope").unwrap_err();
        assert!(err.to_string().contains("nope"));
    }
}
