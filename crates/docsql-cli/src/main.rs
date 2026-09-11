//! DocSQL shell.
//!
//! - `docsql <file.db>`             embedded mode (SQL from stdin)
//! - `docsql :memory:`              embedded in-memory
//! - `docsql connect <addr> [token]` remote mode over the v1 protocol
//!   (`auth <token>;` also works mid-session)
//!
//! Remote mode supports the persistent pub/sub commands inline:
//! `subscribe <ch> [earliest|latest|<id>];`, `psubscribe <pat> [from];`,
//! `unsubscribe [ch];`, `punsubscribe [pat];`, `publish <ch> <msg...>;`,
//! `pubsub channels|numsub|numpat|trim ...;`. Pushed messages print as
//! `[pubsub] message <channel> #<id> <payload>` the moment they arrive.

use docsql_core::engine::{Database, ExecOutcome, QueryResult};
use docsql_core::json::escape_str;
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
        eprintln!("DocSQL: cannot open {path}: {e}");
        std::process::exit(1);
    });
    println!("DocSQL — type SQL statements ending with ';', quit with exit;");
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

/// Remote-mode connection. A dedicated reader thread pulls frames off the
/// socket continuously: RESP_PUSH frames print as they arrive (pub/sub
/// delivery), everything else queues for the pending round trip. Without
/// this, a push would be misread as the next command's response.
struct Remote {
    writer: std::net::TcpStream,
    queue: std::sync::mpsc::Receiver<Frame>,
}

impl Remote {
    fn connect(addr: &str) -> Result<Remote, String> {
        let stream = std::net::TcpStream::connect(addr).map_err(|e| e.to_string())?;
        let reader = stream.try_clone().map_err(|e| e.to_string())?;
        let (tx, rx) = std::sync::mpsc::channel::<Frame>();
        std::thread::spawn(move || reader_loop(reader, tx));
        Ok(Remote {
            writer: stream,
            queue: rx,
        })
    }

    /// Send one frame and wait for its response frame.
    fn round_trip(&mut self, frame: &Frame) -> Result<Frame, String> {
        let bytes = frame.encode().map_err(|e| e.to_string())?;
        self.writer
            .write_all(&bytes)
            .and_then(|_| self.writer.flush())
            .map_err(|e| e.to_string())?;
        loop {
            match self.queue.recv() {
                // Defensive: pushes normally print in the reader thread.
                Ok(f) if f.frame_type == proto::RESP_PUSH => continue,
                Ok(f) => return Ok(f),
                Err(_) => return Err("connection closed".to_string()),
            }
        }
    }
}

fn reader_loop(mut stream: std::net::TcpStream, tx: std::sync::mpsc::Sender<Frame>) {
    loop {
        let mut header = [0u8; proto::HEADER_LEN];
        if stream.read_exact(&mut header).is_err() {
            return;
        }
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        if len > RECV_CAP {
            eprintln!("error: server advertised an oversized frame");
            return;
        }
        let mut buf = header.to_vec();
        let mut payload = vec![0u8; len];
        if stream.read_exact(&mut payload).is_err() {
            return;
        }
        buf.extend_from_slice(&payload);
        let f = match Frame::decode(&buf) {
            Ok((f, _)) => f,
            Err(e) => {
                eprintln!("error: protocol error: {e}");
                return;
            }
        };
        if f.frame_type == proto::RESP_PUSH {
            print_push(&f);
            let _ = std::io::stdout().flush();
            continue;
        }
        if tx.send(f).is_err() {
            return; // main side closed
        }
    }
}

fn print_push(f: &Frame) {
    if let Ok(Value::Object(o)) = docsql_core::json::from_str(&String::from_utf8_lossy(&f.payload))
    {
        let s = |k: &str| o.get(k).and_then(|v| v.as_str()).unwrap_or("");
        let id = o.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
        if s("kind") == "pmessage" {
            println!(
                "[pubsub] pmessage {} {} #{} {}",
                s("pattern"),
                s("channel"),
                id,
                s("payload")
            );
        } else {
            println!("[pubsub] message {} #{} {}", s("channel"), id, s("payload"));
        }
        return;
    }
    println!("[pubsub] {}", String::from_utf8_lossy(&f.payload));
}

/// AUTH against a token-protected server (REQ_AUTH frame). Returns success.
fn auth(remote: &mut Remote, token: &str) -> bool {
    match remote.round_trip(&Frame::new(proto::REQ_AUTH, token.as_bytes().to_vec())) {
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

/// One parsed inline pub/sub command (the `;` already stripped).
#[derive(Debug)]
enum PubsubCmd {
    Subscribe {
        name: String,
        from: String,
        pattern: bool,
    },
    Unsubscribe {
        name: Option<String>,
        pattern: bool,
    },
    Publish {
        channel: String,
        payload: String,
    },
    Channels {
        filter: Option<String>,
    },
    Numsub,
    Numpat,
    Trim {
        channel: String,
        keep: i64,
    },
}

fn parse_pubsub_command(line: &str) -> Option<PubsubCmd> {
    let body = line.trim().trim_end_matches(';').trim();
    let mut words = body.split_whitespace();
    let cmd = words.next()?.to_ascii_lowercase();
    let rest: Vec<&str> = words.collect();
    match cmd.as_str() {
        "subscribe" | "psubscribe" => {
            let name = rest.first()?.to_string();
            let from = rest
                .get(1)
                .map(|s| s.to_string())
                .unwrap_or("latest".into());
            Some(PubsubCmd::Subscribe {
                name,
                from,
                pattern: cmd == "psubscribe",
            })
        }
        "unsubscribe" | "punsubscribe" => Some(PubsubCmd::Unsubscribe {
            name: rest.first().map(|s| s.to_string()),
            pattern: cmd == "punsubscribe",
        }),
        "publish" => {
            let channel = rest.first()?.to_string();
            let payload = rest[1..].join(" ");
            Some(PubsubCmd::Publish { channel, payload })
        }
        "pubsub" => match rest.first()?.to_ascii_lowercase().as_str() {
            "channels" => Some(PubsubCmd::Channels {
                filter: rest.get(1).map(|s| s.to_string()),
            }),
            "numsub" => Some(PubsubCmd::Numsub),
            "numpat" => Some(PubsubCmd::Numpat),
            "trim" => {
                let channel = rest.get(1)?.to_string();
                let keep: i64 = rest.get(2)?.parse().ok()?;
                Some(PubsubCmd::Trim { channel, keep })
            }
            _ => None,
        },
        _ => None,
    }
}

fn run_pubsub_command(remote: &mut Remote, cmd: PubsubCmd) -> bool {
    let (frame_type, payload, confirm) = match &cmd {
        PubsubCmd::Subscribe {
            name,
            from,
            pattern,
        } => {
            let key = if *pattern { "pattern" } else { "channel" };
            let body = format!(
                r#"{{"{key}":"{}","from":"{}"}}"#,
                escape_str(name),
                escape_str(from)
            );
            (
                if *pattern {
                    proto::REQ_PSUBSCRIBE
                } else {
                    proto::REQ_SUBSCRIBE
                },
                body.into_bytes(),
                "subscribed",
            )
        }
        PubsubCmd::Unsubscribe { name, pattern } => {
            let names = match name {
                Some(n) => format!("[\"{}\"]", escape_str(n)),
                None => "[]".to_string(),
            };
            (
                if *pattern {
                    proto::REQ_PUNSUBSCRIBE
                } else {
                    proto::REQ_UNSUBSCRIBE
                },
                names.into_bytes(),
                "unsubscribed",
            )
        }
        PubsubCmd::Publish { channel, payload } => (
            proto::REQ_PUBLISH,
            format!(
                r#"{{"channel":"{}","payload":"{}"}}"#,
                escape_str(channel),
                escape_str(payload)
            )
            .into_bytes(),
            "",
        ),
        PubsubCmd::Channels { filter } => (
            proto::REQ_PUBSUB,
            format!(
                r#"{{"sub":"channels"{} }}"#,
                filter
                    .as_ref()
                    .map(|p| format!(",\"pattern\":\"{}\"", escape_str(p)))
                    .unwrap_or_default()
            )
            .into_bytes(),
            "",
        ),
        PubsubCmd::Numsub => (proto::REQ_PUBSUB, br#"{"sub":"numsub"}"#.to_vec(), ""),
        PubsubCmd::Numpat => (proto::REQ_PUBSUB, br#"{"sub":"numpat"}"#.to_vec(), ""),
        PubsubCmd::Trim { channel, keep } => (
            proto::REQ_PUBSUB,
            format!(
                r#"{{"sub":"trim","channel":"{}","keep":{keep}}}"#,
                escape_str(channel)
            )
            .into_bytes(),
            "trimmed",
        ),
    };
    match remote.round_trip(&Frame::new(frame_type, payload)) {
        Ok(f) if f.frame_type == proto::RESP_ERROR => {
            println!("error: {}", String::from_utf8_lossy(&f.payload));
        }
        Ok(f) => {
            if f.frame_type == proto::RESP_AFFECTED {
                let n = f
                    .payload
                    .get(..8)
                    .and_then(|s| s.try_into().ok())
                    .map_or(0, u64::from_le_bytes);
                match confirm {
                    "subscribed" => println!("(subscribed; {n} active subscription(s))"),
                    "unsubscribed" => println!("(unsubscribed; {n} remain)"),
                    "trimmed" => println!("({n} message(s) trimmed)"),
                    _ => println!("({n} rows affected)"),
                }
            } else {
                print_frame(&f);
            }
        }
        Err(e) => {
            println!("{e}");
            return false;
        }
    }
    true
}

fn remote_shell(addr: &str, token: Option<&str>) {
    let mut remote = match Remote::connect(addr) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("DocSQL: cannot connect {addr}: {e}");
            std::process::exit(1);
        }
    };
    if let Some(t) = token {
        if !auth(&mut remote, t) {
            std::process::exit(2);
        }
    }
    println!(
        "DocSQL → {addr} — SQL over the wire, quit with exit; \
         (`auth <token>;`, `subscribe <ch>;`, `publish <ch> <msg>;`)"
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
        // Inline AUTH: a dedicated auth frame, not SQL.
        if let Some(tok) = trimmed
            .strip_prefix("auth ")
            .or_else(|| trimmed.strip_prefix("AUTH "))
            .map(|t| t.trim().trim_end_matches(';').trim())
            .filter(|t| !t.is_empty())
        {
            if auth(&mut remote, tok) {
                println!("ok");
            }
            continue;
        }
        // Inline pub/sub commands (single line, `;`-terminated like SQL).
        if let Some(cmd) = parse_pubsub_command(trimmed) {
            if !run_pubsub_command(&mut remote, cmd) {
                break;
            }
            continue;
        }
        stmt.push_str(&line);
        stmt.push('\n');
        if !trimmed.ends_with(';') {
            continue;
        }
        let frame = Frame::new(proto::REQ_SQL, proto::encode_sql(stmt.trim()).unwrap());
        let f = match remote.round_trip(&frame) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        };
        print_frame(&f);
        stmt.clear();
    }
}

/// Print one response frame (rows table / affected count / error).
fn print_frame(f: &Frame) {
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
    fn pubsub_command_parsing() {
        match parse_pubsub_command("subscribe news earliest;") {
            Some(PubsubCmd::Subscribe {
                name,
                from,
                pattern: false,
            }) => {
                assert_eq!((name.as_str(), from.as_str()), ("news", "earliest"));
            }
            other => panic!("{other:?}"),
        }
        match parse_pubsub_command("PSUBSCRIBE news.*") {
            Some(PubsubCmd::Subscribe {
                name,
                from,
                pattern: true,
            }) => {
                assert_eq!((name.as_str(), from.as_str()), ("news.*", "latest"));
            }
            other => panic!("{other:?}"),
        }
        match parse_pubsub_command("unsubscribe;") {
            Some(PubsubCmd::Unsubscribe {
                name: None,
                pattern: false,
            }) => {}
            other => panic!("{other:?}"),
        }
        match parse_pubsub_command("publish ch hello wide world;") {
            Some(PubsubCmd::Publish { channel, payload }) => {
                assert_eq!(channel, "ch");
                assert_eq!(payload, "hello wide world");
            }
            other => panic!("{other:?}"),
        }
        match parse_pubsub_command("pubsub trim ch 2;") {
            Some(PubsubCmd::Trim { channel, keep }) => {
                assert_eq!((channel.as_str(), keep), ("ch", 2));
            }
            other => panic!("{other:?}"),
        }
        // Not pub/sub: SQL falls through untouched.
        assert!(parse_pubsub_command("SELECT * FROM t;").is_none());
        assert!(parse_pubsub_command("publish").is_none());
    }

    #[test]
    fn escape_str_specials() {
        assert_eq!(escape_str("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
        assert_eq!(escape_str("plain"), "plain");
        assert_eq!(escape_str("x\u{1}y"), "x\\u0001y");
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
