//! DocSQL shell.
//!
//! - `docsql <file.db>`             embedded mode (SQL from stdin)
//! - `docsql :memory:`              embedded in-memory
//! - `docsql connect <addr> [token]` remote mode over the v1 protocol
//!   (`auth <token>;` also works mid-session)
//! - `docsql connect <addr> --user <name>` username/password login
//!   (password from DOCSQL_PASSWORD or an interactive prompt)
//!
//! Both modes accept:
//! - `-f <script.sql>` / `--file <script.sql>`  execute a script file
//!   instead of stdin (fail-fast: the first SQL error exits 1)
//! - `--csv` / `--json`                         row output format
//!   (default: the aligned table; CSV follows RFC 4180 quoting with NULL as
//!   an empty field; JSON emits an array of column→value objects)
//! - `help;`                                    inline command summary
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

/// Row output format (`--csv` / `--json`; default: aligned table).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Format {
    Table,
    Csv,
    Json,
}

/// Parsed command-line startup mode.
#[derive(Debug, PartialEq, Eq)]
enum CliMode {
    /// `connect <addr> [token]` — remote shell over the v1 protocol.
    Remote {
        addr: String,
        token: Option<String>,
        user: Option<String>,
    },
    /// Embedded shell over a file path or `:memory:`.
    Embedded { path: String },
}

/// The one-shot usage text (`--help` / `-h`; also the tail of any
/// unknown-flag error).
fn usage() -> String {
    "\
DocSQL shell

  docsql <file.db>               embedded mode (SQL from stdin)
  docsql :memory:                embedded in-memory
  docsql connect <addr> [token]  remote mode over the v1 protocol
  docsql connect <addr> --user <name>
                                 username/password login (password comes from
                                 DOCSQL_PASSWORD or a hidden prompt)

Options:
  -f, --file <script.sql>  execute a script file instead of stdin
      --csv | --json       row output format (default: aligned table)
  -h, --help               this text
"
    .to_string()
}

/// Parsed startup options. Pure over an injected argument iterator so the
/// whole flag/positional surface is testable without a process.
#[derive(Debug)]
struct CliArgs {
    format: Format,
    script: Option<String>,
    mode: CliMode,
}

/// Mirror of `main()`'s old argv walk: flags may appear anywhere, the first
/// non-flag consumes a mode, and `connect` may carry `--user <name>` with
/// the trailing positional as the token. Unknown `-`-prefixed arguments are
/// rejected — they used to fall through as positionals, so a stray
/// `docsql --help` quietly created a database literally named `--help`.
fn parse_args<I: Iterator<Item = String>>(it: I) -> Result<CliArgs, String> {
    let mut format = Format::Table;
    let mut script: Option<String> = None;
    let mut rest: Vec<String> = Vec::new();
    let mut it = it.peekable();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--csv" => format = Format::Csv,
            "--json" => format = Format::Json,
            "--table" => format = Format::Table,
            "-f" | "--file" => match it.next() {
                Some(p) => script = Some(p),
                None => return Err(format!("{a} requires a script path")),
            },
            // Connect-scoped flag: consumed by the connect branch below.
            "--user" => rest.push(a),
            _ if a.starts_with('-') => {
                return Err(format!("unknown option {a}\n\n{}", usage()));
            }
            _ => rest.push(a),
        }
    }
    if rest.first().map(String::as_str) == Some("connect") {
        let addr = rest
            .get(1)
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:7600".into());
        // `connect <addr> --user <name> [token]` — username/password login
        // (REQ_AUTH_USER); the password comes from DOCSQL_PASSWORD or an
        // interactive prompt, never from argv (it would leak via ps).
        let mut user: Option<String> = None;
        let mut positional: Vec<String> = Vec::new();
        let mut it = rest.iter().skip(2);
        while let Some(a) = it.next() {
            if a == "--user" {
                user = it.next().cloned();
            } else if a.starts_with('-') {
                return Err(format!("unknown option {a}\n\n{}", usage()));
            } else {
                positional.push(a.clone());
            }
        }
        // Token precedence: positional (with a leak warning), else the
        // DOCSQL_TOKEN environment variable — argv is visible in `ps` and
        // shell history, so scripts should prefer the env.
        let token = match positional.first().cloned() {
            Some(t) => {
                eprintln!(
                    "warning: passing the token on the command line exposes it to `ps`; \
                     prefer DOCSQL_TOKEN"
                );
                Some(t)
            }
            None => std::env::var("DOCSQL_TOKEN").ok().filter(|t| !t.is_empty()),
        };
        return Ok(CliArgs {
            format,
            script,
            mode: CliMode::Remote { addr, token, user },
        });
    }
    let path = rest
        .first()
        .cloned()
        .unwrap_or_else(|| ":memory:".to_string());
    Ok(CliArgs {
        format,
        script,
        mode: CliMode::Embedded { path },
    })
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", usage());
        return;
    }
    let args = parse_args(argv.into_iter()).unwrap_or_else(|e| {
        eprintln!("DocSQL: {e}");
        std::process::exit(2);
    });
    let format = args.format;
    let script = args.script;
    match args.mode {
        CliMode::Remote { addr, token, user } => {
            remote_shell(
                &addr,
                token.as_deref(),
                user.as_deref(),
                format,
                script.as_deref(),
            );
        }
        CliMode::Embedded { path } => {
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
            let stdin = std::io::BufReader::new(std::io::stdin().lock());
            run_embedded(&mut db, format, script.as_deref(), stdin);
        }
    }
}

/// True when the buffer ends with a top-level `;` and every quote/comment
/// it opened is closed: only then is it safe to split and execute. A `;`
/// inside a string literal or a quoted identifier must not terminate the
/// statement (the old `line.ends_with(';')` check executed a truncated
/// `VALUES ('line1;` and reported a parse error).
fn statements_ready(sql: &str) -> bool {
    #[derive(PartialEq)]
    enum S {
        Code,
        Single,
        Double,
        Backtick,
        Line,
        Block,
    }
    let b = sql.as_bytes();
    let mut st = S::Code;
    let mut last_semi = false;
    let mut i = 0;
    while i < b.len() {
        match st {
            S::Code => match b[i] {
                b'\'' => st = S::Single,
                b'"' => st = S::Double,
                b'`' => st = S::Backtick,
                b'-' if b.get(i + 1) == Some(&b'-') => {
                    st = S::Line;
                    i += 1;
                }
                b'/' if b.get(i + 1) == Some(&b'*') => {
                    st = S::Block;
                    i += 1;
                }
                b';' => last_semi = true,
                c if !c.is_ascii_whitespace() => last_semi = false,
                _ => {}
            },
            S::Single => {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 1;
                    } else {
                        st = S::Code;
                    }
                }
            }
            S::Double => {
                if b[i] == b'"' {
                    if b.get(i + 1) == Some(&b'"') {
                        i += 1;
                    } else {
                        st = S::Code;
                    }
                }
            }
            S::Backtick => {
                if b[i] == b'`' {
                    if b.get(i + 1) == Some(&b'`') {
                        i += 1;
                    } else {
                        st = S::Code;
                    }
                }
            }
            S::Line => {
                if b[i] == b'\n' {
                    st = S::Code;
                }
            }
            S::Block => {
                if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    st = S::Code;
                    i += 1;
                }
            }
        }
        i += 1;
    }
    // A line comment is terminated by end-of-input just like by a newline;
    // any other open state means the statement is incomplete.
    matches!(st, S::Code | S::Line) && last_semi
}

/// Split a complete buffer into executable statements (quote/comment aware).
/// A split failure returns the buffer as one chunk so the engine reports the
/// real parse error.
fn split_ready(buf: &str) -> Vec<String> {
    match docsql_core::stmt::split_statements(buf) {
        Ok(parts) => parts.into_iter().filter(|p| !p.trim().is_empty()).collect(),
        Err(_) => vec![buf.trim().to_string()],
    }
}

/// Statement loop shared by interactive stdin and `-f` script files: lines
/// accumulate until a complete `;`-terminated statement, then execute. In
/// script mode an SQL error fails fast (exit 1); interactive sessions report
/// the error and keep their normal exit status. A dangling trailing
/// statement without `;` still executes at EOF.
fn run_embedded(
    db: &mut Database,
    format: Format,
    script: Option<&str>,
    input: impl std::io::Read,
) {
    let source: Box<dyn std::io::Read> = match script {
        Some(p) => match std::fs::File::open(p) {
            Ok(f) => Box::new(f),
            Err(e) => {
                eprintln!("DocSQL: cannot open script {p}: {e}");
                std::process::exit(1);
            }
        },
        None => Box::new(input),
    };
    let interactive = script.is_none();
    let mut stmt = String::new();
    let mut done = false;
    for line in std::io::BufReader::new(source).lines() {
        let line = line.unwrap_or_else(|e| {
            eprintln!("read error: {e}");
            std::process::exit(1);
        });
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if interactive && (trimmed == "exit;" || trimmed == "exit") {
            done = true;
            break;
        }
        if trimmed == "help;" || trimmed == "help" {
            print_embedded_help();
            continue;
        }
        stmt.push_str(&line);
        stmt.push('\n');
        if !statements_ready(&stmt) {
            continue;
        }
        for part in split_ready(&stmt) {
            match db.execute(&part) {
                Ok(ExecOutcome::Rows(r)) => print_rows(&r, format),
                Ok(ExecOutcome::Affected(n)) => println!("({n} rows affected)"),
                Err(e) => {
                    eprintln!("error: {e}");
                    if !interactive {
                        std::process::exit(1);
                    }
                }
            }
        }
        stmt.clear();
    }
    // Scripts tolerate a missing final `;`.
    if !done && !stmt.trim().is_empty() {
        for part in split_ready(&stmt) {
            match db.execute(&part) {
                Ok(ExecOutcome::Rows(r)) => print_rows(&r, format),
                Ok(ExecOutcome::Affected(n)) => println!("({n} rows affected)"),
                Err(e) => {
                    eprintln!("error: {e}");
                    if !interactive {
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}

fn print_embedded_help() {
    println!(
        "exit;                         leave the shell\n\
         help;                         this summary\n\
         SQL ends with ';' — multiple statements per line are executed in order\n\
         flags: --csv | --json (row output), -f <script.sql> (batch, fail-fast)"
    );
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
        // A server that accepts but never answers (wedged node, dead
        // middlebox) must fail the script with a nonzero exit instead of
        // hanging it forever. `DOCSQL_CLI_TIMEOUT_MS` overrides (0 =
        // disable); the default stays clear of legitimate long queries.
        let budget = match std::env::var("DOCSQL_CLI_TIMEOUT_MS") {
            Ok(v) => v
                .parse::<u64>()
                .ok()
                .filter(|ms| *ms > 0)
                .map(std::time::Duration::from_millis),
            _ => Some(std::time::Duration::from_secs(600)),
        };
        loop {
            let got = match budget {
                Some(t) => match self.queue.recv_timeout(t) {
                    Ok(f) => Some(Ok(f)),
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Some(Err(())),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                },
                None => match self.queue.recv() {
                    Ok(f) => Some(Ok(f)),
                    Err(_) => Some(Err(())),
                },
            };
            match got {
                // Defensive: pushes normally print in the reader thread.
                Some(Ok(f)) if f.frame_type == proto::RESP_PUSH => continue,
                Some(Ok(f)) => return Ok(f),
                Some(Err(())) => return Err("connection closed".to_string()),
                None => {
                    return Err("no response within the response budget \
                         (set DOCSQL_CLI_TIMEOUT_MS to adjust, 0 disables)"
                        .to_string())
                }
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

/// Read one line from stdin with terminal echo disabled, restoring the
/// original termios on every path. Plain `read_line` used to leave the
/// password in the terminal scrollback. The FFI is the only way to do
/// this without spawning a subprocess (production code must not); scoped
/// to these two calls and nothing else.
#[cfg(unix)]
fn read_password_hidden() -> std::io::Result<String> {
    use std::os::unix::io::AsRawFd;
    let fd = std::io::stdin().as_raw_fd();
    let mut term: libc::termios = unsafe { std::mem::zeroed() };
    // Not a tty (pipe/file input): echo is already a non-issue.
    if unsafe { libc::tcgetattr(fd, &mut term) } != 0 {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        return Ok(line.trim_end_matches(['\r', '\n']).to_string());
    }
    let original = term;
    term.c_lflag &= !libc::ECHO;
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &term) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut line = String::new();
    let read = std::io::stdin().read_line(&mut line);
    // Restore before reporting the read result: the terminal must never
    // stay echo-less, even when stdin failed.
    unsafe { libc::tcsetattr(fd, libc::TCSANOW, &original) };
    read?;
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

#[cfg(not(unix))]
fn read_password_hidden() -> std::io::Result<String> {
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

/// Username/password login (REQ_AUTH_USER, JSON body). The password is
/// taken from DOCSQL_PASSWORD or an interactive hidden prompt.
fn auth_user(remote: &mut Remote, user: &str) -> bool {
    let password = match std::env::var("DOCSQL_PASSWORD") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprint!("password for {user}: ");
            let _ = std::io::stderr().flush();
            let line = match read_password_hidden() {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("cannot read password from stdin: {e}");
                    return false;
                }
            };
            eprintln!();
            line
        }
    };
    let body = docsql_core::json::to_string(&Value::Object(docsql_core::value::Object::from([
        ("user".to_string(), Value::Str(user.to_string())),
        ("password".to_string(), Value::Str(password)),
    ])));
    match remote.round_trip(&Frame::new(proto::REQ_AUTH_USER, body.into_bytes())) {
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

fn run_pubsub_command(remote: &mut Remote, cmd: PubsubCmd, format: Format) -> bool {
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
            eprintln!("error: {}", String::from_utf8_lossy(&f.payload));
        }
        Ok(f) => {
            if f.frame_type == proto::RESP_AFFECTED {
                let n = proto::decode_affected(&f.payload);
                match confirm {
                    "subscribed" => println!("(subscribed; {n} active subscription(s))"),
                    "unsubscribed" => println!("(unsubscribed; {n} remain)"),
                    "trimmed" => println!("({n} message(s) trimmed)"),
                    _ => println!("({n} rows affected)"),
                }
            } else {
                print_frame(&f, format);
            }
        }
        Err(e) => {
            eprintln!("{e}");
            return false;
        }
    }
    true
}

fn remote_shell(
    addr: &str,
    token: Option<&str>,
    user: Option<&str>,
    format: Format,
    script: Option<&str>,
) {
    let mut remote = match Remote::connect(addr) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("DocSQL: cannot connect {addr}: {e}");
            std::process::exit(1);
        }
    };
    if let Some(u) = user {
        if !auth_user(&mut remote, u) {
            std::process::exit(2);
        }
    } else if let Some(t) = token {
        if !auth(&mut remote, t) {
            std::process::exit(2);
        }
    }
    println!(
        "DocSQL → {addr} — SQL over the wire, quit with exit; \
         (`auth <token>;`, `subscribe <ch>;`, `publish <ch> <msg>;`)"
    );
    let source: Box<dyn std::io::Read> = match script {
        Some(p) => match std::fs::File::open(p) {
            Ok(f) => Box::new(f),
            Err(e) => {
                eprintln!("DocSQL: cannot open script {p}: {e}");
                std::process::exit(1);
            }
        },
        None => Box::new(std::io::stdin()),
    };
    let interactive = script.is_none();
    let mut stmt = String::new();
    let mut stmt_failed = false;
    for line in std::io::BufReader::new(source).lines() {
        let line = line.unwrap_or_else(|e| {
            eprintln!("read error: {e}");
            std::process::exit(1);
        });
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if interactive && (trimmed == "exit" || trimmed == "exit;") {
            break;
        }
        if trimmed == "help;" || trimmed == "help" {
            print_remote_help();
            continue;
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
            if !run_pubsub_command(&mut remote, cmd, format) {
                // A failed control round trip leaves the session unusable —
                // exit nonzero so scripts see the transport loss.
                std::process::exit(1);
            }
            continue;
        }
        stmt.push_str(&line);
        stmt.push('\n');
        if !statements_ready(&stmt) {
            continue;
        }
        for part in split_ready(&stmt) {
            let frame = Frame::new(proto::REQ_SQL, proto::encode_sql(&part).unwrap());
            let f = match remote.round_trip(&frame) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
            };
            if !print_frame(&f, format) && !interactive {
                std::process::exit(1);
            }
            stmt_failed = stmt_failed || f.frame_type == proto::RESP_ERROR;
        }
        stmt.clear();
    }
    // Scripts tolerate a missing final `;`.
    if !stmt.trim().is_empty() {
        for part in split_ready(&stmt) {
            let frame = Frame::new(proto::REQ_SQL, proto::encode_sql(&part).unwrap());
            match remote.round_trip(&frame) {
                Ok(f) => {
                    if !print_frame(&f, format) && !interactive {
                        std::process::exit(1);
                    }
                    stmt_failed = stmt_failed || f.frame_type == proto::RESP_ERROR;
                }
                Err(e) => eprintln!("{e}"),
            }
        }
    }
    // Interactive sessions end normally even after earlier SQL errors;
    // scripts surface the failure in their exit status.
    if stmt_failed && !interactive {
        std::process::exit(1);
    }
}

fn print_remote_help() {
    println!(
        "auth <token>;                switch credential mid-session\n\
         subscribe <ch> [from];       persistent subscription (earliest|latest|<id>)\n\
         psubscribe <pat> [from];     pattern subscription\n\
         unsubscribe [ch];            punsubscribe [pat];\n\
         publish <ch> <msg...>;       durable publish, returns [id, receivers]\n\
         pubsub channels|numsub|numpat|trim <ch> <n>;\n\
         help;                        this summary\n\
         exit;                        leave the shell\n\
         SQL ends with ';' — flags: --csv | --json, -f <script.sql>"
    );
}

/// Print one response frame (rows table / affected count / error). Returns
/// false for error frames so script mode can fail fast. Errors go to
/// stderr in every format.
fn print_frame(f: &Frame, format: Format) -> bool {
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
                print_rows(&QueryResult { columns, rows }, format);
            } else {
                eprintln!("protocol error: undecodable rows payload");
            }
            true
        }
        proto::RESP_AFFECTED => {
            let n = proto::decode_affected(&f.payload);
            println!("({n} rows affected)");
            true
        }
        _ => {
            eprintln!("error: {}", String::from_utf8_lossy(&f.payload));
            false
        }
    }
}

fn print_rows(r: &QueryResult, format: Format) {
    match format {
        Format::Table => print!("{render}", render = render_rows(r)),
        Format::Csv => print!("{}", render_csv(r)),
        Format::Json => print!("{}", render_json(r)),
    }
}

/// RFC 4180 CSV: quote fields containing the separator, quotes or newlines
/// (doubling the quotes); NULL renders as an empty field.
pub fn render_csv(r: &QueryResult) -> String {
    let mut out = String::new();
    out.push_str(
        &r.columns
            .iter()
            .map(|c| csv_cell(c))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');
    for row in &r.rows {
        let cells: Vec<String> = row
            .iter()
            .map(|v| match v {
                Value::Null => String::new(),
                other => csv_cell(&other.to_string()),
            })
            .collect();
        out.push_str(&cells.join(","));
        out.push('\n');
    }
    out
}

fn csv_cell(s: &str) -> String {
    // CSV formula injection guard: a cell starting with =, +, -, @ (or a
    // tab/CR, which Excel also treats as formula introducers) would execute
    // in Excel/LibreOffice/Sheets when the exported file is opened. A stored
    // `=WEBSERVICE(...)` from any writer must not run on the analyst's
    // machine — prefix a quote, the standard neutralizer.
    let needs_guard = s
        .as_bytes()
        .first()
        .is_some_and(|c| matches!(c, b'=' | b'+' | b'-' | b'@' | b'\t' | b'\r'));
    let guarded: std::borrow::Cow<str> = if needs_guard {
        std::borrow::Cow::Owned(format!("'{s}"))
    } else {
        std::borrow::Cow::Borrowed(s)
    };
    if guarded.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", guarded.replace('"', "\"\""))
    } else {
        guarded.into_owned()
    }
}

/// JSON rows: an array of column→value objects, straight from the wire
/// payload shape (NULL → null).
pub fn render_json(r: &QueryResult) -> String {
    let mut out = String::from("[");
    for (i, row) in r.rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        for (j, (col, v)) in r.columns.iter().zip(row.iter()).enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&format!(
                "\"{}\":{}",
                escape_str(col),
                docsql_core::json::to_string(v)
            ));
        }
        out.push('}');
    }
    out.push_str("]\n");
    out
}

/// Format a result table exactly as the shell prints it (pure, testable).
pub fn render_rows(r: &QueryResult) -> String {
    if r.rows.is_empty() {
        return "(no rows)\n".to_string();
    }
    // Widths count CHARS while the pad below pads chars: measuring bytes
    // left CJK cells ragged (a 3-char/9-byte string padded as 9).
    let mut widths: Vec<usize> = r.columns.iter().map(|c| c.chars().count()).collect();
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
                *w = (*w).max(c.chars().count());
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
    fn render_typed_scalars_in_table_and_json() {
        let r = QueryResult {
            columns: vec!["m".into(), "b".into()],
            rows: vec![vec![
                Value::Decimal("1234.50".parse().unwrap()),
                Value::Bytes(vec![0xde, 0xad]),
            ]],
        };
        // 表格:十进制精确文本、BLOB 十六进制文本。
        let out = render_rows(&r);
        assert!(out.contains("1234.50"), "{out}");
        assert!(out.contains("x'dead'"), "{out}");
        // JSON 导出保持无损标记。
        let json = render_json(&r);
        assert!(json.contains(r#""m":{"$dec":"1234.50"}"#), "{json}");
        assert!(json.contains(r#""b":{"$bytes":[222,173]}"#), "{json}");
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

    #[test]
    fn csv_json_renderers() {
        let r = QueryResult {
            columns: vec!["id".into(), "name".into(), "note".into()],
            rows: vec![
                vec![Value::Int(1), Value::Str("a,b".into()), Value::Null],
                vec![
                    Value::Int(2),
                    Value::Str("say \"hi\"".into()),
                    Value::Str("x\ny".into()),
                ],
            ],
        };
        // RFC 4180: special fields quoted (quotes doubled), NULL → empty.
        assert_eq!(
            render_csv(&r),
            "id,name,note\n1,\"a,b\",\n2,\"say \"\"hi\"\"\",\"x\ny\"\n"
        );
        // Header-only output for empty results.
        assert_eq!(
            render_csv(&QueryResult {
                columns: vec!["a".into()],
                rows: vec![],
            }),
            "a\n"
        );
        assert_eq!(
            render_json(&QueryResult {
                columns: vec!["a".into()],
                rows: vec![],
            })
            .trim(),
            "[]"
        );
        // JSON: array of column→value objects, NULL → null.
        let js = render_json(&r);
        let parsed = docsql_core::json::from_str(js.trim()).unwrap();
        match parsed {
            Value::Array(items) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    Value::Object(o) => {
                        assert_eq!(o.get("id"), Some(&Value::Int(1)));
                        assert_eq!(o.get("name"), Some(&Value::Str("a,b".into())));
                        assert_eq!(o.get("note"), Some(&Value::Null));
                    }
                    other => panic!("{other:?}"),
                }
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn script_mode_runs_and_tolerates_missing_final_semicolon() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sql");
        std::fs::write(
            &path,
            "CREATE TABLE t (id INT);\n\
             INSERT INTO t VALUES (7);\n\
             SELECT 1 AS one;\n\
             SELECT COUNT(*) AS c FROM t\n",
        )
        .unwrap();
        let mut db = Database::in_memory().unwrap();
        run_embedded(
            &mut db,
            Format::Csv,
            Some(path.to_str().unwrap()),
            std::io::empty(),
        );
        // The dangling INSERT (no final `;`) still executed.
        match db.execute("SELECT COUNT(*) FROM t").unwrap() {
            ExecOutcome::Rows(r) => {
                assert_eq!(r.rows[0][0], Value::Int(1), "script statements ran");
            }
            other => panic!("{other:?}"),
        }
    }

    fn as_remote(
        args: CliArgs,
    ) -> (
        String,
        Option<String>,
        Option<String>,
        Format,
        Option<String>,
    ) {
        match args.mode {
            CliMode::Remote { addr, token, user } => (addr, token, user, args.format, args.script),
            other => panic!("expected remote mode, got {other:?}"),
        }
    }

    #[test]
    fn parse_args_embedded_default() {
        let empty = parse_args(std::iter::empty::<String>()).unwrap();
        assert_eq!(
            empty.mode,
            CliMode::Embedded {
                path: ":memory:".into()
            }
        );
        assert_eq!(empty.format, Format::Table);
        let file = parse_args(["some.db"].into_iter().map(String::from)).unwrap();
        assert_eq!(
            file.mode,
            CliMode::Embedded {
                path: "some.db".into()
            }
        );
    }

    #[test]
    fn parse_args_flags_anywhere_and_script_consumption() {
        let args = parse_args(
            ["--csv", "d.db", "-f", "/tmp/a.sql", "--json"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();
        assert_eq!(args.format, Format::Json);
        assert_eq!(args.script.as_deref(), Some("/tmp/a.sql"));
        assert_eq!(
            args.mode,
            CliMode::Embedded {
                path: "d.db".into()
            }
        );
        // `--file` spelling alongside.
        let args = parse_args(
            ["--file", "b.sql", "--table", "x"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();
        assert_eq!(args.format, Format::Table);
        assert_eq!(args.script.as_deref(), Some("b.sql"));
        // A dangling -f with no argument is a loud parse error.
        let err = parse_args(["-f"].into_iter().map(String::from)).unwrap_err();
        assert!(err.contains("requires a script path"), "err: {err}");
    }

    #[test]
    fn parse_args_rejects_unknown_flags_instead_of_db_paths() {
        // `docsql --help` used to open a database literally named `--help`
        // (plus its WAL) in the working directory.
        for argv in [
            vec!["--help"],
            vec!["-x", "d.db"],
            vec!["d.db", "--unknown"],
            vec!["connect", "n:7600", "--usr", "tok"],
        ] {
            let err = parse_args(argv.into_iter().map(String::from)).unwrap_err();
            assert!(err.contains("unknown option"), "err: {err}");
        }
    }

    #[test]
    fn parse_args_connect_mode() {
        let (addr, token, user, format, script) = as_remote(
            parse_args(
                ["connect", "node-a:7600", "--json", "tok-123"]
                    .into_iter()
                    .map(String::from),
            )
            .unwrap(),
        );
        assert_eq!(addr, "node-a:7600");
        assert_eq!(token.as_deref(), Some("tok-123"));
        assert!(user.is_none());
        assert_eq!(format, Format::Json);
        assert!(script.is_none());

        let (addr, token, user, format, _) = as_remote(
            parse_args(
                ["--csv", "connect", "h:1", "--user", "alice", "-f", "/x.s"]
                    .into_iter()
                    .map(String::from),
            )
            .unwrap(),
        );
        assert_eq!((addr.as_str(), format), ("h:1", Format::Csv));
        assert_eq!(user.as_deref(), Some("alice"));
        assert!(token.is_none());

        // `connect` with no address defaults the loopback port.
        let (addr, _, _, _, _) =
            as_remote(parse_args(["connect"].into_iter().map(String::from)).unwrap());
        assert_eq!(addr, "127.0.0.1:7600");

        // A token after the address lands as the positional, not the user.
        let (_, token, user, _, _) = as_remote(
            parse_args(
                ["connect", "h:1", "--user", "bob", "tok-9"]
                    .into_iter()
                    .map(String::from),
            )
            .unwrap(),
        );
        assert_eq!(user.as_deref(), Some("bob"));
        assert_eq!(token.as_deref(), Some("tok-9"));
    }

    fn server_config_test(db: &std::path::Path, address: &str) -> docsql_server::ServerConfig {
        docsql_server::ServerConfig {
            db_path: db.to_path_buf(),
            listen: address.to_string(),
            auth_token: Some("cli-test-token-123".into()),
            read_token: None,
            max_conn: 16,
            idle_timeout_secs: 0,
            auth_lock_threshold: 0,
            cluster_token: None,
            replicate_to: None,
            peers: Vec::new(),
            advertise: None,
            read_only: false,
            transport_key: None,
            async_commit: false,
            catchup_window: 0,
            backup_interval_secs: 0,
            backup_keep: 0,
            backup_dir: None,
            statement_timeout_ms: 0,
        }
    }

    /// Drive `remote_shell` against a real server started in-process
    /// (tokio::spawn — never a subprocess). Covers the wire layer
    /// (Remote::connect/round_trip, reader_loop, frame printing), inline
    /// pub/sub commands and token auth end to end.
    #[test]
    fn remote_shell_e2e_over_in_process_server() {
        let dir = tempfile::tempdir().unwrap();
        // Reserve an ephemeral port, then hand it to the server.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let cfg = server_config_test(&dir.path().join("db"), &addr.to_string());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.spawn(docsql_server::run(cfg));
        // Wait for the listener, then run the shell against a small script.
        let mut up = false;
        for _ in 0..200 {
            if std::net::TcpStream::connect(addr).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(up, "server never accepted connections");
        let script = dir.path().join("r.sql");
        std::fs::write(
            &script,
            "CREATE TABLE t (id INT PRIMARY KEY, v TEXT);\n\
             INSERT INTO t VALUES (1, 'x'), (2, 'y');\n\
             SELECT id, v FROM t ORDER BY id;\n",
        )
        .unwrap();
        remote_shell(
            &addr.to_string(),
            Some("cli-test-token-123"),
            None,
            Format::Csv,
            Some(script.to_str().unwrap()),
        );
        handle.abort();
        rt.shutdown_timeout(std::time::Duration::from_secs(2));
    }

    /// Cover the inline pub/sub command path (parse → frame → response)
    /// against a live in-process server, plus the reader-thread push
    /// printing (RESP_PUSH) via a subscription.
    #[test]
    fn remote_shell_pubsub_e2e_over_in_process_server() {
        let dir = tempfile::tempdir().unwrap();
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let cfg = server_config_test(&dir.path().join("db"), &addr.to_string());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.spawn(docsql_server::run(cfg));
        let mut up = false;
        for _ in 0..200 {
            if std::net::TcpStream::connect(addr).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(up, "server never accepted connections");
        // subscribe + publish round trip through the inline command parser.
        let script = dir.path().join("p.sql");
        std::fs::write(
            &script,
            "publish mych hello world;\n\
             subscribe mych earliest;\n\
             psubscribe my.*;\n\
             pubsub numsub;\n\
             pubsub numpat;\n\
             unsubscribe mych;\n\
             punsubscribe my.*;\n\
             pubsub channels;\n\
             pubsub trim mych 1;\n\
             help;\n\
             auth cli-test-token-123;\n\
             SELECT 1\n",
        )
        .unwrap();
        remote_shell(
            &addr.to_string(),
            Some("cli-test-token-123"),
            None,
            Format::Table,
            Some(script.to_str().unwrap()),
        );
        handle.abort();
        rt.shutdown_timeout(std::time::Duration::from_secs(2));
    }

    #[test]
    fn print_frame_error_and_affected_arms() {
        // Error frames go to stderr and signal script failure.
        assert!(!print_frame(
            &Frame::new(proto::RESP_ERROR, b"boom".to_vec()),
            Format::Table
        ));
        assert!(print_frame(
            &Frame::new(proto::RESP_AFFECTED, 3u64.to_le_bytes().to_vec()),
            Format::Table
        ));
        // A malformed RESP_ROWS payload must not panic the shell.
        assert!(print_frame(
            &Frame::new(proto::RESP_ROWS, b"not json".to_vec()),
            Format::Table
        ));
    }

    #[test]
    fn parse_args_user_flag_without_value() {
        // `connect h:1 --user` — the missing value makes --user None and the
        // bare "connect" path still resolves (parses without panicking).
        let (_, _, user, _, _) = as_remote(
            parse_args(["connect", "h:1", "--user"].into_iter().map(String::from)).unwrap(),
        );
        assert!(user.is_none());
    }

    #[test]
    fn print_push_renders_pubsub_payloads() {
        // RESP_PUSH JSON bodies render message/pmessage lines; any other
        // body falls back to raw text (smoke: must not panic).
        print_push(&Frame::new(
            proto::RESP_PUSH,
            br#"{"kind":"message","channel":"news","id":7,"payload":"hi"}"#.to_vec(),
        ));
        print_push(&Frame::new(
            proto::RESP_PUSH,
            br#"{"kind":"pmessage","pattern":"n.*","channel":"news","id":8,"payload":"yo"}"#
                .to_vec(),
        ));
        print_push(&Frame::new(proto::RESP_PUSH, b"plain".to_vec()));
    }

    #[test]
    fn run_embedded_interactive_accumulates_and_survives_errors() {
        // 交互分支(script=None):多行累积、help、exit、错误后继续。
        let mut db = Database::in_memory().unwrap();
        let input = std::io::Cursor::new(
            "CREATE TABLE t (\n  id INT PRIMARY KEY,\n  v TEXT\n);\n\
             help;\n\
             SELECT * FROM missing_tbl;\n\
             INSERT INTO t VALUES (1, 'ok');\n\
             exit;\n\
             INSERT INTO t VALUES (2, 'after-exit-ignored');\n",
        );
        run_embedded(&mut db, Format::Table, None, input);
        match db.execute("SELECT COUNT(*) FROM t").unwrap() {
            ExecOutcome::Rows(r) => assert_eq!(r.rows[0][0], Value::Int(1)),
            other => panic!("{other:?}"),
        }
        // 交互模式在 EOF 处理悬挂语句(无结尾分号也执行)。
        let mut db2 = Database::in_memory().unwrap();
        let input2 = std::io::Cursor::new("CREATE TABLE u (id INT);\nINSERT INTO u VALUES (9)\n");
        run_embedded(&mut db2, Format::Csv, None, input2);
        match db2.execute("SELECT COUNT(*) FROM u").unwrap() {
            ExecOutcome::Rows(r) => assert_eq!(r.rows[0][0], Value::Int(1)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn reader_loop_queues_frames_prints_pushes_and_stops_on_garbage() {
        // 一条本地 TCP 对直接驱动读线程:普通帧进队列、RESP_PUSH 不进
        // 队列(打印即弃)、超限帧长与坏帧都让线程收线(通道关闭)。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let server = listener.accept().unwrap().0;
        let (tx, rx) = std::sync::mpsc::channel::<Frame>();
        std::thread::spawn(move || reader_loop(server, tx));
        let mut client = client;
        let send = |s: &mut std::net::TcpStream, f: &Frame| {
            s.write_all(&f.encode().unwrap()).unwrap();
            s.flush().unwrap();
        };
        send(
            &mut client,
            &Frame::new(proto::RESP_AFFECTED, 5u64.to_le_bytes().to_vec()),
        );
        let f = rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert_eq!(f.frame_type, proto::RESP_AFFECTED);
        // 推送帧由读线程消化,永不出现在队列里。
        send(
            &mut client,
            &Frame::new(
                proto::RESP_PUSH,
                br#"{"kind":"message","channel":"c","id":1,"payload":"p"}"#.to_vec(),
            ),
        );
        send(
            &mut client,
            &Frame::new(proto::RESP_AFFECTED, 6u64.to_le_bytes().to_vec()),
        );
        let f = rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert_eq!(
            proto::decode_affected(&f.payload),
            6,
            "push must not consume a queue slot"
        );
        // 声称超限的帧长:线程打印并退出,队列随之关闭。
        let mut bogus = vec![0u8; proto::HEADER_LEN];
        bogus[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        client.write_all(&bogus).unwrap();
        client.flush().unwrap();
        assert!(rx.recv_timeout(std::time::Duration::from_secs(2)).is_err());
    }

    #[test]
    fn reader_loop_exits_when_queue_or_stream_closes() {
        // 主侧先关队列:下一帧的 tx.send 失败,线程收线。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let server = listener.accept().unwrap().0;
        let (tx, rx) = std::sync::mpsc::channel::<Frame>();
        std::thread::spawn(move || reader_loop(server, tx));
        drop(rx);
        let mut client = client;
        let f = Frame::new(proto::RESP_AFFECTED, 1u64.to_le_bytes().to_vec());
        client.write_all(&f.encode().unwrap()).unwrap();
        client.flush().unwrap();
        // 线程退出后写端会观察到断连(等待即可,不强断言时序)。
        std::thread::sleep(std::time::Duration::from_millis(200));

        // 帧头读了一半对端就关闭:read_exact 失败,线程收线不 panic。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr2 = listener.local_addr().unwrap();
        let client2 = std::net::TcpStream::connect(addr2).unwrap();
        let server2 = listener.accept().unwrap().0;
        let (tx2, rx2) = std::sync::mpsc::channel::<Frame>();
        std::thread::spawn(move || reader_loop(server2, tx2));
        let mut client2 = client2;
        client2.write_all(&[0u8; 5]).unwrap(); // 半个帧头
        client2.flush().unwrap();
        drop(client2); // 然后断开
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(rx2
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err());
    }

    #[test]
    fn reader_loop_exits_on_undecodable_frame() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let server = listener.accept().unwrap().0;
        let (tx, rx) = std::sync::mpsc::channel::<Frame>();
        std::thread::spawn(move || reader_loop(server, tx));
        let mut client = client;
        // 合法帧头 + 破损载荷:decode 失败必须收线而不是死循环。
        let mut buf = vec![0u8; proto::HEADER_LEN];
        buf[16..20].copy_from_slice(&8u32.to_le_bytes());
        buf.extend_from_slice(&[0xff; 8]);
        client.write_all(&buf).unwrap();
        client.flush().unwrap();
        assert!(rx.recv_timeout(std::time::Duration::from_secs(2)).is_err());
    }

    #[test]
    fn auth_reports_rejections_and_connection_loss() {
        // 错误 token:RESP_ERROR → false。
        let dir = tempfile::tempdir().unwrap();
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let cfg = server_config_test(&dir.path().join("db"), &addr.to_string());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.spawn(docsql_server::run(cfg));
        let mut up = false;
        for _ in 0..200 {
            if std::net::TcpStream::connect(addr).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(up, "server never accepted connections");
        let mut remote = Remote::connect(&addr.to_string()).unwrap();
        assert!(!auth(&mut remote, "wrong-token-value"));
        // 正确 token 仍可用(同一连接上重试)。
        assert!(auth(&mut remote, "cli-test-token-123"));
        handle.abort();
        rt.shutdown_timeout(std::time::Duration::from_secs(2));

        // 对端接受后立刻关闭:round_trip 收不到帧 → "connection closed"。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let drop_addr = listener.local_addr().unwrap();
        let dropper = std::thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            drop(s);
        });
        let mut dead = Remote::connect(&drop_addr.to_string()).unwrap();
        dropper.join().unwrap();
        assert!(!auth(&mut dead, "any-token"));
    }

    #[test]
    fn auth_user_uses_env_password_and_reports_failure() {
        let dir = tempfile::tempdir().unwrap();
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let cfg = server_config_test(&dir.path().join("db"), &addr.to_string());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handle = rt.spawn(docsql_server::run(cfg));
        let mut up = false;
        for _ in 0..200 {
            if std::net::TcpStream::connect(addr).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(up, "server never accepted connections");
        // DOCSQL_PASSWORD 提供口令(无用户存在 → 登录被拒,但不走交互提示)。
        std::env::set_var("DOCSQL_PASSWORD", "pw-from-env-1");
        let mut remote = Remote::connect(&addr.to_string()).unwrap();
        assert!(!auth_user(&mut remote, "ghost"));
        std::env::remove_var("DOCSQL_PASSWORD");
        handle.abort();
        rt.shutdown_timeout(std::time::Duration::from_secs(2));
    }

    #[test]
    fn print_frame_tolerates_malformed_rows_payloads() {
        // rows 数组元素不是数组:按空行处理,不 panic。
        let payload = br#"{"columns":["a"],"rows":[1,2]}"#.to_vec();
        assert!(print_frame(
            &Frame::new(proto::RESP_ROWS, payload),
            Format::Table
        ));
        // columns 不是数组:同样降级。
        let payload = br#"{"columns":"a","rows":[[1]]}"#.to_vec();
        assert!(print_frame(
            &Frame::new(proto::RESP_ROWS, payload),
            Format::Csv
        ));
    }
}

#[cfg(test)]
mod statement_ready_tests {
    use super::{split_ready, statements_ready};

    #[test]
    fn semicolons_inside_literals_do_not_terminate() {
        assert!(!statements_ready("INSERT INTO d VALUES ('line1;"));
        assert!(statements_ready("INSERT INTO d VALUES ('line1; line2');"));
        assert!(!statements_ready("SELECT 'a''b;"));
        assert!(statements_ready("SELECT 'a''b;';"));
        assert!(!statements_ready("SELECT \"ready?;"));
        assert!(statements_ready("SELECT \"ready?;\";"));
    }

    #[test]
    fn backtick_identifiers_and_doubled_backticks() {
        // 反引号标识符里的 `;` 是数据;成对反引号是转义。
        assert!(statements_ready("SELECT `a;b` FROM t;"));
        assert!(!statements_ready("SELECT `a;b FROM t;"));
        assert!(!statements_ready("SELECT `x``y;"));
        assert!(statements_ready("SELECT `x``y`;"));
        // 未闭合的块注释 / 未闭合反引号都不算就绪。
        assert!(!statements_ready("SELECT `open;"));
        assert!(!statements_ready("SELECT 1; /* still open"));
    }

    #[test]
    fn quoted_identifiers_with_doubled_quotes_and_comment_newlines() {
        // 双引号标识符内的成对双引号是转义。
        assert!(statements_ready("SELECT \"a\"\"b;\" FROM t;"));
        assert!(!statements_ready("SELECT \"a\"\"b;"));
        // 行注释遇换行回到代码态,其后的分号照常终结。
        assert!(statements_ready("SELECT 1 -- wait\n;"));
        assert!(!statements_ready("SELECT 1 -- wait\nSELECT 2"));
    }

    #[test]
    fn comments_and_whitespace_after_semicolon() {
        assert!(statements_ready("SELECT 1;"));
        assert!(statements_ready("SELECT 1; -- done"));
        assert!(statements_ready("SELECT 1; /* done */"));
        assert!(!statements_ready("SELECT 1 -- not done yet"));
        assert!(!statements_ready("SELECT 1"));
        assert!(!statements_ready("SELECT 1 /* ; */"));
    }

    #[test]
    fn split_ready_handles_multi_statement_lines_and_bad_sql() {
        assert_eq!(split_ready("SELECT 1; SELECT 2;").len(), 2);
        // A parse failure comes back as one chunk so the engine reports it.
        assert_eq!(
            split_ready("SELEC bogus;"),
            vec!["SELEC bogus;".to_string()]
        );
        assert!(split_ready("SELECT 1; -- trailing\n").len() == 1);
    }
}
