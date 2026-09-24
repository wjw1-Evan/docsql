//! docsql server binary.
//!
//!     docsql-server <db-path> <listen>
//!
//! Cluster env vars:
//!     DOCSQL_REPLICATE_TO=host:port   forward writes to a replica
//!     DOCSQL_READ_ONLY=1              replica mode (reject client writes)
//!     DOCSQL_PEERS=a:port,b:port      symmetric cluster: no primary/replica
//!                                     roles — any node accepts writes and
//!                                     fans them out to every peer
//!     DOCSQL_ADVERTISE=host:port      how peers should reach this node
//!                                     when it joins: a fresh node (no user
//!                                     tables) with DOCSQL_PEERS set pulls a
//!                                     full snapshot from the first peer
//!                                     that answers and registers itself
//!                                     cluster-wide (peers keep the
//!                                     registration until they restart —
//!                                     persist it by also listing the node
//!                                     in their DOCSQL_PEERS)
//!     DOCSQL_CLUSTER_TOKEN=<secret>   inter-node credential: fan-out
//!                                     authenticates with it and only
//!                                     connections that present it may send
//!                                     FLAG_REPLICATION frames (clients
//!                                     authenticated with DOCSQL_TOKEN
//!                                     cannot forge node traffic). All
//!                                     nodes in a cluster must share it;
//!                                     unset = legacy behavior
//!     DOCSQL_READ_TOKEN=<secret>      least-privilege client credential:
//!                                     connections authenticated with it may
//!                                     read and subscribe but not write
//!     DOCSQL_MAX_CONN=<n>             max concurrent connections
//!                                     (resource control; default 1024,
//!                                     0 = unlimited)
//!     DOCSQL_IDLE_TIMEOUT=<secs>      close connections idle this long
//!                                     (session timeout; 0 = unlimited;
//!                                     subscription clients should send
//!                                     periodic PING keepalives when set)
//!     DOCSQL_KEY=<64 hex chars>       AES-256-GCM transport encryption
//!                                     (shared by clients and cluster peers)
//!     DOCSQL_SLOW_MS=<float>          slow-query threshold for stderr log
//!                                     (default 100); DOCSQL_LOG_FILE=<path>
//!                                     appends every statement as JSONL;
//!                                     query via SELECT ... FROM docsql_log
//!     DOCSQL_ASYNC_COMMIT=1           group fsyncs every ~2ms instead of
//!                                     one fsync per write (higher write
//!                                     throughput, ms-scale loss window)
//!     DOCSQL_CATCHUP_WINDOW=<n>       catch-up journal retention in
//!                                     entries (default 100000; 0 =
//!                                     unbounded) — how far a rejoined
//!                                     peer can incrementally catch up
//!                                     before a full snapshot is needed
//!     DOCSQL_BACKUP_INTERVAL_SECS=<n> automatic backup cadence in
//!                                     seconds (default 86400 = daily;
//!                                     0 = disabled). Each backup is a
//!                                     logical SQL dump written under
//!                                     the write path, so it is a
//!                                     consistent point-in-time snapshot
//!     DOCSQL_BACKUP_KEEP=<n>          backups retained per node, oldest
//!                                     pruned (default 7)
//!     DOCSQL_BACKUP_DIR=<path>        backup directory (default
//!                                     <db-dir>/backups — /data/backups
//!                                     in the containers, inside the
//!                                     data volume)
//!     DOCSQL_STATEMENT_TIMEOUT_MS=<n> wall-clock budget per CLIENT
//!                                     statement (0 = unlimited, the
//!                                     default). A statement exceeding it
//!                                     fails with a timeout error — the
//!                                     server-side kill switch against
//!                                     runaway queries on the single
//!                                     writer. Replication apply and
//!                                     restore replay are exempt.

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let cfg = match config_from_env(&args, |name| std::env::var(name).ok()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("refusing to start: {e}");
            std::process::exit(2);
        }
    };
    // Plaintext transport on a non-loopback bind exposes every frame —
    // including AUTH tokens and row data — to the local network. Warn
    // loudly at startup (national-security testing probes exactly this).
    if !listen_is_loopback(&cfg.listen) && cfg.transport_key.is_none() {
        eprintln!(
            "warning: listening on a non-loopback address without DOCSQL_KEY — \
             traffic is plaintext; set DOCSQL_KEY (and DOCSQL_TOKEN) for any exposed deployment"
        );
    }
    docsql_server::run(cfg).await
}

fn listen_is_loopback(listen: &str) -> bool {
    listen
        .rsplit_once(':')
        .and_then(|(host, _)| {
            let host = host.trim_start_matches('[').trim_end_matches(']');
            host.parse::<std::net::IpAddr>().ok()
        })
        .map(|ip| ip.is_loopback())
        // Unparseable host (a hostname): treat as NON-loopback so the
        // plaintext warning prints — it may resolve anywhere, and staying
        // quiet is the one irrecoverable direction.
        .unwrap_or(false)
}

/// Assemble the [ServerConfig] from positional args and the environment.
/// Every env value is validated up front (token strength, numeric parsing,
/// key hex) so a bad config refuses to boot loudly instead of running with
/// an unintended default. Pure over an injected env provider so tests can
/// cover the whole decision surface without a process.
fn config_from_env(
    args: &[String],
    getenv: impl Fn(&str) -> Option<String>,
) -> Result<docsql_server::ServerConfig, String> {
    let db_path = args.get(1).cloned().unwrap_or_else(|| "docsql.db".into());
    let listen = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7600".into());
    let token = getenv("DOCSQL_TOKEN").filter(|t| !t.is_empty());
    let read_token = getenv("DOCSQL_READ_TOKEN").filter(|t| !t.is_empty());
    let cluster_token = getenv("DOCSQL_CLUSTER_TOKEN").filter(|t| !t.is_empty());
    // Credential complexity floor: refuse to boot with a secret that falls
    // to a trivial online-guessing campaign.
    for (name, value) in [
        ("DOCSQL_TOKEN", &token),
        ("DOCSQL_READ_TOKEN", &read_token),
        ("DOCSQL_CLUSTER_TOKEN", &cluster_token),
    ] {
        if let Some(t) = value {
            docsql_server::check_token_strength(name, t).map_err(|e| e.to_string())?;
        }
    }
    // Default finite: each connection is a tokio task with a read buffer
    // budget and a writer channel — the old unlimited default left the one
    // resource dimension with no ceiling at all (accept-until-fd-exhaustion
    // under a connection flood). Explicit DOCSQL_MAX_CONN=0 restores
    // unlimited.
    let max_conn = env_num("DOCSQL_MAX_CONN", 1024, &getenv)?;
    let idle_timeout_secs = env_num("DOCSQL_IDLE_TIMEOUT", 0, &getenv)?;
    // Trim like DOCSQL_PEERS: a stray space makes every fan-out to the
    // upstream fail (observed only as sync-log errors).
    let replicate_to = getenv("DOCSQL_REPLICATE_TO")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let peers = getenv("DOCSQL_PEERS")
        .map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let advertise = getenv("DOCSQL_ADVERTISE")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    // Boolean envs: trim like every other env, and refuse anything but the
    // documented spellings — a trailing CR/space in a .env used to make
    // DOCSQL_READ_ONLY=1␍ read as `false`, silently booting a replica
    // writable (fail-open on a security flag).
    let read_only = env_bool("DOCSQL_READ_ONLY", &getenv)?;
    let async_commit = env_bool("DOCSQL_ASYNC_COMMIT", &getenv)?;
    let catchup_window = env_num("DOCSQL_CATCHUP_WINDOW", 100_000, &getenv)?;
    let backup_interval_secs = env_num("DOCSQL_BACKUP_INTERVAL_SECS", 86_400, &getenv)?;
    let backup_keep = env_num("DOCSQL_BACKUP_KEEP", 7, &getenv)?;
    let statement_timeout_ms = env_num("DOCSQL_STATEMENT_TIMEOUT_MS", 0, &getenv)?;
    // Startup-fail-fast only; the consumer is docsql_core::kdf's once-read
    // override (test suites lower it to keep auth-path e2e meaningful).
    if let Some(raw) = getenv("DOCSQL_PBKDF2_ITERATIONS") {
        let n: u64 = raw
            .trim()
            .parse()
            .map_err(|_| "DOCSQL_PBKDF2_ITERATIONS must be an integer".to_string())?;
        if n == 0 || n > docsql_core::kdf::MAX_PBKDF2_ITERATIONS as u64 {
            return Err(format!(
                "DOCSQL_PBKDF2_ITERATIONS must be 1..={}",
                docsql_core::kdf::MAX_PBKDF2_ITERATIONS
            ));
        }
        if n < 10_000 {
            eprintln!(
                "warning: DOCSQL_PBKDF2_ITERATIONS={n} is far below the 210000 default — \
                 every NEW credential becomes cheap to brute-force offline (existing \
                 credentials keep their stored iteration count)"
            );
        }
    }
    let backup_dir = getenv("DOCSQL_BACKUP_DIR")
        .map(std::path::PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty());
    let transport_key = match getenv("DOCSQL_KEY") {
        Some(k) if !k.trim().is_empty() => {
            let key =
                docsql_server::crypto::parse_key_hex(&k).map_err(|e| format!("DOCSQL_KEY: {e}"))?;
            // An all-zero key is a PUBLIC key: routing encryption through it
            // is worse than no encryption because it reads as protected.
            // Same policy as weak tokens: refuse to boot.
            if key.iter().all(|&b| b == 0) {
                return Err("DOCSQL_KEY is all zeros — generate a real 32-byte key \
                     (e.g. openssl rand -hex 32)"
                    .into());
            }
            Some(key)
        }
        _ => None,
    };
    Ok(docsql_server::ServerConfig {
        db_path: std::path::PathBuf::from(db_path),
        listen,
        auth_token: token,
        read_token,
        max_conn,
        idle_timeout_secs,
        auth_lock_threshold: docsql_server::AUTH_LOCK_THRESHOLD,
        cluster_token,
        replicate_to,
        peers,
        advertise,
        read_only,
        transport_key,
        async_commit,
        catchup_window,
        backup_interval_secs,
        backup_keep,
        backup_dir,
        statement_timeout_ms,
    })
}

/// Numeric env with fail-fast validation: a malformed value (typo like
/// `DOCSQL_MAX_CONN=10O`) must refuse startup loudly, not silently fall
/// back to the default and leave the operator with the wrong limits.
fn env_num<T: std::str::FromStr>(
    name: &str,
    default: T,
    getenv: &impl Fn(&str) -> Option<String>,
) -> Result<T, String> {
    match getenv(name) {
        Some(v) if !v.trim().is_empty() => v
            .trim()
            .parse::<T>()
            .map_err(|_| format!("{name}: invalid integer value {v:?}")),
        _ => Ok(default),
    }
}

/// Boolean env: only the documented spellings pass, everything else refuses
/// to boot loudly (same fail-fast rule as the numeric envs). A trailing
/// CR/space in a .env used to make `DOCSQL_READ_ONLY=1␍` read as false —
/// fail-open on a security flag.
fn env_bool(name: &str, getenv: &impl Fn(&str) -> Option<String>) -> Result<bool, String> {
    match getenv(name) {
        Some(v) if !v.trim().is_empty() => match v.trim() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            other => Err(format!(
                "{name}: invalid boolean value {other:?} (expected 1/0/true/false)"
            )),
        },
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn env<'m>(map: &'m [(&'m str, &'m str)]) -> impl Fn(&str) -> Option<String> + 'm {
        move |n| {
            map.iter()
                .find(|(k, _)| *k == n)
                .map(|(_, v)| v.to_string())
        }
    }

    /// ServerConfig has no Debug; assert the error text straight off.
    fn cfg_err(args: &[&str], env: impl Fn(&str) -> Option<String>) -> String {
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        match config_from_env(&args, env) {
            Ok(_) => panic!("config unexpectedly accepted"),
            Err(e) => e,
        }
    }

    #[test]
    fn defaults_without_args_or_env() {
        let cfg = config_from_env(&[], no_env).unwrap();
        assert_eq!(cfg.db_path, PathBuf::from("docsql.db"));
        assert_eq!(cfg.listen, "127.0.0.1:7600");
        assert!(cfg.auth_token.is_none());
        assert!(cfg.read_token.is_none());
        assert!(cfg.cluster_token.is_none());
        // Default finite (resource ceiling on tasks/buffers/fds); explicit
        // DOCSQL_MAX_CONN=0 restores unlimited.
        assert_eq!(cfg.max_conn, 1024);
        assert_eq!(cfg.catchup_window, 100_000);
        assert_eq!(cfg.backup_interval_secs, 86_400);
        assert_eq!(cfg.backup_keep, 7);
        assert_eq!(cfg.statement_timeout_ms, 0);
        assert!(cfg.peers.is_empty());
        assert!(!cfg.read_only);
        assert!(!cfg.async_commit);
        assert!(cfg.transport_key.is_none());
    }

    #[test]
    fn positional_args_override_defaults() {
        let cfg = config_from_env(
            &[
                "docsql-server".into(),
                "/tmp/db.sql".into(),
                "0.0.0.0:9999".into(),
            ],
            no_env,
        )
        .unwrap();
        assert_eq!(cfg.db_path, PathBuf::from("/tmp/db.sql"));
        assert_eq!(cfg.listen, "0.0.0.0:9999");
    }

    #[test]
    fn weak_tokens_refuse_to_boot() {
        let err = cfg_err(&[], env(&[("DOCSQL_TOKEN", "aaaa")]));
        assert!(err.contains("DOCSQL_TOKEN"), "err: {err}");
        let err = cfg_err(&[], env(&[("DOCSQL_CLUSTER_TOKEN", "x")]));
        assert!(err.contains("DOCSQL_CLUSTER_TOKEN"), "err: {err}");
    }

    #[test]
    fn strong_tokens_are_accepted() {
        let cfg = config_from_env(
            &[],
            env(&[
                ("DOCSQL_TOKEN", "correct-horse-battery-staple"),
                ("DOCSQL_READ_TOKEN", "read-only-secret-42"),
            ]),
        )
        .unwrap();
        assert_eq!(
            cfg.auth_token.as_deref(),
            Some("correct-horse-battery-staple")
        );
        assert_eq!(cfg.read_token.as_deref(), Some("read-only-secret-42"));
    }

    #[test]
    fn peers_split_and_trim() {
        let cfg = config_from_env(
            &[],
            env(&[
                ("DOCSQL_PEERS", " a:1 ,b:2,"),
                ("DOCSQL_ADVERTISE", " self:9 "),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.peers, vec!["a:1", "b:2"]);
        assert_eq!(cfg.advertise.as_deref(), Some("self:9"));
        let cfg = config_from_env(&[], env(&[("DOCSQL_PEERS", "")])).unwrap();
        assert!(cfg.peers.is_empty());
    }

    #[test]
    fn numeric_env_parses_defaults_and_fails_loudly() {
        let cfg = config_from_env(
            &[],
            env(&[
                ("DOCSQL_MAX_CONN", "50"),
                ("DOCSQL_IDLE_TIMEOUT", "300"),
                ("DOCSQL_CATCHUP_WINDOW", "0"),
                ("DOCSQL_BACKUP_KEEP", "3"),
                ("DOCSQL_BACKUP_INTERVAL_SECS", "30"),
                ("DOCSQL_STATEMENT_TIMEOUT_MS", "1000"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.max_conn, 50);
        assert_eq!(cfg.idle_timeout_secs, 300);
        assert_eq!(cfg.catchup_window, 0);
        assert_eq!(cfg.backup_keep, 3);
        assert_eq!(cfg.backup_interval_secs, 30);
        assert_eq!(cfg.statement_timeout_ms, 1000);
        // Malformed integers refuse; a whitespace-only value falls back to
        // the default rather than failing.
        for v in ["10O", "abc"] {
            let err = cfg_err(&[], env(&[("DOCSQL_MAX_CONN", v)]));
            assert!(err.contains("DOCSQL_MAX_CONN"), "{v:?}: err {err}");
        }
        let cfg = config_from_env(&[], env(&[("DOCSQL_MAX_CONN", "  ")])).unwrap();
        // Whitespace falls back to the (finite) default, not unlimited.
        assert_eq!(cfg.max_conn, 1024);
    }

    #[test]
    fn flags_and_booleans() {
        let cfg = config_from_env(
            &[],
            env(&[
                ("DOCSQL_READ_ONLY", "1"),
                ("DOCSQL_ASYNC_COMMIT", "1"),
                ("DOCSQL_REPLICATE_TO", " host:7 "),
            ]),
        )
        .unwrap();
        assert!(cfg.read_only);
        assert!(cfg.async_commit);
        // Replicate-to is trimmed like DOCSQL_PEERS: a stray space would
        // make every fan-out to the upstream fail.
        assert_eq!(cfg.replicate_to.as_deref(), Some("host:7"));
        let cfg = config_from_env(&[], env(&[("DOCSQL_READ_ONLY", "0")])).unwrap();
        assert!(!cfg.read_only);
    }

    #[test]
    fn backup_dir_filtered_when_empty() {
        let cfg = config_from_env(&[], env(&[("DOCSQL_BACKUP_DIR", "/data/backups")])).unwrap();
        assert_eq!(cfg.backup_dir, Some(PathBuf::from("/data/backups")));
        let cfg = config_from_env(&[], env(&[("DOCSQL_BACKUP_DIR", "")])).unwrap();
        assert!(cfg.backup_dir.is_none());
    }

    #[test]
    fn transport_key_parses_or_refuses_on_bad_hex() {
        let good = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let cfg = config_from_env(&[], env(&[("DOCSQL_KEY", good)])).unwrap();
        assert!(cfg.transport_key.is_some());
        let err = cfg_err(&[], env(&[("DOCSQL_KEY", "not-hex")]));
        assert!(err.contains("DOCSQL_KEY"), "err: {err}");
        let cfg = config_from_env(&[], env(&[("DOCSQL_KEY", "")])).unwrap();
        assert!(cfg.transport_key.is_none());
    }

    #[test]
    fn listen_loopback_detection() {
        assert!(listen_is_loopback("127.0.0.1:7600"));
        assert!(listen_is_loopback("[::1]:7600"));
        assert!(!listen_is_loopback("0.0.0.0:7600"));
        assert!(!listen_is_loopback("192.168.1.5:7600"));
        // Unparseable host: treated as NON-loopback so the plaintext
        // warning prints (it may resolve anywhere).
        assert!(!listen_is_loopback("docsql-a:7600"));
    }

    #[test]
    fn env_num_error_messages_carry_name_and_value() {
        let err = env_num("DOCSQL_MAX_CONN", 0, &env(&[("DOCSQL_MAX_CONN", "nope")])).unwrap_err();
        assert!(
            err.contains("DOCSQL_MAX_CONN") && err.contains("nope"),
            "err: {err}"
        );
    }
}
