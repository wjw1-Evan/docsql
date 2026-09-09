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
//!                                     (resource control; 0 = unlimited)
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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).cloned().unwrap_or_else(|| "docsql.db".into());
    let listen = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7600".into());
    let token = std::env::var("DOCSQL_TOKEN").ok().filter(|t| !t.is_empty());
    let read_token = std::env::var("DOCSQL_READ_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let cluster_token = std::env::var("DOCSQL_CLUSTER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    // Credential complexity floor: refuse to boot with a secret that falls
    // to a trivial online-guessing campaign.
    for (name, value) in [
        ("DOCSQL_TOKEN", &token),
        ("DOCSQL_READ_TOKEN", &read_token),
        ("DOCSQL_CLUSTER_TOKEN", &cluster_token),
    ] {
        if let Some(t) = value {
            if let Err(e) = docsql_server::check_token_strength(name, t) {
                eprintln!("refusing to start: {e}");
                std::process::exit(2);
            }
        }
    }
    let max_conn = std::env::var("DOCSQL_MAX_CONN")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let idle_timeout_secs = std::env::var("DOCSQL_IDLE_TIMEOUT")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let replicate_to = std::env::var("DOCSQL_REPLICATE_TO")
        .ok()
        .filter(|s| !s.is_empty());
    let peers = std::env::var("DOCSQL_PEERS")
        .map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let advertise = std::env::var("DOCSQL_ADVERTISE")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let read_only = std::env::var("DOCSQL_READ_ONLY")
        .map(|v| v == "1")
        .unwrap_or(false);
    let async_commit = std::env::var("DOCSQL_ASYNC_COMMIT")
        .map(|v| v == "1")
        .unwrap_or(false);
    let transport_key = match std::env::var("DOCSQL_KEY") {
        Ok(k) if !k.trim().is_empty() => Some(
            docsql_server::crypto::parse_key_hex(&k).unwrap_or_else(|e| panic!("DOCSQL_KEY: {e}")),
        ),
        _ => None,
    };
    // Plaintext transport on a non-loopback bind exposes every frame —
    // including AUTH tokens and row data — to the local network. Warn
    // loudly at startup (national-security testing probes exactly this).
    let bind_loopback = listen
        .rsplit_once(':')
        .and_then(|(host, _)| {
            let host = host.trim_start_matches('[').trim_end_matches(']');
            host.parse::<std::net::IpAddr>().ok()
        })
        .map(|ip| ip.is_loopback())
        .unwrap_or(true);
    if !bind_loopback && transport_key.is_none() {
        eprintln!(
            "warning: listening on a non-loopback address without DOCSQL_KEY — \
             traffic is plaintext; set DOCSQL_KEY (and DOCSQL_TOKEN) for any exposed deployment"
        );
    }
    docsql_server::run(docsql_server::ServerConfig {
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
    })
    .await
}
