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
    docsql_server::run(docsql_server::ServerConfig {
        db_path: std::path::PathBuf::from(db_path),
        listen,
        auth_token: token,
        replicate_to,
        peers,
        read_only,
        transport_key,
        async_commit,
    })
    .await
}
