//! docsql web console binary — a management tool for DocSQL nodes. It keeps
//! no data of its own: every data operation connects to the managed node
//! (positional argument / DOCSQL_UPSTREAM, else the first DOCSQL_PEERS
//! entry) over the wire protocol.
//!
//!     docsql-web [managed-node] [listen]

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let listen = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7700".into());
    let token = std::env::var("DOCSQL_TOKEN").ok().filter(|t| !t.is_empty());
    // Cluster nodes to monitor and switch between on the status page.
    let peers: Vec<String> = std::env::var("DOCSQL_PEERS")
        .unwrap_or_default()
        .split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    // The default managed node: explicit argument first, then the
    // DOCSQL_UPSTREAM environment, then the first configured peer.
    // All three sources are trimmed — peers already are above.
    let upstream = args
        .get(1)
        .cloned()
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty())
        .or_else(|| {
            std::env::var("DOCSQL_UPSTREAM")
                .ok()
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
        })
        .or_else(|| peers.first().cloned());
    match &upstream {
        Some(u) => eprintln!("docsql web console on http://{listen} (managing {u})"),
        None => eprintln!(
            "docsql web console on http://{listen} (no managed node configured — \
             data endpoints report an error until one is set)"
        ),
    }
    docsql_web::run(
        docsql_web::WebConfig {
            upstream,
            token,
            peers,
        },
        &listen,
    )
    .await
}
