//! docsql server binary.

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).cloned().unwrap_or_else(|| "docsql.db".into());
    let listen = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7600".into());
    let token = std::env::var("DOCSQL_TOKEN").ok().filter(|t| !t.is_empty());
    docsql_server::run(docsql_server::ServerConfig {
        db_path: std::path::PathBuf::from(db_path),
        listen,
        auth_token: token,
    })
    .await
}
