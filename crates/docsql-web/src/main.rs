//! docsql web console binary.
//!
//!     docsql-web <db-path> <listen>

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).cloned().unwrap_or_else(|| "docsql.db".into());
    let listen = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7700".into());
    let token = std::env::var("DOCSQL_TOKEN").ok().filter(|t| !t.is_empty());
    eprintln!("docsql web console on http://{listen}");
    docsql_web::run(
        docsql_web::WebConfig {
            db_path: std::path::PathBuf::from(db_path),
            token,
        },
        &listen,
    )
    .await
}
