//! docsql web console binary — a management tool for DocSQL nodes. It keeps
//! no data of its own: every data operation connects to the managed node
//! (positional argument / DOCSQL_UPSTREAM, else the first DOCSQL_PEERS
//! entry) over the wire protocol.
//!
//!     docsql-web [managed-node] [listen]

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
    match &cfg.upstream {
        Some(u) => eprintln!("docsql web console on http://{} (managing {u})", cfg.listen),
        None => eprintln!(
            "docsql web console on http://{} (no managed node configured — \
             data endpoints report an error until one is set)",
            cfg.listen
        ),
    }
    if let Some(tls) = &cfg.tls {
        eprintln!("docsql web console serving HTTPS (cert {})", tls.cert_path);
    }
    let listen = cfg.listen.clone();
    docsql_web::run(cfg.map_api(), &listen).await
}

/// TLS on/off from the environment. Native TLS serves HTTPS directly only
/// when both cert and key are set; `DOCSQL_WEB_TLS_CERT` without
/// `DOCSQL_WEB_TLS_KEY` (or vice versa) refuses startup.
fn tls_from_env(
    getenv: &impl Fn(&str) -> Option<String>,
) -> Result<Option<docsql_web::TlsConfig>, String> {
    let cert = getenv("DOCSQL_WEB_TLS_CERT").filter(|p| !p.is_empty());
    let key = getenv("DOCSQL_WEB_TLS_KEY").filter(|p| !p.is_empty());
    match (cert, key) {
        (Some(cert), Some(key)) => Ok(Some(docsql_web::TlsConfig {
            cert_path: cert,
            key_path: key,
        })),
        (Some(_), None) | (None, Some(_)) => {
            Err("DOCSQL_WEB_TLS_CERT and DOCSQL_WEB_TLS_KEY must be set together".into())
        }
        (None, None) => Ok(None),
    }
}

/// Assemble the [WebConfig] from positional args and the environment. Pure
/// over an injected env provider so tests can cover the resolution order
/// (positional > DOCSQL_UPSTREAM > first peer) and the TLS pairing rule.
fn config_from_env(
    args: &[String],
    getenv: impl Fn(&str) -> Option<String>,
) -> Result<WebCfg, String> {
    let listen = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7700".into());
    let token = getenv("DOCSQL_TOKEN").filter(|t| !t.is_empty());
    let auth_file = getenv("DOCSQL_WEB_AUTH_FILE").filter(|p| !p.is_empty());
    let peers: Vec<String> = getenv("DOCSQL_PEERS")
        .unwrap_or_default()
        .split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    // The default managed node: explicit argument first, then the
    // DOCSQL_UPSTREAM environment, then the first configured peer.
    let upstream = args
        .get(1)
        .cloned()
        .map(|a| a.trim().to_string())
        .filter(|a| !a.is_empty())
        .or_else(|| {
            getenv("DOCSQL_UPSTREAM")
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
        })
        .or_else(|| peers.first().cloned());
    // Fail fast on a nonsensical override (the consumer is
    // docsql_core::kdf's once-read iterations setting).
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
    }
    let tls = tls_from_env(&getenv)?;
    Ok(WebCfg {
        listen,
        token,
        auth_file,
        peers,
        upstream,
        tls,
    })
}

/// Resolved startup options (private): the exact shape handed to
/// [docsql_web::run]. Split from the public [docsql_web::WebConfig] only so
/// the binary can log `listen` before entering the server.
struct WebCfg {
    listen: String,
    token: Option<String>,
    auth_file: Option<String>,
    peers: Vec<String>,
    upstream: Option<String>,
    tls: Option<docsql_web::TlsConfig>,
}

impl WebCfg {
    fn map_api(self) -> docsql_web::WebConfig {
        docsql_web::WebConfig {
            upstream: self.upstream,
            token: self.token,
            peers: self.peers,
            auth_file: self.auth_file,
            tls: self.tls,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn defaults_without_args_or_env() {
        let cfg = config_from_env(&args(&[]), no_env).unwrap();
        assert_eq!(cfg.listen, "127.0.0.1:7700");
        assert!(cfg.upstream.is_none());
        assert!(cfg.token.is_none());
        assert!(cfg.auth_file.is_none());
        assert!(cfg.peers.is_empty());
        assert!(cfg.tls.is_none());
    }

    #[test]
    fn upstream_resolution_order() {
        // Positional arg wins over everything.
        let cfg = config_from_env(
            &args(&["docsql-web", "node-a:7600", "0.0.0.0:7800"]),
            no_env,
        )
        .unwrap();
        assert_eq!(cfg.upstream.as_deref(), Some("node-a:7600"));
        assert_eq!(cfg.listen, "0.0.0.0:7800");
        // DOCSQL_UPSTREAM wins over the first peer.
        let cfg = config_from_env(
            &args(&[]),
            env(&[("DOCSQL_UPSTREAM", " a:1 "), ("DOCSQL_PEERS", "b:2,c:3")]),
        )
        .unwrap();
        assert_eq!(cfg.upstream.as_deref(), Some("a:1"));
        assert_eq!(cfg.peers, vec!["b:2", "c:3"]);
        // No arg/env but peers configured: first peer.
        let cfg = config_from_env(&args(&[]), env(&[("DOCSQL_PEERS", "b:2,c:3")])).unwrap();
        assert_eq!(cfg.upstream.as_deref(), Some("b:2"));
    }

    #[test]
    fn peers_split_and_trim_and_ignore_blanks() {
        let cfg = config_from_env(
            &args(&["docsql-web", "   ", "x:1"]),
            env(&[("DOCSQL_PEERS", " a:1 ,,b:2,")]),
        )
        .unwrap();
        assert_eq!(cfg.peers, vec!["a:1", "b:2"]);
        // A blank positional upstream is dropped, not taken literally — the
        // resolution falls through to the first configured peer.
        assert_eq!(cfg.upstream.as_deref(), Some("a:1"));
        // Same blank, but no peers: env then takes over.
        let cfg = config_from_env(
            &args(&["docsql-web", "   "]),
            env(&[("DOCSQL_UPSTREAM", "z:9")]),
        )
        .unwrap();
        assert_eq!(cfg.upstream.as_deref(), Some("z:9"));
    }

    #[test]
    fn tls_pairing_rule() {
        let cfg = config_from_env(
            &args(&[]),
            env(&[("DOCSQL_WEB_TLS_CERT", "/c"), ("DOCSQL_WEB_TLS_KEY", "/k")]),
        )
        .unwrap();
        let tls = cfg.tls.unwrap();
        assert_eq!(tls.cert_path, "/c");
        assert_eq!(tls.key_path, "/k");
        // One sans the other must refuse startup.
        assert!(config_from_env(&args(&[]), env(&[("DOCSQL_WEB_TLS_CERT", "/c")])).is_err());
        assert!(config_from_env(&args(&[]), env(&[("DOCSQL_WEB_TLS_KEY", "/k")])).is_err());
        assert!(config_from_env(&args(&[]), no_env).unwrap().tls.is_none());
    }

    #[test]
    fn token_and_auth_file_filters_blanks() {
        let cfg = config_from_env(
            &args(&[]),
            env(&[
                ("DOCSQL_TOKEN", "t"),
                ("DOCSQL_WEB_AUTH_FILE", "/auth/users.json"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.token.as_deref(), Some("t"));
        assert_eq!(cfg.auth_file.as_deref(), Some("/auth/users.json"));
        let cfg = config_from_env(
            &args(&[]),
            env(&[("DOCSQL_TOKEN", ""), ("DOCSQL_WEB_AUTH_FILE", "")]),
        )
        .unwrap();
        assert!(cfg.token.is_none() && cfg.auth_file.is_none());
    }

    #[test]
    fn map_api_carries_every_field() {
        let cfg = config_from_env(
            &args(&["docsql-web", "u:1", "0.0.0.0:7800"]),
            env(&[
                ("DOCSQL_TOKEN", "t"),
                ("DOCSQL_WEB_AUTH_FILE", "/auth/users.json"),
                ("DOCSQL_PEERS", "p:1,p:2"),
            ]),
        )
        .unwrap();
        let api = cfg.map_api();
        assert_eq!(api.upstream.as_deref(), Some("u:1"));
        assert_eq!(api.token.as_deref(), Some("t"));
        assert_eq!(api.auth_file.as_deref(), Some("/auth/users.json"));
        assert_eq!(api.peers, vec!["p:1".to_string(), "p:2".to_string()]);
        assert!(api.tls.is_none());
    }
}
