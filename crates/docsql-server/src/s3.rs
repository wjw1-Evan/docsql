//! Minimal S3-compatible object-storage client for remote backup copies
//! (`DOCSQL_BACKUP_S3_*`): the disaster-recovery half of the backup story —
//! a data volume lost to disk failure must not take the only backup with
//! it. The local `.sql`/`.sha256` pair stays the source of truth; each
//! finished backup and incremental segment is additionally PUT to an
//! S3-compatible bucket (AWS S3, MinIO, S3-capable NAS gateways), and a
//! restore whose local file is missing fetches it back before replaying.
//!
//! Hand-rolled like every network/crypto surface in this crate (the
//! project carries no general HTTP client): AWS SigV4 request signing over
//! one HTTP/1.1 request per connection, path-style addressing
//! (`https://endpoint/bucket/key` — the form MinIO and on-prem gateways
//! speak), `https` endpoints wrapped in the same rustls connector as the
//! data plane (`DOCSQL_BACKUP_S3_CA` = verify against a trust anchor, no
//! CA = encrypt-only, mirroring `DOCSQL_TLS_CONNECT`). HMAC-SHA256 comes
//! from `docsql_core::kdf` — no external crypto crate.
//!
//! Deliberate limits, each loud rather than silent: one PUT per object
//! (no multipart — S3's 5 GiB single-PUT ceiling is refused up front, not
//! discovered mid-upload), no connection reuse (a backup fires a handful
//! of requests per day), object bodies are streamed (never hold a whole
//! O(database) dump in memory). Per-phase budgets: connect + handshake
//! 10s, response head 30s; body streaming has no timer — multi-gigabyte
//! restores over slow WAN must finish regardless, and a dead peer
//! surfaces through TCP instead. Signature correctness is pinned by
//! independently generated vectors (RFC 4231-certified HMAC chain) in the
//! module tests.

use crate::tls::{self, ConnStream};
use docsql_core::kdf::{self, hmac_sha256, sha256};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Connect + TLS-handshake budget for one request's socket phase.
const S3_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Budget for the peer to produce response head bytes (status + headers).
const S3_HEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// S3 single-PUT object ceiling (multipart upload is not supported).
const MAX_SINGLE_PUT: u64 = 5 * 1024 * 1024 * 1024;
/// Response-body cap for the requests whose body is read into memory
/// (LIST XML, error pages). Object GETs stream to disk and never hit this.
const MAX_INLINE_BODY: usize = 16 * 1024 * 1024;

/// Remote-copy target, parsed once at startup from `DOCSQL_BACKUP_S3_*`
/// (see [`s3_config_from_env`]). Secrets never leave this struct — the
/// status payload reports endpoint/bucket/prefix only.
#[derive(Clone, Debug)]
pub struct S3BackupConfig {
    /// `http://host[:port]` or `https://host[:port]`, no trailing slash.
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// SigV4 region (default `us-east-1`; S3-compatible stores usually
    /// accept any value).
    pub region: String,
    /// Optional key prefix (`""` or `a/b`) — multi-node fleets share one
    /// bucket by pointing each node at a distinct prefix.
    pub prefix: String,
    /// Remote retention for full backups (`DOCSQL_BACKUP_S3_KEEP`; 0 =
    /// follow the local `DOCSQL_BACKUP_KEEP`, resolved by the caller).
    pub keep: usize,
    /// PEM trust anchor for https endpoints; None = encrypt-only.
    pub ca: Option<PathBuf>,
    /// Derived from the endpoint scheme.
    pub tls: bool,
}

impl S3BackupConfig {
    /// Remote retention actually in force: explicit value, else the local
    /// keep (0 = "unset" sentinel from the env parse).
    pub fn effective_keep(&self, local_keep: usize) -> usize {
        if self.keep == 0 {
            local_keep.max(1)
        } else {
            self.keep
        }
    }

    /// Full object key for a backup file name (`backup-*.sql`,
    /// `incr-*.sql`, or with the `.sha256` suffix).
    pub fn object_key(&self, name: &str) -> String {
        if self.prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", self.prefix, name)
        }
    }
}

/// Parsed + validated remote-copy config from the environment. Returns
/// `Ok(None)` with nothing set; ANY of the four required values set with
/// one missing is a hard error — a half-configured remote copy must
/// refuse to boot rather than silently keep backups local-only.
pub fn s3_config_from_env(
    getenv: &impl Fn(&str) -> Option<String>,
) -> Result<Option<S3BackupConfig>, String> {
    let endpoint = getenv("DOCSQL_BACKUP_S3_ENDPOINT").unwrap_or_default();
    let bucket = getenv("DOCSQL_BACKUP_S3_BUCKET").unwrap_or_default();
    let access_key = getenv("DOCSQL_BACKUP_S3_ACCESS_KEY").unwrap_or_default();
    let secret_key = getenv("DOCSQL_BACKUP_S3_SECRET_KEY").unwrap_or_default();
    let required = [
        ("DOCSQL_BACKUP_S3_ENDPOINT", &endpoint),
        ("DOCSQL_BACKUP_S3_BUCKET", &bucket),
        ("DOCSQL_BACKUP_S3_ACCESS_KEY", &access_key),
        ("DOCSQL_BACKUP_S3_SECRET_KEY", &secret_key),
    ];
    let set = required
        .iter()
        .filter(|(_, v)| !v.trim().is_empty())
        .count();
    if set == 0 {
        return Ok(None);
    }
    if set < 4 {
        let missing: Vec<&str> = required
            .iter()
            .filter(|(_, v)| v.trim().is_empty())
            .map(|(n, _)| *n)
            .collect();
        return Err(format!(
            "remote backup copy is half-configured: {} must all be set together \
             (or all unset to keep backups local-only)",
            missing.join(", ")
        ));
    }
    let endpoint = endpoint.trim().trim_end_matches('/').to_string();
    let tls = if let Some(rest) = endpoint.strip_prefix("https://") {
        if rest.is_empty() {
            return Err("DOCSQL_BACKUP_S3_ENDPOINT: https endpoint has no host".into());
        }
        true
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        if rest.is_empty() {
            return Err("DOCSQL_BACKUP_S3_ENDPOINT: http endpoint has no host".into());
        }
        false
    } else {
        return Err(format!(
            "DOCSQL_BACKUP_S3_ENDPOINT must start with http:// or https://, got {endpoint:?}"
        ));
    };
    let bucket = bucket.trim().to_string();
    if !valid_bucket_name(&bucket) {
        return Err(format!(
            "DOCSQL_BACKUP_S3_BUCKET {bucket:?} is not a valid S3 bucket name \
             (3..=63 chars, lowercase letters/digits/dots/dashes)"
        ));
    }
    let region = getenv("DOCSQL_BACKUP_S3_REGION")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "us-east-1".to_string());
    let prefix = getenv("DOCSQL_BACKUP_S3_PREFIX")
        .map(|s| s.trim().trim_matches('/').to_string())
        .unwrap_or_default();
    let keep = match getenv("DOCSQL_BACKUP_S3_KEEP") {
        Some(v) if !v.trim().is_empty() => v
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("DOCSQL_BACKUP_S3_KEEP: invalid integer value {v:?}"))?
            .max(1),
        _ => 0,
    };
    let ca = getenv("DOCSQL_BACKUP_S3_CA")
        .map(std::path::PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty());
    Ok(Some(S3BackupConfig {
        endpoint,
        bucket,
        access_key: access_key.trim().to_string(),
        secret_key: secret_key.trim().to_string(),
        region,
        prefix,
        keep,
        ca,
        tls,
    }))
}

fn valid_bucket_name(bucket: &str) -> bool {
    (3..=63).contains(&bucket.len())
        && bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
}

/// S3 client over one HTTP/1.1 request per connection. The TLS connector
/// (if any) is built once — a bad CA file fails at startup, not on the
/// nightly backup.
pub struct S3Client {
    cfg: S3BackupConfig,
    tls_connector: Option<tokio_rustls::TlsConnector>,
}

impl S3Client {
    pub fn new(cfg: S3BackupConfig) -> std::io::Result<Self> {
        let tls_connector = if cfg.tls {
            Some(tls::load_tls_connector(cfg.ca.as_deref())?)
        } else {
            None
        };
        Ok(S3Client { cfg, tls_connector })
    }

    pub fn config(&self) -> &S3BackupConfig {
        &self.cfg
    }

    /// PUT an object streamed from `path`. `payload_sha256` is the
    /// caller's already-known digest (backup files are hashed when their
    /// sidecar is written) — SigV4 signs the payload hash, so it must be
    /// exact.
    pub async fn put_file(
        &self,
        key: &str,
        path: &Path,
        payload_sha256: &str,
    ) -> Result<(), String> {
        let len = std::fs::metadata(path)
            .map_err(|e| format!("s3 put {key}: stat: {e}"))?
            .len();
        if len > MAX_SINGLE_PUT {
            return Err(format!(
                "s3 put {key}: {len} bytes exceed the single-PUT limit of \
                 {MAX_SINGLE_PUT} bytes (multipart upload is not supported)"
            ));
        }
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|e| format!("s3 put {key}: open: {e}"))?;
        let (host, port, authority) = endpoint_parts(&self.cfg.endpoint)?;
        let head = signed_head(
            &self.cfg,
            "PUT",
            &format!("/{}/{}", self.cfg.bucket, percent_encode_path(key)),
            "",
            &authority,
            payload_sha256,
            Some(len),
        );
        let mut conn = dial_and_send(&host, port, &head, &self.tls_connector).await?;
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let n = file
                .read(&mut chunk)
                .await
                .map_err(|e| format!("s3 put {key}: read: {e}"))?;
            if n == 0 {
                break;
            }
            conn.write_all(&chunk[..n])
                .await
                .map_err(|e| format!("s3 put {key}: send: {e}"))?;
        }
        conn.flush().await.ok();
        let (h, leftover) = read_head(&mut conn, "put", key).await?;
        drain_inline(&mut conn, h.content_length, leftover).await?;
        Ok(())
    }

    /// PUT a small in-memory object (checksum sidecars).
    pub async fn put_bytes(&self, key: &str, body: &[u8]) -> Result<(), String> {
        if body.len() as u64 > MAX_SINGLE_PUT {
            return Err(format!(
                "s3 put {key}: {} bytes exceed the single-PUT limit",
                body.len()
            ));
        }
        let (host, port, authority) = endpoint_parts(&self.cfg.endpoint)?;
        let head = signed_head(
            &self.cfg,
            "PUT",
            &format!("/{}/{}", self.cfg.bucket, percent_encode_path(key)),
            "",
            &authority,
            &kdf::hex(&sha256(body)),
            Some(body.len() as u64),
        );
        let mut conn = dial_and_send(&host, port, &head, &self.tls_connector).await?;
        conn.write_all(body)
            .await
            .map_err(|e| format!("s3 put {key}: send: {e}"))?;
        conn.flush().await.ok();
        let (h, leftover) = read_head(&mut conn, "put", key).await?;
        drain_inline(&mut conn, h.content_length, leftover).await?;
        Ok(())
    }

    /// GET an object streaming into `dest` (owner-only tmp file, atomic
    /// rename). The transfer is length-audited: a short body is a
    /// truncated download, refused before the rename.
    pub async fn get_file(&self, key: &str, dest: &Path) -> Result<u64, String> {
        let (host, port, authority) = endpoint_parts(&self.cfg.endpoint)?;
        let head = signed_head(
            &self.cfg,
            "GET",
            &format!("/{}/{}", self.cfg.bucket, percent_encode_path(key)),
            "",
            &authority,
            &empty_sha256(),
            None,
        );
        let mut conn = dial_and_send(&host, port, &head, &self.tls_connector).await?;
        let (h, leftover) = read_head(&mut conn, "get", key).await?;
        let Some(len) = h.content_length else {
            return Err(format!(
                "s3 get {key}: response has no Content-Length (chunked object \
                 transfer is not supported)"
            ));
        };
        let mut tmp = dest.as_os_str().to_os_string();
        tmp.push(".s3dl");
        let tmp = PathBuf::from(tmp);
        let res = stream_body_to_file(&mut conn, &tmp, len, leftover).await;
        match res {
            Ok(()) => {
                crate::backup::rename_synced(&tmp, dest)
                    .map_err(|e| format!("s3 get {key}: rename: {e}"))?;
                Ok(len)
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(format!("s3 get {key}: {e}"))
            }
        }
    }

    /// LIST object keys under a prefix (ListObjectsV2, paginated). Returns
    /// keys relative to the bucket root.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let (host, port, authority) = endpoint_parts(&self.cfg.endpoint)?;
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut params: Vec<(&str, String)> = vec![
                ("list-type", "2".to_string()),
                ("max-keys", "1000".to_string()),
                ("prefix", prefix.to_string()),
            ];
            if let Some(t) = &token {
                params.push(("continuation-token", t.clone()));
            }
            params.sort_by(|a, b| a.0.cmp(b.0));
            let query = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, percent_encode_query(v)))
                .collect::<Vec<_>>()
                .join("&");
            let head = signed_head(
                &self.cfg,
                "GET",
                &format!("/{}", self.cfg.bucket),
                &query,
                &authority,
                &empty_sha256(),
                None,
            );
            let mut conn = dial_and_send(&host, port, &head, &self.tls_connector).await?;
            let (h, leftover) = read_head(&mut conn, "list", "").await?;
            if h.content_length.is_none() {
                return Err("s3 list: response has no Content-Length".into());
            }
            let body = read_inline(&mut conn, h.content_length.unwrap(), leftover).await?;
            let (page, truncated, next) = parse_list_v2(&String::from_utf8_lossy(&body));
            keys.extend(page);
            if !truncated {
                return Ok(keys);
            }
            token = next;
            if token.is_none() {
                return Err("s3 list: truncated response carries no continuation token".into());
            }
        }
    }

    /// DELETE an object (absent objects are a no-op per S3 semantics).
    pub async fn delete(&self, key: &str) -> Result<(), String> {
        let (host, port, authority) = endpoint_parts(&self.cfg.endpoint)?;
        let head = signed_head(
            &self.cfg,
            "DELETE",
            &format!("/{}/{}", self.cfg.bucket, percent_encode_path(key)),
            "",
            &authority,
            &empty_sha256(),
            None,
        );
        let mut conn = dial_and_send(&host, port, &head, &self.tls_connector).await?;
        let (h, leftover) = read_head(&mut conn, "delete", key).await?;
        drain_inline(&mut conn, h.content_length, leftover).await?;
        Ok(())
    }
}

/// sha256 of the empty string — the SigV4 payload hash for bodyless
/// requests (GET/LIST/DELETE). Computed, not transcribed; per-request
/// cost is microseconds.
fn empty_sha256() -> String {
    kdf::hex(&sha256(&[]))
}

/// `host`, `port` and the full `host[:port]` authority out of a validated
/// endpoint. The authority IS the Host header (and the SigV4 canonical
/// host value) — signing one string and sending another breaks signatures
/// on non-default ports.
/// Only http/https reach this (the env parse rejects everything else);
/// a second guard here keeps a future caller from signing for `ftp`.
fn endpoint_parts(endpoint: &str) -> Result<(String, u16, String), String> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or_else(|| format!("s3 endpoint {endpoint:?} has no scheme"))?;
    let default_port = match scheme {
        "https" => 443,
        "http" => 80,
        other => return Err(format!("s3 endpoint scheme {other:?} is not http/https")),
    };
    if rest.contains('/') {
        // A path component used to be silently dropped (`gw.example.com/s3`
        // signed and PUT against `gw.example.com`), surfacing far from the
        // misconfiguration as per-upload 404/signature errors. Reject at
        // startup instead; DOCSQL_BACKUP_S3_PREFIX is the supported spelling
        // for key prefixes.
        return Err(format!(
            "s3 endpoint {endpoint:?} carries a path component; use host[:port] \
             (set DOCSQL_BACKUP_S3_PREFIX for key prefixes)"
        ));
    }
    let authority = rest.to_string();
    let authority_view: &str = &authority;
    // [v6]:port, host:port, bare host.
    if let Some(rest) = authority_view.strip_prefix('[') {
        let (h, p) = rest
            .split_once(']')
            .ok_or_else(|| format!("s3 endpoint {endpoint:?} has an unclosed IPv6 bracket"))?;
        let port = p
            .strip_prefix(':')
            .map(|p| p.parse::<u16>())
            .transpose()
            .map_err(|_| format!("s3 endpoint {endpoint:?} has a bad port"))?;
        Ok((h.to_string(), port.unwrap_or(default_port), authority))
    } else {
        match authority_view.rsplit_once(':') {
            Some((h, p)) => {
                let port = p
                    .parse::<u16>()
                    .map_err(|_| format!("s3 endpoint {endpoint:?} has a bad port"))?;
                Ok((h.to_string(), port, authority))
            }
            None => Ok((authority_view.to_string(), default_port, authority)),
        }
    }
}

/// Percent-encode a key path segment set: everything outside the
/// unreserved set plus `/` (S3 keys are slash-separated trees).
fn percent_encode_path(s: &str) -> String {
    percent_encode(s, true)
}

/// Percent-encode a query value: `/` has no hierarchical meaning here.
fn percent_encode_query(s: &str) -> String {
    percent_encode(s, false)
}

fn percent_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// UTC stamps for SigV4: the full `YYYYMMDDTHHMMSSZ` form and the day
/// scope `YYYYMMDD`.
fn amz_dates(ms: u64) -> (String, String) {
    let secs = ms / 1000;
    let (y, mo, d) = docsql_core::engine::civil_from_days((secs / 86400) as i64);
    let (h, mi, s) = ((secs % 86400) / 3600, (secs % 3600) / 60, secs % 60);
    (
        format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z"),
        format!("{y:04}{mo:02}{d:02}"),
    )
}

/// SigV4 signing core (pure; pinned by test vectors): returns the
/// Authorization header value for one request.
fn sigv4_authorization(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    host: &str,
    payload_hash: &str,
    amz_date: &str,
    cfg: &S3BackupConfig,
) -> String {
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!("{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let creq_hash = kdf::hex(&sha256(canonical_request.as_bytes()));
    let day = &amz_date[..8.min(amz_date.len())];
    let scope = format!("{day}/{}/s3/aws4_request", cfg.region);
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{creq_hash}");
    let k_date = hmac_sha256(format!("AWS4{}", cfg.secret_key).as_bytes(), day.as_bytes());
    let k_region = hmac_sha256(&k_date, cfg.region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = kdf::hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        cfg.access_key, scope, signed_headers, signature
    )
}

/// Full request head (headers + trailing blank line) for one S3 request.
fn signed_head(
    cfg: &S3BackupConfig,
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    host_header: &str,
    payload_hash: &str,
    content_length: Option<u64>,
) -> String {
    let (amz_date, _) = amz_dates(docsql_core::now_ms());
    let auth = sigv4_authorization(
        method,
        canonical_uri,
        canonical_query,
        host_header,
        payload_hash,
        &amz_date,
        cfg,
    );
    let mut head = format!(
        "{method} {canonical_uri}{} HTTP/1.1\r\nhost: {host_header}\r\nx-amz-date: {amz_date}\r\nx-amz-content-sha256: {payload_hash}\r\nauthorization: {auth}\r\nconnection: close\r\n",
        if canonical_query.is_empty() {
            String::new()
        } else {
            format!("?{canonical_query}")
        }
    );
    if let Some(len) = content_length {
        head.push_str(&format!("content-length: {len}\r\n"));
    }
    head.push_str("\r\n");
    head
}

/// Dial the endpoint (plain or TLS), write the request head, return the
/// connection for body//response phases.
async fn dial_and_send(
    host: &str,
    port: u16,
    head: &str,
    tls_connector: &Option<tokio_rustls::TlsConnector>,
) -> Result<Box<dyn ConnStream>, String> {
    let stream = tokio::time::timeout(S3_CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| "s3: connect timed out".to_string())?
        .map_err(|e| format!("s3: connect: {e}"))?;
    let conn: Box<dyn ConnStream> = match tls_connector {
        None => Box::new(stream),
        Some(c) => {
            let name = server_name(host)?;
            let tls_stream = tokio::time::timeout(S3_CONNECT_TIMEOUT, c.connect(name, stream))
                .await
                .map_err(|_| "s3: TLS handshake timed out".to_string())?
                .map_err(|e| format!("s3: TLS handshake: {e}"))?;
            Box::new(tls_stream)
        }
    };
    let mut conn = conn;
    conn.write_all(head.as_bytes())
        .await
        .map_err(|e| format!("s3: send: {e}"))?;
    conn.flush().await.ok();
    Ok(conn)
}

fn server_name(host: &str) -> Result<rustls::pki_types::ServerName<'static>, String> {
    rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| format!("s3: invalid TLS peer name {host:?}: {e}"))
}

struct RespHead {
    content_length: Option<u64>,
}

/// Read and parse the response head; non-2xx becomes an error carrying a
/// snippet of the error body (S3's XML <Message>). The bytes read past
/// the head (chunked reads regularly swallow the first body bytes) come
/// back as the leftover — every body consumer starts from them.
async fn read_head(
    conn: &mut Box<dyn ConnStream>,
    op: &str,
    name: &str,
) -> Result<(RespHead, Vec<u8>), String> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let head_end = loop {
        if let Some(pos) = find_head_end(&buf) {
            break pos;
        }
        if buf.len() > 128 * 1024 {
            return Err(format!("s3 {op} {name}: response head exceeds 128 KiB"));
        }
        let n = tokio::time::timeout(S3_HEAD_TIMEOUT, conn.read(&mut chunk))
            .await
            .map_err(|_| format!("s3 {op} {name}: response head timed out"))?
            .map_err(|e| format!("s3 {op} {name}: receive: {e}"))?;
        if n == 0 {
            return Err(format!(
                "s3 {op} {name}: connection closed before a response head arrived"
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let leftover = buf[head_end + 4..].to_vec();
    let head_text = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head_text.lines();
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| format!("s3 {op} {name}: unparseable status line {status_line:?}"))?;
    let mut content_length = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse::<u64>().ok();
            }
        }
    }
    if !(200..300).contains(&status) {
        // Consume a bounded error body so the operator sees S3's message.
        let mut body = leftover;
        if let Some(len) = content_length {
            body = read_inline(conn, len.min(8 * 1024), body)
                .await
                .unwrap_or_default();
        }
        let snippet: String = String::from_utf8_lossy(&body).chars().take(300).collect();
        return Err(format!(
            "s3 {op} {name}: HTTP {status}{}",
            if snippet.is_empty() {
                String::new()
            } else {
                format!(": {snippet}")
            }
        ));
    }
    Ok((RespHead { content_length }, leftover))
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Read exactly `len` bytes (bounded, into memory) — LIST XML / error
/// pages. Starts from the bytes already read past the head.
async fn read_inline(
    conn: &mut Box<dyn ConnStream>,
    len: u64,
    mut leftover: Vec<u8>,
) -> Result<Vec<u8>, String> {
    if len > MAX_INLINE_BODY as u64 {
        return Err(format!(
            "s3: response body of {len} bytes exceeds the inline cap of {MAX_INLINE_BODY}"
        ));
    }
    let len = len as usize;
    if leftover.len() >= len {
        leftover.truncate(len);
        return Ok(leftover);
    }
    let have = leftover.len();
    let mut body = vec![0u8; len];
    body[..have].copy_from_slice(&leftover);
    conn.read_exact(&mut body[have..])
        .await
        .map_err(|e| format!("s3: receive body: {e}"))?;
    Ok(body)
}

/// Discard a response body (bounded) — PUT/DELETE success bodies.
async fn drain_inline(
    conn: &mut Box<dyn ConnStream>,
    len: Option<u64>,
    leftover: Vec<u8>,
) -> Result<(), String> {
    if let Some(len) = len {
        read_inline(conn, len, leftover).await?;
    }
    Ok(())
}

/// Stream a GET body of exactly `len` bytes into a fresh owner-only file
/// (tmp name supplied by the caller; caller renames on success). Starts
/// from the bytes already read past the head.
async fn stream_body_to_file(
    conn: &mut Box<dyn ConnStream>,
    tmp: &Path,
    len: u64,
    mut leftover: Vec<u8>,
) -> Result<(), String> {
    // tokio's OpenOptions carries the unix mode() natively — no trait
    // import needed (a plain std import reads as unused here).
    let mut builder = tokio::fs::OpenOptions::new();
    builder.write(true).create(true).truncate(true);
    #[cfg(unix)]
    builder.mode(0o600);
    let mut out = builder
        .open(tmp)
        .await
        .map_err(|e| format!("create: {e}"))?;
    let mut remaining = len;
    let mut chunk = vec![0u8; 64 * 1024];
    while remaining > 0 {
        // Head reads can swallow body bytes: they land first.
        let from_leftover = (leftover.len() as u64).min(remaining) as usize;
        if from_leftover > 0 {
            out.write_all(&leftover[..from_leftover])
                .await
                .map_err(|e| format!("write: {e}"))?;
            leftover.drain(..from_leftover);
            remaining -= from_leftover as u64;
            continue;
        }
        let want = chunk.len().min(remaining as usize);
        let n = conn
            .read(&mut chunk[..want])
            .await
            .map_err(|e| format!("receive: {e}"))?;
        if n == 0 {
            return Err(format!(
                "truncated transfer: connection closed after {} of {len} bytes",
                len - remaining
            ));
        }
        out.write_all(&chunk[..n])
            .await
            .map_err(|e| format!("write: {e}"))?;
        remaining -= n as u64;
    }
    out.flush().await.ok();
    out.sync_all().await.map_err(|e| format!("sync: {e}"))?;
    Ok(())
}

/// Minimal ListObjectsV2 XML parse: the keys on this page, whether more
/// pages follow, and the continuation token when they do.
fn parse_list_v2(xml: &str) -> (Vec<String>, bool, Option<String>) {
    let mut keys = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + 5..];
        let Some(end) = after.find("</Key>") else {
            break;
        };
        keys.push(xml_unescape(&after[..end]));
        rest = &after[end + 6..];
    }
    let truncated = xml.contains("<IsTruncated>true</IsTruncated>");
    let token = xml.find("<NextContinuationToken>").and_then(|s| {
        let after = &xml[s + 23..];
        after
            .find("</NextContinuationToken>")
            .map(|e| xml_unescape(&after[..e]))
    });
    (keys, truncated, token)
}

/// Unescape the five XML predefined entities in object keys. Our keys are
/// generated names plus an operator prefix; anything beyond the standard
/// entities stays literal (S3 only emits the predefined set here).
fn xml_unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&") // last: never double-unescape
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> S3BackupConfig {
        S3BackupConfig {
            endpoint: "http://s3.example.local:9000".into(),
            bucket: "db-backups".into(),
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            region: "us-east-1".into(),
            prefix: "fleet-a".into(),
            keep: 0,
            ca: None,
            tls: false,
        }
    }

    /// Independently generated vectors (python3 hashlib, HMAC chain first
    /// certified against RFC 4231 known answers): PUT with a port-bearing
    /// host and GET with query string + bare host.
    #[test]
    fn sigv4_matches_independently_generated_vectors() {
        let cfg = test_cfg();
        let body = b"DROP TABLE IF EXISTS \"t\";\nCREATE TABLE t (id INT);\n";
        let payload_hash = kdf::hex(&sha256(body));
        assert_eq!(
            payload_hash,
            "d9b2e5e3bf29578fa5922c7ea1848c315d6753e2122d29450a53671e57c219ce"
        );

        let auth = sigv4_authorization(
            "PUT",
            "/db-backups/fleet-a/backup-20260910T081530123Z.sql",
            "",
            "s3.example.local:9000",
            &payload_hash,
            "20260910T081530Z",
            &cfg,
        );
        let sig = auth.rsplit("Signature=").next().unwrap();
        assert_eq!(
            sig, "84bfbb895cc56dd6f59540ecb734b8ec7d2c8fd0972aeada8cab452e0e4c5c77",
            "auth: {auth}"
        );
        assert!(auth.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260910/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="
        ), "auth: {auth}");

        let auth2 = sigv4_authorization(
            "GET",
            "/db-backups",
            "list-type=2&max-keys=1000&prefix=inc-",
            "s3.example.local",
            &payload_hash,
            "20260910T081530Z",
            &cfg,
        );
        let sig2 = auth2.rsplit("Signature=").next().unwrap();
        assert_eq!(
            sig2, "b9187b8755882d95d24c46cd778f9183897777073c7e1ed736d66fbc2ebaaebb",
            "auth2: {auth2}"
        );
    }

    #[test]
    fn signed_head_includes_length_and_query() {
        let cfg = test_cfg();
        let head = signed_head(
            &cfg,
            "PUT",
            "/db-backups/fleet-a/backup-x.sql",
            "",
            "s3.example.local:9000",
            "abc",
            Some(42),
        );
        assert!(head.starts_with("PUT /db-backups/fleet-a/backup-x.sql HTTP/1.1\r\n"));
        assert!(head.contains("host: s3.example.local:9000\r\n"));
        assert!(head.contains("content-length: 42\r\n"));
        assert!(head.contains("connection: close\r\n"));
        assert!(head.ends_with("\r\n\r\n"));
        // No query string → no "?" in the request line.
        assert!(!head.contains("?"));
        let head = signed_head(
            &cfg,
            "GET",
            "/db-backups",
            "list-type=2&prefix=inc-",
            "s3.example.local:9000",
            "abc",
            None,
        );
        assert!(head.starts_with("GET /db-backups?list-type=2&prefix=inc- HTTP/1.1\r\n"));
        assert!(!head.contains("content-length"));
    }

    #[test]
    fn endpoint_parts_handles_schemes_and_ports() {
        assert_eq!(
            endpoint_parts("http://minio:9000").unwrap(),
            ("minio".to_string(), 9000, "minio:9000".to_string())
        );
        assert_eq!(
            endpoint_parts("https://s3.us-east-1.amazonaws.com").unwrap(),
            (
                "s3.us-east-1.amazonaws.com".to_string(),
                443,
                "s3.us-east-1.amazonaws.com".to_string()
            )
        );
        assert_eq!(
            endpoint_parts("http://127.0.0.1").unwrap(),
            ("127.0.0.1".to_string(), 80, "127.0.0.1".to_string())
        );
        assert_eq!(
            endpoint_parts("https://[::1]:9001").unwrap(),
            ("::1".to_string(), 9001, "[::1]:9001".to_string())
        );
        assert!(endpoint_parts("ftp://x").is_err());
    }

    #[test]
    fn percent_encoding_follows_sigv4_unreserved_set() {
        assert_eq!(percent_encode_path("backup-1.sql"), "backup-1.sql");
        assert_eq!(percent_encode_path("a b/c&d"), "a%20b/c%26d");
        assert_eq!(percent_encode_query("a b/c&d"), "a%20b%2Fc%26d");
        assert_eq!(percent_encode_path("a~b-c_d.e"), "a~b-c_d.e");
        assert_eq!(percent_encode_path("é"), "%C3%A9");
    }

    #[test]
    fn amz_dates_format_two_scopes() {
        // 2026-09-10T08:15:30Z
        let (full, day) = amz_dates(1_789_028_130_000);
        assert_eq!(full, "20260910T081530Z");
        assert_eq!(day, "20260910");
        // SigV4 requires the full stamp to start with the day scope.
        assert!(full.starts_with(&day));
    }

    #[test]
    fn list_xml_parse_extracts_keys_and_pagination() {
        let xml = concat!(
            "<?xml version=\"1.0\"?><ListBucketResult>",
            "<IsTruncated>true</IsTruncated>",
            "<NextContinuationToken>tok/en&amp;1</NextContinuationToken>",
            "<Contents><Key>fleet-a/backup-1.sql</Key></Contents>",
            "<Contents><Key>fleet-a/incr-2.sql</Key></Contents>",
            "</ListBucketResult>"
        );
        let (keys, truncated, token) = parse_list_v2(xml);
        assert_eq!(keys, vec!["fleet-a/backup-1.sql", "fleet-a/incr-2.sql"]);
        assert!(truncated);
        assert_eq!(token.as_deref(), Some("tok/en&1"));
        let (keys, truncated, token) = parse_list_v2("<ListBucketResult></ListBucketResult>");
        assert!(keys.is_empty() && !truncated && token.is_none());
    }

    #[test]
    fn object_key_joins_prefix() {
        let mut cfg = test_cfg();
        assert_eq!(cfg.object_key("backup-1.sql"), "fleet-a/backup-1.sql");
        cfg.prefix = String::new();
        assert_eq!(cfg.object_key("backup-1.sql"), "backup-1.sql");
    }

    #[test]
    fn effective_keep_follows_local_when_unset() {
        let mut cfg = test_cfg();
        cfg.keep = 0;
        assert_eq!(cfg.effective_keep(7), 7);
        cfg.keep = 3;
        assert_eq!(cfg.effective_keep(7), 3);
    }

    #[test]
    fn env_parse_requires_all_four_values() {
        let none = s3_config_from_env(&|_| None).unwrap();
        assert!(none.is_none());
        let one = |name: &str| s3_config_from_env(&|k| (k == name).then(|| "x".to_string()));
        let err = one("DOCSQL_BACKUP_S3_ENDPOINT").unwrap_err();
        assert!(err.contains("half-configured"), "{err}");
        assert!(err.contains("DOCSQL_BACKUP_S3_SECRET_KEY"), "{err}");
    }

    #[test]
    fn env_parse_validates_endpoint_scheme_bucket_and_defaults() {
        let full = [
            ("DOCSQL_BACKUP_S3_ENDPOINT", " https://s3.amazonaws.com/ "),
            ("DOCSQL_BACKUP_S3_BUCKET", "db-backups"),
            ("DOCSQL_BACKUP_S3_ACCESS_KEY", " AKID "),
            ("DOCSQL_BACKUP_S3_SECRET_KEY", " secret "),
            ("DOCSQL_BACKUP_S3_PREFIX", "/prod/fleet-a/"),
            ("DOCSQL_BACKUP_S3_CA", "/etc/ca.pem"),
        ];
        let cfg = s3_config_from_env(&|k| {
            full.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        })
        .unwrap()
        .unwrap();
        assert_eq!(cfg.endpoint, "https://s3.amazonaws.com");
        assert!(cfg.tls);
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.prefix, "prod/fleet-a");
        assert_eq!(cfg.keep, 0);
        assert_eq!(cfg.ca.as_deref(), Some(Path::new("/etc/ca.pem")));

        // Explicit keep + region; a bad scheme / bucket / keep refuse.
        let with = |map: &[(&str, &str)]| {
            s3_config_from_env(&|k| {
                map.iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            })
        };
        let base = [
            ("DOCSQL_BACKUP_S3_ENDPOINT", "http://minio:9000"),
            ("DOCSQL_BACKUP_S3_BUCKET", "db-backups"),
            ("DOCSQL_BACKUP_S3_ACCESS_KEY", "akid"),
            ("DOCSQL_BACKUP_S3_SECRET_KEY", "secret"),
            ("DOCSQL_BACKUP_S3_REGION", "eu-central-1"),
            ("DOCSQL_BACKUP_S3_KEEP", "5"),
        ];
        let cfg = with(&base).unwrap().unwrap();
        assert!(!cfg.tls);
        assert_eq!(cfg.region, "eu-central-1");
        assert_eq!(cfg.keep, 5);

        let err = with(&[
            ("DOCSQL_BACKUP_S3_ENDPOINT", "s3.amazonaws.com"),
            ("DOCSQL_BACKUP_S3_BUCKET", "db-backups"),
            ("DOCSQL_BACKUP_S3_ACCESS_KEY", "a"),
            ("DOCSQL_BACKUP_S3_SECRET_KEY", "b"),
        ])
        .unwrap_err();
        assert!(err.contains("http://"), "{err}");
        let err = with(&[
            ("DOCSQL_BACKUP_S3_ENDPOINT", "http://minio:9000"),
            ("DOCSQL_BACKUP_S3_BUCKET", "My_Bucket"),
            ("DOCSQL_BACKUP_S3_ACCESS_KEY", "a"),
            ("DOCSQL_BACKUP_S3_SECRET_KEY", "b"),
        ])
        .unwrap_err();
        assert!(err.contains("bucket name"), "{err}");
        let err = with(&[
            ("DOCSQL_BACKUP_S3_ENDPOINT", "http://minio:9000"),
            ("DOCSQL_BACKUP_S3_BUCKET", "db-backups"),
            ("DOCSQL_BACKUP_S3_ACCESS_KEY", "a"),
            ("DOCSQL_BACKUP_S3_SECRET_KEY", "b"),
            ("DOCSQL_BACKUP_S3_KEEP", "lots"),
        ])
        .unwrap_err();
        assert!(err.contains("DOCSQL_BACKUP_S3_KEEP"), "{err}");
    }

    #[test]
    fn xml_unescape_handles_entities_and_ampersands() {
        assert_eq!(xml_unescape("plain.sql"), "plain.sql");
        assert_eq!(xml_unescape("a&amp;b"), "a&b");
        assert_eq!(xml_unescape("&amp;lt;"), "&lt;");
        assert_eq!(xml_unescape("a&lt;b&gt;"), "a<b>");
    }

    #[test]
    fn head_parser_accepts_status_and_length() {
        // find_head_end drives read_head's framing; assert on it directly
        // (the status/body-error paths need a live connection and are
        // covered by the mock-server e2e).
        let raw = b"HTTP/1.1 200 OK\r\ncontent-length: 12\r\nx-noise: 1\r\n\r\nBODY";
        let end = find_head_end(raw).unwrap();
        let head = String::from_utf8_lossy(&raw[..end]).to_string();
        assert!(head.contains("200 OK"));
        assert!(head.contains("content-length: 12"));
        // `end` points at the head terminator; the body starts after it.
        assert_eq!(&raw[end..end + 4], b"\r\n\r\n");
        assert_eq!(&raw[end + 4..], b"BODY");
    }
}
