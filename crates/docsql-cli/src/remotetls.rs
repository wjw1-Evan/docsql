//! Sync TLS client for the remote shell (`DOCSQL_TLS_CONNECT=1`, optional
//! `DOCSQL_TLS_CA`) — the CLI twin of the server's outbound dialer.
//!
//! The CLI is a deliberately synchronous std program (no tokio in the
//! main deps), and its remote mode needs full duplex: a reader thread
//! prints RESP_PUSH deliveries while the main thread sends requests.
//! rustls has no split-half API for one connection, so the session lives
//! behind one mutex and the socket is non-blocking after the handshake:
//! the reader thread polls `read_tls` in short lock scopes (never blocking
//! inside the lock — the writer would starve), the writer writes plaintext
//! and flushes records under the same lock. Poll granularity is 3 ms,
//! irrelevant for an interactive shell.

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use docsql_core::proto::{self, Frame};

/// Handshake wall-clock budget (blocking socket with a read timeout does
/// the actual bounding; this deadline catches a peer that dribbles bytes).
const HANDSHAKE_BUDGET: Duration = Duration::from_secs(15);
/// Reader poll interval on a quiet connection.
const POLL_INTERVAL: Duration = Duration::from_millis(3);

/// One established TLS connection to a protocol endpoint.
pub struct TlsLink {
    session: Arc<Mutex<Session>>,
}

struct Session {
    conn: rustls::ClientConnection,
    /// Non-blocking after the handshake; every access happens under the
    /// session mutex (read_tls from the reader thread, write_tls from the
    /// writer side).
    sock: TcpStream,
}

impl Drop for TlsLink {
    fn drop(&mut self) {
        // Best-effort close_notify so the server logs a clean EOF instead
        // of a TLS truncation error for every CLI exit.
        if let Ok(mut s) = self.session.lock() {
            let Session { conn, sock } = &mut *s;
            conn.send_close_notify();
            let _ = conn.write_tls(sock);
        }
    }
}

/// Server certificate verifier for the encrypt-only posture: accepts any
/// certificate but still enforces the handshake's signature schemes. Not
/// a MITM defense — set DOCSQL_TLS_CA for that (same contract as the
/// server's outbound connector).
#[derive(Debug)]
struct EncryptOnlyVerifier(rustls::crypto::WebPkiSupportedAlgorithms);

impl rustls::client::danger::ServerCertVerifier for EncryptOnlyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.supported_schemes()
    }
}

fn client_config(ca: Option<&Path>) -> Result<Arc<rustls::ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls protocol versions: {e}"))?;
    let config = match ca {
        None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(EncryptOnlyVerifier(
                provider.signature_verification_algorithms,
            )))
            .with_no_client_auth(),
        Some(path) => {
            let mut roots = rustls::RootCertStore::empty();
            let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
                std::fs::File::open(path).map_err(|e| format!("DOCSQL_TLS_CA: {e}"))?,
            ))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("DOCSQL_TLS_CA PEM: {e}"))?;
            if certs.is_empty() {
                return Err("DOCSQL_TLS_CA PEM contains no certificates".into());
            }
            roots.add_parsable_certificates(certs);
            builder.with_root_certificates(roots).with_no_client_auth()
        }
    };
    Ok(Arc::new(config))
}

/// Host part of a dial target (`host:port`, `[v6]:port`, bare host).
fn host_part(target: &str) -> &str {
    if let Some(rest) = target.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    target.rsplit_once(':').map(|(h, _)| h).unwrap_or(target)
}

impl TlsLink {
    /// Connect and drive the TLS handshake to completion on a blocking
    /// socket (bounded by a read timeout + deadline), then switch to
    /// non-blocking for the polled reader/writer split.
    pub fn connect(target: &str, ca: Option<&Path>) -> Result<TlsLink, String> {
        // Config first: a bad CA / unparseable target fails before any
        // socket work (the test pins this ordering).
        let host = host_part(target);
        let name = rustls::pki_types::ServerName::try_from(host.to_string())
            .map_err(|e| format!("TLS peer name {host:?} from {target:?} invalid: {e}"))?;
        let config = client_config(ca)?;
        // Bounded connect (same 10s budget as the plain path): a
        // firewalled target wedged the whole CLI in OS SYN retries.
        let mut last = String::from("invalid address");
        let mut sock = None;
        for sa in target
            .to_socket_addrs()
            .map_err(|e| format!("resolve {target:?}: {e}"))?
        {
            match TcpStream::connect_timeout(&sa, Duration::from_secs(10)) {
                Ok(s) => {
                    sock = Some(s);
                    break;
                }
                Err(e) => last = e.to_string(),
            }
        }
        let mut sock = sock.ok_or_else(|| format!("connect {target}: {last}"))?;
        let _ = sock.set_nodelay(true);
        sock.set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| e.to_string())?;
        sock.set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| e.to_string())?;
        let mut conn =
            rustls::ClientConnection::new(config, name).map_err(|e| format!("tls client: {e}"))?;
        let deadline = Instant::now() + HANDSHAKE_BUDGET;
        while conn.is_handshaking() {
            while conn.wants_write() {
                conn.write_tls(&mut sock)
                    .map_err(|e| format!("tls handshake write: {e}"))?;
            }
            if !conn.wants_read() {
                break;
            }
            match conn.read_tls(&mut sock) {
                Ok(0) => return Err("connection closed during TLS handshake".into()),
                Ok(_) => {
                    // Progress is NOT completion: a peer that dribbles one
                    // byte every few seconds never trips the 5s socket
                    // timeout, so the budget must be checked on EVERY read
                    // (that is what HANDSHAKE_BUDGET exists for).
                    if Instant::now() > deadline {
                        return Err("TLS handshake timed out".into());
                    }
                    conn.process_new_packets()
                        .map_err(|e| format!("tls handshake: {e}"))?;
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    if Instant::now() > deadline {
                        return Err("TLS handshake timed out".into());
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => return Err(format!("tls handshake read: {e}")),
            }
        }
        if conn.is_handshaking() {
            return Err("TLS handshake did not complete".into());
        }
        sock.set_nonblocking(true).map_err(|e| e.to_string())?;
        // Some platforms refuse the non-blocking switch while a timeout is
        // armed; clear both explicitly.
        let _ = sock.set_read_timeout(None);
        let _ = sock.set_write_timeout(None);
        Ok(TlsLink {
            session: Arc::new(Mutex::new(Session { conn, sock })),
        })
    }

    /// Write one frame's bytes: plaintext into the session, then flush
    /// any pending records to the socket (retrying on a full send buffer).
    pub fn send(&self, bytes: &[u8]) -> Result<(), String> {
        let mut s = self
            .session
            .lock()
            .map_err(|_| "tls session poisoned".to_string())?;
        let Session { conn, sock } = &mut *s;
        conn.writer()
            .write_all(bytes)
            .map_err(|e| format!("tls write: {e}"))?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while conn.wants_write() {
            match conn.write_tls(sock) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        return Err("tls socket stalled flushing a frame".into());
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => return Err(format!("tls write: {e}")),
            }
        }
        Ok(())
    }

    /// Spawn the push/reader thread: polls TLS records in short lock
    /// scopes, decodes frames, hands RESP_PUSH to `on_push` and queues
    /// everything else for the pending round trip.
    pub fn spawn_reader(
        &self,
        tx: std::sync::mpsc::Sender<Frame>,
        on_push: impl Fn(&Frame) + Send + 'static,
    ) {
        let session = self.session.clone();
        std::thread::spawn(move || tls_reader_loop(session, tx, on_push));
    }
}

fn tls_reader_loop(
    session: Arc<Mutex<Session>>,
    tx: std::sync::mpsc::Sender<Frame>,
    on_push: impl Fn(&Frame),
) {
    let mut plain = Vec::with_capacity(16 * 1024);
    loop {
        let mut got_record = false;
        {
            let Ok(mut s) = session.lock() else {
                return;
            };
            let Session { conn, sock } = &mut *s;
            loop {
                match conn.read_tls(sock) {
                    Ok(0) => return, // EOF
                    Ok(_) => {
                        got_record = true;
                        if conn.process_new_packets().is_err() {
                            eprintln!("error: tls protocol error");
                            return;
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(_) => return,
                }
            }
            // Drain freshly decrypted plaintext under the same lock (the
            // reader() borrow lives on the connection).
            let mut chunk = [0u8; 8192];
            loop {
                match s.conn.reader().read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => plain.extend_from_slice(&chunk[..n]),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(_) => return,
                }
            }
        }
        // Frame decode outside the lock: pure buffer work.
        loop {
            match Frame::decode(&plain) {
                Ok((f, n)) => {
                    plain.drain(..n);
                    if f.frame_type == proto::RESP_PUSH {
                        on_push(&f);
                    } else if tx.send(f).is_err() {
                        return; // main side closed
                    }
                }
                Err(docsql_core::proto::ProtoError::Truncated(..)) => break,
                Err(e) => {
                    eprintln!("error: protocol error: {e}");
                    return;
                }
            }
        }
        if plain.len() > proto::HEADER_LEN + crate::RECV_CAP {
            eprintln!("error: server advertised an oversized frame");
            return;
        }
        if !got_record {
            std::thread::sleep(POLL_INTERVAL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_part_shapes() {
        assert_eq!(host_part("127.0.0.1:7600"), "127.0.0.1");
        assert_eq!(host_part("node-a:7600"), "node-a");
        assert_eq!(host_part("[::1]:7600"), "::1");
        assert_eq!(host_part("bare"), "bare");
    }

    #[test]
    fn connect_refuses_garbage_ca_loudly() {
        let tmp = tempfile::tempdir().unwrap();
        let ca = tmp.path().join("ca.pem");
        std::fs::write(&ca, b"garbage").unwrap();
        // A garbage CA must fail before any socket work; a closed port
        // would surface a connect error instead.
        let err = match TlsLink::connect("127.0.0.1:1", Some(&ca)) {
            Err(e) => e,
            Ok(_) => panic!("garbage CA unexpectedly accepted"),
        };
        assert!(err.contains("no certificates"), "{err}");
    }
}
