//! Data-plane native TLS: listener (DOCSQL_TLS_CERT + DOCSQL_TLS_KEY) and
//! outbound dialing (DOCSQL_TLS_CONNECT, optional DOCSQL_TLS_CA).
//!
//! The console has served its own rustls listener since the HTTPS batch;
//! this module gives the SQL protocol the same treatment so the data plane
//! no longer needs the proprietary AES-GCM frame seal (DOCSQL_KEY) — or a
//! TLS-terminating reverse proxy — to be safe on the wire. TLS wraps the
//! socket below the v1 framing, so the protocol, the frame seal and the
//! replication machinery are unchanged; a plaintext client dialing a TLS
//! listener (or the reverse) fails the handshake loudly, there is no
//! protocol-detection downgrade.
//!
//! Certificate posture is self-signed-first, like every small fleet: with
//! no DOCSQL_TLS_CA the outbound connector encrypts WITHOUT verifying the
//! peer certificate (startup logs say so), which still defeats passive
//! capture and most on-path attackers; pointing DOCSQL_TLS_CA at a real
//! trust anchor turns on full chain + name verification.

use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// Object-safe transport every connection handler works against: plain TCP
/// or a TLS session over TCP. Boxing keeps `Conn` and the outbound peer
/// helpers monomorphic (one frame loop, not one per transport); the vtable
/// hop is nanoseconds against syscall + fsync latency on every path that
/// matters here.
pub trait ConnStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ConnStream for T {}

/// Owned connection handle used by `handle_connection` and the outbound
/// peer helpers.
pub type BoxConn = Box<dyn ConnStream>;

/// TLS handshake budget on the accept side. Generous rather than tight:
/// remote clients on slow links legitimately take seconds, and the slot is
/// already held (a handshake bomb cannot exceed one budget per slot).
pub const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Load the PEM cert chain + private key into a TLS acceptor. Fails loudly
/// at startup: a node asked to serve TLS must not silently fall back to
/// plaintext.
pub fn load_tls_acceptor(cert: &Path, key: &Path) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::CertificateDer;
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(cert)?))
            .collect::<Result<_, _>>()
            .map_err(|e| std::io::Error::other(format!("tls cert PEM: {e}")))?;
    if certs.is_empty() {
        return Err(std::io::Error::other(
            "tls cert PEM contains no certificates",
        ));
    }
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(key)?))?
        .ok_or_else(|| std::io::Error::other("tls key PEM contains no private key"))?;
    // ring provider: same crypto backend as the build image, no cmake.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| std::io::Error::other(format!("tls protocol versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| std::io::Error::other(format!("tls config: {e}")))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Outbound TLS connector. `ca = None` encrypts without verifying the
/// server certificate (self-signed fleets; the default posture, logged at
/// startup); `ca = Some(path)` loads a PEM trust anchor and enforces full
/// chain + server-name verification.
pub fn load_tls_connector(ca: Option<&Path>) -> std::io::Result<tokio_rustls::TlsConnector> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| std::io::Error::other(format!("tls protocol versions: {e}")))?;
    let config = match ca {
        None => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(EncryptOnlyVerifier(
                provider.signature_verification_algorithms,
            )))
            .with_no_client_auth(),
        Some(path) => {
            let mut roots = rustls::RootCertStore::empty();
            let certs =
                rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| std::io::Error::other(format!("tls CA PEM: {e}")))?;
            if certs.is_empty() {
                return Err(std::io::Error::other("tls CA PEM contains no certificates"));
            }
            roots.add_parsable_certificates(certs);
            builder.with_root_certificates(roots).with_no_client_auth()
        }
    };
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// Server certificate verifier for the encrypt-only posture: accepts any
/// certificate but still enforces the handshake's signature schemes, so a
/// broken peer cannot downgrade the record layer itself. Not a MITM defense
/// — that is what DOCSQL_TLS_CA is for.
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

/// Host part of a dial target (`host:port`, `[v6]:port`, bare host).
fn host_part(target: &str) -> &str {
    if let Some(rest) = target.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    target.rsplit_once(':').map(|(h, _)| h).unwrap_or(target)
}

/// Dial one protocol endpoint: plain TCP, or TLS when `tls` is set. TCP
/// options (keepalive, nodelay) go on the raw socket before any TLS wrap.
/// Shared by every outbound leg (fan-out, catch-up, join/hold, backup) and
/// re-exported for the web console's node dials.
pub async fn dial_protocol(
    target: &str,
    tls: Option<&tokio_rustls::TlsConnector>,
) -> std::io::Result<BoxConn> {
    let stream = tokio::time::timeout(super::CONNECT_TIMEOUT, TcpStream::connect(target)).await??;
    let stream = super::set_tcp_keepalive(stream);
    match tls {
        None => Ok(Box::new(stream)),
        Some(connector) => {
            let host = host_part(target);
            let name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| {
                std::io::Error::other(format!(
                    "TLS peer name {host:?} from target {target:?} is not valid: {e}"
                ))
            })?;
            let tls_stream =
                tokio::time::timeout(super::CONNECT_TIMEOUT, connector.connect(name, stream))
                    .await??;
            Ok(Box::new(tls_stream))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_part_extracts_from_every_target_shape() {
        assert_eq!(host_part("127.0.0.1:7600"), "127.0.0.1");
        assert_eq!(host_part("node-a:7600"), "node-a");
        assert_eq!(host_part("[::1]:7600"), "::1");
        assert_eq!(host_part("bare-host"), "bare-host");
    }

    #[test]
    fn connector_without_ca_builds_and_with_ca_rejects_garbage() {
        // Encrypt-only connector must build; a CA path that is not PEM
        // certificates must fail loudly, not silently verify nothing.
        assert!(load_tls_connector(None).is_ok());
        let tmp = tempfile::tempdir().unwrap();
        let bad = tmp.path().join("ca.pem");
        std::fs::write(&bad, b"not a pem").unwrap();
        let err = match load_tls_connector(Some(&bad)) {
            Err(e) => e,
            Ok(_) => panic!("garbage CA unexpectedly accepted"),
        };
        assert!(err.to_string().contains("no certificates"), "{err}");
    }

    #[test]
    fn acceptor_rejects_missing_or_garbage_files() {
        let tmp = tempfile::tempdir().unwrap();
        let cert = tmp.path().join("c.pem");
        let key = tmp.path().join("k.pem");
        std::fs::write(&cert, b"garbage").unwrap();
        std::fs::write(&key, b"garbage").unwrap();
        let err = match load_tls_acceptor(&cert, &key) {
            Err(e) => e,
            Ok(_) => panic!("garbage cert pair unexpectedly accepted"),
        };
        assert!(err.to_string().contains("no certificates"), "{err}");
        let err = match load_tls_acceptor(&tmp.path().join("absent.pem"), &key) {
            Err(e) => e,
            Ok(_) => panic!("absent cert file unexpectedly accepted"),
        };
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }
}
