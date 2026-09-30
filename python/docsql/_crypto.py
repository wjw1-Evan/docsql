"""Optional AES-256-GCM frame sealing (DOCSQL_KEY), mirroring
`crates/docsql-server/src/crypto.rs` and the .NET client's implementation:

    payload' = nonce[12] || ciphertext+tag[16]
    aad      = frame_type:u16 LE || flags:u16 LE || conn_challenge[16]

The nonce is a per-connection random 8-byte prefix plus a strictly
monotonic 32-bit counter (little-endian), so a connection never reuses a
(nonce, key) pair. Requires the optional `cryptography` package; the
driver otherwise raises on DOCSQL_KEY connections and recommends native
TLS (`tls=True`), which needs no third-party dependency.
"""

import os
import struct

from ._proto import ProtocolError

try:  # optional dependency
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    _AESGCM = AESGCM
    _HAVE_CRYPTO = True
except ImportError:  # pragma: no cover - exercised via skip logic in tests
    _AESGCM = None
    _HAVE_CRYPTO = False


def have_crypto():
    return _HAVE_CRYPTO


def require_crypto():
    if not _HAVE_CRYPTO:
        raise ProtocolError(
            "DOCSQL_KEY connections need the optional 'cryptography' package "
            "(pip install cryptography) — or use native TLS instead (tls=True)"
        )


class Sealer:
    """Outbound seal + inbound open for one DOCSQL_KEY connection."""

    def __init__(self, key_bytes):
        require_crypto()
        if len(key_bytes) != 32:
            raise ProtocolError(f"DOCSQL_KEY must be 32 bytes, got {len(key_bytes)}")
        self._cipher = _AESGCM(key_bytes)
        self._prefix = os.urandom(8)
        self._counter = 0
        # Inbound replay guard (see the server's ReplayGuard): seals from
        # one peer share a prefix and carry strictly increasing counters;
        # a captured frame replayed on this connection must be rejected
        # even though it decrypts.
        self._peer_prefix = None
        self._peer_last = -1

    @staticmethod
    def _aad(frame_type, flags, challenge):
        return struct.pack("<HH", frame_type, flags) + challenge

    def seal(self, frame_type, flags, plaintext, challenge):
        self._counter += 1
        if self._counter > 0xFFFF_FFFF:
            raise ProtocolError(
                "nonce counter exhausted (>2^32 frames on this connection); reconnect"
            )
        nonce = self._prefix + struct.pack("<I", self._counter)
        sealed = self._cipher.encrypt(
            nonce, plaintext, self._aad(frame_type, flags, challenge)
        )
        return nonce + sealed

    def open(self, frame_type, flags, sealed, challenge):
        if len(sealed) < 12 + 16:
            raise ProtocolError("sealed payload too short")
        nonce, ct = sealed[:12], sealed[12:]
        prefix = nonce[:8]
        counter = struct.unpack("<I", nonce[8:12])[0]
        if self._peer_prefix is None:
            self._peer_prefix = prefix
        if prefix != self._peer_prefix:
            raise ProtocolError("peer nonce prefix changed mid-connection")
        if counter <= self._peer_last:
            raise ProtocolError("replayed or reordered frame rejected (nonce counter)")
        self._peer_last = counter
        return self._cipher.decrypt(
            nonce, ct, self._aad(frame_type, flags, challenge)
        )
