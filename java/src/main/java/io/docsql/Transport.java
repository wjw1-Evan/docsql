package io.docsql;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/** One connected socket: framed reads/writes, optional TLS, optional AES-GCM sealing. */
public final class Transport {

    private final Socket socket;
    private final Sealer sealer;
    private volatile boolean closed;
    private final Object ioLock = new Object();
    private final int timeoutMs;
    private byte[] pollBuf = new byte[0];
    private long partialSince = -1;
    private static final long PARTIAL_FRAME_MAX_WAIT_MS = 30_000;
    private final String desc;
    final byte[] challenge;

    public Transport(String host, int port, boolean tls, String tlsCaPath, String tlsHostname,
            String keyHex, int timeoutMs) {
        this.timeoutMs = timeoutMs;
        this.desc = host + ":" + port;
        Socket raw;
        try {
            raw = new Socket();
            raw.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
        } catch (IOException e) {
            throw new DocsqlException.Operational("connect to " + desc + " failed: " + e, e);
        }
        Socket s = raw;
        try {
            if (tls) {
                s = wrapTls(raw, host, port, tlsCaPath, tlsHostname);
            }
            if (keyHex != null) {
                this.sealer = new Sealer(Sealer.parseKeyHex(keyHex));
                this.socket = s;
                this.challenge = readHello();
            } else {
                this.sealer = null;
                this.socket = s;
                this.challenge = null;
            }
        } catch (RuntimeException e) {
            try {
                s.close();
            } catch (IOException ignore) {
                // best effort
            }
            throw e;
        }
    }

    /**
     * Accept an unencrypted RESP_ERROR before the hello (connection-limit rejection rides before
     * any per-connection state), take the 16-byte challenge, everything else is a loud failure.
     */
    private byte[] readHello() {
        byte[] head = readExact(Wire.HEADER_LEN);
        int len = Wire.decodePayloadLen(head);
        java.nio.ByteBuffer hb = java.nio.ByteBuffer.wrap(head).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        hb.position(4);
        int flags = hb.getShort() & 0xFFFF;
        int ftype = hb.getShort() & 0xFFFF;
        if (ftype == Wire.RESP_ERROR) {
            byte[] payload = readExact(len);
            throw new DocsqlException.Operational(
                    new String(payload, java.nio.charset.StandardCharsets.UTF_8));
        }
        byte[] payload = readExact(len);
        if (ftype != Wire.RESP_HELLO || len != 16) {
            throw new DocsqlException.Interface(
                    "keyed node did not send its 16-byte RESP_HELLO challenge (version mismatch?)");
        }
        return payload;
    }

    private Socket wrapTls(Socket raw, String host, int port, String caPath, String tlsHostname) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm;
            if (caPath != null && !caPath.isEmpty()) {
                tm = caTrustManager(caPath);
            } else {
                tm = trustAll();
            }
            ctx.init(null, new javax.net.ssl.TrustManager[] {tm}, new java.security.SecureRandom());
            SSLSocketFactory f = ctx.getSocketFactory();
            SSLSocket ssl = (SSLSocket) f.createSocket(raw, host, port, true);
            javax.net.ssl.SSLParameters p = ssl.getSSLParameters();
            String sni = tlsHostname != null && !tlsHostname.isEmpty() ? tlsHostname : host;
            p.setServerNames(java.util.Collections.singletonList(
                    (SNIServerName) new SNIHostName(sni)));
            if (caPath != null && !caPath.isEmpty()) {
                p.setEndpointIdentificationAlgorithm("HTTPS");
            }
            ssl.setSSLParameters(p);
            ssl.startHandshake();
            return ssl;
        } catch (IOException | java.security.GeneralSecurityException e) {
            throw new DocsqlException.Operational("TLS handshake with " + desc + " failed: " + e, e);
        }
    }

    private static X509TrustManager trustAll() {
        return new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
                // encrypt-only posture: traffic is protected, the peer certificate is NOT
                // verified (self-signed fleets, identical to the Rust/.NET/Python dialers)
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType) {
                // encrypt-only posture: traffic is protected, the peer certificate is NOT
                // verified (self-signed fleets, identical to the Rust/.NET/Python dialers)
            }

            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }

    private static X509TrustManager caTrustManager(String caPath) {
        try {
            java.security.cert.CertificateFactory cf =
                    java.security.cert.CertificateFactory.getInstance("X.509");
            java.security.KeyStore ks = java.security.KeyStore.getInstance(
                    java.security.KeyStore.getDefaultType());
            ks.load(null, null);
            java.util.Collection<? extends java.security.cert.Certificate> cas =
                    cf.generateCertificates(new java.io.FileInputStream(caPath));
            int i = 0;
            for (java.security.cert.Certificate ca : cas) {
                ks.setCertificateEntry("docsql-ca-" + (i++), ca);
            }
            javax.net.ssl.TrustManagerFactory tmf =
                    javax.net.ssl.TrustManagerFactory.getInstance(
                            javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            for (javax.net.ssl.TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509TrustManager) {
                    return (X509TrustManager) tm;
                }
            }
            throw new DocsqlException.Interface("no X509TrustManager for CA " + caPath);
        } catch (IOException | java.security.GeneralSecurityException e) {
            throw new DocsqlException.Interface("failed to load TLS CA " + caPath + ": " + e, e);
        }
    }

    void poison() {
        closed = true;
        try {
            socket.close();
        } catch (IOException ignore) {
            // best effort
        }
    }

    boolean isClosed() {
        return closed || socket.isClosed();
    }

    private InputStream in() throws IOException {
        return socket.getInputStream();
    }

    private OutputStream out() throws IOException {
        return socket.getOutputStream();
    }

    private byte[] readExact(int n) {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            try {
                socket.setSoTimeout(timeoutMs);
                int got = in().read(buf, off, n - off);
                if (got < 0) {
                    poison();
                    throw new DocsqlException.Interface("connection closed by server");
                }
                off += got;
            } catch (java.net.SocketTimeoutException e) {
                poison();
                throw new DocsqlException.Operational(
                        "read timed out after " + (timeoutMs / 1000.0) + "s", e);
            } catch (IOException e) {
                poison();
                throw new DocsqlException.Interface("connection lost during read: " + e, e);
            }
        }
        return buf;
    }

    void writeFrame(int type, int flags, byte[] payload) {
        if (closed) {
            throw new DocsqlException.Interface("connection is closed");
        }
        if (sealer != null) {
            flags |= Wire.FLAG_ENCRYPTED;
            payload = sealer.seal(type, flags, payload, challenge);
        }
        byte[] wire = Wire.encodeFrame(type, flags, payload);
        try {
            socket.setSoTimeout(timeoutMs);
            out().write(wire);
            out().flush();
        } catch (IOException e) {
            poison();
            throw new DocsqlException.Interface("connection lost during write: " + e, e);
        }
    }

    /** Send one frame and read the reply, skipping stray RESP_PUSH frames (defensive). */
    Transport.Frame roundTrip(int type, byte[] payload) {
        synchronized (ioLock) {
            writeFrame(type, 0, payload);
            while (true) {
                byte[] head = readExact(Wire.HEADER_LEN);
                int len = Wire.decodePayloadLen(head);
                java.nio.ByteBuffer hb = java.nio.ByteBuffer.wrap(head)
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN);
                hb.position(4);
                int flags = hb.getShort() & 0xFFFF;
                int ftype = hb.getShort() & 0xFFFF;
                byte[] rpayload = readExact(len);
                Transport.Frame f = finalizeFrame(ftype, flags, rpayload);
                if (f.type == Wire.RESP_PUSH) {
                    continue;
                }
                return f;
            }
        }
    }

    void close() {
        synchronized (ioLock) {
            closed = true;
            try {
                socket.close();
            } catch (IOException ignore) {
                // best effort
            }
        }
    }

    static final class Frame {
        final int type;
        final int flags;
        final byte[] payload;

        Frame(int type, int flags, byte[] payload) {
            this.type = type;
            this.flags = flags;
            this.payload = payload;
        }
    }

    Transport.Frame readFrame() {
        synchronized (ioLock) {
            byte[] head = readExact(Wire.HEADER_LEN);
            int len = Wire.decodePayloadLen(head);
            java.nio.ByteBuffer hb = java.nio.ByteBuffer.wrap(head)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN);
            hb.position(4);
            int flags = hb.getShort() & 0xFFFF;
            int type = hb.getShort() & 0xFFFF;
            byte[] payload = readExact(len);
            return finalizeFrame(type, flags, payload);
        }
    }

    private Transport.Frame finalizeFrame(int type, int flags, byte[] payload) {
        if (sealer != null) {
            if ((flags & Wire.FLAG_ENCRYPTED) == 0) {
                poison();
                throw new DocsqlException.Interface(
                        "server sent an unencrypted frame 0x"
                                + Integer.toHexString(type) + " on a keyed connection");
            }
            try {
                payload = sealer.open(type, flags, payload, challenge);
            } catch (DocsqlException e) {
                poison();
                throw e;
            } catch (RuntimeException e) {
                poison();
                throw new DocsqlException.Interface("frame decryption failed: " + e, e);
            }
        }
        return new Transport.Frame(type, flags, payload);
    }

    /**
     * Read one frame with a bounded wait; null when the poll interval elapsed fully idle.
     * Partial frames stay buffered across polls; a partial frame may only wait
     * PARTIAL_FRAME_MAX_WAIT_MS before the link is declared dead (mid-frame peer death leaves
     * no EOF — only silence).
     */
    Transport.Frame readFramePoll(int pollMs) {
        while (true) {
            if (pollBuf.length >= Wire.HEADER_LEN) {
                byte[] head = java.util.Arrays.copyOfRange(pollBuf, 0, Wire.HEADER_LEN);
                int len = Wire.decodePayloadLen(head);
                if (pollBuf.length >= Wire.HEADER_LEN + len) {
                    byte[] payload = java.util.Arrays.copyOfRange(
                            pollBuf, Wire.HEADER_LEN, Wire.HEADER_LEN + len);
                    byte[] rest = java.util.Arrays.copyOfRange(
                            pollBuf, Wire.HEADER_LEN + len, pollBuf.length);
                    pollBuf = rest;
                    partialSince = -1;
                    java.nio.ByteBuffer hb = java.nio.ByteBuffer.wrap(head)
                            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
                    hb.position(4);
                    int flags = hb.getShort() & 0xFFFF;
                    int type = hb.getShort() & 0xFFFF;
                    return finalizeFrame(type, flags, payload);
                }
            }
            try {
                socket.setSoTimeout(pollMs);
                byte[] chunk = new byte[65536];
                int got;
                try {
                    got = in().read(chunk);
                } catch (java.net.SocketTimeoutException ste) {
                    if (pollBuf.length > 0) {
                        long now = System.nanoTime();
                        if (partialSince < 0) {
                            partialSince = now;
                        } else if (now - partialSince > PARTIAL_FRAME_MAX_WAIT_MS * 1_000_000L) {
                            poison();
                            throw new DocsqlException.Interface(
                                    "transport stalled mid-frame for over "
                                            + (PARTIAL_FRAME_MAX_WAIT_MS / 1000) + "s; connection poisoned");
                        }
                        continue;
                    }
                    partialSince = -1;
                    return null;
                }
                if (got < 0) {
                    poison();
                    throw new DocsqlException.Interface("connection closed by server");
                }
                byte[] grown = new byte[pollBuf.length + got];
                System.arraycopy(pollBuf, 0, grown, 0, pollBuf.length);
                System.arraycopy(chunk, 0, grown, pollBuf.length, got);
                pollBuf = grown;
            } catch (IOException e) {
                poison();
                throw new DocsqlException.Interface("connection lost during read: " + e, e);
            }
        }
    }
}
