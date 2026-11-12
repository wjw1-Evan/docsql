package io.docsql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-256-GCM frame sealing (DOCSQL_KEY), mirroring the server's crypto and the Python/.NET
 * clients:
 *
 * <pre>
 * payload' = nonce[12] || ciphertext+tag[16]
 * aad      = frame_type:u16 LE || flags:u16 LE || conn_challenge[16]
 * </pre>
 *
 * <p>The nonce is a per-connection random 8-byte prefix plus a strictly monotonic 32-bit counter
 * (little-endian), so a connection never reuses a (nonce, key) pair. The inbound replay guard
 * verifies-then-advances: the watermark only moves after the GCM tag validates.
 */
public final class Sealer {

    private final SecretKeySpec key;
    private final byte[] prefix = new byte[8];
    private long counter = 0;
    private byte[] peerPrefix;
    private long peerLast = -1;

    public Sealer(byte[] keyBytes) {
        if (keyBytes == null || keyBytes.length != 32) {
            throw new DocsqlException.Interface("DOCSQL_KEY must be 32 bytes, got "
                    + (keyBytes == null ? "null" : keyBytes.length));
        }
        this.key = new SecretKeySpec(keyBytes, "AES");
        new SecureRandom().nextBytes(prefix);
    }

    public static byte[] parseKeyHex(String hexKey) {
        String text = hexKey == null ? "" : hexKey.trim();
        if (text.length() != 64) {
            throw new DocsqlException.Interface("DOCSQL_KEY must be 64 hex chars, got " + text.length());
        }
        return hexToBytes(text);
    }

    static byte[] hexToBytes(String hex) {
        int n = hex.length();
        byte[] out = new byte[n / 2];
        for (int i = 0; i < n; i += 2) {
            out[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
        }
        return out;
    }

    private static byte[] aad(int frameType, int flags, byte[] challenge) {
        return ByteBuffer.allocate(4 + (challenge == null ? 0 : challenge.length))
                .order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) frameType)
                .putShort((short) flags)
                .put(challenge == null ? new byte[0] : challenge)
                .array();
    }

    public synchronized byte[] seal(int frameType, int flags, byte[] plaintext, byte[] challenge) {
        counter++;
        if (counter > 0xFFFF_FFFFL) {
            throw new DocsqlException.Interface(
                    "nonce counter exhausted (>2^32 frames on this connection); reconnect");
        }
        byte[] nonce = new byte[12];
        System.arraycopy(prefix, 0, nonce, 0, 8);
        nonce[8] = (byte) counter;
        nonce[9] = (byte) (counter >>> 8);
        nonce[10] = (byte) (counter >>> 16);
        nonce[11] = (byte) (counter >>> 24);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce));
            cipher.updateAAD(aad(frameType, flags, challenge));
            byte[] sealed = cipher.doFinal(plaintext);
            byte[] out = new byte[12 + sealed.length];
            System.arraycopy(nonce, 0, out, 0, 12);
            System.arraycopy(sealed, 0, out, 12, sealed.length);
            return out;
        } catch (Exception e) {
            throw new DocsqlException.Interface("frame encryption failed: " + e, e);
        }
    }

    public synchronized byte[] open(int frameType, int flags, byte[] sealed, byte[] challenge) {
        if (sealed == null || sealed.length < 28) {
            throw new DocsqlException.Protocol("sealed payload too short");
        }
        byte[] nonce = Arrays.copyOfRange(sealed, 0, 12);
        byte[] ct = Arrays.copyOfRange(sealed, 12, sealed.length);
        byte[] thisPrefix = Arrays.copyOfRange(nonce, 0, 8);
        long counter = ByteBuffer.wrap(nonce, 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
        if (peerPrefix == null) {
            peerPrefix = thisPrefix;
        }
        if (!Arrays.equals(thisPrefix, peerPrefix) || counter <= peerLast) {
            throw new DocsqlException.Protocol(
                    "replayed, reordered or cross-connection frame rejected (nonce)");
        }
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, nonce));
            cipher.updateAAD(aad(frameType, flags, challenge));
            byte[] plain = cipher.doFinal(ct);
            peerLast = counter;
            return plain;
        } catch (Exception e) {
            throw new DocsqlException.Interface("frame decryption failed: " + e, e);
        }
    }
}
