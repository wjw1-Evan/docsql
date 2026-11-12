package io.docsql;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * E2E support: spawns a real docsql-server process per suite (argument-list ProcessBuilder,
 * mirroring the Python conftest's Popen shape — never a shell). The server binary path comes
 * from DOCSQL_SERVER_BIN or the repo's target/debug/docsql-server.
 */
public final class E2eServer {

    public final Process proc;
    public final int port;

    private E2eServer(Process proc, int port, String dbPath) {
        this.proc = proc;
        this.port = port;
    }

    public static E2eServer start(String name, Map<String, String> extraEnv) throws Exception {
        String bin = System.getenv("DOCSQL_SERVER_BIN");
        if (bin == null || bin.isEmpty()) {
            bin = "../target/debug/docsql-server";
        }
        java.nio.file.Path work = java.nio.file.Files.createTempDirectory("docsql-java-e2e-");
        java.nio.file.Path db = work.resolve(name + ".db");
        int port = freePort();
        ProcessBuilder pb = new ProcessBuilder(bin, db.toString(), "127.0.0.1:" + port);
        pb.environment().putAll(extraEnv);
        pb.inheritIO();
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(
                new java.io.File(work.toFile(), name + ".log")));
        Process p = pb.start();
        // bounded TCP wait loop
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (!p.isAlive()) {
                throw new RuntimeException("server " + name + " exited early with " + p.exitValue());
            }
            try (Socket s = new Socket()) {
                s.connect(new java.net.InetSocketAddress("127.0.0.1", port), 500);
                return new E2eServer(p, port, db.toString());
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        p.destroyForcibly();
        throw new RuntimeException("server " + name + " did not come up");
    }

    public void stop() {
        proc.destroy();
        try {
            if (!proc.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                proc.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static int freePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    public static void main(String[] args) throws Exception {
        E2eServer s = E2eServer.start("smoke", new HashMap<>());
        try {
            ConnectConfig cfg = ConnectConfig.builder()
                    .host("127.0.0.1").port(s.port)
                    .token("tok")
                    .build();
            try (DocsqlConnection c = new DocsqlConnection(cfg)) {
                Result r = c.execute("SELECT 42");
                System.out.println("smoke: " + Json.write(r.rows.get(0)[0]));
            }
        } finally {
            s.stop();
        }
        System.out.println("E2E smoke OK");
    }
}
