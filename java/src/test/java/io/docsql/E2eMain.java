package io.docsql;

/** E2E runner: starts servers, runs all groups, reports pass/fail, exit 1 on failure. */
public final class E2eMain {

    public static void main(String[] args) throws Exception {
        boolean unitToo = args.length > 0 && args[0].equals("with-unit");
        if (unitToo) {
            UnitTests.runAll();
        }
        E2eTests.startAll();
        try {
            E2eTests.basicTests();
            E2eTests.txTests();
            E2eTests.preparedTests();
            E2eTests.tlsTests();
            E2eTests.pubsubTests();
            E2eTests.pubsubResumeTests();
            E2eTests.pubsubPatternTests();
            E2eTests.authTests();
            E2eTests.jdbcTests();
            E2eTests.savepointTests();
        } finally {
            E2eTests.stopAll();
        }
        System.out.println("e2e: " + E2eTests.passed + " passed, " + E2eTests.failed + " failed");
        if (E2eTests.failed > 0) {
            System.exit(1);
        }
    }
}
