package io.docsql;

/** Zero-dependency test runner: exit 1 on any failure. */
public final class UnitTestMain {

    public static void main(String[] args) {
        UnitTests.runAll();
        System.out.println("unit: " + UnitTests.passed + " passed, " + UnitTests.failed + " failed");
        if (UnitTests.failed > 0) {
            System.exit(1);
        }
    }
}
