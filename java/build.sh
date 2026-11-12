#!/usr/bin/env bash
# DocSQL Java SDK — zero-dependency build (javac + jar, no Maven/Gradle).
# Usage:
#   ./build.sh            compile + jar + unit tests (no server needed)
#   ./build.sh --no-test  compile + jar only
# JDK lookup: $JAVA_HOME → /usr/libexec/java_home (macOS) → common Homebrew/Adoptium paths.
set -euo pipefail
cd "$(dirname "$0")"

if [ -n "${JAVA_HOME:-}" ]; then
    JAVA="$JAVA_HOME/bin"
else
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
        JAVA="$(/usr/libexec/java_home --version 17+ 2>/dev/null)/bin"
    fi
    if [ -z "${JAVA:-}" ] || [ ! -x "$JAVA/javac" ]; then
        for cand in /tmp/docsql-jdk/jdk-*/Contents/Home /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
                    /Library/Java/JavaVirtualMachines/*/Contents/Home; do
            if [ -x "$cand/bin/javac" ]; then
                JAVA="$cand/bin"
                JDK_SRC="local tarball"
                break
            /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
            fi
        done
    fi
fi
if [ -z "${JAVA:-}" ] || [ ! -x "$JAVA/javac" ]; then
    echo "error: no JDK 17 found (set JAVA_HOME; Temurin 17 works, compiles with --release 8)" >&2
    exit 1
fi
echo "Using JDK: $JAVA"

rm -rf build/classes build/test-classes
mkdir -p build/classes build/test-classes

echo "== javac main =="
find src/main/java -name '*.java' > build/main-sources.txt
"$JAVA/javac" --release 8 -encoding UTF-8 -d build/classes @build/main-sources.txt

cp -R src/main/resources/* build/classes/ 2>/dev/null || true

echo "== jar =="
mkdir -p build/jar/META-INF/services
cp -R src/main/resources/* build/jar/ 2>/dev/null || true
cp -R build/classes/* build/jar/ 2>/dev/null || true
"$JAVA/jar" --create --file build/docsql-jdbc-0.9.0.jar -C build/jar .

echo "== javac test =="
find src/test/java -name '*.java' > build/test-sources.txt
"$JAVA/javac" --release 8 -encoding UTF-8 -cp build/docsql-jdbc-0.9.0.jar -d build/test-classes @build/test-sources.txt

if [ "$#" -gt 0 ] && [ "$1" = "--no-test" ]; then
    echo "BUILD OK (no tests)"
    exit 0
fi

echo "== unit tests =="
"$JAVA/java" -cp build/docsql-jdbc-0.9.0.jar:build/test-classes io.docsql.UnitTestMain
echo "BUILD OK"
