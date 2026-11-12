#!/usr/bin/env bash
# Run the e2e suite (build.sh must have run first).
set -euo pipefail
cd "$(dirname "$0")"

if [ -n "${JAVA_HOME:-}" ]; then
    JAVA="$JAVA_HOME/bin"
else
    JAVA="$(/usr/libexec/java_home --version 17+ 2>/dev/null)/bin"
    if [ ! -x "$JAVA/java" ]; then
        for cand in /tmp/docsql-jdk/jdk-*/Contents/Home /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home; do
            if [ -x "$cand/bin/java" ]; then
                JAVA="$cand/bin"
                break
            fi
        done
    fi
fi
echo "Using JDK: $JAVA"
java -cp build/docsql-jdbc-0.9.0.jar:build/test-classes io.docsql.E2eMain with-unit
