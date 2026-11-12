#!/usr/bin/env bash
# DocSQL Java SDK — full test entry (mirrors python/'s pytest approach):
#   1. ensure docsql-server binary (cargo build -p docsql-server, skipped when present)
#   2. compile + jar + unit tests + e2e against real servers
# Usage: ./run-tests.sh        (DOCSQL_SKIP_CARGO_BUILD=1 skips the cargo step)
set -euo pipefail
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
export DOCSQL_SERVER_BIN="${DOCSQL_SERVER_BIN:-$REPO_ROOT/target/debug/docsql-server}"

if [ ! -x "$DOCSQL_SERVER_BIN" ] && [ "${DOCSQL_SKIP_CARGO_BUILD:-}" != "1" ]; then
    echo "== cargo build -p docsql-server =="
    if [ -x "$HOME/.cargo/bin/cargo" ]; then
        "$HOME/.cargo/bin/cargo" build -p docsql-server
    else
        cargo build -p docsql-server
    fi
fi
if [ ! -x "$DOCSQL_SERVER_BIN" ]; then
    echo "error: docsql-server binary not found at $DOCSQL_SERVER_BIN" >&2
    echo "       run: cargo build -p docsql-server" >&2
    exit 1
fi

echo "== build + unit + e2e =="
./build.sh
DOCSQL_SERVER_BIN="$DOCSQL_SERVER_BIN" ./build-run-e2e.sh
