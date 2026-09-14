#!/bin/bash
# Full deployment run: single-node + multi-node profiles, both test suites.
# Usage (from the repo root):
#   ./deploy/run-tests.sh                            # build local image (:local,
#                                                    # in-build cargo test gate) then test
#   DOCSQL_DEV_IMAGE_TAG=<tag> ./deploy/run-tests.sh # test an existing image, no build
#
# Data safety: the suites need a clean slate, so they run on dedicated
# throwaway volumes (DOCSQL_DEV_DATA_PREFIX=docsql-dev-testdata). The regular
# dev volumes (docsql-dev-data-*) and the console account files are never
# removed; the dev stack that was running before the run is brought back up
# on exit. Wipe real data only via ./deploy/reset-data.sh.
set -eu
cd "$(dirname "$0")"
DEPLOY_DIR="$PWD"

# Test-stack overrides (never the user's config): raw unauthenticated curls
# need the console account gate off, and the client token stays empty for
# deterministic open-mode assertions. Export (not inline) — the join test in
# multinode-test.sh recreates node-d through compose and must inherit both.
export DOCSQL_WEB_AUTH_FILE=""
export DOCSQL_DEV_TOKEN=""
export DOCSQL_BACKUP_INTERVAL_SECS="5"
export DOCSQL_DEV_DATA_PREFIX="docsql-dev-testdata"

TAG="${DOCSQL_DEV_IMAGE_TAG:-local}"
PROFILES="--profile single --profile cluster"
DATA_SUFFIXES="a b c d single"

# Which profiles the user had running, so the stack can be restored on exit.
# Captured before the teardown below; nothing running = nothing to restore.
UP_PROFILES=""
was_up() { docker ps --filter "name=^$1$" --format '{{.Names}}' | grep -q .; }
if was_up docsql-a || was_up docsql-b || was_up docsql-c || was_up docsql-web; then
  UP_PROFILES="$UP_PROFILES --profile cluster"
fi
if was_up docsql-single || was_up docsql-web-single; then
  UP_PROFILES="$UP_PROFILES --profile single"
fi
if was_up docsql-d; then
  UP_PROFILES="$UP_PROFILES --profile join"
fi

# Always runs (test failure, Ctrl-C, success): stop the test stack, drop its
# throwaway volumes, then put the user's stack back exactly as it was. The
# teardown deliberately avoids `down -v`, so no user volume is ever removed.
restore_user_stack() {
  # The suites run after a `cd ..` to the repo root: compose needs the
  # deploy directory (compose file + .env) for this cleanup.
  cd "$DEPLOY_DIR"
  DOCSQL_DEV_DATA_PREFIX="docsql-dev-testdata" \
    docker compose --profile single --profile cluster --profile join down --remove-orphans >/dev/null 2>&1 || true
  for s in $DATA_SUFFIXES; do
    docker volume rm -f "docsql-dev-testdata-${s}" >/dev/null 2>&1 || true
  done
  if [ -n "$UP_PROFILES" ]; then
    echo "== restore the previous dev stack ($UP_PROFILES) =="
    # Fall back to the deployment's own configuration (.env / defaults) —
    # the test overrides above must not leak into the restored stack.
    unset DOCSQL_DEV_DATA_PREFIX DOCSQL_WEB_AUTH_FILE DOCSQL_DEV_TOKEN \
      DOCSQL_BACKUP_INTERVAL_SECS DOCSQL_DEV_IMAGE_TAG
    docker compose $UP_PROFILES up -d >/dev/null 2>&1 \
      || echo "warning: could not restore the dev stack — run 'cd deploy && docker compose $UP_PROFILES up -d'" >&2
  fi
}
trap restore_user_stack EXIT

if [ -z "${DOCSQL_DEV_IMAGE_TAG:-}" ]; then
  echo "== build local image (ghcr.io/wjw1-evan/docsql:${TAG}; in-build cargo test gate) =="
  DOCSQL_DEV_IMAGE_TAG="$TAG" docker compose $PROFILES build >/dev/null
fi
echo "== stop the running dev stack (data volumes untouched) =="
docker compose --profile single --profile cluster --profile join down --remove-orphans >/dev/null 2>&1 || true
echo "== create fresh throwaway test volumes (${DOCSQL_DEV_DATA_PREFIX}-*) =="
for s in $DATA_SUFFIXES; do
  v="${DOCSQL_DEV_DATA_PREFIX}-${s}"
  docker volume rm -f "$v" >/dev/null 2>&1 || true
  # A failed rm (volume still in use by a straggler container) must abort,
  # not silently continue on stale data that then breaks count assertions.
  if docker volume inspect "$v" >/dev/null 2>&1; then
    echo "ERROR: volume $v still exists after rm (in use?)" >&2
    exit 1
  fi
  docker volume create "$v" >/dev/null
done
DOCSQL_DEV_IMAGE_TAG="$TAG" docker compose $PROFILES up -d >/dev/null
echo "== wait for nodes and web consoles =="
for port in 17600 17601 17602 17603 17700 17710; do
  ok=""
  for _ in $(seq 1 60); do
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then ok=1; break; fi
    sleep 0.5
  done
  if [ -z "$ok" ]; then
    echo "ERROR: port $port never came up" >&2
    docker ps -a --filter name=docsql
    exit 1
  fi
done
# Web console readiness: the port being open does not mean HTTP is served yet.
for port in 17700 17710; do
  ok=""
  for _ in $(seq 1 30); do
    if curl -fsS -o /dev/null "http://127.0.0.1:$port/" 2>/dev/null; then ok=1; break; fi
    sleep 0.5
  done
  if [ -z "$ok" ]; then
    echo "ERROR: web console on port $port never answered HTTP" >&2
    docker ps -a --filter name=docsql
    exit 1
  fi
done
docker ps --filter name=docsql --format "{{.Names}}: {{.Status}}"
cd ..
echo
echo "########## single-node suite ##########"
./deploy/single-test.sh
echo
echo "########## multi-node suite ##########"
./deploy/multinode-test.sh
