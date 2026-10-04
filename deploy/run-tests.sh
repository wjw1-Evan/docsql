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

# Mutual exclusion: two concurrent runs share the fixed project name,
# container names and test volumes — the second `compose down` would rip
# the stack out from under the first suite. flock is a Linux CI staple but
# optional on macOS dev boxes.
if command -v flock >/dev/null 2>&1; then
  exec 9>>"${TMPDIR:-/tmp}/docsql-run-tests.lock"
  if ! flock -n 9; then
    echo "another run-tests.sh is already running; refusing to share the dev stack" >&2
    exit 1
  fi
fi

# Snapshot the caller's own env BEFORE the test overrides below replace it:
# the restored stack must come back with the user's image tag / data prefix /
# token / auth file, not the deployment defaults. EVERY variable needs the
# set/unset distinction: docker compose lets an exported EMPTY value beat
# deploy/.env (shell env outranks the .env file), so restoring an originally
# unset variable as "" would push the user's real .env configuration out of
# the restored stack. DOCSQL_WEB_AUTH_FILE is stricter still (compose uses
# `${VAR-}` there: empty string MEANS "gate disabled").
USER_DEV_IMAGE_TAG="${DOCSQL_DEV_IMAGE_TAG:-}"
HAD_DEV_IMAGE_TAG="${DOCSQL_DEV_IMAGE_TAG+set}"
USER_DATA_PREFIX="${DOCSQL_DEV_DATA_PREFIX:-}"
HAD_DATA_PREFIX="${DOCSQL_DEV_DATA_PREFIX+set}"
USER_DEV_TOKEN="${DOCSQL_DEV_TOKEN:-}"
HAD_DEV_TOKEN="${DOCSQL_DEV_TOKEN+set}"
USER_BACKUP_INTERVAL="${DOCSQL_BACKUP_INTERVAL_SECS:-}"
HAD_BACKUP_INTERVAL="${DOCSQL_BACKUP_INTERVAL_SECS+set}"
USER_WEB_AUTH_FILE="${DOCSQL_WEB_AUTH_FILE:-}"
HAD_WEB_AUTH_FILE="${DOCSQL_WEB_AUTH_FILE+set}"
# The dev compose reads these from deploy/.env as well: a locally present
# .env (recommended for prod deployments) would otherwise leak non-default
# values into the test stack and break its determinism from outside.
USER_READ_TOKEN="${DOCSQL_READ_TOKEN:-}"
HAD_READ_TOKEN="${DOCSQL_READ_TOKEN+set}"
USER_MAX_CONN="${DOCSQL_MAX_CONN:-}"
HAD_MAX_CONN="${DOCSQL_MAX_CONN+set}"
USER_IDLE_TIMEOUT="${DOCSQL_IDLE_TIMEOUT:-}"
HAD_IDLE_TIMEOUT="${DOCSQL_IDLE_TIMEOUT+set}"
USER_BACKUP_KEEP="${DOCSQL_BACKUP_KEEP:-}"
HAD_BACKUP_KEEP="${DOCSQL_BACKUP_KEEP+set}"
USER_ADVERTISE="${DOCSQL_ADVERTISE:-}"
HAD_ADVERTISE="${DOCSQL_ADVERTISE+set}"
# TLS 四件同纪律:dev compose 与 prod 同目录共享 deploy/.env —— 为 prod
# 启用的 TLS 值会透传进测试栈,节点拿到容器内不存在的证书路径启动即退,
# 测试死在 "port never came up" 且报错晦涩(与被测代码无关)。
USER_TLS_CERT="${DOCSQL_TLS_CERT:-}"
HAD_TLS_CERT="${DOCSQL_TLS_CERT+set}"
USER_TLS_KEY="${DOCSQL_TLS_KEY:-}"
HAD_TLS_KEY="${DOCSQL_TLS_KEY+set}"
USER_TLS_CONNECT="${DOCSQL_TLS_CONNECT:-}"
HAD_TLS_CONNECT="${DOCSQL_TLS_CONNECT+set}"
USER_TLS_CA="${DOCSQL_TLS_CA:-}"
HAD_TLS_CA="${DOCSQL_TLS_CA+set}"
# Image repository override (fork CI): the workflow exports IMAGE pointing
# at the fork's own GHCR address (ghcr.io/<owner>/<repo>); route it into the
# dev compose's image name so the deploy test pulls what was just built
# instead of the upstream repository. Only when non-empty — exporting ""
# would override a deploy/.env DOCSQL_DEV_IMAGE just like the variables
# above (set/unset discipline).
USER_DEV_IMAGE="${DOCSQL_DEV_IMAGE:-}"
HAD_DEV_IMAGE="${DOCSQL_DEV_IMAGE+set}"
if [ -n "${IMAGE:-}" ]; then
  export DOCSQL_DEV_IMAGE="$IMAGE"
fi

# Test-stack overrides (never the user's config): raw unauthenticated curls
# need the console account gate off, and the client token stays empty for
# deterministic open-mode assertions. Export (not inline) — the join test in
# multinode-test.sh recreates node-d through compose and must inherit both.
export DOCSQL_WEB_AUTH_FILE=""
export DOCSQL_DEV_TOKEN=""
export DOCSQL_BACKUP_INTERVAL_SECS="5"
export DOCSQL_DEV_DATA_PREFIX="docsql-dev-testdata"
# Neutralize the rest of the .env passthroughs (empty = server defaults).
export DOCSQL_READ_TOKEN=""
export DOCSQL_MAX_CONN=""
export DOCSQL_IDLE_TIMEOUT=""
export DOCSQL_BACKUP_KEEP=""
export DOCSQL_ADVERTISE=""
export DOCSQL_TLS_CERT=""
export DOCSQL_TLS_KEY=""
export DOCSQL_TLS_CONNECT=""
export DOCSQL_TLS_CA=""

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
# TOOK_OVER gates the whole handler: an early failure (build error, port
# wait) happens BEFORE this script stops the user's dev stack — running the
# teardown then would needlessly bounce a healthy stack (down + re-up).
TOOK_OVER=0
# restore_env <name> <had-marker> <value>: put one caller variable back
# exactly as it was — re-export the captured value, or unset what the test
# overrides had exported. Exporting "" is NOT equivalent to unsetting here:
# compose resolves ${VAR:-} from the environment first and an exported empty
# value wins over deploy/.env, so "originally unset" must come back unset.
restore_env() {
  if [ "$2" = set ]; then
    export "$1=$3"
  else
    unset "$1"
  fi
}
restore_user_stack() {
  if [ "$TOOK_OVER" -ne 1 ]; then
    return 0
  fi
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
    # Replay the caller's own environment (captured at entry): a stack started
    # with DOCSQL_DEV_IMAGE_TAG=ci / a custom data prefix / an auth file must
    # come back exactly as it was, not silently retagged to :local defaults.
    # Set/unset symmetrically for every variable (restore_env): an exported
    # empty value overrides deploy/.env in compose, so an originally-unset
    # variable must be restored as unset, never as "".
    restore_env DOCSQL_DEV_IMAGE_TAG "$HAD_DEV_IMAGE_TAG" "$USER_DEV_IMAGE_TAG"
    restore_env DOCSQL_DEV_DATA_PREFIX "$HAD_DATA_PREFIX" "$USER_DATA_PREFIX"
    restore_env DOCSQL_DEV_TOKEN "$HAD_DEV_TOKEN" "$USER_DEV_TOKEN"
    restore_env DOCSQL_BACKUP_INTERVAL_SECS "$HAD_BACKUP_INTERVAL" "$USER_BACKUP_INTERVAL"
    restore_env DOCSQL_READ_TOKEN "$HAD_READ_TOKEN" "$USER_READ_TOKEN"
    restore_env DOCSQL_MAX_CONN "$HAD_MAX_CONN" "$USER_MAX_CONN"
    restore_env DOCSQL_IDLE_TIMEOUT "$HAD_IDLE_TIMEOUT" "$USER_IDLE_TIMEOUT"
    restore_env DOCSQL_BACKUP_KEEP "$HAD_BACKUP_KEEP" "$USER_BACKUP_KEEP"
    restore_env DOCSQL_ADVERTISE "$HAD_ADVERTISE" "$USER_ADVERTISE"
    restore_env DOCSQL_TLS_CERT "$HAD_TLS_CERT" "$USER_TLS_CERT"
    restore_env DOCSQL_TLS_KEY "$HAD_TLS_KEY" "$USER_TLS_KEY"
    restore_env DOCSQL_TLS_CONNECT "$HAD_TLS_CONNECT" "$USER_TLS_CONNECT"
    restore_env DOCSQL_TLS_CA "$HAD_TLS_CA" "$USER_TLS_CA"
    restore_env DOCSQL_WEB_AUTH_FILE "$HAD_WEB_AUTH_FILE" "$USER_WEB_AUTH_FILE"
    restore_env DOCSQL_DEV_IMAGE "$HAD_DEV_IMAGE" "$USER_DEV_IMAGE"
    docker compose $UP_PROFILES up -d >/dev/null 2>&1 \
      || echo "warning: could not restore the dev stack — run 'cd deploy && docker compose $UP_PROFILES up -d'" >&2
  fi
}
trap restore_user_stack EXIT

if [ -z "${DOCSQL_DEV_IMAGE_TAG:-}" ]; then
  # The image build COPYs the WORKING TREE (not any git commit): staged or
  # untracked changes ride into the image, so a green run here can certify
  # a tree state that the subsequent push does not contain. Refuse (with
  # an escape hatch for intentional dirty-tree experiments).
  if [ "${DOCSQL_ALLOW_DIRTY:-}" != 1 ] && ! git -C "$DEPLOY_DIR/.." diff --quiet 2>/dev/null      || [ "${DOCSQL_ALLOW_DIRTY:-}" != 1 ] && [ -n "$(git -C "$DEPLOY_DIR/.." status --porcelain 2>/dev/null)" ]; then
    echo "ERROR: working tree is dirty — the test image would contain uncommitted changes." >&2
    echo "       Commit/stash first, or set DOCSQL_ALLOW_DIRTY=1 to test the dirty tree anyway." >&2
    exit 1
  fi
  echo "== build local image (${DOCSQL_DEV_IMAGE:-ghcr.io/wjw1-evan/docsql}:${TAG}; in-build cargo test gate) =="
  DOCSQL_DEV_IMAGE_TAG="$TAG" docker compose $PROFILES build >/dev/null
fi
echo "== stop the running dev stack (data volumes untouched) =="
docker compose --profile single --profile cluster --profile join down --remove-orphans >/dev/null 2>&1 || true
# From here on the user's stack is down and the exit handler owes a restore.
TOOK_OVER=1
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
