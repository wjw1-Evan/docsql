#!/bin/bash
# Full deployment run: single-node + multi-node profiles, both test suites.
# Usage (from the repo root):
#   ./deploy/run-tests.sh                          # build local image (:local, with the
#                                                  # in-build cargo test gate) then test
#   DOCSQL_DEV_IMAGE_TAG=<tag> ./deploy/run-tests.sh # test an existing image, no build
set -eu
cd "$(dirname "$0")"
# The deployment tests drive the web console with raw unauthenticated curls;
# empty DOCSQL_WEB_AUTH_FILE disables the console account gate for the test
# stack (compose's unset-only default would otherwise turn it on).
export DOCSQL_WEB_AUTH_FILE=""
# Backup assertions need backups on a test-friendly cadence (single-test.sh
# chapter 8 polls for files and drives a retention/restore round-trip).
# Empty in compose = server default (daily) — too slow for tests.
export DOCSQL_BACKUP_INTERVAL_SECS="5"
TAG="${DOCSQL_DEV_IMAGE_TAG:-local}"
PROFILES="--profile single --profile cluster"
if [ -z "${DOCSQL_DEV_IMAGE_TAG:-}" ]; then
  echo "== build local image (ghcr.io/wjw1-evan/docsql:${TAG}; in-build cargo test gate) =="
  DOCSQL_DEV_IMAGE_TAG="$TAG" docker compose $PROFILES build >/dev/null
fi
echo "== reset deployment (fresh volumes, both profiles) =="
# `join` (node-d) must be included: its container would otherwise keep the
# docsql-dev-data-d volume attached and the clean-slate check below fails.
docker compose --profile single --profile cluster --profile join down -v --remove-orphans >/dev/null 2>&1 || true
# Data lives in external volumes that `down -v` cannot remove — recreate
# them explicitly so every test run starts from a clean slate (and so they
# exist at all on a fresh CI runner). Dev-only names: prod's docsql-data-*
# (real data) and prod join's docsql-prod-data-d are never touched.
for v in docsql-dev-data-a docsql-dev-data-b docsql-dev-data-c docsql-dev-data-d docsql-dev-data-single; do
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
