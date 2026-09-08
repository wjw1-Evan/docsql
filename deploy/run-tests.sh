#!/bin/bash
# Full deployment run: single-node + multi-node profiles, both test suites.
# Usage (from the repo root):
#   ./deploy/run-tests.sh                          # build local image (:local, with the
#                                                  # in-build cargo test gate) then test
#   DOCSQL_IMAGE_TAG=<tag> ./deploy/run-tests.sh   # test an existing image, no build
set -eu
cd "$(dirname "$0")"
TAG="${DOCSQL_IMAGE_TAG:-local}"
PROFILES="--profile single --profile cluster"
if [ -z "${DOCSQL_IMAGE_TAG:-}" ]; then
  echo "== build local image (ghcr.io/wjw1-evan/docsql:${TAG}; in-build cargo test gate) =="
  DOCSQL_IMAGE_TAG="$TAG" docker compose $PROFILES build >/dev/null
fi
echo "== reset deployment (fresh volumes, both profiles) =="
docker compose $PROFILES down -v --remove-orphans >/dev/null 2>&1 || true
DOCSQL_IMAGE_TAG="$TAG" docker compose $PROFILES up -d >/dev/null
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
