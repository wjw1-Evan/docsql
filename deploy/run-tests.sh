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
echo "== wait for nodes =="
for port in 17600 17601 17602 17603; do
  for _ in $(seq 1 60); do
    nc -z 127.0.0.1 "$port" 2>/dev/null && break
    sleep 0.5
  done
done
sleep 2
docker ps --filter name=docsql --format "{{.Names}}: {{.Status}}"
cd ..
echo
echo "########## single-node suite ##########"
./deploy/single-test.sh
echo
echo "########## multi-node suite ##########"
./deploy/multinode-test.sh
