#!/bin/bash
# Full multi-node deployment run: fresh cluster + test suite.
# Usage: ./deploy/run-tests.sh   (from the repo root)
set -eu
cd "$(dirname "$0")"
echo "== reset cluster (fresh volumes) =="
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d >/dev/null
echo "== wait for nodes =="
for port in 17601 17602 17603; do
  for _ in $(seq 1 60); do
    nc -z 127.0.0.1 "$port" 2>/dev/null && break
    sleep 0.5
  done
done
sleep 2
docker ps --filter name=docsql --format "{{.Names}}: {{.Status}}"
cd ..
echo
./deploy/multinode-test.sh
