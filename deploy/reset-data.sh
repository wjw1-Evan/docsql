#!/bin/bash
# Delete ALL DocSQL data. Releases, container recreation and
# `docker compose down -v` never touch the external data volumes —
# this script is the intended way to wipe data (e.g. before a test
# run or to start over).
set -eu
cd "$(dirname "$0")"
docker compose --profile single --profile cluster --profile join down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -f docker-compose.prod.yml --profile single --profile cluster --profile join down -v --remove-orphans >/dev/null 2>&1 || true
# docsql-data-web / docsql-data-web-single are legacy side-store volumes of
# the old embedded-engine console (no longer mounted by any service) —
# wiping them here clears historical leftovers too. The dev stack keeps its
# own docsql-dev-data-* set and prod join's node uses docsql-prod-data-d;
# all of them are wiped here. The *_web-auth* named volumes hold the web
# consoles' username/password files — wiping them restores first-use setup.
for v in docsql-data-a docsql-data-b docsql-data-c docsql-data-d docsql-data-web docsql-data-single docsql-data-web-single \
         docsql-prod-data-d \
         docsql-dev-data-a docsql-dev-data-b docsql-dev-data-c docsql-dev-data-d docsql-dev-data-single \
         docsql-dev_web-auth docsql-dev_web-auth-single \
         docsql-prod_web-auth docsql-prod_web-auth-single; do
  docker volume rm -f "$v" >/dev/null 2>&1 || true
done
echo "DocSQL data volumes removed (stack stopped)"
