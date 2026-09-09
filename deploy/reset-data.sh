#!/bin/bash
# Delete ALL DocSQL data. Releases, container recreation and
# `docker compose down -v` never touch the external data volumes —
# this script is the intended way to wipe data (e.g. before a test
# run or to start over).
set -eu
cd "$(dirname "$0")"
docker compose --profile single --profile cluster --profile join down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -f docker-compose.prod.yml --profile single --profile cluster --profile join down -v --remove-orphans >/dev/null 2>&1 || true
for v in docsql-data-a docsql-data-b docsql-data-c docsql-data-d docsql-data-web docsql-data-single docsql-data-web-single; do
  docker volume rm -f "$v" >/dev/null 2>&1 || true
done
echo "DocSQL data volumes removed (stack stopped)"
