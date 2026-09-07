#!/bin/bash
# Single-node deployment test against the compose `single` profile.
# Nodes: one standalone node — node-single (:17600), web console (:17710).
# No peers configured: reads and writes stay local. When the cluster profile
# is also up (node-a), additionally verifies data isolation between the
# standalone node and the cluster.
set -u
# All traffic runs inside the compose network via the image's own CLI
# (no host toolchain needed); host ports stay mapped for external access.
A="node-single:7600"
CTR="docsql-single"
PASS=0; FAIL=0
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

sql() { printf "%s\nexit;\n" "$2" | docker exec -i "$CTR" docsql-cli connect "$1" 2>/dev/null; }
kv()  { docker exec "$CTR" docsql-cli kv "$1" "${@:2}" 2>/dev/null; }

echo "== 1. standalone SQL =="
out=$(sql "$A" "CREATE TABLE solo (id INT PRIMARY KEY, tag TEXT);")
echo "$out" | grep -q "rows affected" && ok "create table" || bad "create table: $out"
out=$(sql "$A" "INSERT INTO solo VALUES (1, 'one'), (2, 'two');")
echo "$out" | grep -q "2 rows affected" && ok "insert 2 rows" || bad "insert: $out"
out=$(sql "$A" "SELECT id, tag FROM solo WHERE id = 1;")
echo "$out" | grep -q "one" && ok "select row back" || bad "select: $out"
out=$(sql "$A" "SELECT COUNT(id) FROM solo;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$out" = "2" ] && ok "count = 2" || bad "count: '$out'"

echo "== 2. standalone KV =="
out=$(kv "$A" SET solo:greeting standalone)
[ "$out" = "ok" ] && ok "kv SET" || bad "kv SET: $out"
out=$(kv "$A" GET solo:greeting)
[ "$out" = "standalone" ] && ok "kv GET roundtrip" || bad "kv GET: '$out'"
out=$(kv "$A" INCR solo:counter)
[ "$out" = "1" ] && ok "kv INCR" || bad "kv INCR: '$out'"

echo "== 3. transactions (local) =="
before=$(sql "$A" "SELECT COUNT(id) FROM solo;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
sql "$A" "BEGIN;" >/dev/null
sql "$A" "INSERT INTO solo VALUES (9, 'tx');" >/dev/null
sql "$A" "ROLLBACK;" >/dev/null
after=$(sql "$A" "SELECT COUNT(id) FROM solo;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$before" = "$after" ] && ok "rollback (count $before == $after)" || bad "rollback: $before != $after"

echo "== 4. persistence across container restart =="
docker restart "$CTR" >/dev/null
up=""
for _ in $(seq 1 60); do
  out=$(sql "$A" "SELECT id FROM solo WHERE id = 2;" 2>/dev/null)
  echo "$out" | grep -qE "^[[:space:]]*2[[:space:]]*$" && { up=1; break; }
  sleep 0.5
done
[ -n "$up" ] && ok "node back up after restart" || bad "node did not come back"
out=$(sql "$A" "SELECT COUNT(id) FROM solo;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$out" = "2" ] && ok "sql rows survive restart (count = 2)" || bad "rows lost on restart: '$out'"
out=$(kv "$A" GET solo:greeting)
[ "$out" = "standalone" ] && ok "kv survives restart" || bad "kv lost on restart: '$out'"

echo "== 5. web console (:17710) =="
W="http://127.0.0.1:17710"
body=$(curl -s "$W/")
echo "$body" | grep -q "docsql console" && ok "web UI served" || bad "web UI: $(echo "$body" | head -c 80)"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"CREATE TABLE web_solo (id INT PRIMARY KEY, note TEXT)"}')
echo "$r" | grep -q '"affected"' && ok "web sql create" || bad "web sql create: $r"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"INSERT INTO web_solo VALUES (1, '\''fromweb'\'')"}')
echo "$r" | grep -q '"count":1' && ok "web sql insert" || bad "web sql insert: $r"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT id, note FROM web_solo"}')
echo "$r" | grep -q '"fromweb"' && ok "web sql select" || bad "web sql select: $r"
r=$(curl -s "$W/api/stats")
echo "$r" | grep -q '"tables"' && ok "web stats" || bad "web stats: $r"

echo "== 6. isolation from the cluster profile =="
if docker ps --filter "name=docsql-a" --format "{{.Names}}" | grep -q .; then
  sql "$A" "CREATE TABLE solo_only (k TEXT);" >/dev/null
  out=$(printf 'SELECT k FROM solo_only;\nexit;\n' | docker exec -i docsql-a docsql-cli connect node-a:7600 2>/dev/null)
  echo "$out" | grep -qi "does not exist" && ok "cluster cannot see single's tables" || bad "leak cluster<-single: $out"
  printf 'CREATE TABLE cluster_only (k TEXT);\nexit;\n' | docker exec -i docsql-a docsql-cli connect node-a:7600 >/dev/null 2>&1
  out=$(sql "$A" "SELECT k FROM cluster_only;" 2>&1)
  echo "$out" | grep -qi "does not exist" && ok "single cannot see cluster's tables" || bad "leak single<-cluster: $out"
else
  echo "SKIP: cluster profile not running (isolation checks)"
fi

echo
echo "RESULT: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
