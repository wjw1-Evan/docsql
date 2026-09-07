#!/bin/bash
# Multi-node deployment test against the docker-compose symmetric cluster.
# Nodes: three equal peers — node-a (:17601), node-b (:17602), node-c (:17603).
# Any node accepts writes and fans them out to its DOCSQL_PEERS.
set -u
# All traffic runs inside the compose network via the image's own CLI
# (no host toolchain needed); host ports stay mapped for external access.
A="node-a:7600"
B="node-b:7600"
C="node-c:7600"
ctr_of() {
  case "$1" in
    node-a*) echo docsql-a ;;
    node-b*) echo docsql-b ;;
    *)       echo docsql-c ;;
  esac
}
PASS=0; FAIL=0
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

sql() { printf "%s\nexit;\n" "$2" | docker exec -i "$(ctr_of "$1")" docsql-cli connect "$1" 2>/dev/null; }
kv()  { docker exec "$(ctr_of "$1")" docsql-cli kv "$1" "${@:2}" 2>/dev/null; }

# wait_row <node> <sql> <ERE pattern>: poll until the query output matches.
wait_row() {
  local out=""
  for _ in $(seq 1 40); do
    out=$(sql "$1" "$2")
    echo "$out" | grep -qE "$3" && break
    sleep 0.5
  done
  echo "$out" | grep -qE "$3"
}

echo "== 1. node-a accepts SQL writes =="
out=$(sql "$A" "CREATE TABLE nodes (id INT PRIMARY KEY, host TEXT);")
echo "$out" | grep -q "rows affected" && ok "create table on a" || bad "create table on a: $out"
out=$(sql "$A" "INSERT INTO nodes VALUES (1, 'a'), (2, 'b');")
echo "$out" | grep -q "2 rows affected" && ok "insert 2 rows on a" || bad "insert on a: $out"

echo "== 2. writes on a replicate to b and c =="
wait_row "$B" "SELECT id FROM nodes WHERE id = 1;" "^[[:space:]]*1[[:space:]]*$" && ok "b sees a's write" || bad "b missing a's row"
wait_row "$C" "SELECT id FROM nodes WHERE id = 2;" "^[[:space:]]*2[[:space:]]*$" && ok "c sees a's write" || bad "c missing a's row"

echo "== 3. node-b accepts writes (no primary role) =="
out=$(sql "$B" "INSERT INTO nodes VALUES (3, 'b-write');")
echo "$out" | grep -q "1 rows affected" && ok "write on b accepted" || bad "write on b rejected: $out"
wait_row "$A" "SELECT id FROM nodes WHERE id = 3;" "^[[:space:]]*3[[:space:]]*$" && ok "a sees b's write" || bad "a missing b's row"
wait_row "$C" "SELECT id FROM nodes WHERE id = 3;" "^[[:space:]]*3[[:space:]]*$" && ok "c sees b's write" || bad "c missing b's row"

echo "== 4. node-c accepts writes (every node equal) =="
out=$(sql "$C" "INSERT INTO nodes VALUES (4, 'c-write');")
echo "$out" | grep -q "1 rows affected" && ok "write on c accepted" || bad "write on c rejected: $out"
wait_row "$A" "SELECT id FROM nodes WHERE id = 4;" "^[[:space:]]*4[[:space:]]*$" && ok "a sees c's write" || bad "a missing c's row"
wait_row "$B" "SELECT id FROM nodes WHERE id = 4;" "^[[:space:]]*4[[:space:]]*$" && ok "b sees c's write" || bad "b missing c's row"

echo "== 5. KV replication =="
out=$(kv "$B" SET cluster:greeting deployed)
[ "$out" = "ok" ] && ok "kv SET on b" || bad "kv SET on b: $out"
found=""
for i in $(seq 1 40); do
  found=$(kv "$A" GET cluster:greeting)
  [ "$found" = "deployed" ] && break
  sleep 0.5
done
[ "$found" = "deployed" ] && ok "kv replicated b -> a" || bad "kv replication b->a: '$found'"
found=""
for i in $(seq 1 40); do
  found=$(kv "$C" GET cluster:greeting)
  [ "$found" = "deployed" ] && break
  sleep 0.5
done
[ "$found" = "deployed" ] && ok "kv replicated b -> c" || bad "kv replication b->c: '$found'"

echo "== 6. pub/sub =="
# (push delivery is on the TCP port; validated in the Rust e2e suite — here we
# verify PUBLISH returns a subscriber count of 0 without one connected.)
out=$(kv "$A" PUBLISH ops "hello-nodes")
echo "$out" | grep -qE "^[0-9]+$" && ok "publish returns count" || bad "publish: $out"

echo "== 7. transactions over the wire (node-b) =="
before=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
sql "$B" "BEGIN;" >/dev/null
sql "$B" "INSERT INTO nodes VALUES (10, 'tx');" >/dev/null
sql "$B" "ROLLBACK;" >/dev/null
after=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$before" = "$after" ] && ok "rollback over network (count $before == $after)" || bad "tx rollback: $before != $after"

echo "== 8. convergence: all nodes agree =="
ca=$(sql "$A" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
cb=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
cc=$(sql "$C" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$ca" = "$cb" ] && [ "$cb" = "$cc" ] && ok "row count converged: a=$ca b=$cb c=$cc" || bad "divergence: a=$ca b=$cb c=$cc"

echo "== 9. web console in docker =="
W="http://127.0.0.1:17700"
# 页面
body=$(curl -s "$W/")
echo "$body" | grep -q "docsql console" && ok "web UI served" || bad "web UI: $(echo "$body" | head -c 80)"
# SQL 执行
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"CREATE TABLE web_check (id INT PRIMARY KEY, note TEXT)"}')
echo "$r" | grep -q '"affected"' && ok "web sql create" || bad "web sql create: $r"
r=$(curl -s -X POST "$W/api/sql" -H "Content-Type: application/json" -d "{\"sql\":\"INSERT INTO web_check VALUES (1, 'fromweb')\"}")
echo "$r" | grep -q '"count":1' && ok "web sql insert" || bad "web sql insert: $r"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT id, note FROM web_check"}')
echo "$r" | grep -q '"fromweb"' && ok "web sql select" || bad "web sql select: $r"
# SQL 错误返回错误而非崩溃
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT * FROM missing"}')
echo "$r" | grep -q '"error"' && ok "web sql error surfaced" || bad "web sql error: $r"
# KV + 键浏览
curl -s -X POST "$W/api/kv" -H 'Content-Type: application/json' -d '{"command":"SET","args":["web:key","ok"]}' >/dev/null
r=$(curl -s "$W/api/keys")
echo "$r" | grep -q '"web:key"' && ok "web kv set + keys browser" || bad "web keys: $r"
# 统计
r=$(curl -s "$W/api/stats")
echo "$r" | grep -q '"kv_keys":1' && ok "web stats" || bad "web stats: $r"
# 认证开关(DOCSQL_TOKEN 未设置时应放行;此处仅验证无 token 可访问)
code=$(curl -s -o /dev/null -w "%{http_code}" "$W/api/stats")
[ "$code" = "200" ] && ok "web no-auth mode accessible" || bad "web http code: $code"

echo
echo "RESULT: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
