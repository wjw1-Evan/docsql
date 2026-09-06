#!/bin/bash
# Multi-node deployment test against the docker-compose cluster.
# Nodes: primary (:17601), read-only replica (:17602), independent shard (:17603).
set -u
CLI="./target/release/docsql-cli"
P="127.0.0.1:17601"
R="127.0.0.1:17602"
S="127.0.0.1:17603"
PASS=0; FAIL=0
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

sql() { printf "%s\nexit;\n" "$2" | $CLI connect "$1" 2>/dev/null; }
kv()  { $CLI kv "$1" "${@:2}" 2>/dev/null; }

echo "== 1. primary accepts SQL writes =="
out=$(sql "$P" "CREATE TABLE nodes (id INT PRIMARY KEY, host TEXT);")
echo "$out" | grep -q "rows affected" && ok "create table" || bad "create table: $out"
out=$(sql "$P" "INSERT INTO nodes VALUES (1, 'primary'), (2, 'replica');")
echo "$out" | grep -q "2 rows affected" && ok "insert 2 rows" || bad "insert: $out"

echo "== 2. replication to read-only replica =="
for i in $(seq 1 40); do
  out=$(sql "$R" "SELECT id FROM nodes WHERE id = 1;")
  echo "$out" | grep -qE "^[[:space:]]*1[[:space:]]*$" && break
  sleep 0.5
done
echo "$out" | grep -qE "^[[:space:]]*1[[:space:]]*$" && ok "replica sees replicated row" || bad "replica missing row: $out"
out=$(sql "$R" "SELECT COUNT(id) FROM nodes;")
echo "$out" | grep -q " 2 " && ok "replica row count = 2" || bad "replica count: $out"

echo "== 3. replica rejects client writes (read-only) =="
out=$(sql "$R" "INSERT INTO nodes VALUES (3, 'nope');" 2>&1)
echo "$out" | grep -q "read-only" && ok "read-only enforced" || bad "write not rejected: $out"

echo "== 4. KV replication =="
out=$(kv "$P" SET cluster:greeting deployed)
[ "$out" = "ok" ] && ok "kv SET on primary" || bad "kv SET: $out"
found=""
for i in $(seq 1 40); do
  found=$(kv "$R" GET cluster:greeting)
  [ "$found" = "deployed" ] && break
  sleep 0.5
done
[ "$found" = "deployed" ] && ok "kv replicated to replica" || bad "kv replication: '$found'"

echo "== 5. pub/sub between clients on primary =="
# (push delivery is on the TCP port; validated in the Rust e2e suite — here we
# verify PUBLISH returns a subscriber count of 0 without one connected.)
out=$(kv "$P" PUBLISH ops "hello-nodes")
echo "$out" | grep -qE "^[0-9]+$" && ok "publish returns count" || bad "publish: $out"

echo "== 6. failover: promote replica =="
out=$(kv "$R" PROMOTE)
[ "$out" = "promoted" ] && ok "promote accepted" || bad "promote: $out"
out=$(sql "$R" "INSERT INTO nodes VALUES (3, 'promoted-write');")
echo "$out" | grep -q "1 rows affected" && ok "promoted replica accepts writes" || bad "post-promote write: $out"
out=$(sql "$R" "SELECT COUNT(id) FROM nodes;")
echo "$out" | grep -q " 3 " && ok "replica row count after failover = 3" || bad "post count: $out"

echo "== 7. independent shard isolation =="
out=$(sql "$S" "CREATE TABLE shard_local (k TEXT);")
echo "$out" | grep -q "rows affected" && ok "shard create" || bad "shard create: $out"
out=$(sql "$S" "INSERT INTO shard_local VALUES ('b-only');")
echo "$out" | grep -q "1 rows affected" && ok "shard write" || bad "shard write: $out"
out=$(sql "$S" "SELECT k FROM shard_local;")
echo "$out" | grep -q "b-only" && ok "shard read" || bad "shard read: $out"
out=$(sql "$R" "SELECT k FROM shard_local;" 2>&1)
echo "$out" | grep -qi "does not exist" && ok "shard data isolated from primary/replica" || bad "leak across shard: $out"

echo "== 8. transactions over the wire (primary) =="
before=$(sql "$P" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
sql "$P" "BEGIN;" >/dev/null
sql "$P" "INSERT INTO nodes VALUES (10, 'tx');" >/dev/null
sql "$P" "ROLLBACK;" >/dev/null
after=$(sql "$P" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$before" = "$after" ] && ok "rollback over network (count $before == $after)" || bad "tx rollback: $before != $after"

echo
echo "RESULT: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
