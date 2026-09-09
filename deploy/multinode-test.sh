#!/bin/bash
# Multi-node deployment test against the docker-compose symmetric cluster.
# Nodes: three equal peers — node-a (:17601), node-b (:17602), node-c (:17603);
# chapter 12 adds node-d (:17604) via the join profile and verifies the
# automatic full-state bootstrap.
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
# 事务必须在同一会话内执行:BEGIN 的连接拥有该事务,连接断开即回滚,
# 外来连接的写也不会并入它。
sqltx() { local a="$1"; shift; { printf '%s\n' "$@"; echo "exit;"; } | docker exec -i "$(ctr_of "$a")" docsql-cli connect "$a" 2>/dev/null; }

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

# absent <node> <sql> <ERE pattern>: poll and require the query to keep
# returning no match — for writes that will never arrive (no catch-up).
absent() {
  for _ in $(seq 1 6); do
    out=$(sql "$1" "$2")
    echo "$out" | grep -qE "$3" && return 1
    sleep 0.5
  done
  return 0
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

echo "== 5. transactions over the wire (node-b) =="
before=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
sqltx "$B" "BEGIN;" "INSERT INTO nodes VALUES (10, 'tx');" "ROLLBACK;" >/dev/null
after=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$before" = "$after" ] && ok "rollback over network (count $before == $after)" || bad "tx rollback: $before != $after"

echo "== 6. convergence: all nodes agree =="
ca=$(sql "$A" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
cb=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
cc=$(sql "$C" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$ca" = "$cb" ] && [ "$cb" = "$cc" ] && ok "row count converged: a=$ca b=$cb c=$cc" || bad "divergence: a=$ca b=$cb c=$cc"

echo "== 7. web console in docker =="
W="http://127.0.0.1:17700"
# 页面
body=$(curl -s "$W/")
echo "$body" | grep -q "DocSQL console" && ok "web UI served" || bad "web UI: $(echo "$body" | head -c 80)"
# SQL 执行
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"CREATE TABLE web_check (id INT PRIMARY KEY, note TEXT)"}')
echo "$r" | grep -q '"affected"' && ok "web sql create" || bad "web sql create: $r"
r=$(curl -s -X POST "$W/api/sql" -H "Content-Type: application/json" -d "{\"sql\":\"INSERT INTO web_check VALUES (1, 'fromweb')\"}")
echo "$r" | grep -q '"count":1' && ok "web sql insert" || bad "web sql insert: $r"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT id, note FROM web_check"}')
echo "$r" | grep -q '"fromweb"' && ok "web sql select" || bad "web sql select: $r"
# 控制台不落库:web 的默认写入落在管理目标 node-a,并按集群拓扑扇出收敛。
out=$(sql "$A" "SELECT note FROM web_check WHERE id = 1;")
echo "$out" | grep -q "fromweb" && ok "web writes land on managed node-a" || bad "web data not on node-a: $out"
wait_row "$C" "SELECT COUNT(id) FROM web_check;" "^[[:space:]]*1[[:space:]]*$" \
  && ok "web-created data converged cluster-wide" || bad "web data not converged on node-c"
# SQL 错误返回错误而非崩溃
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT * FROM missing"}')
echo "$r" | grep -q '"error"' && ok "web sql error surfaced" || bad "web sql error: $r"
# 统计
r=$(curl -s "$W/api/stats")
echo "$r" | grep -q '"tables"' && ok "web stats" || bad "web stats: $r"
# 集群状态页:web 按 DOCSQL_PEERS 探测三个节点并取回状态报告
r=$(curl -s "$W/api/cluster")
echo "$r" | grep -q '"node-a:7600"' && ok "web cluster lists node-a" || bad "web cluster nodes: $r"
echo "$r" | grep -q '"reachable":true' && echo "$r" | grep -q '"durable_lsn"' && ok "web cluster probes reachable nodes with status" || bad "web cluster probe: $r"
# 认证开关(DOCSQL_TOKEN 未设置时应放行;此处仅验证无 token 可访问)
code=$(curl -s -o /dev/null -w "%{http_code}" "$W/api/stats")
[ "$code" = "200" ] && ok "web no-auth mode accessible" || bad "web http code: $code"
# 节点切换:控制台以客户端身份连接指定 DOCSQL_PEERS 节点执行(REQ_AUTH + REQ_SQL)
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"node":"node-a:7600","sql":"CREATE TABLE via_proxy (id INT PRIMARY KEY, src TEXT)"}')
echo "$r" | grep -q '"affected"' && ok "node switch: create on node-a" || bad "switch create: $r"
# 多语句批次逐句发送,批次形状与默认目标一致
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d "{\"node\":\"node-b:7600\",\"sql\":\"INSERT INTO via_proxy VALUES (1, 'from-b'); INSERT INTO via_proxy VALUES (2, 'also-b');\"}")
echo "$r" | grep -q '"kind":"batch"' && echo "$r" | grep -q '"count":1' && ok "node switch: batch insert on node-b" || bad "switch batch: $r"
# 写在 b 上执行并按其集群配置扇出;从 c 经控制台读回(同步扇出,立即可见)
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"node":"node-c:7600","sql":"SELECT COUNT(*) FROM via_proxy"}')
echo "$r" | grep -q '"rows":\[\[2\]\]' && ok "node switch: read from node-c (converged)" || bad "switch read: $r"
# 远程 meta 与默认目标 /api/meta 同构(REQ_META 共享同一组装代码)
r=$(curl -s "$W/api/meta?node=node-a:7600")
echo "$r" | grep -q '"via_proxy"' && echo "$r" | grep -q '"index_defs"' && ok "node switch: remote meta same shape" || bad "switch meta: $r"
r=$(curl -s "$W/api/stats?node=node-a:7600")
echo "$r" | grep -q '"uptime_ms"' && ok "node switch: remote stats" || bad "switch stats: $r"
# 未配置的节点地址必须拒绝(可连接目标仅限 DOCSQL_PEERS)
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"node":"10.0.0.1:7600","sql":"SELECT 1"}')
echo "$r" | grep -q '"error"' && ok "node switch: unconfigured node rejected" || bad "switch guard: $r"

echo "== 8. persistent pub/sub across nodes =="
# 8.1 实时投递:node-a 后台订阅(输出落宿主临时文件),node-b 发布。
ch="ops-$(date +%s)"
tmp=$(mktemp)
( printf "subscribe %s latest;\n" "$ch"; sleep 4; printf "exit;\n" ) \
  | docker exec -i docsql-a docsql-cli connect "$A" >"$tmp" 2>/dev/null &
sub=$!
# 轮询等订阅确认(docker exec 冷启动可能 >1s,固定 sleep 会抢跑)。
for _ in $(seq 1 40); do
  grep -q "subscribed" "$tmp" && break
  sleep 0.5
done
out=$(printf "publish %s hello-from-b;\nexit;\n" "$ch" | docker exec -i docsql-b docsql-cli connect "$B" 2>/dev/null)
wait $sub
# publish 回 [id, receivers] 表格;receivers 是行尾单元格,断言 = 1。
echo "$out" | grep -qE "\|[[:space:]]*1[[:space:]]*$" && ok "publish on b reports 1 live receiver" || bad "publish receivers: $out"
grep -q "\[pubsub\] message $ch #" "$tmp" && grep -q "hello-from-b" "$tmp" \
  && ok "a receives b's publish live" || bad "live push: $(cat "$tmp")"

# 8.2 复制的消息在第三节点也持久化(docsql_pubsub 视图)。
n=$(sql "$C" "SELECT COUNT(*) FROM docsql_pubsub;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ -n "$n" ] && [ "$n" -ge 1 ] && ok "c persisted the replicated message (count=$n)" || bad "c pubsub store count: '$n'"

# 8.3 重启 node-a 后 from=earliest 仍能回放(WAL 持久化)。
docker restart docsql-a >/dev/null
up=""
for _ in $(seq 1 40); do
  sql "$A" "SELECT 1;" >/dev/null 2>&1 && { up=1; break; }
  sleep 0.5
done
if [ -n "$up" ]; then
  tmp2=$(mktemp)
  ( printf "subscribe %s earliest;\n" "$ch"; sleep 2; printf "exit;\n" ) \
    | docker exec -i docsql-a docsql-cli connect "$A" >"$tmp2" 2>/dev/null
  # 回放帧紧随确认;等回放内容到齐再断言。
  for _ in $(seq 1 20); do
    grep -q "hello-from-b" "$tmp2" && break
    sleep 0.5
  done
  grep -q "hello-from-b" "$tmp2" && ok "pubsub history replays after restart" || bad "replay after restart: $(cat "$tmp2")"
  rm -f "$tmp2"
else
  bad "node-a did not come back after restart"
fi
rm -f "$tmp"

echo "== 9. node offline during writes, then back online =="
# 9.1 停掉 node-c:离线期间 a/b 的写必须照常成功(fan-out 是 fire-and-forget,
#     死 peer 只打日志,不失败、不挂起本地写路径)。
docker stop docsql-c >/dev/null
out=$(sql "$A" "INSERT INTO nodes VALUES (20, 'during-outage');")
echo "$out" | grep -q "1 rows affected" && ok "write on a succeeds while c is offline" || bad "write during c outage: $out"

# 9.2 b 上的事务在离线期间照常提交(缓冲写在 COMMIT 时扇出)。
sqltx "$B" "BEGIN;" "INSERT INTO nodes VALUES (21, 'tx-outage');" "COMMIT;" >/dev/null
wait_row "$A" "SELECT id FROM nodes WHERE id = 21;" "^[[:space:]]*21[[:space:]]*$" \
  && ok "tx on b commits during outage (a sees id=21)" || bad "outage tx missing on a"

# 9.3 c 重新上线,等协议端口就绪。
docker start docsql-c >/dev/null
up=""
for _ in $(seq 1 40); do
  sql "$C" "SELECT 1;" >/dev/null 2>&1 && { up=1; break; }
  sleep 0.5
done
[ -n "$up" ] && ok "c back online after restart" || bad "c did not come back"

# 9.4 c 保留离线前的数据(重启只掉内存,盘上状态完好)。
out=$(sql "$C" "SELECT host FROM nodes WHERE id = 4;")
echo "$out" | grep -q "c-write" && ok "c retains pre-outage data (id=4)" || bad "c lost pre-outage data: $out"

# 9.5 反熵修复(重启加入时触发):c 起来后对比各 peer 的表摘要,发现分歧
#     即经 hold 冻结→快照→单事务清空重放,整体采纳集群状态——离线期间
#     a/b 的增删改自动补齐(旧行为「无追赶,永久丢失」已由修复取代)。
wait_row "$C" "SELECT id FROM nodes WHERE id = 20;" "^[[:space:]]*20[[:space:]]*$" \
  && ok "c auto-caught-up outage write id=20 (rejoin repair)" || bad "c did not catch up id=20"
wait_row "$C" "SELECT id FROM nodes WHERE id = 21;" "^[[:space:]]*21[[:space:]]*$" \
  && ok "c auto-caught-up outage tx id=21 (rejoin repair)" || bad "c did not catch up id=21"
# CLI 输出带连接横幅(含端口数字),计数只认「整行纯数字」的结果行。
cnt_a=$(sql "$A" "SELECT COUNT(id) FROM nodes;" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]')
cnt_b=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]')
cnt_c=$(sql "$C" "SELECT COUNT(id) FROM nodes;" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]')
[ -n "$cnt_a" ] && [ "$cnt_a" = "$cnt_b" ] && [ "$cnt_b" = "$cnt_c" ] \
  && ok "a/b/c row counts converged after repair ($cnt_a)" \
  || bad "post-repair divergence: a=$cnt_a b=$cnt_b c=$cnt_c"

# 9.6 恢复后 a 的新写重新到达 c(每次写新建连接,peer 可达即恢复)。
out=$(sql "$A" "INSERT INTO nodes VALUES (22, 'after-recovery');")
wait_row "$C" "SELECT id FROM nodes WHERE id = 22;" "^[[:space:]]*22[[:space:]]*$" \
  && ok "post-recovery write on a reaches c" || bad "c missed post-recovery write id=22"

# 9.7 c 恢复为全功能对等点:自己也能写并扇出给 a/b。
out=$(sql "$C" "INSERT INTO nodes VALUES (23, 'c-rejoined');")
wait_row "$A" "SELECT id FROM nodes WHERE id = 23;" "^[[:space:]]*23[[:space:]]*$" \
  && wait_row "$B" "SELECT id FROM nodes WHERE id = 23;" "^[[:space:]]*23[[:space:]]*$" \
  && ok "rejoined c's write reaches a and b" || bad "rejoined c's write did not fan out"

echo "== 10. partition: both sides accept writes, divergence until a restart heals it =="
# 分区周期后 c 的自名解析(容器内解析 node-c)可能持续损坏,直到 compose 网络
# 被重建(down -v)才恢复——分区测试中所有 c 侧交互一律走 127.0.0.1 回环。
sqlc() { printf "%s\nexit;\n" "$1" | docker exec -i docsql-c docsql-cli connect 127.0.0.1:7600 2>/dev/null; }
wait_row_c() {
  local out=""
  for _ in $(seq 1 40); do
    out=$(sqlc "$1")
    echo "$out" | grep -qE "$2" && break
    sleep 0.5
  done
  echo "$out" | grep -qE "$2"
}
absent_c() {
  for _ in $(seq 1 6); do
    out=$(sqlc "$1")
    echo "$out" | grep -qE "$2" && return 1
    sleep 0.5
  done
  return 0
}

# 10.1 把 c 从 compose 网络摘掉(容器还活着):分区期间 a 写成功。
net=$(docker inspect docsql-c --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker network disconnect "$net" docsql-c
out=$(sql "$A" "INSERT INTO nodes VALUES (30, 'partition-a');")
echo "$out" | grep -q "1 rows affected" && ok "write on a succeeds during partition" || bad "write during partition: $out"

# 10.2 分区期间 c 仍运行且本地可写(回环连接;扇出失败不影响本地提交)。
out=$(sqlc "INSERT INTO nodes VALUES (31, 'partition-c');")
echo "$out" | grep -q "1 rows affected" && ok "c accepts local writes while partitioned" || bad "partitioned c rejected write: $out"

# 10.3 分区愈合:重连时用 --alias 显式重新注册服务名。实测(Docker Desktop)
#      裸 docker network connect 重连后 node-c 别名不再注册进嵌入 DNS,
#      a→node-c 与 c 自名解析都失效,且 docker restart 也不恢复;--alias
#      重连即刻恢复双向解析。服务器进程未停,数据无涉。
docker network connect --alias node-c "$net" docsql-c
up=""
for _ in $(seq 1 40); do
  docker exec docsql-a getent hosts node-c >/dev/null 2>&1 && { up=1; break; }
  sleep 0.5
done
[ -n "$up" ] && ok "c reachable by name after rejoin (--alias reconnect)" || bad "c unreachable after rejoin"
wait_row "$A" "SELECT id FROM nodes WHERE id = 30;" "^[[:space:]]*30[[:space:]]*$" \
  && wait_row "$B" "SELECT id FROM nodes WHERE id = 30;" "^[[:space:]]*30[[:space:]]*$" \
  && ok "a and b hold a's partition write (id=30)" || bad "partition write id=30 missing on a/b"
wait_row_c "SELECT id FROM nodes WHERE id = 31;" "^[[:space:]]*31[[:space:]]*$" \
  && ok "c holds its own partition write (id=31)" || bad "c lost its own partition write"
absent "$A" "SELECT id FROM nodes WHERE id = 31;" "^[[:space:]]*31[[:space:]]*$" \
  && absent "$B" "SELECT id FROM nodes WHERE id = 31;" "^[[:space:]]*31[[:space:]]*$" \
  && ok "a and b permanently miss c's partition write (silent divergence)" || bad "partition write leaked across the partition"
absent_c "SELECT id FROM nodes WHERE id = 30;" "^[[:space:]]*30[[:space:]]*$" \
  && ok "c permanently misses a's partition write (silent divergence)" || bad "partition write leaked across the partition"

# 10.4 重归队后新写恢复全扇出(c 侧经回环验证)。
out=$(sql "$A" "INSERT INTO nodes VALUES (32, 'after-rejoin');")
wait_row "$B" "SELECT id FROM nodes WHERE id = 32;" "^[[:space:]]*32[[:space:]]*$" \
  && wait_row_c "SELECT id FROM nodes WHERE id = 32;" "^[[:space:]]*32[[:space:]]*$" \
  && ok "post-rejoin write reaches all nodes" || bad "post-rejoin write did not fan out"

# 10.5 反熵修复的触发点是重启:仅重连(10.3)不修,重启才对齐。c 重启后
#     对比 a/b(双方一致,构成多数方)并整体采纳其快照:c 分区期间的独有
#     写 id=31 被多数方状态覆盖(策略:无行级合并,少数方独有写不保留),
#     全网恢复一致。
docker restart docsql-c >/dev/null
up=""
for _ in $(seq 1 40); do
  sqlc "SELECT 1;" >/dev/null 2>&1 && { up=1; break; }
  sleep 0.5
done
[ -n "$up" ] && ok "c back online after partition-repair restart" || bad "c did not come back after restart"
wait_row_c "SELECT id FROM nodes WHERE id = 30;" "^[[:space:]]*30[[:space:]]*$" \
  && ok "c adopts majority state (id=30) on restart" || bad "c did not adopt majority id=30"
absent_c "SELECT id FROM nodes WHERE id = 31;" "^[[:space:]]*31[[:space:]]*$" \
  && ok "minority-only partition write id=31 overwritten by majority (documented policy)" \
  || bad "id=31 unexpectedly survived the repair"
cnt_a=$(sql "$A" "SELECT COUNT(id) FROM nodes;" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]')
cnt_b=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]')
cnt_c=$(sqlc "SELECT COUNT(id) FROM nodes;" | grep -E '^[[:space:]]*[0-9]+[[:space:]]*$' | head -1 | tr -d '[:space:]')
[ -n "$cnt_a" ] && [ "$cnt_a" = "$cnt_b" ] && [ "$cnt_b" = "$cnt_c" ] \
  && ok "cluster fully converged after partition repair ($cnt_a rows)" \
  || bad "post-partition-repair divergence: a=$cnt_a b=$cnt_b c=$cnt_c"

echo "== 11. auto-GUID primary keys converge across nodes =="
# 随机 GUID 不能像 INT AUTOINCREMENT 的 max+1 那样在对端确定性重算:
# 写入节点必须把生成的 id 定值下发给对等节点(引擎回写显式值的 INSERT)。
# UUIDv7 形态:小写十六进制,版本位 '7',变体位 8/9/a/b。
UUID_RE='[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
out=$(sql "$A" "CREATE TABLE gdocs (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT);")
echo "$out" | grep -q "rows affected" && ok "create guid table on a" || bad "create guid table on a: $out"
out=$(sql "$A" "INSERT INTO gdocs (v) VALUES ('origin');")
echo "$out" | grep -q "1 rows affected" && ok "insert without id on a" || bad "guid insert on a: $out"
GID=$(sql "$A" "SELECT id FROM gdocs WHERE v = 'origin';" | grep -oE "$UUID_RE" | head -1)
[ -n "$GID" ] && ok "generated UUIDv7 on a ($GID)" || bad "no UUIDv7 on a: $out"
# 对等节点拿到完全相同的 id(而非各自重新生成)
wait_row "$B" "SELECT COUNT(*) FROM gdocs WHERE id = '$GID';" "^[[:space:]]*1[[:space:]]*$" \
  && ok "b holds a's exact guid" || bad "b missing a's guid"
wait_row "$C" "SELECT COUNT(*) FROM gdocs WHERE id = '$GID';" "^[[:space:]]*1[[:space:]]*$" \
  && ok "c holds a's exact guid" || bad "c missing a's guid"
# 事务内从另一节点写(NULL id),提交后全网收敛且总行数一致
# (事务归 BEGIN 它的连接所有,语句经同一会话顺序执行)
out=$(sqltx "$B" "BEGIN;" "INSERT INTO gdocs (id, v) VALUES (NULL, 'tx');" "COMMIT;")
echo "$out" | grep -q "1 rows affected" && ok "guid insert in tx on b" || bad "guid tx insert: $out"
wait_row "$A" "SELECT COUNT(*) FROM gdocs;" "^[[:space:]]*2[[:space:]]*$" \
  && wait_row "$C" "SELECT COUNT(*) FROM gdocs;" "^[[:space:]]*2[[:space:]]*$" \
  && ok "transactional guid write from b converges (2 rows everywhere)" \
  || bad "guid tx write did not converge"
out=$(sql "$B" "SELECT id FROM gdocs WHERE v = 'tx';")
GID2=$(echo "$out" | grep -oE "$UUID_RE" | head -1)
[ -n "$GID2" ] && [ "$GID2" != "$GID" ] && ok "b's generated guid differs ($GID2)" || bad "b's guid missing or duplicate: $out"

echo "== 12. joining node bootstraps the full cluster state automatically =="
# 12.1 起一个全新的第四节点(join profile):指向 a/b/c 并通告自身地址。
#      卷必须干净——非空节点不参与 bootstrap(保留本地数据),这里显式
#      重建保证幂等。dev compose(默认文件)从仓库 deploy/ 目录起。
D="node-d:7600"
ctr_of_d() { echo docsql-d; }
sqld() { printf "%s\nexit;\n" "$1" | docker exec -i "$(ctr_of_d)" docsql-cli connect "$D" 2>/dev/null; }
wait_row_d() {
  local out=""
  for _ in $(seq 1 60); do
    out=$(sqld "$1")
    echo "$out" | grep -qE "$2" && break
    sleep 0.5
  done
  echo "$out" | grep -qE "$2"
}
( cd "$(dirname "$0")" && docker compose --profile cluster --profile join stop node-d >/dev/null 2>&1 || true
  docker compose --profile cluster --profile join rm -f node-d >/dev/null 2>&1 || true
  docker volume rm -f docsql-dev-data-d >/dev/null 2>&1 || true
  docker volume create docsql-dev-data-d >/dev/null
  DOCSQL_DEV_IMAGE_TAG="${DOCSQL_DEV_IMAGE_TAG:-local}" docker compose --profile cluster --profile join up -d node-d >/dev/null )
ok_d=""
for _ in $(seq 1 60); do
  sqld "SELECT 1;" >/dev/null 2>&1 && { ok_d=1; break; }
  sleep 0.5
done
[ -n "$ok_d" ] && ok "node-d (join profile) came up" || bad "node-d did not come up"

# 12.2 历史数据自动到达:第 1-11 章写过的表和行(nodes、gdocs、via_proxy)
#      无需任何手动步骤全部同步到 d。
wait_row_d "SELECT COUNT(id) FROM nodes;" "^[[:space:]]*[0-9]+[[:space:]]*$" \
  && ok "d bootstrapped historical table nodes" || bad "d missing nodes table"
wait_row_d "SELECT COUNT(*) FROM gdocs;" "^[[:space:]]*2[[:space:]]*$" \
  && ok "d bootstrapped gdocs incl. exact GUID values" || bad "d missing gdocs rows"
# 约束形状一并到达:UNIQUE 仍在 d 上强制执行。
out=$(sqld "INSERT INTO nodes VALUES (1, 'dup-on-d');")
echo "$out" | grep -qi "error\|duplicate\|unique" && ok "d enforces UNIQUE after bootstrap" || bad "d lost UNIQUE: $out"
# auto-GUID 的 id 与集群完全一致(第 11 章生成的 GID)。
[ -n "${GID:-}" ] && wait_row_d "SELECT COUNT(*) FROM gdocs WHERE id = '$GID';" "^[[:space:]]*1[[:space:]]*$" \
  && ok "d holds the cluster's exact auto-GUID" || bad "d's guid diverged for $GID"

# 12.3 动态注册:a/b/c 把 d 当作对等节点(后续写扇出到 d)。
out=$(sql "$A" "INSERT INTO nodes VALUES (40, 'after-join-a');")
echo "$out" | grep -q "1 rows affected" && ok "write on a after d joined" || bad "write on a after join: $out"
wait_row_d "SELECT id FROM nodes WHERE id = 40;" "^[[:space:]]*40[[:space:]]*$" \
  && ok "a's post-join write reaches d" || bad "d missed a's post-join write"

# 12.4 d 是全功能对等点:自己写并扇出给 a/b/c。
out=$(sqld "INSERT INTO nodes VALUES (41, 'from-d');")
echo "$out" | grep -q "1 rows affected" && ok "write on d accepted" || bad "write on d rejected: $out"
wait_row "$A" "SELECT id FROM nodes WHERE id = 41;" "^[[:space:]]*41[[:space:]]*$" \
  && ok "d's write reaches a" || bad "a missing d's write"
wait_row "$B" "SELECT id FROM nodes WHERE id = 41;" "^[[:space:]]*41[[:space:]]*$" \
  && ok "d's write reaches b" || bad "b missing d's write"
wait_row_c "SELECT id FROM nodes WHERE id = 41;" "^[[:space:]]*41[[:space:]]*$" \
  && ok "d's write reaches c" || bad "c missing d's write"

# 12.5 收敛:d 与 a/b 行数一致(c 因第 9-10 章刻意的离线/分区分歧不在
#      等值断言内,这正是无反熵追赶的既有特征)。d 的历史来自 a 的快照
#      加加入后的全网写,故 a=b=d。
ca=$(sql "$A" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
cb=$(sql "$B" "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
cd_=$(sqld "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$ca" = "$cb" ] && [ "$cb" = "$cd_" ] \
  && ok "joined mesh converged: a=$ca b=$cb d=$cd_" || bad "divergence: a=$ca b=$cb d=$cd_"

# 12.6 d 重启后数据保留(不再 bootstrap,直接带数据服务)。
docker restart docsql-d >/dev/null
up=""
for _ in $(seq 1 40); do
  sqld "SELECT 1;" >/dev/null 2>&1 && { up=1; break; }
  sleep 0.5
done
if [ -n "$up" ]; then
  out=$(sqld "SELECT COUNT(id) FROM nodes;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
  [ "$out" = "$cd_" ] && ok "d retains synced data across restart ($out rows)" || bad "d lost data on restart: $out != $cd_"
  out=$(sql "$A" "INSERT INTO nodes VALUES (42, 'post-d-restart');")
  wait_row_d "SELECT id FROM nodes WHERE id = 42;" "^[[:space:]]*42[[:space:]]*$" \
    && ok "writes reach restarted d" || bad "restarted d missed new write"
else
  bad "node-d did not come back after restart"
fi

echo
echo "RESULT: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
