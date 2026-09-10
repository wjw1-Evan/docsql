#!/bin/bash
# Single-node deployment test against the compose `single` profile.
# Nodes: one standalone node — node-single (:17600), web console (:17710).
# No peers configured on the node: reads and writes stay local. The web
# console manages node-single over the wire protocol (no side store), and
# its DOCSQL_PEERS entry feeds the cluster-status page. When the cluster
# profile is also up (node-a), additionally verifies data isolation between
# the standalone node and the cluster.
set -u
# All traffic runs inside the compose network via the image's own CLI
# (no host toolchain needed); host ports stay mapped for external access.
A="node-single:7600"
CTR="docsql-single"
PASS=0; FAIL=0
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

sql() { printf "%s\nexit;\n" "$2" | docker exec -i "$CTR" docsql-cli connect "$1" 2>/dev/null; }
# 事务必须在同一会话内执行:BEGIN 的连接拥有该事务,连接断开即回滚,
# 外来连接的写也不会并入它。
sqltx() { local a="$1"; shift; { printf '%s\n' "$@"; echo "exit;"; } | docker exec -i "$CTR" docsql-cli connect "$a" 2>/dev/null; }

echo "== 1. standalone SQL =="
out=$(sql "$A" "CREATE TABLE solo (id INT PRIMARY KEY, tag TEXT);")
echo "$out" | grep -q "rows affected" && ok "create table" || bad "create table: $out"
out=$(sql "$A" "INSERT INTO solo VALUES (1, 'one'), (2, 'two');")
echo "$out" | grep -q "2 rows affected" && ok "insert 2 rows" || bad "insert: $out"
out=$(sql "$A" "SELECT id, tag FROM solo WHERE id = 1;")
echo "$out" | grep -q "one" && ok "select row back" || bad "select: $out"
out=$(sql "$A" "SELECT COUNT(id) FROM solo;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$out" = "2" ] && ok "count = 2" || bad "count: '$out'"

echo "== 2. transactions (local) =="
before=$(sql "$A" "SELECT COUNT(id) FROM solo;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
sqltx "$A" "BEGIN;" "INSERT INTO solo VALUES (9, 'tx');" "ROLLBACK;" >/dev/null
after=$(sql "$A" "SELECT COUNT(id) FROM solo;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
[ "$before" = "$after" ] && ok "rollback (count $before == $after)" || bad "rollback: $before != $after"

echo "== 3. persistence across container restart =="
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

echo "== 4. web console (:17710) =="
W="http://127.0.0.1:17710"
body=$(curl -s "$W/")
echo "$body" | grep -q "DocSQL console" && ok "web UI served" || bad "web UI: $(echo "$body" | head -c 80)"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"CREATE TABLE web_solo (id INT PRIMARY KEY, note TEXT)"}')
echo "$r" | grep -q '"affected"' && ok "web sql create" || bad "web sql create: $r"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"INSERT INTO web_solo VALUES (1, '\''fromweb'\'')"}')
echo "$r" | grep -q '"count":1' && ok "web sql insert" || bad "web sql insert: $r"
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT id, note FROM web_solo"}')
echo "$r" | grep -q '"fromweb"' && ok "web sql select" || bad "web sql select: $r"
r=$(curl -s "$W/api/stats")
echo "$r" | grep -q '"tables"' && ok "web stats" || bad "web stats: $r"
r=$(curl -s "$W/api/cluster")
echo "$r" | grep -q '"node-single:7600"' && echo "$r" | grep -q '"reachable":true' \
  && ok "web cluster page probes node-single" || bad "web cluster: $r"
# 控制台是管理工具、不落库:web 写入的数据必须从节点原生端口可见。
out=$(sql "$A" "SELECT note FROM web_solo WHERE id = 1;")
echo "$out" | grep -q "fromweb" && ok "web writes land on the node (visible via :17600)" || bad "web data not on node: $out"
# 反向:节点上建的表对 web 同样可见(同一份存储,无独立小库)。
sql "$A" "CREATE TABLE cli_side (id INT PRIMARY KEY);" >/dev/null
sql "$A" "INSERT INTO cli_side VALUES (7);" >/dev/null
r=$(curl -s -X POST "$W/api/sql" -H 'Content-Type: application/json' -d '{"sql":"SELECT COUNT(id) AS n FROM cli_side"}')
echo "$r" | grep -q '"rows":\[\[1\]\]' && ok "node tables visible via web (same store)" || bad "cli table via web: $r"

echo "== 5. isolation from the cluster profile =="
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

echo "== 6. pub/sub (persistent, single node) =="
ch="solo-$(date +%s)"
tmp=$(mktemp)
# 后台订阅(输出落宿主临时文件),另一连接发布,断言实时推送。
( printf "subscribe %s latest;\n" "$ch"; sleep 3; printf "exit;\n" ) \
  | docker exec -i "$CTR" docsql-cli connect "$A" >"$tmp" 2>/dev/null &
sub=$!
# 轮询等订阅确认(docker exec 冷启动可能 >1s,固定 sleep 会抢跑)。
sub_ready=""
for _ in $(seq 1 40); do
  grep -q "subscribed" "$tmp" && { sub_ready=1; break; }
  sleep 0.5
done
out=$(printf "publish %s solo-msg;\nexit;\n" "$ch" | docker exec -i "$CTR" docsql-cli connect "$A" 2>/dev/null)
wait $sub
echo "$out" | grep -qE "\|[[:space:]]*1[[:space:]]*$" && ok "publish reports 1 live receiver" || bad "publish: $out"
grep -q "\[pubsub\] message $ch #" "$tmp" && grep -q "solo-msg" "$tmp" \
  && ok "live push on single node" || bad "live push: $(cat "$tmp")"

# 重启后 from=earliest 回放(消息随 WAL 持久化)。
docker restart "$CTR" >/dev/null
up=""
for _ in $(seq 1 60); do
  sql "$A" "SELECT 1;" >/dev/null 2>&1 && { up=1; break; }
  sleep 0.5
done
if [ -n "$up" ]; then
  ( printf "subscribe %s earliest;\n" "$ch"; sleep 2; printf "exit;\n" ) \
    | docker exec -i "$CTR" docsql-cli connect "$A" >"$tmp" 2>/dev/null
  # 回放帧紧随确认;等确认+回放都到齐再断言。
  for _ in $(seq 1 20); do
    grep -q "solo-msg" "$tmp" && break
    sleep 0.5
  done
  grep -q "solo-msg" "$tmp" && ok "pubsub history survives restart (replay)" || bad "replay: $(cat "$tmp")"
else
  bad "node did not come back for pubsub replay"
fi
rm -f "$tmp"

echo "== 7. auto-GUID primary key (UUIDv7) =="
UUID_RE='[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
out=$(sql "$A" "CREATE TABLE sg (id GUID PRIMARY KEY AUTOINCREMENT, v TEXT);")
echo "$out" | grep -q "rows affected" && ok "create guid table" || bad "create guid table: $out"
out=$(sql "$A" "INSERT INTO sg (v) VALUES ('one');")
echo "$out" | grep -q "1 rows affected" && ok "insert without id" || bad "guid insert: $out"
G1=$(sql "$A" "SELECT id FROM sg WHERE v = 'one';" | grep -oE "$UUID_RE" | head -1)
[ -n "$G1" ] && ok "generated UUIDv7 ($G1)" || bad "no UUIDv7: $out"
out=$(sql "$A" "INSERT INTO sg (id, v) VALUES ('00000000-0000-7000-8000-000000000001', 'explicit');")
echo "$out" | grep -q "1 rows affected" && ok "explicit id accepted" || bad "explicit guid insert: $out"
out=$(sql "$A" "SELECT v FROM sg ORDER BY id;")
L1=$(echo "$out" | grep -n "explicit" | head -1 | cut -d: -f1)
L2=$(echo "$out" | grep -n "one" | head -1 | cut -d: -f1)
[ -n "$L1" ] && [ -n "$L2" ] && [ "$L1" -lt "$L2" ] \
  && ok "explicit (all-zero) guid sorts first" || bad "guid ordering: $out"
# 重启后:旧行保留,自动生成继续(catalog 的 autoguid 标志持久化)
docker restart "$CTR" >/dev/null
up=""
for _ in $(seq 1 60); do
  sql "$A" "SELECT 1;" >/dev/null 2>&1 && { up=1; break; }
  sleep 0.5
done
if [ -n "$up" ]; then
  out=$(sql "$A" "INSERT INTO sg (v) VALUES ('after-restart');")
  G2=$(sql "$A" "SELECT id FROM sg WHERE v = 'after-restart';" | grep -oE "$UUID_RE" | head -1)
  cnt=$(sql "$A" "SELECT COUNT(id) FROM sg;" | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " ")
  [ -n "$G2" ] && [ "$cnt" = "3" ] && ok "autoguid survives restart (still generating, 3 rows)" \
    || bad "autoguid after restart: id='$G2' count='$cnt'"
else
  bad "node did not come back for guid restart check"
fi

echo "== 8. automatic backups (interval 5s via run-tests.sh) =="
# 自动备份:定时逻辑 SQL 快照落在节点数据卷的 /data/backups(随卷持久);
# 恢复 = 重放备份文件(整库替换,集群内会经扇出传播收敛)。
# 容器内文件一律 docker exec cat 中转,命令走参数列表,不拼 shell 字符串。
BK="/data/backups"
backup_names() { docker exec "$CTR" ls "$BK" 2>/dev/null | grep -E '^backup-.*\.sql$' | sort; }
newest() { backup_names | tail -1; }
# 备份文件出现(首拍即触发,栈就绪后应已有;轮询兜底)。
bk=""
for _ in $(seq 1 60); do
  bk=$(newest)
  [ -n "$bk" ] && break
  sleep 1
done
[ -n "$bk" ] && ok "backup file created" || bad "no backup file appeared"
# 内容是完整 SQL 快照(DDL + 数据)。
if [ -n "$bk" ]; then
  out=$(docker exec "$CTR" cat "$BK/$bk")
  echo "$out" | grep -q "CREATE TABLE" && echo "$out" | grep -q "INSERT" \
    && ok "backup contains DDL + data" || bad "backup content: $(echo "$out" | head -c 120)"
else
  bad "backup content (no file)"
fi
# 定时器持续产出,保留策略不超上限(keep 默认 7)。
n=0
for _ in $(seq 1 30); do
  n=$(backup_names | wc -l | tr -d " ")
  [ "${n:-0}" -ge 2 ] && break
  sleep 1
done
if [ "${n:-0}" -ge 2 ] && [ "${n:-0}" -le 7 ]; then
  ok "timer keeps producing, retention holds ($n files, keep=7)"
else
  bad "backup count out of range: '$n'"
fi
# web 控制台备份页数据源(GET /api/backup)与手动触发(POST,节点异步执行)。
r=$(curl -s "$W/api/backup")
echo "$r" | grep -q '"count"' && echo "$r" | grep -q 'backup-' \
  && ok "web backup list" || bad "web backup list: $r"
r=$(curl -s -X POST "$W/api/backup")
echo "$r" | grep -q '"ok":true' && ok "web backup trigger" || bad "web backup trigger: $r"
# 恢复演练:取一份含 sg 全部行的快照 → DROP 该表 → 重放备份 → 行数回来。
sgbk=""
for _ in $(seq 1 30); do
  bk=$(newest)
  [ -n "$bk" ] || { sleep 1; continue; }
  out=$(docker exec "$CTR" cat "$BK/$bk")
  echo "$out" | grep -q "after-restart" && { sgbk="$bk"; break; }
  sleep 1
done
if [ -n "$sgbk" ]; then
  sql "$A" "DROP TABLE sg;" >/dev/null 2>&1
  gone=""
  for _ in $(seq 1 10); do
    out=$(sql "$A" "SELECT COUNT(id) FROM sg;" 2>&1)
    echo "$out" | grep -qi "does not exist" && { gone=1; break; }
    sleep 0.3
  done
  if [ -n "$gone" ]; then
    docker exec "$CTR" cat "$BK/$sgbk" | docker exec -i "$CTR" docsql-cli connect "$A" >/dev/null 2>&1
    restored=""
    for _ in $(seq 1 20); do
      out=$(sql "$A" "SELECT COUNT(id) FROM sg;" 2>/dev/null)
      echo "$out" | grep -qE "^[[:space:]]*3[[:space:]]*$" && { restored=1; break; }
      sleep 0.5
    done
    [ -n "$restored" ] && ok "restore drill: dropped table back with 3 rows" \
      || bad "restore drill: $(sql "$A" "SELECT COUNT(id) FROM sg;" 2>&1)"
  else
    bad "restore drill: drop did not take effect"
  fi
else
  bad "restore drill: no snapshot carrying sg rows"
fi
# web 控制台恢复(POST /api/backup/restore):建表 → 等快照带上它 → DROP →
# 恢复 → 数据回来。节点经正常写路径重放,整库收敛到快照时点。
sql "$A" "CREATE TABLE wbk (id INT PRIMARY KEY, v TEXT);" >/dev/null 2>&1
sql "$A" "INSERT INTO wbk VALUES (9, 'webrestore');" >/dev/null 2>&1
wname=""
for _ in $(seq 1 30); do
  w=$(newest)
  [ -n "$w" ] || { sleep 1; continue; }
  out=$(docker exec "$CTR" cat "$BK/$w" 2>/dev/null)
  echo "$out" | grep -q "webrestore" && { wname="$w"; break; }
  sleep 1
done
if [ -n "$wname" ]; then
  sql "$A" "DROP TABLE wbk;" >/dev/null 2>&1
  r=$(curl -s -X POST "$W/api/backup/restore" -H 'Content-Type: application/json' \
    -d "{\"file\":\"$wname\",\"confirm\":\"$wname\"}")
  echo "$r" | grep -q '"ok":true' && ok "web restore accepted" || bad "web restore trigger: $r"
  wr=""
  for _ in $(seq 1 20); do
    out=$(sql "$A" "SELECT v FROM wbk WHERE id = 9;" 2>/dev/null)
    echo "$out" | grep -q "webrestore" && { wr=1; break; }
    sleep 0.5
  done
  [ -n "$wr" ] && ok "web restore: dropped table back with data" \
    || bad "web restore: $(sql "$A" "SELECT COUNT(id) FROM wbk;" 2>&1)"
else
  bad "web restore: no snapshot carrying wbk rows"
fi

echo
echo "RESULT: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
