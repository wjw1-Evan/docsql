#!/bin/bash
# 部署验收脚本的共享片段。single-test.sh 与 multinode-test.sh 曾把断言
# 计数、每节点 SQL 转发、重启/追平等待循环与「纯数字行提取」逐字复制
# (等待循环 ≥9 份、数字提取 ≥12 份);改动只应发生在这里。
#
# 用法:调用方先设置好节点寻址(单一容器或 ctr_of 映射),再
#   source "$(dirname "$0")/lib.sh"
# 然后定义各自的 sql()/sqltx() 一行包装(容器解析方式不同)。

PASS=0; FAIL=0
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

# sql_on <container> <addr> <sql>:一条语句经镜像自带 CLI 执行。
sql_on() { printf "%s\nexit;\n" "$3" | docker exec -i "$1" docsql-cli connect "$2" 2>&1; }

# sqltx_on <container> <addr> <stmt>...:同一会话内多语句(事务归属该连接)。
sqltx_on() { local ctr="$1" addr="$2"; shift 2; { printf '%s\n' "$@"; echo "exit;"; } | docker exec -i "$ctr" docsql-cli connect "$addr" 2>&1; }

# wait_ready <container> <addr> [tries] [interval]:轮询到节点可用。
wait_ready() {
  local ctr="$1" addr="$2" tries="${3:-40}" interval="${4:-0.5}" out=""
  for _ in $(seq 1 "$tries"); do
    out=$(sql_on "$ctr" "$addr" "SELECT 1;")
    echo "$out" | grep -qE "^[[:space:]]*1[[:space:]]*$" && return 0
    sleep "$interval"
  done
  return 1
}

# wait_row <container> <addr> <sql> <ERE>:轮询到查询输出匹配。
wait_row_on() {
  local out=""
  for _ in $(seq 1 40); do
    out=$(sql_on "$1" "$2" "$3")
    echo "$out" | grep -qE "$4" && break
    sleep 0.5
  done
  echo "$out" | grep -qE "$4"
}

# absent_on <container> <addr> <sql> <ERE>:轮询确认查询始终不匹配。
absent_on() {
  local out=""
  for _ in $(seq 1 6); do
    out=$(sql_on "$1" "$2" "$3")
    echo "$out" | grep -qE "$4" && return 1
    sleep 0.5
  done
  return 0
}

# num:过滤器——从 CLI 输出取第一行纯数字(COUNT 等计数行)。
num() { grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | head -1 | tr -d " "; }

# UUIDv7 正则(auto-GUID 断言共用)。
UUID_RE='[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
