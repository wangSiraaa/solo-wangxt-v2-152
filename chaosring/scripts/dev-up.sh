#!/usr/bin/env bash
# 启动演示集群: 1 个路由器 + 6 个本地 KV 节点。
# 依赖: 已运行的 PostgreSQL(可由 pg-local.sh 准备), 已构建的 bin/{router,node}。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export CHAOS_ADMIN_TOKEN="${CHAOS_ADMIN_TOKEN:-admin-secret}"
export CHAOS_ENROLL_SECRET="${CHAOS_ENROLL_SECRET:-enroll-secret}"
PG_DSN="${CHAOS_PG_DSN:-host=/tmp port=5432 user=chaos dbname=chaosring sslmode=disable}"
export CHAOS_PG_DSN="$PG_DSN"

mkdir -p run

start_node() {
  local n="$1" p="$2"
  echo "starting node n$n on :$p"
  nohup ./bin/node \
    --id "n$n" --listen "127.0.0.1:$p" \
    --data "./run/data-n$n" \
    --token "tok-n$n-secret" \
    --admin-token "$CHAOS_ADMIN_TOKEN" \
    > "./run/node-n$n.log" 2>&1 &
}

for spec in A:9101 B:9102 C:9103 D:9104 E:9105 F:9106; do
  start_node "${spec%:*}" "${spec#*:}"
done

sleep 1
echo "starting router on :9001"
nohup ./bin/router --listen 127.0.0.1:9001 --dsn "$PG_DSN" \
  --admin-token "$CHAOS_ADMIN_TOKEN" --enrollment-secret "$CHAOS_ENROLL_SECRET" \
  > ./run/router.log 2>&1 &

sleep 2
echo "---- router log ----"
tail -n 3 ./run/router.log
echo
echo "集群已启动。运行教学演示:  ./bin/demo walkthrough --include-unsafe"
echo "(首次 walkthrough 会自动注册 nA..nF、建首环并灌种子键)"
