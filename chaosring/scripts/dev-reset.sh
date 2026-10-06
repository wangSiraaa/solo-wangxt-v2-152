#!/usr/bin/env bash
# 重置演示状态: 清空 PostgreSQL 编排数据 + 删除节点 WAL 数据目录。
# 节点与路由器进程会先停止。之后重新 dev-up.sh 即可得到空集群。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
"$ROOT/scripts/dev-down.sh" || true

PGBIN="${PGBIN:-$HOME/pg/usr/lib/postgresql/15/bin}"
PSQL=("$PGBIN/psql" -h /tmp -U chaos -d chaosring)

"${PSQL[@]}" -c "
TRUNCATE migration_items, migrations, key_registry,
        vnodes, ring_weights, rings, nodes RESTART IDENTITY CASCADE;"

rm -rf ./run/data-nA ./run/data-nB ./run/data-nC \
       ./run/data-nD ./run/data-nE ./run/data-nF
rm -f ./run/*.log

echo "reset complete: database truncated, node WALs removed."
