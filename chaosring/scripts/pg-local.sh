#!/usr/bin/env bash
# 在无 root 的开发机上, 用解压版 PostgreSQL 15 准备本地数据库。
#
# 做的事情:
#   1. 若 $HOME/pgdata 不存在, 用解压后的 initdb 初始化集群(trust 认证, unix socket /tmp)
#   2. 启动 postgres(unix socket, port 5432, 不监听 TCP)
#   3. 创建 chaos/chaosring 角色与数据库(幂等)
#
# Debian 包可通过下面方式取得(无需 root):
#   apt-get download postgresql-15 postgresql-client-15 libpq5
#   for f in *.deb; do dpkg-deb -x "$f" "$HOME/pg"; done
set -euo pipefail

PGBIN="${PGBIN:-$HOME/pg/usr/lib/postgresql/15/bin}"
PGDATA="${PGDATA:-$HOME/pgdata}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:$HOME/pg/usr/lib/aarch64-linux-gnu"

if [ ! -x "$PGBIN/postgres" ]; then
  echo "未找到 $PGBIN/postgres, 请先解压 PostgreSQL deb 包到 \$HOME/pg" >&2
  exit 1
fi

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "initializing cluster at $PGDATA"
  initdb_bin="$PGBIN/initdb"
  "$initdb_bin" -D "$PGDATA" -U chaos --auth=trust --encoding=UTF8
  printf "\nlisten_addresses = ''\nunix_socket_directories = '/tmp'\nport = 5432\n" \
    >> "$PGDATA/postgresql.conf"
fi

if ! "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/pg.log" start
  sleep 2
fi

"$PGBIN/psql" -h /tmp -U chaos -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='chaosring'" \
  | grep -q 1 || "$PGBIN/createdb" -h /tmp -U chaos chaosring

echo "PostgreSQL ready at unix socket /tmp db=chaosring user=chaos"
