#!/usr/bin/env bash
# 启动内置 PostgreSQL(用户态安装,数据目录 /workspace/tools/pgdata)
set -e
PGBIN=/workspace/tools/pg/bin
PGDATA=/workspace/tools/pgdata
if [ ! -d "$PGDATA" ]; then
  "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust -E UTF8
fi
"$PGBIN/pg_ctl" -D "$PGDATA" -o "-p 5432 -k /tmp -c listen_addresses=localhost" \
  -l /workspace/tools/pg.log start
# 建库(幂等)
cat > /tmp/cachelab-createdb.go <<'EOF'
package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func main() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), "CREATE DATABASE cachelab")
	if err != nil {
		fmt.Println("note:", err)
	} else {
		fmt.Println("created database cachelab")
	}
}
EOF
cd /workspace/cachelab && source go.env && go run /tmp/cachelab-createdb.go
