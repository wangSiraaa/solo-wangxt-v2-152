#!/usr/bin/env bash
# 构建全部二进制到 bin/。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p bin
go build -o bin/router ./cmd/router
go build -o bin/node   ./cmd/node
go build -o bin/demo   ./cmd/demo
echo "built bin/router bin/node bin/demo"
