#!/usr/bin/env bash
# 停止演示集群进程。
set -u
pkill -f "chaosring/bin/router" 2>/dev/null || true
pkill -f "chaosring/bin/node" 2>/dev/null || true
# 兼容直接在 run 目录用相对路径启动的情况
pkill -f "bin/router --listen" 2>/dev/null || true
sleep 1
echo "stopped router and nodes (PostgreSQL 未受影响, 由 pg-local.sh 单独管理)"
