# chaosring — 一致性哈希缓存集群的"新增节点时哪些键会迁移"教学实现

用 **Go + gRPC** 实现的路由服务与本地键值节点，用 **PostgreSQL** 持久化
环版本、虚拟节点与迁移进度。无前端，全部通过 gRPC / CLI 演示。

它围绕缓存集群教学中最容易被混淆的一件事构建：

> **复制完成（数据字节已到新节点）≠ 已安全切换（路由已经切到新环）。**

因此系统把"复制"和"路由切换"做成两个**显式、可独立观测**的阶段，
并让"路由变化量"和"实际迁移量"可以逐条对账。

---

## 1. 架构

```
                         ┌──────────────────────────────────────────┐
                         │                PostgreSQL                 │
                         │  rings / ring_weights / vnodes             │
                         │  nodes / key_registry / migrations(+items) │
                         └────────────────────▲─────────────────────┘
                                              │ 编排状态(不存业务值)
   gRPC (管理面 + 数据面)                      │
  clients ───────────────▶  ┌─────────────────┴───────────────────┐
   Get/Put/Delete           │              Router                  │
   Rebalance/...            │  - 固定哈希一致性环(内存缓存, 库可重建) │
                            │  - 迁移编排: 复制 → (显式)提交切换      │
                            │  - 在途请求可按旧环版本 ring_hint 解析  │
                            └───┬───────────┬───────────┬───────────┘
                                │ node_token │           │
                     gRPC       ▼           ▼           ▼
                            ┌────────┐  ┌────────┐  ┌────────┐
                            │ node nA│  │ node nB│  │ node nC│ ...
                            │ 正式map │  │ 正式map │  │ 正式map │
                            │ +staging│  │ +staging│  │ +staging│
                            │  WAL    │  │  WAL    │  │  WAL    │
                            └────────┘  └────────┘  └────────┘
```

- **Router**（`cmd/router`，`internal/routersvc`）：路由 + 集群管理 + 迁移编排。
- **Node**（`cmd/node`，`internal/nodesvc`）：本地 KV，带只追加 WAL
  （`internal/kvlog`），以及按 `migration_id` 隔离的 **staging 暂存区**。
- **PostgreSQL**（`internal/store`）：只存编排状态，业务值只存在节点上。
- **demo**（`cmd/demo`）：gRPC 命令行客户端 + 带中文解说的 `walkthrough`。

---

## 2. 固定的哈希算法与字节序（有公开测试向量）

路由的确定性是整个系统可对账的前提，因此：

- 哈希固定为 **FNV-1a 64-bit**，在 `internal/hashring/hashring.go` 中
  **手写实现常量与乘加步骤**，不使用 Go 的 `hash/maphash` 等语言默认哈希；
- 多字节整数统一 **大端**（`encoding/binary.BigEndian`），字符串按 UTF-8 字节；
- 虚拟节点标签固定为 `"vn1:" + nodeID + ":" + decimal(index)`，
  每单位权重 `128` 个虚拟节点（`VNodesPerWeight`）。

`internal/hashring/hashring_test.go` 用 FNV 官方公开向量锁定哈希：

| 输入 | FNV-1a 64-bit |
|---|---|
| `""` | `0xcbf29ce484222325` |
| `"a"` | `0xaf63dc4c8601ec8c` |
| `"foobar"` | `0x85944171f73967e8` |

并锁定路由金向量（三节点等权环）：

| 键 | 哈希 | 属主 |
|---|---|---|
| `user:1` | `0xf7fd9aaa75081ceb` | `nC` |
| `user:2` | `0xf7fd9baa75081e9e` | `nC` |
| `user:3` | `0xf7fd9caa75082051` | `nC` |
| `user:100` | `0x5f2807232998b4e3` | `nA` |
| `order:42` | `0x0cef508660eaa9b1` | `nC` |
| `hotkey:🔥` | `0x41099e446530a03d` | `nA` |

> 教学注意：FNV 对**连续后缀键**（如 `key:000..key:199`）会明显聚集，
> 这是真实现象而不是 bug。演示种子键使用高熵命名；`KeyDistribution` 能直接展示分布。

### 权重变化如何尽量少动键

权重只影响该节点虚拟节点的**下标范围** `[0, weight*128)`：

- 上调权重：只**新增**高位下标的虚拟节点，旧标签位置不变；
- 下调权重：只删除高位下标；
- 权重 `0`：节点保留成员身份（仍登记在环权重快照中）但**没有任何虚拟节点**，
  不承载任何新键 —— 即"摘流"。

---

## 3. 复制与切换为什么必须分离

迁移单状态机（`migrations` + `migration_items`）：

```
Rebalance ──▶ planned ──StartMigration──▶ replicating
                       │                    │
                       │              全部 StageCopy 成功
                       │                    ▼
                       │               replicated        ◀── 只是字节落到 staging
                       │                    │
        Abort ◀────────┘                    │ CommitSwitch(显式)
                  aborted                   ▼
                                       committed          ◀── 此刻路由才切到新环
```

- **复制阶段**：Router 从旧主 `Get`，向新主 `StageCopy` —— 只写新主的
  staging 暂存区，**绝不进入正式 map**。active 环不变，路由结果不变。
- **切换阶段**：`CommitSwitch` 才会 (1) 要求所有条目 `replicated`，
  (2) 对新主发 `Promote`（暂存区提升为正式数据），(3) 在单个数据库事务里
  把目标环从 `staging` 提升为 `active`、旧环降为 `old`，(4) 改派键属主。
- **迁移窗口内的写**：若某键在在途迁移中，`Put/Delete` 会**双写**
  （旧主正式区 + 新主 staging），保证切换瞬间不丢窗口内更新（含墓碑）。

**拒绝条件**：只要还有 `pending/failed` 条目，`CommitSwitch(force=false)`
直接返回 `FailedPrecondition`。`force=true` 仅用于教学演示反面后果。

### 旧环仍可服务在途请求

- 环状态：`staging → active → old → retired`。
- 被替代的环进入 `old` 宽限期；客户端可携带 `ring_hint` 让请求继续按旧环解析。
- `RetireOldRing` 显式退役后，旧环请求得到明确的 `retired=true`，
  系统不会悄悄把它改路由到新环。

---

## 4. 路由变化量 ↔ 实际迁移量，可对照

- **潜在/路由变化量**：`hashring.Diff(oldRing, newRing, keys)`，只对
  **已登记存在的键**计算（`key_registry`），环变化本身不等于要迁的键。
- **实际迁移量**：迁移明细逐条推进 `pending → replicated → promoted`。
- gRPC `RingDiff(from_ring, to_ring)` 用固定哈希**独立复算**变化量，
  与 `MigrationInfo.promoted` 直接比较。

`walkthrough` 会打印并断言：

```
独立 RingDiff 复算变化量=37, 迁移 promoted=37
```

---

## 5. 未知节点不能随意加入

三重门：

1. **管理面**所有写操作需要 `admin_token`（常量时间比较）；
2. 节点注册还需要带外预共享的 **`enrollment_secret`**，错误秘密返回
   `PermissionDenied: unknown nodes may not join`；
3. 节点数据面每次调用都要带注册时颁发/预置的 **`node_token`**，
   伪造令牌被节点直接拒绝。

此外：

- 新注册节点初始权重为 0（登记但不上环），必须显式 `Rebalance` 才承载流量；
- `Rebalance` 的权重表必须**覆盖全部已登记节点**，且**不得引用未登记节点**。

---

## 6. 中断恢复

- **节点侧**：所有正式写与 staging 写都先 fsync 到 WAL。进程崩溃重启后
  重放 WAL 恢复 `data` 与各 `staging`；末尾半行（torn write）被忽略。
- **路由器侧**：启动时与 `Resume` API 都会扫描未终结迁移：
  - 目标环仍是 `staging`：只做**幂等断点续传**复制，**绝不自动切换**；
  - 若崩溃发生在"环已 active 但迁移单没置 committed"的窗口，补做幂等记账
    （重发 Promote、改派属主、置 committed）；
  - 节点仍不可用则迁移保持 `failed`，列入 `failed_migrations`，等待
    `RetryMigration` 或 `AbortMigration`。

---

## 7. 快速开始（无 root 的 Linux）

需要 Go 1.23+ 与 PostgreSQL 15。仓库脚本假设：

- Go 在 PATH；
- 解压版 PostgreSQL 在 `$HOME/pg`（或用 `scripts/pg-local.sh` 准备）。

```bash
# 1) 准备 PostgreSQL（首次）
scripts/pg-local.sh

# 2) 构建
scripts/build.sh

# 3) 启动 6 节点 + 路由器
scripts/dev-up.sh

# 4) 一键带解说演示（自动注册 nA..nF、建首环、灌 122 个种子键）
./bin/demo walkthrough --include-unsafe
```

重置到空集群：

```bash
scripts/dev-reset.sh && scripts/dev-up.sh
```

### 手动逐步操作（课堂演示）

```bash
./bin/demo nodes
./bin/demo put user:1 hello
./bin/demo get user:1
./bin/demo route user:1            # 看键落在哪个节点

./bin/demo rebalance '{"nA":1,"nB":1,"nC":1,"nD":1,"nE":0,"nF":0}' 'add nD'
./bin/demo migrate start    m-2    # 复制到 staging（路由不变）
./bin/demo migrate progress m-2    # 查看逐条进度
./bin/demo route user:1            # 仍然是旧属主
./bin/demo migrate commit   m-2    # 显式切换（此刻路由才变）

./bin/demo distribution 2          # 键分布: vnodes / owned_keys / share%
./bin/demo hot                     # 全局与节点级热点统计
./bin/demo node-fail nE            # 故障注入
./bin/demo migrate retry    m-4    # 断点续传
./bin/demo retire 1                # 退役旧环
./bin/demo resume                  # 中断恢复
```

---

## 8. 三个教学检查案例（都被自动化测试覆盖）

运行（需要可连的 PostgreSQL）：

```bash
go test ./...
go test -race ./internal/routersvc -run TestEndToEnd -v
```

`internal/routersvc/integration_test.go` 用真实 PostgreSQL + 真实 gRPC
节点（节点在随机端口、带 WAL）验证：

1. **增加节点**：计划迁移量 == `Diff` 路由变化量 == 实际 `promoted` 量；
   复制完不切换；迁移窗口双写在切换后不丢（更新与删除）。
2. **权重为零**：目标节点 `vnodes=0`、无键路由到它，其键全部迁出。
3. **迁移途中失败**：目标节点宕机 → 条目 `failed`、迁移单 `failed`，
   `CommitSwitch` 被拒绝、active 环不变、数据仍可读；节点重启重放 WAL 后
   `RetryMigration` 断点续传，再安全切换并通过全量数据完整性校验
   （注册表属主 == 新环路由属主 == 节点上确实有值）。

另外 `walkthrough --include-unsafe` 实证反面教材：在节点宕机时
`commit --force` 强行切换，恢复后这些键**被路由到新主但读不到值**，
直观说明"不能把复制完成等同已安全切换"。

---

## 9. 目录结构

```
proto/chaosring.proto            gRPC 契约
internal/hashring/               固定 FNV-1a 环 + 公开测试向量/金向量
internal/kvlog/                  节点只追加 WAL(JSON lines, fsync, 崩溃重放)
internal/nodesvc/                本地 KV 节点: 正式 map + staging + 故障注入
internal/store/                  PostgreSQL: schema(embed 迁移) + CRUD
  migrations/0001_init.sql
internal/routersvc/              路由/迁移编排/观测/恢复 + 端到端测试
internal/pb/                     protoc 生成代码
cmd/router  cmd/node  cmd/demo   三个可执行程序
scripts/                         pg-local / build / dev-up/down/reset
```
