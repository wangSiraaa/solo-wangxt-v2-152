# cachelab —— 一致性哈希缓存集群教学系统

演示**新增节点时哪些键会迁移**:Go + gRPC 实现路由服务与本地键值节点,
PostgreSQL 持久化环版本、虚拟节点与迁移进度。无前端,通过 gRPC API
与集成测试呈现全部教学过程。

## 架构

```
            ┌────────────┐   RegisterNode/Join(令牌校验)
            │  Router    │◄──────────────┐
            │ (控制面+   │               │
            │  路由查询) │               │
            └─────┬──────┘               │
        元数据落库 │ gRPC                 │ gRPC
            ┌─────▼──────┐        ┌──────┴──────┐
            │ PostgreSQL │        │ KVNode × N  │◄──── 客户端直连读写
            │ rings      │        │ (内存KV +   │      (先问 Router 路由)
            │ vnodes     │        │  迁移控制面) │
            │ migrations │        └─────────────┘
            │ migration_ │
            │ ranges     │   ExportRange → ImportBatch → 校验 → 切换 → 清理
            └────────────┘
```

- **Router**(`cmd/routerd`):环版本管理、路由查询、迁移编排、分布/热点聚合。
- **KVNode**(`cmd/kvnode`):本地内存键值存储;数据面 Get/Put/Delete 开放,
  迁移控制面(ExportRange/ImportBatch/DeleteRange/Stats)需 `x-node-token` 元数据。
- **PostgreSQL**:`rings`(版本)、`vnodes`(虚拟节点)、`migrations` +
  `migration_ranges`(逐区间进度)、`nodes`(注册表)、`cluster_state`(活跃版本)。
  进度落库是中断恢复的基础。
- **demo**(`cmd/demo`):自包含教学演示,一条命令跑完"加节点"全流程。

## 哈希算法(固定、可验证)

- 算法:**FNV-1a 64**(公开常量 offset `0xcbf29ce484222325`、素数 `0x100000001b3`),
  后接 **splitmix64 终末混合**(公开常数)提供雪崩效应——否则仅尾部不同的输入
  (顺序键、`nodeID#index`)会挤在环上一小段弧内。
- 环上位置:`point = Mix64(FNV1a64(material))`。
  - 键:`material = key 的字节`
  - 虚拟节点:`material = nodeID || '#' || big-endian uint32(index)`(**字节序显式固定**)
- 不依赖任何语言默认哈希(Go `maphash`/map 哈希含随机种子,跨进程不可复现)。
- 正确性由**公开测试向量**锁定(`internal/hash/fnv_test.go`):
  `""→0xcbf29ce484222325`、`"a"→0xaf63dc4c8601ec8c`、`"foobar"→0x85944171f73967e8`
  (来源:isthe.com/chongo/tech/comp/fnv),并与 Go 标准库 `hash/fnv` 交叉验证。

## 环版本与迁移协议(复制与切换分离)

```
SetWeight(node, w)                # 权重变化(含新增节点、权重归零)
  → 构建新环(STAGING),diff 新旧环 → 迁移计划(PREPARED)+ 逐区间记录
  → 活跃环不变,旧环继续服务在途请求

StartMigration(plan)              # 计划 → COPYING
  → 每区间:源节点计数(expected_keys,"路由变化量")
  → 流式 ExportRange → 目标 ImportBatch(幂等 upsert)→ 区间 COPIED
  → 全部完成 → 计划 COPIED        # 注意:此时路由仍未切换!

SwitchRing(plan)                  # 复制完成 ≠ 可以安全切换
  → 逐区间重新核对 源键数 == 目标键数
  → 任一不符:拒绝切换,该区间打回 PENDING 重拷
  → 全部一致:事务内切换活跃环,广播新版本号

Cleanup(plan)                     # 仅 SWITCHED 后允许
  → 源节点 DeleteRange,计划 DONE
```

不变式:

1. 任何时刻只有一个活跃环;旧环版本保留,`Route(key, oldVersion)` 仍可为
   在途请求提供一致视图。
2. 迁移窗口内(COPYING/COPIED 未切换),落在迁移区间的写由 `RouteWrite`
   返回镜像目标**双写**,保证复制窗口内的写不丢失。
3. 节点拒绝携带过旧环版本的写(`FailedPrecondition`),防止切换后写落到
   已迁出的节点上。
4. 源数据在 Cleanup 前始终保留——它是切换前的最后备份。

## 安全:未知节点不能加入

- 节点必须先经 `RegisterNode` 预注册(node_id + addr + token),
  `Join` 时校验令牌:未知节点 → `PermissionDenied`,错误令牌 → `Unauthenticated`。
- Router 调用节点控制面时携带 `x-node-token` 元数据,节点侧拦截器校验。

## API 一览(proto/cachelab/v1/cachelab.proto)

| RPC | 说明 |
| --- | --- |
| `RegisterNode` / `Join` / `ListNodes` | 节点预注册 / 令牌加入 / 列表 |
| `SetWeight` / `ListRings` | 权重变更生成新环与计划 / 环版本历史 |
| `Route` / `RouteWrite` | 读路由(可指定旧版本)/ 写路由(主+镜像) |
| `StartMigration` / `ResumeMigration` | 启动复制(支持故障注入)/ 中断续跑 |
| `MigrationStatus` | 进度:路由变化量(expected)对照实际迁移量(copied) |
| `SwitchRing` / `Cleanup` | 校验后切换 / 清理源数据 |
| `KeyDistribution` / `HotKeys` | 键分布(预测占比 vs 实际键数)/ 热点统计 |

## 运行

```bash
source go.env                       # Go/ protoc/ pg 工具链路径 + CACHELAB_PG_DSN
./scripts/pg-start.sh               # 启动内置 PostgreSQL(若未运行)

go test ./internal/hash ./internal/ring -v   # 哈希向量 + 环性质
go test ./internal/integration -v            # 四大场景端到端
go run ./cmd/demo                             # 自包含教学演示

# 或分开起进程:
go run ./cmd/routerd -addr 127.0.0.1:7000 &
go run ./cmd/kvnode -id n1 -token tok-n1 -addr 127.0.0.1:7101 &
```

## 教学检查点(集成测试覆盖)

| 场景 | 测试 | 验证点 |
| --- | --- | --- |
| 新增节点 | `TestAddNode_MigrationMatchesRouteChange` | 路由变化量 = 实际迁移量 = 路由改变的键数;COPIED 后活跃环不变;旧环可查;清理后总数一致 |
| 权重为零 | `TestWeightZero_DrainsNode` | 节点完全退出,数据零丢失零重复;权重恢复后重新进环 |
| 迁移途中失败 | `TestMigrationInterrupted_ResumeAndVerify` | 注入崩溃后计划保持 COPYING;断点续跑;目标宕机丢数据时**切换被拒绝**并重拷 |
| 未知节点 | `TestUnknownNode_CannotJoin` | 未注册/错令牌/无令牌调用控制面均被拒 |
| 分布与热点 | `TestDistributionAndHotKeys` | 预测占比与实际键数对照;热点键 TopN |
| 迁移中写入 | `TestWritesDuringMigration_AreMirrored` | 复制窗口内的写经双写不丢失,切换校验通过 |
