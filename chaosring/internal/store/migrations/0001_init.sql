-- chaosring 元数据存储: 环版本、虚拟节点、节点清单、键注册表、迁移进度。
-- 注意: 实际键值不在这里, 而在各本地 KV 节点上; PostgreSQL 只保存"编排状态",
-- 这样路由器重启后可以精确恢复: 哪个环是 active、哪些键该往哪迁、迁到了哪一步。

BEGIN;

CREATE TABLE IF NOT EXISTS schema_meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

-- 节点注册表。未知节点不能加入: 注册必须出示管理员令牌 + 预共享 enrollment_secret,
-- 节点令牌是随机不透明串, 路由器下发给 gRPC 节点做调用鉴权。
CREATE TABLE IF NOT EXISTS nodes (
    node_id          TEXT PRIMARY KEY,
    address          TEXT NOT NULL,
    weight           INTEGER NOT NULL CHECK (weight >= 0),
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    node_token       TEXT NOT NULL,
    registered_ring  BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 环版本。active 环只有一个; 旧环在宽限期内保留以供在途请求按旧版本解析。
CREATE TABLE IF NOT EXISTS rings (
    ring_version   BIGINT PRIMARY KEY,
    reason         TEXT NOT NULL DEFAULT '',
    -- staging: 新环已建好但尚未承载正式流量(复制进行中)
    -- active:  当前承载正式流量; old: 被替代, 宽限期内仍服务在途请求;
    -- retired: 显式退役, 路由拒绝再按它解析。
    state          TEXT NOT NULL CHECK (state IN ('active','staging','old','retired')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at   TIMESTAMPTZ
);

-- 环成员权重快照(重建环只依赖这张表, 与 nodes 当前权重解耦, 保证旧环可复现)。
CREATE TABLE IF NOT EXISTS ring_weights (
    ring_version BIGINT NOT NULL REFERENCES rings(ring_version),
    node_id      TEXT NOT NULL,
    weight       INTEGER NOT NULL,
    PRIMARY KEY (ring_version, node_id)
);

-- 虚拟节点落环位置。哈希值以 BIGINT 存储; 查询时解释为无符号 64 位
-- (应用层统一保证, SQL 内只做相等/排序, 负数是 > MaxInt64 的无符号值)。
CREATE TABLE IF NOT EXISTS vnodes (
    ring_version BIGINT NOT NULL REFERENCES rings(ring_version),
    vnode_hash   BIGINT NOT NULL,   -- uint64 的位模式
    node_id      TEXT NOT NULL,
    vnode_index  INTEGER NOT NULL,
    label        TEXT NOT NULL,
    PRIMARY KEY (ring_version, vnode_hash, node_id)
);
CREATE INDEX IF NOT EXISTS idx_vnodes_ring_hash ON vnodes (ring_version, vnode_hash);

-- 已知键注册表。路由器据此枚举"存在的键"来生成真实迁移计划:
-- 环变化是潜在变化量, 实际迁移量只统计这里登记过、且属主真的改变的键。
CREATE TABLE IF NOT EXISTS key_registry (
    key            TEXT PRIMARY KEY,
    key_hash       BIGINT NOT NULL,
    current_owner  TEXT NOT NULL,         -- 当前正式承载该键的节点
    first_seen     BIGINT NOT NULL,       -- 写入时的环版本
    version        BIGINT NOT NULL DEFAULT 0,  -- 单调写版本, 随 Put/Delete 推进
    deleted        BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_keyreg_owner ON key_registry (current_owner);
CREATE INDEX IF NOT EXISTS idx_keyreg_hash ON key_registry (key_hash);

-- 迁移单。复制与切换严格分离:
--   planned -> replicating -> replicated(全部复制完成)
--   -> committed(显式提交并切换 active 环); 中途可 abort。
-- replicated 绝不等于 committed: 只复制不切换, 旧环仍然承载正式流量。
CREATE TABLE IF NOT EXISTS migrations (
    id           TEXT PRIMARY KEY,
    from_ring    BIGINT NOT NULL,
    to_ring      BIGINT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN
                   ('planned','replicating','replicated','committed','aborted','failed')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_migrations_state ON migrations (state);

-- 迁移明细, 逐条记录键的复制状态, 支撑断点续传与"实际迁移量对账"。
CREATE TABLE IF NOT EXISTS migration_items (
    migration_id  TEXT NOT NULL REFERENCES migrations(id) ON DELETE CASCADE,
    key           TEXT NOT NULL,
    key_hash      BIGINT NOT NULL,
    from_node     TEXT NOT NULL,
    to_node       TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN
                    ('pending','replicating','replicated','failed','promoted')),
    last_error    TEXT NOT NULL DEFAULT '',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (migration_id, key)
);
CREATE INDEX IF NOT EXISTS idx_mitems_state ON migration_items (migration_id, state);

INSERT INTO schema_meta(k, v) VALUES ('version', '1')
  ON CONFLICT (k) DO NOTHING;

COMMIT;
