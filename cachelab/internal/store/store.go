// Package store 负责集群元数据的 PostgreSQL 持久化:
// 节点注册表、环版本、虚拟节点、迁移计划与逐区间进度。
// 所有进度落库是"中断恢复"的基础:Router 崩溃重启后可从库中续跑。
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"cachelab/internal/ring"
)

// 节点状态
const (
	NodeRegistered = "REGISTERED" // 已预注册,未加入
	NodeOnline     = "ONLINE"     // 已通过令牌校验加入
)

// 环状态
const (
	RingStaging = "STAGING" // 已生成,未启用
	RingActive  = "ACTIVE"  // 当前路由版本
	RingRetired = "RETIRED" // 已被替换,保留供在途请求与审计
)

// 迁移计划状态
const (
	PlanPrepared = "PREPARED" // 已生成计划
	PlanCopying  = "COPYING"  // 复制进行中(可中断、可续跑)
	PlanCopied   = "COPIED"   // 全部区间复制完成 —— 注意:不等于已切换!
	PlanSwitched = "SWITCHED" // 路由已切到新环
	PlanDone     = "DONE"     // 源数据已清理
)

// 区间状态
const (
	RangePending = "PENDING"
	RangeCopying = "COPYING"
	RangeCopied  = "COPIED"
	RangeCleaned = "CLEANED"
)

type Node struct {
	NodeID string
	Addr   string
	Token  string
	Weight int
	State  string
}

type Range struct {
	RangeID      int
	StartHex     string
	EndHex       string
	FromNode     string
	ToNode       string
	State        string
	ExpectedKeys int64
	CopiedKeys   int64
}

type Plan struct {
	PlanID      int64
	FromVersion int64
	ToVersion   int64
	State       string
	Ranges      []Range
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate 建表(幂等)。
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS nodes (
  node_id   TEXT PRIMARY KEY,
  addr      TEXT NOT NULL,
  token     TEXT NOT NULL,
  weight    INT  NOT NULL DEFAULT 1,
  state     TEXT NOT NULL DEFAULT 'REGISTERED',
  joined_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS rings (
  version    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  state      TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS vnodes (
  ring_version BIGINT NOT NULL REFERENCES rings(version),
  point_hex    CHAR(16) NOT NULL,
  node_id      TEXT NOT NULL,
  PRIMARY KEY (ring_version, point_hex)
);
CREATE TABLE IF NOT EXISTS migrations (
  plan_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_version BIGINT NOT NULL,
  to_version   BIGINT NOT NULL,
  state        TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS migration_ranges (
  plan_id       BIGINT NOT NULL REFERENCES migrations(plan_id),
  range_id      INT NOT NULL,
  start_hex     CHAR(16) NOT NULL,
  end_hex       CHAR(16) NOT NULL,
  from_node     TEXT NOT NULL,
  to_node       TEXT NOT NULL,
  state         TEXT NOT NULL DEFAULT 'PENDING',
  expected_keys BIGINT NOT NULL DEFAULT 0,
  copied_keys   BIGINT NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (plan_id, range_id)
);
CREATE TABLE IF NOT EXISTS cluster_state (
  id          BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  active_ring BIGINT NOT NULL
);`)
	return err
}

// ResetForTest 清空所有表(仅测试用)。
func (s *Store) ResetForTest(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE nodes, rings, vnodes, migrations, migration_ranges, cluster_state RESTART IDENTITY CASCADE`)
	return err
}

// ---------------- 节点注册表 ----------------

func (s *Store) RegisterNode(ctx context.Context, n Node) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO nodes (node_id, addr, token, weight) VALUES ($1,$2,$3,$4)
ON CONFLICT (node_id) DO UPDATE SET addr=EXCLUDED.addr, token=EXCLUDED.token`,
		n.NodeID, n.Addr, n.Token, n.Weight)
	return err
}

var ErrNodeNotFound = errors.New("node not registered")

func (s *Store) GetNode(ctx context.Context, nodeID string) (Node, error) {
	var n Node
	err := s.pool.QueryRow(ctx,
		`SELECT node_id, addr, token, weight, state FROM nodes WHERE node_id=$1`, nodeID).
		Scan(&n.NodeID, &n.Addr, &n.Token, &n.Weight, &n.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return n, ErrNodeNotFound
	}
	return n, err
}

func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_id, addr, token, weight, state FROM nodes ORDER BY node_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.NodeID, &n.Addr, &n.Token, &n.Weight, &n.State); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// CheckJoin 校验节点加入请求:节点必须已预注册且令牌匹配。
func (s *Store) CheckJoin(ctx context.Context, nodeID, token string) (Node, error) {
	n, err := s.GetNode(ctx, nodeID)
	if err != nil {
		return n, err
	}
	if n.Token != token {
		return n, errors.New("invalid token")
	}
	return n, nil
}

func (s *Store) MarkOnline(ctx context.Context, nodeID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE nodes SET state=$2, joined_at=now() WHERE node_id=$1`, nodeID, NodeOnline)
	return err
}

// SetWeight 更新权重并返回当前全部权重表(用于构建新环)。
func (s *Store) SetWeight(ctx context.Context, nodeID string, weight int) (map[string]int, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE nodes SET weight=$2 WHERE node_id=$1`, nodeID, weight)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNodeNotFound
	}
	return s.Weights(ctx)
}

func (s *Store) Weights(ctx context.Context) (map[string]int, error) {
	nodes, err := s.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	w := map[string]int{}
	for _, n := range nodes {
		if n.State == NodeOnline || n.State == NodeRegistered {
			w[n.NodeID] = n.Weight
		}
	}
	return w, nil
}

// ---------------- 环版本 ----------------

// CreateRing 在事务中写入新环及其全部虚拟节点,返回版本号。
func (s *Store) CreateRing(ctx context.Context, r *ring.Ring, state string) (int64, error) {
	var version int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO rings (state) VALUES ($1) RETURNING version`, state).Scan(&version); err != nil {
			return err
		}
		vns := r.VNodes()
		rows := make([][]any, len(vns))
		for i, v := range vns {
			rows[i] = []any{version, fmt.Sprintf("%016x", v.Point), v.NodeID}
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"vnodes"},
			[]string{"ring_version", "point_hex", "node_id"}, pgx.CopyFromRows(rows))
		return err
	})
	return version, err
}

func (s *Store) LoadRing(ctx context.Context, version int64) (*ring.Ring, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT point_hex, node_id FROM vnodes WHERE ring_version=$1 ORDER BY point_hex`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vn []ring.VNode
	for rows.Next() {
		var hex string
		var v ring.VNode
		if err := rows.Scan(&hex, &v.NodeID); err != nil {
			return nil, err
		}
		if _, err := fmt.Sscanf(hex, "%016x", &v.Point); err != nil {
			return nil, fmt.Errorf("bad point_hex %q: %w", hex, err)
		}
		vn = append(vn, v)
	}
	return ring.FromVNodes(vn), rows.Err()
}

type RingInfo struct {
	Version    int64
	State      string
	VNodeCount int
}

func (s *Store) ListRings(ctx context.Context) ([]RingInfo, error) {
	rows, err := s.pool.Query(ctx, `
SELECT r.version, r.state, count(v.point_hex)
FROM rings r LEFT JOIN vnodes v ON v.ring_version = r.version
GROUP BY r.version, r.state ORDER BY r.version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RingInfo
	for rows.Next() {
		var ri RingInfo
		if err := rows.Scan(&ri.Version, &ri.State, &ri.VNodeCount); err != nil {
			return nil, err
		}
		out = append(out, ri)
	}
	return out, rows.Err()
}

func (s *Store) SetRingState(ctx context.Context, version int64, state string) error {
	_, err := s.pool.Exec(ctx, `UPDATE rings SET state=$2 WHERE version=$1`, version, state)
	return err
}

func (s *Store) ActiveRing(ctx context.Context) (int64, error) {
	var v int64
	err := s.pool.QueryRow(ctx, `SELECT active_ring FROM cluster_state WHERE id=TRUE`).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil // 集群尚未初始化
	}
	return v, err
}

// BootstrapRing 初始化集群:创建首个环并置为活跃。
func (s *Store) BootstrapRing(ctx context.Context, r *ring.Ring) (int64, error) {
	var version int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO rings (state) VALUES ($1) RETURNING version`, RingActive).Scan(&version); err != nil {
			return err
		}
		vns := r.VNodes()
		rows := make([][]any, len(vns))
		for i, v := range vns {
			rows[i] = []any{version, fmt.Sprintf("%016x", v.Point), v.NodeID}
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"vnodes"},
			[]string{"ring_version", "point_hex", "node_id"}, pgx.CopyFromRows(rows)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO cluster_state (id, active_ring) VALUES (TRUE, $1)
			ON CONFLICT (id) DO UPDATE SET active_ring=EXCLUDED.active_ring`, version)
		return err
	})
	return version, err
}

// SwitchActive 原子切换活跃环,并将旧环标记为 RETIRED。
func (s *Store) SwitchActive(ctx context.Context, oldV, newV int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE cluster_state SET active_ring=$1 WHERE id=TRUE`, newV); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE rings SET state=$1 WHERE version=$2`, RingActive, newV); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE rings SET state=$1 WHERE version=$2`, RingRetired, oldV)
		return err
	})
}

// ---------------- 迁移计划 ----------------

// CreatePlan 写入迁移计划与全部区间,返回 plan_id。
func (s *Store) CreatePlan(ctx context.Context, fromV, toV int64, ranges []ring.Range, hexFmt func(uint64) string) (int64, error) {
	var planID int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO migrations (from_version, to_version, state) VALUES ($1,$2,$3) RETURNING plan_id`,
			fromV, toV, PlanPrepared).Scan(&planID); err != nil {
			return err
		}
		rows := make([][]any, len(ranges))
		for i, r := range ranges {
			rows[i] = []any{planID, i, hexFmt(r.Start), hexFmt(r.End), r.From, r.To, RangePending}
		}
		if len(rows) > 0 {
			_, err := tx.CopyFrom(ctx, pgx.Identifier{"migration_ranges"},
				[]string{"plan_id", "range_id", "start_hex", "end_hex", "from_node", "to_node", "state"},
				pgx.CopyFromRows(rows))
			return err
		}
		return nil
	})
	return planID, err
}

func (s *Store) GetPlan(ctx context.Context, planID int64) (*Plan, error) {
	p := &Plan{PlanID: planID}
	err := s.pool.QueryRow(ctx,
		`SELECT from_version, to_version, state FROM migrations WHERE plan_id=$1`, planID).
		Scan(&p.FromVersion, &p.ToVersion, &p.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("plan %d not found", planID)
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT range_id, start_hex, end_hex, from_node, to_node, state, expected_keys, copied_keys
FROM migration_ranges WHERE plan_id=$1 ORDER BY range_id`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var r Range
		if err := rows.Scan(&r.RangeID, &r.StartHex, &r.EndHex, &r.FromNode, &r.ToNode,
			&r.State, &r.ExpectedKeys, &r.CopiedKeys); err != nil {
			return nil, err
		}
		p.Ranges = append(p.Ranges, r)
	}
	return p, rows.Err()
}

// UnfinishedPlan 返回当前未完成的计划(最多一个;集群同一时间只允许一个计划)。
func (s *Store) UnfinishedPlan(ctx context.Context) (*Plan, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
SELECT plan_id FROM migrations
WHERE state IN ($1,$2,$3,$4) ORDER BY plan_id DESC LIMIT 1`,
		PlanPrepared, PlanCopying, PlanCopied, PlanSwitched).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.GetPlan(ctx, id)
}

func (s *Store) SetPlanState(ctx context.Context, planID int64, state string) error {
	_, err := s.pool.Exec(ctx, `UPDATE migrations SET state=$2 WHERE plan_id=$1`, planID, state)
	return err
}

func (s *Store) SetRangeExpected(ctx context.Context, planID int64, rangeID int, expected int64) error {
	_, err := s.pool.Exec(ctx, `
UPDATE migration_ranges SET expected_keys=$3, updated_at=now() WHERE plan_id=$1 AND range_id=$2`,
		planID, rangeID, expected)
	return err
}

func (s *Store) SetRangeState(ctx context.Context, planID int64, rangeID int, state string, copied int64) error {
	_, err := s.pool.Exec(ctx, `
UPDATE migration_ranges SET state=$3, copied_keys=$4, updated_at=now() WHERE plan_id=$1 AND range_id=$2`,
		planID, rangeID, state, copied)
	return err
}

// ResetRangeForRetry 切换前校验失败时,把区间打回 PENDING 以便重拷。
func (s *Store) ResetRangeForRetry(ctx context.Context, planID int64, rangeID int) error {
	_, err := s.pool.Exec(ctx, `
UPDATE migration_ranges SET state=$3, copied_keys=0, updated_at=now() WHERE plan_id=$1 AND range_id=$2`,
		planID, rangeID, RangePending)
	return err
}

// RangeUpdatedAt 用于测试观察进度推进。
func (s *Store) RangeUpdatedAt(ctx context.Context, planID int64, rangeID int) (time.Time, error) {
	var t time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT updated_at FROM migration_ranges WHERE plan_id=$1 AND range_id=$2`, planID, rangeID).Scan(&t)
	return t, err
}
