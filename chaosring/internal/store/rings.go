package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateRing 原子写入新版本环(元数据 + 权重快照 + 虚拟节点), 初始为 staging。
// 版本号由数据库决定, 避免多实例协调。返回分配到的版本号。
func (s *Store) CreateRing(ctx context.Context, reason string, weights map[string]int, vnodes []VNodeRow) (int64, error) {
	var version int64
	err := s.tx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(ring_version),0)+1 FROM rings`).Scan(&version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO rings(ring_version, reason, state) VALUES ($1,$2,'staging')`,
			version, reason); err != nil {
			return err
		}
		for id, w := range weights {
			if _, err := tx.Exec(ctx,
				`INSERT INTO ring_weights(ring_version, node_id, weight) VALUES ($1,$2,$3)`,
				version, id, w); err != nil {
				return err
			}
		}
		// 批量插入虚拟节点; 显式事务保证环要么完整可见, 要么不可见。
		for _, v := range vnodes {
			if _, err := tx.Exec(ctx, `
				INSERT INTO vnodes(ring_version, vnode_hash, node_id, vnode_index, label)
				VALUES ($1,$2,$3,$4,$5)`,
				version, hashToPG(v.Hash), v.NodeID, v.Index, v.Label); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return version, nil
}

// SeedFirstRing 建首个 active 环(集群初始化用)。
func (s *Store) SeedFirstRing(ctx context.Context, reason string, weights map[string]int, vnodes []VNodeRow) (int64, error) {
	v, err := s.CreateRing(ctx, reason, weights, vnodes)
	if err != nil {
		return 0, err
	}
	if err := s.tx(ctx, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx,
			`UPDATE rings SET state='active', activated_at=now() WHERE ring_version=$1`, v)
		return e
	}); err != nil {
		return 0, err
	}
	return v, nil
}

// GetRing 读取环元数据与权重快照。
func (s *Store) GetRing(ctx context.Context, version int64) (*RingRecord, error) {
	r := &RingRecord{Version: version, Weights: map[string]int{}}
	var state string
	err := s.pool.QueryRow(ctx,
		`SELECT reason, state, activated_at FROM rings WHERE ring_version=$1`,
		version).Scan(&r.Reason, &state, &r.ActivatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: ring %d", ErrNotFound, version)
		}
		return nil, err
	}
	r.State = RingState(state)
	rows, err := s.pool.Query(ctx,
		`SELECT node_id, weight FROM ring_weights WHERE ring_version=$1 ORDER BY node_id`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var w int
		if err := rows.Scan(&id, &w); err != nil {
			return nil, err
		}
		r.Weights[id] = w
	}
	return r, rows.Err()
}

// ListVNodes 返回某环的全部虚拟节点, 按哈希(无符号位模式解释)升序。
// 注意 SQL 里 BIGINT 排序与 uint64 排序在跨越 MaxInt64 时不一致,
// 因此这里取出后在应用层重排, 不把排序责任交给数据库。
func (s *Store) ListVNodes(ctx context.Context, version int64) ([]VNodeRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT vnode_hash, node_id, vnode_index, label
		FROM vnodes WHERE ring_version=$1`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VNodeRow
	for rows.Next() {
		var v VNodeRow
		var h int64
		if err := rows.Scan(&h, &v.NodeID, &v.Index, &v.Label); err != nil {
			return nil, err
		}
		v.Hash = hashFromPG(h)
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ActiveRingVersion 返回当前 active 环版本。
func (s *Store) ActiveRingVersion(ctx context.Context) (int64, error) {
	var v int64
	err := s.pool.QueryRow(ctx,
		`SELECT ring_version FROM rings WHERE state='active'`).Scan(&v)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("%w: no active ring", ErrNotFound)
		}
		return 0, err
	}
	return v, nil
}

// PromoteRing 在单事务内切换 active 环:
// 旧 active -> old(宽限期), 指定的 staging 环 -> active。
// 复制完成不触发本方法; 只有显式提交迁移才会调用, 二者因此严格分离。
func (s *Store) PromoteRing(ctx context.Context, newVersion int64) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM rings WHERE ring_version=$1 FOR UPDATE`,
			newVersion).Scan(&state); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: ring %d", ErrNotFound, newVersion)
			}
			return err
		}
		if RingState(state) == RingActive {
			return fmt.Errorf("ring %d already active", newVersion)
		}
		if RingState(state) != RingStaging {
			return fmt.Errorf("ring %d in state %s, cannot promote", newVersion, state)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE rings SET state='old' WHERE state='active'`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE rings SET state='active', activated_at=now() WHERE ring_version=$1`,
			newVersion); err != nil {
			return err
		}
		return nil
	})
}

// RetireRing 把旧环标记为退役, 之后在途请求携带该版本会得到 retired=true。
func (s *Store) RetireRing(ctx context.Context, version int64) error {
	ct, err := s.pool.Exec(ctx,
		`UPDATE rings SET state='retired' WHERE ring_version=$1 AND state='old'`, version)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("ring %d not in 'old' state, refusing to retire", version)
	}
	return nil
}

// ListRings 列出全部环版本及状态(教学展示环谱系), 按版本升序。
func (s *Store) ListRings(ctx context.Context) ([]RingRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ring_version, reason, state, activated_at
		FROM rings ORDER BY ring_version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RingRecord
	for rows.Next() {
		var r RingRecord
		var state string
		if err := rows.Scan(&r.Version, &r.Reason, &state, &r.ActivatedAt); err != nil {
			return nil, err
		}
		r.State = RingState(state)
		out = append(out, r)
	}
	return out, rows.Err()
}
