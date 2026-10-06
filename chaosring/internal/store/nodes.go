package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// RegisterNode 登记新节点。node_id 冲突返回 ErrNotFound 包装外的普通错误。
// 调用方必须在通过管理员令牌 + enrollment_secret 校验后才调用本方法。
func (s *Store) RegisterNode(ctx context.Context, n Node) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO nodes(node_id, address, weight, enabled, node_token, registered_ring)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		n.ID, n.Address, n.Weight, n.Enabled, n.Token, n.RegisteredRing)
	return err
}

// GetNode 按 ID 取节点。
func (s *Store) GetNode(ctx context.Context, id string) (*Node, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT node_id, address, weight, enabled, node_token, registered_ring, created_at
		FROM nodes WHERE node_id=$1`, id)
	var n Node
	if err := row.Scan(&n.ID, &n.Address, &n.Weight, &n.Enabled, &n.Token,
		&n.RegisteredRing, &n.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &n, nil
}

// NodeToken 返回节点令牌(供 gRPC 出方向调用鉴权)。
func (s *Store) NodeToken(ctx context.Context, id string) (string, error) {
	n, err := s.GetNode(ctx, id)
	if err != nil {
		return "", err
	}
	return n.Token, nil
}

// ListNodes 返回全部已登记节点(按 ID 排序, 保证展示确定性)。
func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, address, weight, enabled, node_token, registered_ring, created_at
		FROM nodes ORDER BY node_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.Address, &n.Weight, &n.Enabled, &n.Token,
			&n.RegisteredRing, &n.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// SetWeights 全量覆盖节点权重(只改登记表, 不动任何环)。
// 未知节点 ID 直接报错, 防止调用方拼写错误把流量打到不存在的节点。
func (s *Store) SetWeights(ctx context.Context, weights map[string]int) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		for id, w := range weights {
			ct, err := tx.Exec(ctx,
				`UPDATE nodes SET weight=$2 WHERE node_id=$1`, id, w)
			if err != nil {
				return err
			}
			if ct.RowsAffected() == 0 {
				return ErrNotFound
			}
		}
		return nil
	})
}

// AllNodeIDs 返回已登记节点 ID 集合, 用于注册参数校验。
func (s *Store) AllNodeIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_id FROM nodes`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}
