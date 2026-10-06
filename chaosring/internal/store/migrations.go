package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateMigration 创建迁移单及全部明细(状态 pending), 整体一个事务。
func (s *Store) CreateMigration(ctx context.Context, m *Migration) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO migrations(id, from_ring, to_ring, state)
			VALUES ($1,$2,$3,$4)`,
			m.ID, m.FromRing, m.ToRing, m.State); err != nil {
			return err
		}
		for _, it := range m.Items {
			if _, err := tx.Exec(ctx, `
				INSERT INTO migration_items(migration_id, key, key_hash, from_node, to_node, state)
				VALUES ($1,$2,$3,$4,$5,$6)`,
				m.ID, it.Key, hashToPG(it.KeyHash), it.FromNode, it.ToNode, it.State); err != nil {
				return err
			}
		}
		return nil
	})
}

// ActiveUncommittedMigration 返回当前"未终结"的迁移(replicating 等),
// 用于保证同一时刻只有一条迁移在途。committed/aborted 不算在途。
func (s *Store) ActiveUncommittedMigration(ctx context.Context) (*Migration, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT id FROM migrations
		WHERE state IN ('planned','replicating','replicated','failed')
		ORDER BY created_at DESC LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.GetMigration(ctx, id)
}

// GetMigration 读取迁移单、明细与计数。
func (s *Store) GetMigration(ctx context.Context, id string) (*Migration, error) {
	m := &Migration{ID: id}
	var state string
	err := s.pool.QueryRow(ctx, `
		SELECT from_ring, to_ring, state, created_at, updated_at
		FROM migrations WHERE id=$1`, id).
		Scan(&m.FromRing, &m.ToRing, &state, &m.CreatedAt, &m.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: migration %s", ErrNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	m.State = state

	rows, err := s.pool.Query(ctx, `
		SELECT key, key_hash, from_node, to_node, state, last_error, updated_at
		FROM migration_items WHERE migration_id=$1
		ORDER BY key_hash, key`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var it MigrationItem
		var h int64
		if err := rows.Scan(&it.Key, &h, &it.FromNode, &it.ToNode,
			&it.State, &it.LastError, &it.UpdatedAt); err != nil {
			return nil, err
		}
		it.KeyHash = hashFromPG(h)
		m.Items = append(m.Items, it)
	}
	return m, rows.Err()
}

// SetMigrationState 推进迁移单状态(updated_at 自动刷新)。
func (s *Store) SetMigrationState(ctx context.Context, id, state string) error {
	ct, err := s.pool.Exec(ctx,
		`UPDATE migrations SET state=$2, updated_at=now() WHERE id=$1`, id, state)
	if err != nil {
		return err
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("%w: migration %s", ErrNotFound, id)
	}
	return nil
}

// SetItemState 更新单条迁移明细状态。
func (s *Store) SetItemState(ctx context.Context, migrationID, key, state, lastErr string) error {
	ct, err := s.pool.Exec(ctx, `
		UPDATE migration_items SET state=$3, last_error=$4, updated_at=now()
		WHERE migration_id=$1 AND key=$2`, migrationID, key, state, lastErr)
	if err != nil {
		return err
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("migration item %s/%s not found", migrationID, key)
	}
	return nil
}

// ItemsInState 取出某迁移中处于指定状态(可多个)的明细, 供重试/续传。
func (s *Store) ItemsInState(ctx context.Context, migrationID string, states ...string) ([]MigrationItem, error) {
	if len(states) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT key, key_hash, from_node, to_node, state, last_error, updated_at
		FROM migration_items
		WHERE migration_id=$1 AND state = ANY($2)
		ORDER BY key_hash, key`, migrationID, states)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MigrationItem
	for rows.Next() {
		var it MigrationItem
		var h int64
		if err := rows.Scan(&it.Key, &h, &it.FromNode, &it.ToNode,
			&it.State, &it.LastError, &it.UpdatedAt); err != nil {
			return nil, err
		}
		it.KeyHash = hashFromPG(h)
		out = append(out, it)
	}
	return out, rows.Err()
}

// Counters 返回迁移明细各状态计数。
func (s *Store) MigrationCounters(ctx context.Context, id string) (Counters, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT state, count(*) FROM migration_items WHERE migration_id=$1 GROUP BY state`, id)
	if err != nil {
		return Counters{}, err
	}
	defer rows.Close()
	var c Counters
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return Counters{}, err
		}
		c.Total += n
		switch st {
		case ItemPending, ItemReplicating, ItemFailed:
			c.Pending += n
		case ItemReplicated:
			c.Replicated += n
		case ItemPromoted:
			c.Promoted += n
		}
		if st == ItemFailed {
			c.Failed += n
		}
	}
	// 语义澄清: "已完成切换"看 promoted, 而 replicated 只是复制落盘。
	return c, rows.Err()
}

// MarkAllPromoted 提交成功后把本迁移全部 replicated 条目标 promoted。
func (s *Store) MarkAllPromoted(ctx context.Context, migrationID string) (int64, error) {
	ct, err := s.pool.Exec(ctx, `
		UPDATE migration_items SET state='promoted', updated_at=now()
		WHERE migration_id=$1 AND state='replicated'`, migrationID)
	return ct.RowsAffected(), err
}

// ListMigrations 返回全部迁移单(不含明细), 按创建顺序升序。
func (s *Store) ListMigrations(ctx context.Context) ([]Migration, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, from_ring, to_ring, state, created_at, updated_at
		FROM migrations ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Migration
	for rows.Next() {
		var m Migration
		var state string
		if err := rows.Scan(&m.ID, &m.FromRing, &m.ToRing, &state,
			&m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		m.State = state
		out = append(out, m)
	}
	return out, rows.Err()
}

// MigrationByToRing 返回以某环为目标的迁移(旧环退役时据此找旧主键)。
func (s *Store) MigrationByToRing(ctx context.Context, ring int64) (*Migration, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`SELECT id FROM migrations WHERE to_ring=$1 ORDER BY created_at DESC LIMIT 1`,
		ring).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: migration for ring %d", ErrNotFound, ring)
	}
	if err != nil {
		return nil, err
	}
	return s.GetMigration(ctx, id)
}
