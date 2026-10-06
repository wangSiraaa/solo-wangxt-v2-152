package store

import "context"

// TruncateForTests 清空全部编排数据表。仅供集成测试使用。
func (s *Store) TruncateForTests(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		TRUNCATE migration_items, migrations, key_registry,
		          vnodes, ring_weights, rings, nodes RESTART IDENTITY CASCADE`)
	return err
}
