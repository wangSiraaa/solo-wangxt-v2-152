package store

import "context"

// RegisterWrite 登记一次正式写并分配该键的单调新版本号。
// 首见的键以 owner/ringVersion 落册; 之后仅推进版本与墓碑标记。
// 属主在迁移提交前保持为旧环节点, 由 ReassignOwner/提交逻辑统一改派。
func (s *Store) RegisterWrite(ctx context.Context, key string, h uint64, owner string, ringVersion int64, deleted bool) (int64, error) {
	var version int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO key_registry(key, key_hash, current_owner, first_seen, version, deleted)
		VALUES ($1,$2,$3,$4,1,$5)
		ON CONFLICT (key) DO UPDATE
		  SET version    = key_registry.version + 1,
		      deleted    = EXCLUDED.deleted,
		      updated_at = now()
		RETURNING version`,
		key, hashToPG(h), owner, ringVersion, deleted).Scan(&version)
	return version, err
}
