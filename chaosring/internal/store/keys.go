package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// UpsertKeyWrite 登记一次正式写(Put)。版本单调: 只有新版本才推进,
// 防止旧环在途写把新值覆盖回去。返回受影响行数(0=被版本判定拒绝)。
func (s *Store) UpsertKeyWrite(ctx context.Context, key string, h uint64, owner string, ringVersion int64, version uint64, deleted bool) (int64, error) {
	ct, err := s.pool.Exec(ctx, `
		INSERT INTO key_registry(key, key_hash, current_owner, first_seen, version, deleted)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (key) DO UPDATE
		  SET current_owner = EXCLUDED.current_owner,
		      version       = EXCLUDED.version,
		      deleted       = EXCLUDED.deleted,
		      updated_at    = now()
		  WHERE EXCLUDED.version >= key_registry.version`,
		key, hashToPG(h), owner, ringVersion, int64(version), deleted)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

// ReassignOwner 切换提交后批量改属主(与版本推进无关, 提交时使用)。
func (s *Store) ReassignOwner(ctx context.Context, ownerByKey map[string]string, newRing int64) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		for k, owner := range ownerByKey {
			ct, err := tx.Exec(ctx,
				`UPDATE key_registry SET current_owner=$2, updated_at=now() WHERE key=$1`,
				k, owner)
			if err != nil {
				return err
			}
			if ct.RowsAffected() != 1 {
				return ErrNotFound
			}
			_ = newRing
		}
		return nil
	})
}

// GetKey 读取键登记信息。
func (s *Store) GetKey(ctx context.Context, key string) (*KeyRec, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT key, key_hash, current_owner, first_seen, version, deleted, updated_at
		FROM key_registry WHERE key=$1`, key)
	var k KeyRec
	var h int64
	if err := row.Scan(&k.Key, &h, &k.CurrentOwner, &k.FirstSeen,
		&k.Version, &k.Deleted, &k.UpdatedAt); err != nil {
		return nil, err
	}
	k.Hash = hashFromPG(h)
	return &k, nil
}

// AllKeys 返回注册表中的全部键(含已删除墓碑键, 迁移必须连删除一起传播)。
func (s *Store) AllKeys(ctx context.Context) ([]KeyRec, error) {
	return s.queryKeys(ctx, `
		SELECT key, key_hash, current_owner, first_seen, version, deleted, updated_at
		FROM key_registry ORDER BY key`)
}

// KeysByPrefix 按前缀采样(分布统计 API 使用)。
func (s *Store) KeysByPrefix(ctx context.Context, prefix string, limit uint32) ([]KeyRec, error) {
	if limit == 0 {
		limit = 100000
	}
	return s.queryKeys(ctx, `
		SELECT key, key_hash, current_owner, first_seen, version, deleted, updated_at
		FROM key_registry WHERE key LIKE $1 ORDER BY key LIMIT $2`, prefix+"%", limit)
}

func (s *Store) queryKeys(ctx context.Context, sql string, args ...any) ([]KeyRec, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyRec
	for rows.Next() {
		var k KeyRec
		var h int64
		if err := rows.Scan(&k.Key, &h, &k.CurrentOwner, &k.FirstSeen,
			&k.Version, &k.Deleted, &k.UpdatedAt); err != nil {
			return nil, err
		}
		k.Hash = hashFromPG(h)
		out = append(out, k)
	}
	return out, rows.Err()
}

// CountLiveKeys 正式(非墓碑)键数量。
func (s *Store) CountLiveKeys(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM key_registry WHERE NOT deleted`).Scan(&n)
	return n, err
}
