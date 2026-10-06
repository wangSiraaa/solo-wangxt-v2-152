package store

import (
	"context"
	"embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// ErrNotFound 通用未命中。
var ErrNotFound = errors.New("store: not found")

// Store 是 PostgreSQL 后端, 并发安全(内部为 pgxpool)。
type Store struct {
	pool *pgxpool.Pool
}

// Open 创建连接池并执行幂等 schema 迁移。
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect pg: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	// 文件名自带序号, embed.ReadDir 已排序
	for _, e := range entries {
		sqlBytes, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply %s: %w", e.Name(), err)
		}
	}
	return nil
}

// Close 关闭连接池。
func (s *Store) Close() { s.pool.Close() }

// Ping 连通性检查。
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// hashToPG / hashFromPG 在 uint64 与 BIGINT(有符号补码位模式)之间转换。
// 字节序规则同样适用于这里: 我们只搬运位模式, 从不在 SQL 侧做算术。
func hashToPG(h uint64) int64   { return int64(h) }
func hashFromPG(v int64) uint64 { return uint64(v) }

// 在事务中执行 fn。
func (s *Store) tx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
