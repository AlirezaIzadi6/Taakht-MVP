// Package db opens Postgres pools and applies embedded SQL migrations.
package db

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool size defaults, overridable with DB_MAX_CONNS and DB_MIN_CONNS. Four services share one Postgres
// (max_connections=200 in deploy/docker-compose.yml): 4 x 20 leaves room for admin and test sessions.
const (
	DefaultMaxConns = 20
	DefaultMinConns = 2
)

// PoolSizes returns the pool bounds from DB_MAX_CONNS and DB_MIN_CONNS (defaults 20 and 2). Values must be
// positive integers (min may be 0) and min must not exceed max.
func PoolSizes() (maxConns, minConns int32, err error) {
	maxConns, minConns = DefaultMaxConns, DefaultMinConns
	if v := os.Getenv("DB_MAX_CONNS"); v != "" {
		n, perr := strconv.ParseInt(v, 10, 32)
		if perr != nil || n < 1 {
			return 0, 0, fmt.Errorf("db: DB_MAX_CONNS must be a positive integer, got %q", v)
		}
		maxConns = int32(n)
	}
	if v := os.Getenv("DB_MIN_CONNS"); v != "" {
		n, perr := strconv.ParseInt(v, 10, 32)
		if perr != nil || n < 0 {
			return 0, 0, fmt.Errorf("db: DB_MIN_CONNS must be a non-negative integer, got %q", v)
		}
		minConns = int32(n)
	}
	if minConns > maxConns {
		if os.Getenv("DB_MIN_CONNS") != "" {
			return 0, 0, fmt.Errorf("db: DB_MIN_CONNS (%d) exceeds DB_MAX_CONNS (%d)", minConns, maxConns)
		}
		minConns = maxConns // a tiny DB_MAX_CONNS without DB_MIN_CONNS must still work
	}
	return maxConns, minConns, nil
}

// Open connects to Postgres and verifies the connection. The pool is bounded by DB_MAX_CONNS (default 20) and
// DB_MIN_CONNS (default 2); a pool_max_conns / pool_min_conns in the URL itself wins over the defaults but not
// over an explicitly set environment variable.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("db: open: %w", err)
	}
	if cfg.ConnConfig.Host == "localhost" {
		// "localhost" tries ::1 first; where only 127.0.0.1 is published (Docker Desktop on Windows) every new
		// connection pays for the refused IPv6 attempt (about 4 s measured in .NET). Harmless elsewhere.
		cfg.ConnConfig.Host = "127.0.0.1"
	}
	maxConns, minConns, err := PoolSizes()
	if err != nil {
		return nil, err
	}
	if os.Getenv("DB_MAX_CONNS") != "" || !strings.Contains(url, "pool_max_conns") {
		cfg.MaxConns = maxConns
	}
	if os.Getenv("DB_MIN_CONNS") != "" || !strings.Contains(url, "pool_min_conns") {
		cfg.MinConns = min(minConns, cfg.MaxConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: open: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return pool, nil
}

// Migrate applies the *.sql files in dir of fsys in file-name order, each in its own
// transaction, and records them in schema_migrations. Already applied versions are skipped.
// An advisory lock serializes concurrent starters.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, dir string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("db: migrate: acquire: %w", err)
	}
	defer conn.Release()

	const lockKey = 7_412_001
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return fmt.Errorf("db: migrate: lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockKey) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("db: migrate: create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return fmt.Errorf("db: migrate: read %q: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		version := strings.TrimSuffix(name, ".sql")
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
			return fmt.Errorf("db: migrate: check %s: %w", version, err)
		}
		if done {
			continue
		}
		body, err := fs.ReadFile(fsys, path.Join(dir, name))
		if err != nil {
			return fmt.Errorf("db: migrate: read %s: %w", name, err)
		}
		if err := applyOne(ctx, conn.Conn(), version, string(body)); err != nil {
			return fmt.Errorf("db: migrate: %s: %w", version, err)
		}
	}
	return nil
}

func applyOne(ctx context.Context, conn *pgx.Conn, version, body string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Simple protocol lets one file hold several statements.
	if _, err := tx.Exec(ctx, body, pgx.QueryExecModeSimpleProtocol); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
