// Package testdb creates a throwaway, migrated Postgres database for tests.
package testdb

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/taakht/taakht/src/matching/migrations"
)

// New skips the test when TEST_DATABASE_URL is unset.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	name := "matching_test_" + uuid.NewString()[:8]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", name))
		admin.Close()
	})

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		if e.Name() != "migrations.go" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := migrations.FS.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("migration %s: %v", f, err)
		}
	}
	return pool
}
