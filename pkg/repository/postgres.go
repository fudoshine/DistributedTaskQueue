package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresConfig struct {
	ConnString string // e.g., "postgres://user:pass@localhost:5432/dbname"
}

// RunMigrations applies all pending up migrations from db/migrations/.
// Called once on startup before the pool is used.
// migrationsPath should be "file://db/migrations" (relative to binary location).
func RunMigrations(connString, migrationsPath string) error {
	m, err := migrate.New(migrationsPath, connString)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migration failed: %w", err)
	}

	version, _, _ := m.Version()
	slog.Info("database migrations applied", "version", version)
	return nil
}

// NewPostgresPool establishes a high-performance thread-safe connection pool.
func NewPostgresPool(ctx context.Context, cfg PostgresConfig) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}
	slog.Info("successfully connected to postgres database")
	return pool, nil
}
