package repository

import (
	"context"
	"distributed-queue/pkg/config"
	"distributed-queue/pkg/repository/metrics"
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

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
func NewPostgresPool(ctx context.Context, cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	pgxCfg, err := pgxpool.ParseConfig(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pgx config: %w", err)
	}
	pgxCfg.MaxConns = int32(cfg.MaxConns)
	pgxCfg.MinConns = int32(cfg.MinConns)
	pgxCfg.MaxConnLifetime = cfg.MaxConnLifetime
	pgxCfg.MaxConnIdleTime = cfg.MaxConnIdle
	pgxCfg.MaxConnLifetimeJitter = cfg.MaxConnLifetimeJitter
	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}
	prometheus.MustRegister(metrics.NewPgxPoolCollector(pool))
	slog.Info("successfully connected to postgres database")
	return pool, nil
}
