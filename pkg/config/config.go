package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Redis             RedisConfig
	Postgres          PostgresConfig
	Metrics           MetricsConfig
	Queue             QueueConfig
	Worker            WorkerConfig
	ServerAddr        string
	defaultQueueNames string
}

type RedisConfig struct {
	Address  string
	Password string
	DB       int
}

type PostgresConfig struct {
	ConnString            string
	MigrationPath         string
	MaxRetries            int
	MaxConns              int
	MinConns              int
	MaxConnIdle           time.Duration
	MaxConnLifetime       time.Duration
	MaxConnLifetimeJitter time.Duration
}

type MetricsConfig struct {
	Addr string
}

type QueueConfig struct {
	Name             string
	HoldTTL          time.Duration
	JanitorInterval  time.Duration
	DelayedPollEvery time.Duration
	DepthPollEvery   time.Duration
}
type TaskTypeConfig struct {
	Name []string
}
type WorkerConfig struct {
	Count           int
	ShutdownTimeout time.Duration
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}

func parseDuration(key, fallback string) time.Duration {
	val := getEnv(key, fallback)
	d, err := time.ParseDuration(val)
	if err != nil {
		d, _ = time.ParseDuration(fallback)
	}
	return d
}

func parseInt(key string, fallback int) int {
	val := getEnv(key, strconv.Itoa(fallback))
	i, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return i
}

func Load() (*Config, error) {
	cfg := &Config{
		Redis: RedisConfig{
			Address:  getEnv("REDIS_ADDRESS", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       parseInt("REDIS_DB", 0),
		},
		Postgres: PostgresConfig{
			ConnString:            getEnv("POSTGRES_CONN_STRING", "postgres://postgres:password@localhost:5432/task_queue?sslmode=disable"),
			MigrationPath:         getEnv("POSTGRES_MIGRATION_PATH", "file://db/migrations"),
			MaxRetries:            parseInt("MAX_RETRIES", 3),
			MaxConns:              parseInt("MAX_CONNS", 10),
			MinConns:              parseInt("MIN_CONNS", 2),
			MaxConnIdle:           parseDuration("MAX_CONN_IDLE", "5m"),
			MaxConnLifetime:       parseDuration("MAX_CONN_LIFETIME", "10m"),
			MaxConnLifetimeJitter: parseDuration("MAX_CONN_LIFETIME_JITTER", "1m"),
		},
		Metrics: MetricsConfig{
			Addr: getEnv("METRICS_ADDR", ":9090"),
		},
		Queue: QueueConfig{
			Name:             getEnv("QUEUE_NAME", "queue:video_processing"),
			HoldTTL:          parseDuration("HOLD_TTL", "20s"),
			JanitorInterval:  parseDuration("JANITOR_INTERVAL", "5s"),
			DelayedPollEvery: parseDuration("DELAYED_POLL_INTERVAL", "2s"),
			DepthPollEvery:   parseDuration("DEPTH_POLL_INTERVAL", "5s"),
		},
		Worker: WorkerConfig{
			Count:           parseInt("WORKER_COUNT", 5),
			ShutdownTimeout: parseDuration("SHUTDOWN_TIMEOUT", "30s"),
		},
		ServerAddr:        getEnv("SERVER_ADDRESS", ":8080"),
		defaultQueueNames: getEnv("DEFAULT_QUEUE_NAMES", "queue:video_processing"),
	}

	// Validate config
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Redis.Address == "" {
		return fmt.Errorf("REDIS_ADDRESS cannot be empty")
	}
	if c.Postgres.ConnString == "" {
		return fmt.Errorf("POSTGRES_CONN_STRING cannot be empty")
	}
	if c.Metrics.Addr == "" {
		return fmt.Errorf("METRICS_ADDR cannot be empty")
	}
	if c.Queue.HoldTTL <= 0 {
		return fmt.Errorf("HOLD_TTL must be positive")
	}
	if c.Queue.JanitorInterval <= 0 {
		return fmt.Errorf("JANITOR_INTERVAL must be positive")
	}
	if c.Worker.Count < 1 {
		return fmt.Errorf("WORKER_COUNT must be at least 1")
	}
	if c.Postgres.MaxRetries < 0 {
		return fmt.Errorf("MAX_RETRIES cannot be negative")
	}
	if c.Postgres.MigrationPath == "" {
		return fmt.Errorf("POSTGRES_MIGRATION_PATH cannot be empty")
	}
	if c.Postgres.MaxConnIdle <= 0 {
		return fmt.Errorf("MAX_CONN_IDLE must be positive")
	}
	if c.Postgres.MaxConnLifetime <= 0 {
		return fmt.Errorf("MAX_CONN_LIFETIME must be positive")
	}
	if c.Postgres.MaxConns < 1 {
		return fmt.Errorf("MAX_CONNS must be at least 1")
	}
	if c.Postgres.MinConns < 0 {
		return fmt.Errorf("MIN_CONNS cannot be negative")
	}
	if c.Postgres.MinConns > c.Postgres.MaxConns {
		return fmt.Errorf("MIN_CONNS cannot be greater than MAX_CONNS")
	}
	if c.Postgres.MaxConnLifetimeJitter < 0 {
		return fmt.Errorf("MAX_CONN_LIFETIME_JITTER cannot be negative")
	}
	if c.ServerAddr == "" {
		return fmt.Errorf("SERVER_ADDRESS cannot be empty")
	}
	return nil
}
