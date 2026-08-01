package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type PgxPoolCollector struct {
	pool             *pgxpool.Pool
	acquiredConns    *prometheus.Desc
	idleConns        *prometheus.Desc
	maxConns         *prometheus.Desc
	totalConns       *prometheus.Desc
	newConnsCount    *prometheus.Desc
	acquireCount     *prometheus.Desc
	acquireDuration  *prometheus.Desc
	canceledAcquires *prometheus.Desc
}

func NewPgxPoolCollector(pool *pgxpool.Pool) *PgxPoolCollector {
	return &PgxPoolCollector{
		pool: pool,
		acquiredConns: prometheus.NewDesc(
			"pgxpool_acquired_conns",
			"Currently acquired connections in the pool.",
			nil, nil,
		),
		idleConns: prometheus.NewDesc(
			"pgxpool_idle_conns",
			"Currently idle connections in the pool.",
			nil, nil,
		),
		maxConns: prometheus.NewDesc(
			"pgxpool_max_conns",
			"Configured max connections",
			nil, nil,
		),
		totalConns: prometheus.NewDesc(
			"pgxpool_total_conns",
			"Total connections (idle + acquired) in the pool.",
			nil, nil,
		),
		newConnsCount: prometheus.NewDesc(
			"pgxpool_new_conns_total",
			"Total new connections established",
			nil, nil,
		),
		acquireCount: prometheus.NewDesc(
			"pgxpool_acquire_total",
			"Total successful acquires",
			nil, nil,
		),
		acquireDuration: prometheus.NewDesc(
			"pgxpool_acquire_duration_seconds_total",
			"Cumulative time spent acquiring connections",
			nil, nil,
		),
		canceledAcquires: prometheus.NewDesc(
			"pgxpool_canceled_acquire_total",
			"Total canceled acquires",
			nil, nil,
		),
	}
}

func (c *PgxPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquiredConns
	ch <- c.idleConns
	ch <- c.maxConns
	ch <- c.totalConns
	ch <- c.newConnsCount
	ch <- c.acquireCount
	ch <- c.acquireDuration
	ch <- c.canceledAcquires
}

func (c *PgxPoolCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.pool.Stat()

	ch <- prometheus.MustNewConstMetric(c.acquiredConns, prometheus.GaugeValue, float64(stats.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(stats.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(stats.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(stats.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.newConnsCount, prometheus.CounterValue, float64(stats.NewConnsCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireCount, prometheus.CounterValue, float64(stats.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireDuration, prometheus.CounterValue, stats.AcquireDuration().Seconds())
	ch <- prometheus.MustNewConstMetric(c.canceledAcquires, prometheus.CounterValue, float64(stats.CanceledAcquireCount()))
}
