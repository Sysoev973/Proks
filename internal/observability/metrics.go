package observability

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	registry         *prometheus.Registry
	mu               sync.Mutex
	hits             prometheus.Counter
	misses           prometheus.Counter
	bypasses         prometheus.Counter
	prefetches       prometheus.Counter
	latency          prometheus.Histogram
	pollution        prometheus.Gauge
	queueDepth       prometheus.Gauge
	prefetchDropped  prometheus.Counter
	prefetchCanceled prometheus.Counter
	upstreamErrors   *prometheus.CounterVec
	prefetchExecuted prometheus.Counter
	prefetchFailed   prometheus.Counter
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		hits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_hits_total",
			Help: "Total cache hits.",
		}),
		misses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_misses_total",
			Help: "Total cache misses.",
		}),
		bypasses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_bypasses_total",
			Help: "Total bypassed writes due to admission policy.",
		}),
		prefetches: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_total",
			Help: "Total prefetch decisions enqueued.",
		}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "proxy_request_latency_seconds",
			Help:    "Request latency distribution in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		pollution: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "proxy_cache_pollution_ratio",
			Help: "Latest pollution estimate in the cache.",
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "proxy_prefetch_queue_depth",
			Help: "Current depth of the prefetch queue.",
		}),
		prefetchDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_dropped_total",
			Help: "Total dropped prefetch jobs.",
		}),
		prefetchCanceled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_canceled_total",
			Help: "Total canceled prefetch jobs.",
		}),
		upstreamErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_upstream_errors_total",
			Help: "Total upstream request errors, labeled by kind.",
		}, []string{"kind"}),
		prefetchExecuted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_executed_total",
			Help: "Total prefetch jobs that actually completed and warmed the cache.",
		}),
		prefetchFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_prefetch_failed_total",
			Help: "Total prefetch jobs that failed (non-timeout error).",
		}),
	}
	reg.MustRegister(m.hits, m.misses, m.bypasses, m.prefetches, m.latency, m.pollution, m.queueDepth, m.prefetchDropped, m.prefetchCanceled, m.upstreamErrors, m.prefetchExecuted, m.prefetchFailed)
	return m
}

func (m *Metrics) RecordPrefetchDelta(executedDelta, droppedDelta, canceledDelta, failedDelta uint64) {
	if executedDelta > 0 {
		m.prefetchExecuted.Add(float64(executedDelta))
	}
	if droppedDelta > 0 {
		m.prefetchDropped.Add(float64(droppedDelta))
	}
	if canceledDelta > 0 {
		m.prefetchCanceled.Add(float64(canceledDelta))
	}
	if failedDelta > 0 {
		m.prefetchFailed.Add(float64(failedDelta))
	}
}

func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func (m *Metrics) RecordRequest(status string, reason string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch status {
	case "HIT":
		m.hits.Inc()
	case "MISS":
		m.misses.Inc()
	case "BYPASS":
		m.bypasses.Inc()
	}
	m.latency.Observe(d.Seconds())
	_ = reason
}

func (m *Metrics) RecordPrefetch(enqueued bool, canceled bool, dropped bool) {
	if enqueued {
		m.prefetches.Inc()
	}
	if canceled {
		m.prefetchCanceled.Inc()
	}
	if dropped {
		m.prefetchDropped.Inc()
	}
}

func (m *Metrics) RecordPollution(ratio float64) {
	m.pollution.Set(ratio)
}

func (m *Metrics) RecordQueueDepth(depth int) {
	m.queueDepth.Set(float64(depth))
}

func (m *Metrics) RecordUpstreamError(kind string) {
	m.upstreamErrors.WithLabelValues(kind).Inc()
}

func (m *Metrics) RecordPrefetchEnqueued() {
	m.prefetches.Inc()
}
