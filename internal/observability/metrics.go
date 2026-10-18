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
	prefetchHits     prometheus.Counter
	misses           prometheus.Counter
	bypasses         prometheus.Counter
	stale            prometheus.Counter
	collapsed        prometheus.Counter
	tooLarge         prometheus.Counter
	prefetches       prometheus.Counter
	latency          prometheus.Histogram
	pollution        prometheus.Gauge
	queueDepth       prometheus.Gauge
	prefetchDropped  prometheus.Counter
	prefetchCanceled prometheus.Counter
	upstreamErrors   *prometheus.CounterVec
	prefetchExecuted prometheus.Counter
	prefetchFailed   prometheus.Counter
	invalidations    *prometheus.CounterVec
	upstreamRequests *prometheus.CounterVec
	refreshAhead     prometheus.Counter
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		hits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_hits_total",
			Help: "Total cache hits.",
		}),
		prefetchHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_prefetch_hits_total",
			Help: "Total first hits on entries loaded by prefetch (not included in proxy_cache_hits_total).",
		}),
		misses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_misses_total",
			Help: "Total cache misses.",
		}),
		bypasses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_bypasses_total",
			Help: "Total bypassed writes due to admission policy.",
		}),
		stale: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_stale_total",
			Help: "Total responses served stale (stale-while-revalidate or stale-if-error).",
		}),
		tooLarge: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_cache_too_large_total",
			Help: "Total cacheable responses not stored because they exceed the per-shard byte budget.",
		}),
		collapsed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proxy_upstream_collapsed_total",
			Help: "Total requests that shared another request's in-flight upstream fetch.",
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
		invalidations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_invalidations_total",
			Help: "Total invalidation targets applied, labeled by kind (tag or key).",
		}, []string{"kind"}),
		upstreamRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proxy_upstream_requests_total",
			Help: "Total requests sent upstream, labeled by kind (client, prefetch or revalidate).",
		}, []string{"kind"}),
	}
	m.refreshAhead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "proxy_cache_refresh_ahead_total",
		Help: "Total background refreshes of hot entries started before they expired.",
	})
	reg.MustRegister(m.refreshAhead, m.upstreamRequests, m.prefetchHits, m.hits, m.misses, m.bypasses, m.stale, m.collapsed, m.tooLarge, m.prefetches, m.latency, m.pollution, m.queueDepth, m.prefetchDropped, m.prefetchCanceled, m.upstreamErrors, m.prefetchExecuted, m.prefetchFailed, m.invalidations)
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
	case "PREFETCH":
		m.prefetchHits.Inc()
	case "MISS":
		m.misses.Inc()
	case "BYPASS":
		m.bypasses.Inc()
	case "STALE":
		m.stale.Inc()
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

func (m *Metrics) RecordCollapsed() {
	m.collapsed.Inc()
}

func (m *Metrics) RecordTooLarge() {
	m.tooLarge.Inc()
}

func (m *Metrics) RecordInvalidation(tags, keys int) {
	m.invalidations.WithLabelValues("tag").Add(float64(tags))
	m.invalidations.WithLabelValues("key").Add(float64(keys))
}

func (m *Metrics) RecordRefreshAhead() {
	m.refreshAhead.Inc()
}

func (m *Metrics) RecordUpstreamRequest(kind string) {
	m.upstreamRequests.WithLabelValues(kind).Inc()
}
