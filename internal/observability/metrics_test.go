package observability

import (
	"testing"
	"time"
)

func TestMetricsRecordsLifecycle(t *testing.T) {
	m := NewMetrics()
	m.RecordRequest("HIT", "LRU", 80*time.Millisecond)
	m.RecordRequest("MISS", "STALE", 120*time.Millisecond)
	m.RecordPrefetch(true, false, false)
	m.RecordPollution(0.23)
	m.RecordQueueDepth(3)
	m.RecordUpstreamError("timeout")

	metrics, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	if len(metrics) == 0 {
		t.Fatal("expected at least one metric family")
	}
}

func counter(t *testing.T, m *Metrics, name string, labels ...string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		var total float64
		for _, metric := range family.GetMetric() {
			match := true
			for i := 0; i+1 < len(labels); i += 2 {
				found := false
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == labels[i] && lp.GetValue() == labels[i+1] {
						found = true
					}
				}
				match = match && found
			}
			if !match {
				continue
			}
			if c := metric.GetCounter(); c != nil {
				total += c.GetValue()
			}
			if g := metric.GetGauge(); g != nil {
				total += g.GetValue()
			}
		}
		return total
	}
	return 0
}

func TestMetricsValues(t *testing.T) {
	m := NewMetrics()
	for _, status := range []string{"HIT", "HIT", "MISS", "BYPASS", "STALE", "PREFETCH"} {
		m.RecordRequest(status, "", time.Millisecond)
	}
	m.RecordPrefetchDelta(3, 2, 1, 4)
	m.RecordPrefetchDelta(0, 0, 0, 0)
	m.RecordPrefetch(true, true, true)
	m.RecordPrefetchEnqueued()
	m.RecordCollapsed()
	m.RecordTooLarge()
	m.RecordInvalidation(2, 1)
	m.RecordUpstreamError("timeout")
	m.RecordQueueDepth(7)
	m.RecordPollution(0.5)

	for name, want := range map[string]float64{
		"proxy_cache_hits_total":         2,
		"proxy_cache_misses_total":       1,
		"proxy_cache_bypasses_total":     1,
		"proxy_cache_stale_total":        1,
		"proxy_prefetch_executed_total":  3,
		"proxy_prefetch_dropped_total":   3,
		"proxy_prefetch_canceled_total":  2,
		"proxy_prefetch_failed_total":    4,
		"proxy_prefetch_total":           2,
		"proxy_upstream_collapsed_total": 1,
		"proxy_cache_too_large_total":    1,
		"proxy_upstream_errors_total":    1,
		"proxy_prefetch_queue_depth":     7,
		"proxy_cache_pollution_ratio":    0.5,
	} {
		if got := counter(t, m, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if got := counter(t, m, "proxy_invalidations_total", "kind", "tag"); got != 2 {
		t.Errorf("tag invalidations = %v, want 2", got)
	}
	if got := counter(t, m, "proxy_invalidations_total", "kind", "key"); got != 1 {
		t.Errorf("key invalidations = %v, want 1", got)
	}
}
