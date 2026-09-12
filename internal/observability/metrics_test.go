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
