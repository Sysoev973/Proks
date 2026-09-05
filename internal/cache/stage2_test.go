package cache

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestTinyLFUEstimate(t *testing.T) {
	filter := NewTinyLFU()
	filter.Increment("book:1")
	filter.Increment("book:1")
	filter.Increment("book:2")

	if got := filter.Estimate("book:1"); got != 2 {
		t.Fatalf("expected estimate 2 for book:1, got %d", got)
	}
	if got := filter.Estimate("book:2"); got != 1 {
		t.Fatalf("expected estimate 1 for book:2, got %d", got)
	}
}

func TestTinyLFUAging(t *testing.T) {
	filter := NewTinyLFU()
	filter.Increment("book:3")
	filter.Increment("book:3")
	filter.Increment("book:3")
	filter.Increment("book:3")
	filter.sketch.age()

	if got := filter.Estimate("book:3"); got != 2 {
		t.Fatalf("expected aged estimate 2, got %d", got)
	}
}

func TestLRUWindowPromotion(t *testing.T) {
	window := NewLRUWindow(2)
	if !window.Touch("b1") {
		t.Fatal("expected b1 to be touched")
	}
	if !window.Contains("b1") {
		t.Fatal("expected b1 to be hot in window")
	}
	if window.Len() != 1 {
		t.Fatalf("expected window len 1, got %d", window.Len())
	}
}

func TestTinyLFUAdmission(t *testing.T) {
	window := NewLRUWindow(4)
	window.Touch("book:hot")
	filter := NewTinyLFU()
	filter.Increment("book:hot")
	filter.Increment("book:hot")

	policy := &TinyLFUAdmission{
		Filter:            filter,
		Window:            window,
		MinFrequency:      1,
		MaxPollutionRatio: 0.5,
	}

	if !policy.ShouldStore(Item{Key: "book:hot"}, Metrics{CandidateFrequency: 2, Pollution: 0.1}) {
		t.Fatal("expected hot item to be admitted")
	}
	if policy.ShouldStore(Item{Key: "book:other"}, Metrics{CandidateFrequency: 1, Pollution: 0.9}) {
		t.Fatal("expected polluted item to be rejected")
	}
	if policy.ShouldStore(Item{Key: "book:cold"}, Metrics{CandidateFrequency: 1, Pollution: 0.1, VictimFrequency: 10}) {
		t.Fatal("expected cold low-frequency item to be rejected against victim")
	}
}

func buildHotSetWorkload(size int, hotSetSize int, hotRatio int) []string {
	workload := make([]string, 0, size)
	for i := 0; i < size; i++ {
		if i%100 < hotRatio {
			key := "book:" + strconv.Itoa((i%hotSetSize)+1)
			workload = append(workload, key)
			continue
		}
		workload = append(workload, "book:"+strconv.Itoa(hotSetSize+((i*17)%20)))
	}
	return workload
}

type syntheticResult struct {
	Name      string
	HitRatio  float64
	Duration  time.Duration
	Evictions int
}

func recordRuntimeAccess(c *InMemoryCache, key string) {
	if c == nil {
		return
	}
	if adm, ok := c.admission.(*TinyLFUAdmission); ok {
		if adm.Filter != nil {
			adm.Filter.Increment(key)
		}
		if adm.Window != nil {
			adm.Window.Touch(key)
		}
	}
}

func runtimeMetrics(c *InMemoryCache, key string) Metrics {
	metrics := Metrics{CurrentSize: c.Size(), CandidateFrequency: 1, Pollution: 0}
	if adm, ok := c.admission.(*TinyLFUAdmission); ok {
		if adm.Filter != nil {
			metrics.CandidateFrequency = adm.Filter.Estimate(key)
			if metrics.CandidateFrequency == 0 {
				metrics.CandidateFrequency = 1
			}
		}
		if c.Size() >= c.capacity*3/4 && c.Size() > 0 {
			if snap := c.Snapshot(context.Background()); len(snap.Keys) > 0 {
				victimKey := snap.Keys[len(snap.Keys)-1]
				metrics.VictimKey = victimKey
				metrics.VictimFrequency = adm.Frequency(victimKey)
				metrics.Pollution = adm.PollutantScore(key, victimKey)
			}
		}
	}
	return metrics
}

func runSyntheticCase(policyName string, workload []string, capacity int) syntheticResult {
	start := time.Now()
	var c *InMemoryCache

	switch policyName {
	case "tinylfu":
		filter := NewTinyLFU()
		window := NewLRUWindow(32)
		c = NewInMemoryCache(capacity, NewTinyLFUAdmission(filter, window, 2, 0.7))
	default:
		c = NewInMemoryCache(capacity, nil)
	}

	var hits int
	for _, key := range workload {
		if _, err := c.Get(context.Background(), key); err == nil {
			hits++
			recordRuntimeAccess(c, key)
			continue
		}
		recordRuntimeAccess(c, key)
		metrics := runtimeMetrics(c, key)
		c.Set(context.Background(), Item{Key: key, Value: []byte("payload")}, metrics)
	}

	elapsed := time.Since(start)
	return syntheticResult{
		Name:      policyName,
		HitRatio:  float64(hits) / float64(len(workload)),
		Duration:  elapsed,
		Evictions: 0,
	}
}

func TestSyntheticABComparison(t *testing.T) {
	workload := buildHotSetWorkload(20000, 10, 70)
	baseline := runSyntheticCase("lru", workload, 128)
	tiny := runSyntheticCase("tinylfu", workload, 128)
	if tiny.HitRatio < baseline.HitRatio*0.97 {
		t.Fatalf("expected TinyLFU hit ratio within 3%% of baseline: baseline=%.4f tinylfu=%.4f", baseline.HitRatio, tiny.HitRatio)
	}
	if baseline.Duration == 0 {
		baseline.Duration = time.Nanosecond
	}
	if tiny.Duration > baseline.Duration*5 {
		t.Fatalf("expected TinyLFU latency within 5x of baseline: baseline=%s tinylfu=%s", baseline.Duration, tiny.Duration)
	}
}

func BenchmarkSyntheticAB(b *testing.B) {
	workload := buildHotSetWorkload(5000, 12, 60)
	b.Run("baseline-lru", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runSyntheticCase("lru", workload, 128)
		}
	})
	b.Run("tinylfu-window", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runSyntheticCase("tinylfu", workload, 128)
		}
	})
}

func ExampleTinyLFUAdmission_PollutantScore() {
	filter := NewTinyLFU()
	filter.Increment("book:1")
	filter.Increment("book:1")
	filter.Increment("book:1")
	filter.Increment("book:2")
	filter.Increment("book:2")
	filter.Increment("book:2")
	policy := NewTinyLFUAdmission(filter, NewLRUWindow(16), 2, 0.5)
	fmt.Printf("%.3f\n", policy.PollutantScore("book:1", "book:2"))
	// Output: 0.500
}
