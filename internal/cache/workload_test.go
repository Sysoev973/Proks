package cache

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
)

func zipfWorkload(n, keys int, skew float64, seed int64) []string {
	z := rand.NewZipf(rand.New(rand.NewSource(seed)), skew, 1, uint64(keys-1))
	out := make([]string, n)
	for i := range out {
		out[i] = "item:" + strconv.FormatUint(z.Uint64(), 10)
	}
	return out
}

func scanWorkload(n, hot int, scanShare float64, seed int64) []string {
	rnd := rand.New(rand.NewSource(seed))
	out := make([]string, n)
	scanned := 0
	for i := range out {
		if rnd.Float64() < scanShare {
			out[i] = "scan:" + strconv.Itoa(scanned)
			scanned++
			continue
		}
		out[i] = "hot:" + strconv.Itoa(rnd.Intn(hot))
	}
	return out
}

func newPolicyCache(policyName string, capacity int) *InMemoryCache {
	if policyName == "tinylfu" {
		return NewTinyLFURuntimeCache(capacity, 0)
	}
	return New(Config{Capacity: capacity})
}

func replay(c *InMemoryCache, workload []string) float64 {
	ctx := context.Background()
	adm, _ := c.admission.(*TinyLFUAdmission)
	hits := 0
	for _, key := range workload {
		if adm != nil {
			adm.Filter.Increment(key)
		}
		if _, err := c.Get(ctx, key); err == nil {
			hits++
			if adm != nil {
				adm.Window.Touch(key)
			}
			continue
		}
		metrics := Metrics{Frequency: 1, CandidateFrequency: 1}
		if adm != nil {
			metrics.CandidateFrequency = max(1, adm.Filter.Estimate(key))
			if victim, ok := c.VictimFor(key); ok {
				metrics.VictimKey = victim
				metrics.VictimFrequency = adm.Frequency(victim)
				metrics.Pollution = adm.PollutantScore(key, victim)
			}
		}
		c.Set(ctx, Item{Key: key, Value: []byte("payload")}, metrics)
		if adm != nil {
			adm.Window.Touch(key)
		}
	}
	return float64(hits) / float64(len(workload))
}

func TestTinyLFUBeatsLRU(t *testing.T) {
	for _, tc := range workloads() {
		t.Run(tc.name, func(t *testing.T) {
			lru := replay(newPolicyCache("lru", tc.capacity), tc.workload)
			tiny := replay(newPolicyCache("tinylfu", tc.capacity), tc.workload)
			t.Logf("hit ratio: lru=%.4f tinylfu=%.4f", lru, tiny)
			if tiny < lru+tc.minGain {
				t.Fatalf("expected TinyLFU to beat LRU by at least %.2f: lru=%.4f tinylfu=%.4f", tc.minGain, lru, tiny)
			}
		})
	}
}

type workload struct {
	name     string
	workload []string
	capacity int
	// minGain is the hit ratio TinyLFU must add over LRU.
	minGain float64
}

// workloads have a working set much larger than the cache, where admission
// matters: a long Zipf tail, and a hot set with one-off keys streaming past.
func workloads() []workload {
	return []workload{
		{"zipf", zipfWorkload(200_000, 50_000, 1.1, 1), 1000, 0.02},
		{"scan", scanWorkload(200_000, 800, 0.5, 2), 1000, 0.10},
	}
}

func BenchmarkPolicies(b *testing.B) {
	for _, tc := range workloads() {
		for _, policy := range []string{"lru", "tinylfu"} {
			b.Run(tc.name+"/"+policy, func(b *testing.B) {
				c := newPolicyCache(policy, tc.capacity)
				replay(c, tc.workload) // warm up
				b.ReportAllocs()
				b.ResetTimer()
				var ratio float64
				for done := 0; done < b.N; done += len(tc.workload) {
					n := min(len(tc.workload), b.N-done)
					ratio = replay(c, tc.workload[:n])
				}
				b.ReportMetric(ratio, "hit-ratio")
			})
		}
	}
}

func BenchmarkLookupParallel(b *testing.B) {
	ctx := context.Background()
	c := NewTinyLFURuntimeCache(10_000, 0)
	for i := 0; i < 10_000; i++ {
		c.Set(ctx, Item{Key: "item:" + strconv.Itoa(i), Value: []byte("payload")}, Metrics{})
	}
	keys := zipfWorkload(1<<16, 10_000, 1.1, 3)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Intn(len(keys))
		for pb.Next() {
			_, _, _ = c.Lookup(ctx, keys[i%len(keys)])
			i++
		}
	})
}
