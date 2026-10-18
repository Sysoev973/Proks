package proxy

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/predictor"
)

// refreshSetup stores /hot as if it was fetched 95s ago with 5s of freshness
// left, i.e. in the last 5% of its lifetime.
func refreshSetup(t *testing.T, policy CachePolicy) (*Handler, *atomic.Int32, cache.Item) {
	t.Helper()
	var calls atomic.Int32
	upstream := ordersUpstream(t, &calls)
	c := cache.NewTinyLFURuntimeCache(32, 1<<20)
	h, err := NewHandlerWithPolicy(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL, policy)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	item := cache.Item{
		Key:        keyFor("/hot"),
		Value:      []byte("old"),
		Headers:    http.Header{},
		StatusCode: http.StatusOK,
		StoredAt:   now.Add(-95 * time.Second),
		ExpiresAt:  now.Add(5 * time.Second),
	}
	if !c.Set(context.Background(), item, cache.Metrics{}) {
		t.Fatal("item not stored")
	}
	return h, &calls, item
}

func refreshPolicy() CachePolicy {
	policy := DefaultCachePolicy()
	policy.RefreshAhead = 0.1
	return policy
}

func TestRefreshAheadRenewsHotKeyBeforeExpiry(t *testing.T) {
	h, calls, old := refreshSetup(t, refreshPolicy())
	for i := 0; i < 3; i++ {
		if got := cacheStatus(get(t, h, "/hot")); got != "HIT" {
			t.Fatalf("request %d: expected HIT, got %s", i, got)
		}
	}
	waitFor(t, func() bool {
		item, err := h.cache.Get(context.Background(), old.Key)
		return err == nil && item.ExpiresAt.After(old.ExpiresAt)
	})
	for i := 0; i < 3; i++ {
		if got := cacheStatus(get(t, h, "/hot")); got != "HIT" {
			t.Fatalf("after refresh: expected HIT, got %s", got)
		}
	}
	waitFor(t, func() bool {
		_, busy := h.refreshing.Load(old.Key)
		return !busy
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want exactly one refresh", got)
	}
	reg := h.MetricsRegistry()
	if got := labeledCounter(t, reg, "proxy_upstream_requests_total", "kind", "revalidate"); got != 1 {
		t.Fatalf("revalidate upstream requests = %v, want 1", got)
	}
	if got := counterValue(t, reg, "proxy_cache_refresh_ahead_total"); got != 1 {
		t.Fatalf("proxy_cache_refresh_ahead_total = %v, want 1", got)
	}
}

func TestRefreshAheadSkipsColdKey(t *testing.T) {
	h, calls, _ := refreshSetup(t, refreshPolicy())
	if got := cacheStatus(get(t, h, "/hot")); got != "HIT" {
		t.Fatalf("expected HIT, got %s", got)
	}
	if got := counterValue(t, h.MetricsRegistry(), "proxy_cache_refresh_ahead_total"); got != 0 || calls.Load() != 0 {
		t.Fatalf("a key with one access was refreshed (%v refreshes, %d calls)", got, calls.Load())
	}
}

func TestRefreshAheadSkipsEarlyHits(t *testing.T) {
	policy := refreshPolicy()
	policy.RefreshAhead = 0.01 // 1s of a 100s lifetime; 5s are left
	h, calls, _ := refreshSetup(t, policy)
	for i := 0; i < 5; i++ {
		get(t, h, "/hot")
	}
	if got := counterValue(t, h.MetricsRegistry(), "proxy_cache_refresh_ahead_total"); got != 0 || calls.Load() != 0 {
		t.Fatalf("refreshed before the last share of the lifetime (%v refreshes, %d calls)", got, calls.Load())
	}
}

func TestRefreshAheadRespectsBudget(t *testing.T) {
	h, _, _ := refreshSetup(t, refreshPolicy())
	h.pausePrefetch()
	for i := 0; i < 5; i++ {
		get(t, h, "/hot")
	}
	if got := counterValue(t, h.MetricsRegistry(), "proxy_cache_refresh_ahead_total"); got != 0 {
		t.Fatalf("refresh started while background requests are paused: %v", got)
	}
}

func TestRefreshAheadHasItsOwnBudget(t *testing.T) {
	h, _, _ := refreshSetup(t, refreshPolicy())
	h.SetPrefetchBudget(1)
	h.SetRefreshBudget(1)
	if !h.refreshAllowed() || h.refreshAllowed() {
		t.Fatal("expected a refresh budget of one request per second")
	}
	if !h.prefetchAllowed() {
		t.Fatal("refreshes used up the prefetch budget")
	}
	h.SetRefreshBudget(0)
	for i := 0; i < 100; i++ {
		if !h.refreshAllowed() {
			t.Fatal("expected no limit with a zero refresh budget")
		}
	}
	if h.prefetchAllowed() {
		t.Fatal("refreshes refilled the prefetch budget")
	}
}

func TestRefreshAheadDisabledByDefault(t *testing.T) {
	if p := DefaultCachePolicy(); p.RefreshAhead != 0 {
		t.Fatalf("RefreshAhead default = %v, want 0", p.RefreshAhead)
	}
	h, calls, _ := refreshSetup(t, DefaultCachePolicy())
	for i := 0; i < 5; i++ {
		get(t, h, "/hot")
	}
	if got := counterValue(t, h.MetricsRegistry(), "proxy_cache_refresh_ahead_total"); got != 0 || calls.Load() != 0 {
		t.Fatalf("refresh-ahead ran with the default policy (%v refreshes, %d calls)", got, calls.Load())
	}
}

func TestRefreshAheadIsCapped(t *testing.T) {
	p := CachePolicy{RefreshAhead: 0.9}.normalized()
	if p.RefreshAhead != maxRefreshAhead {
		t.Fatalf("RefreshAhead = %v, want it capped at %v", p.RefreshAhead, maxRefreshAhead)
	}
	if p := (CachePolicy{RefreshAhead: -1}).normalized(); p.RefreshAhead != 0 {
		t.Fatalf("negative RefreshAhead = %v, want 0", p.RefreshAhead)
	}
}
