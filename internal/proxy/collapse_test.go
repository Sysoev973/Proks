package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/predictor"
)

// gatedUpstream counts calls and holds every response until release is closed.
func gatedUpstream(t *testing.T, body string, calls *atomic.Int32, release <-chan struct{}) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentMissesShareOneUpstreamCall(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := gatedUpstream(t, "book", &calls, release)
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	const n = 50
	recorders := make([]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i := range recorders {
		recorders[i] = httptest.NewRecorder()
		wg.Add(1)
		go func(rec *httptest.ResponseRecorder) {
			defer wg.Done()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
		}(recorders[i])
	}
	waitFor(t, func() bool { return calls.Load() == 1 })
	time.Sleep(50 * time.Millisecond) // let the remaining requests join the flight
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 upstream call for %d concurrent misses, got %d", n, got)
	}
	collapsed := 0
	for _, rec := range recorders {
		if rec.Code != http.StatusOK || rec.Body.String() != "book" {
			t.Fatalf("unexpected response %d %q", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("X-Cache-Reason") == "COLLAPSED" {
			collapsed++
		}
	}
	if collapsed != n-1 {
		t.Fatalf("expected %d collapsed followers, got %d", n-1, collapsed)
	}
}

func TestCanceledLeaderDoesNotFailFollowers(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := gatedUpstream(t, "book", &calls, release)
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/books/1", nil).WithContext(ctx))
	}()
	waitFor(t, func() bool { return calls.Load() == 1 })
	cancel()
	<-leaderDone

	follower := httptest.NewRecorder()
	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		h.ServeHTTP(follower, httptest.NewRequest(http.MethodGet, "/books/1", nil))
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-followerDone

	if follower.Code != http.StatusOK || follower.Body.String() != "book" {
		t.Fatalf("follower failed after leader canceled: %d %q", follower.Code, follower.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected the follower to reuse the in-flight call, got %d calls", got)
	}
}

func seedStale(t *testing.T, c cache.Cache, path string, staleFor time.Duration) {
	t.Helper()
	now := time.Now()
	c.Set(context.Background(), cache.Item{
		Key:        BuildCacheKey(httptest.NewRequest(http.MethodGet, path, nil)),
		Value:      []byte("old"),
		StatusCode: http.StatusOK,
		ExpiresAt:  now.Add(-time.Millisecond),
		StaleUntil: now.Add(staleFor),
	}, cache.Metrics{})
}

func TestStaleWhileRevalidateServesStaleAndRefreshesOnce(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := gatedUpstream(t, "new", &calls, release)
	c := cache.NewInMemoryCache(32, nil)
	policy := DefaultCachePolicy()
	policy.StaleWhileRevalidate = time.Minute
	h, err := NewHandlerWithPolicy(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL, policy)
	if err != nil {
		t.Fatal(err)
	}
	seedStale(t, c, "/books/1", time.Minute)

	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
		if rec.Body.String() != "old" || rec.Header().Get("X-Cache-Status") != "STALE" {
			t.Fatalf("expected immediate stale response, got %s %q", rec.Header().Get("X-Cache-Status"), rec.Body.String())
		}
	}
	waitFor(t, func() bool { return calls.Load() == 1 })
	close(release)
	waitFor(t, func() bool {
		item, err := c.Get(context.Background(), BuildCacheKey(httptest.NewRequest(http.MethodGet, "/books/1", nil)))
		return err == nil && string(item.Value) == "new"
	})
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one background refresh for 10 stale hits, got %d", got)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
	if rec.Body.String() != "new" || rec.Header().Get("X-Cache-Status") != "HIT" {
		t.Fatalf("expected refreshed HIT, got %s %q", rec.Header().Get("X-Cache-Status"), rec.Body.String())
	}
	if got := labeledCounter(t, h.MetricsRegistry(), "proxy_upstream_requests_total", "kind", "revalidate"); got != 1 {
		t.Fatalf("revalidate upstream requests = %v, want 1", got)
	}
}

func TestStaleIfErrorFallsBackOnUpstreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	c := cache.NewInMemoryCache(32, nil)
	policy := DefaultCachePolicy()
	policy.StaleIfError = time.Minute
	h, err := NewHandlerWithPolicy(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL, policy)
	if err != nil {
		t.Fatal(err)
	}

	seedStale(t, c, "/books/1", time.Minute)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "old" || rec.Header().Get("X-Cache-Reason") != "STALE_IF_ERROR" {
		t.Fatalf("expected stale fallback on 5xx, got %d %s %q", rec.Code, rec.Header().Get("X-Cache-Reason"), rec.Body.String())
	}

	upstream.Close()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "old" {
		t.Fatalf("expected stale fallback when upstream is down, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestExpiredItemWithoutStaleWindowIsAMiss(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	close(release)
	upstream := gatedUpstream(t, "new", &calls, release)
	c := cache.NewInMemoryCache(32, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	seedStale(t, c, "/books/1", time.Minute)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
	if rec.Body.String() != "new" || rec.Header().Get("X-Cache-Status") != "MISS" {
		t.Fatalf("default policy must not serve stale, got %s %q", rec.Header().Get("X-Cache-Status"), rec.Body.String())
	}
}

func TestStaleWindowsFromCacheControl(t *testing.T) {
	policy := DefaultCachePolicy()
	policy.StaleWhileRevalidate = time.Second
	policy.StaleIfError = time.Second

	headers := http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=30, stale-if-error=600"}}
	if swr, sie := policy.staleWindows(headers); swr != 30*time.Second || sie != 10*time.Minute {
		t.Fatalf("expected directives to override policy, got swr=%s sie=%s", swr, sie)
	}
	headers = http.Header{"Cache-Control": {"stale-while-revalidate=30, must-revalidate"}}
	if swr, sie := policy.staleWindows(headers); swr != 0 || sie != 0 {
		t.Fatalf("must-revalidate must disable stale serving, got swr=%s sie=%s", swr, sie)
	}
	if swr, sie := policy.staleWindows(http.Header{}); swr != time.Second || sie != time.Second {
		t.Fatalf("expected policy defaults, got swr=%s sie=%s", swr, sie)
	}
}
