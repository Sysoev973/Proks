package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/predictor"
	"proks/internal/prefetcher"
)

type denyAdmission struct{}

func (denyAdmission) ShouldStore(_ cache.Item, _ cache.Metrics) bool { return false }

type noopPrefetcher struct{}

func (noopPrefetcher) Enqueue(_ context.Context, _ string, _ prefetcher.PrefetchReason) {}

type recordingPrefetcher struct {
	keys []string
}

func (r *recordingPrefetcher) Enqueue(_ context.Context, key string, _ prefetcher.PrefetchReason) {
	r.keys = append(r.keys, key)
}

func TestBuildCacheKeyNormalizesQueryAndRoute(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/books/123?b=2&a=1&a=3", nil)
	req.Header.Set("X-User-ID", "u1")
	req.Header.Set("X-Tenant", "t1")
	req.Header.Set("X-Locale", "ru")

	key := BuildCacheKey(req)
	if key != "u1|t1|ru|book_view:123|a=1,3&b=2" {
		t.Fatalf("unexpected key: %s", key)
	}
}

func TestServeHTTPHit(t *testing.T) {
	c := cache.NewInMemoryCache(10, nil)
	_ = c.Set(context.Background(), cache.Item{Key: "u|t|l|book_view:1|", Value: []byte("cached"), StatusCode: http.StatusAccepted, ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})

	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/books/1", nil)
	req.Header.Set("X-User-ID", "u")
	req.Header.Set("X-Tenant", "t")
	req.Header.Set("X-Locale", "l")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d", rr.Code)
	}
	if rr.Header().Get("X-Cache-Status") != "HIT" {
		t.Fatalf("expected HIT got %s", rr.Header().Get("X-Cache-Status"))
	}
	if strings.TrimSpace(rr.Body.String()) != "cached" {
		t.Fatalf("unexpected body: %s", rr.Body.String())
	}
}

func TestMaybePrefetchThreshold(t *testing.T) {
	c := cache.NewInMemoryCache(10, nil)
	pf := &recordingPrefetcher{}
	pred := predictor.NewMarkov()
	pred.Update("book_view:1", "book_view:2")
	pred.Update("book_view:1", "book_view:2")
	pred.Update("book_view:1", "book_view:2")
	pred.Update("book_view:1", "book_view:3")
	pred.Update("book_view:1", "book_view:3")

	h, err := NewHandler(c, pred, pf, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}
	h.prefetchThreshold = 0.7
	h.maybePrefetch(context.Background(), "book_view:1")
	if len(pf.keys) != 0 {
		t.Fatalf("prefetch should not trigger below threshold")
	}

	pred.Update("book_view:1", "book_view:2")
	h.maybePrefetch(context.Background(), "book_view:1")
	if len(pf.keys) != 1 || pf.keys[0] != "book_view:2" {
		t.Fatalf("expected prefetch for top candidate book_view:2, got %+v", pf.keys)
	}
}

func TestServeHTTPMiss(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	c := cache.NewInMemoryCache(10, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/books/2", nil)
	req.Header.Set("X-User-ID", "u")
	req.Header.Set("X-Tenant", "t")
	req.Header.Set("X-Locale", "l")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Header().Get("X-Cache-Status") != "MISS" {
		t.Fatalf("expected MISS got %s", rr.Header().Get("X-Cache-Status"))
	}
}

func TestServeHTTPBypass(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `plain`)
	}))
	defer upstream.Close()

	c := cache.NewInMemoryCache(10, denyAdmission{})
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/books/3", nil)
	req.Header.Set("X-User-ID", "u")
	req.Header.Set("X-Tenant", "t")
	req.Header.Set("X-Locale", "l")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Header().Get("X-Cache-Status") != "BYPASS" {
		t.Fatalf("expected BYPASS got %s", rr.Header().Get("X-Cache-Status"))
	}
}

func TestServeHTTPTTLExpiry(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `fresh`)
	}))
	defer upstream.Close()

	c := cache.NewInMemoryCache(10, nil)
	_ = c.Set(context.Background(), cache.Item{
		Key:        "u|t|l|book_view:4|",
		Value:      []byte("stale"),
		StatusCode: http.StatusOK,
		ExpiresAt:  time.Now().Add(-1 * time.Second),
	}, cache.Metrics{})

	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/books/4", nil)
	req.Header.Set("X-User-ID", "u")
	req.Header.Set("X-Tenant", "t")
	req.Header.Set("X-Locale", "l")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Header().Get("X-Cache-Status") != "MISS" {
		t.Fatalf("expected MISS after ttl expiry got %s", rr.Header().Get("X-Cache-Status"))
	}
	if strings.TrimSpace(rr.Body.String()) != "fresh" {
		t.Fatalf("unexpected body: %s", rr.Body.String())
	}
}
