package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"proks/internal/cache"
	"proks/internal/predictor"
)

// libraryRoutes extends bookRoutes with the routes a card leads to.
var libraryRoutes = []Route{
	{Match: "/books/{id}", Tags: []string{"book:{id}"}},
	{Match: "/books/{id}/authors", Tags: []string{"book:{id}"}},
	{Match: "/books", Tags: []string{"list:books"}},
	{Match: "/genres/{id}", Tags: []string{"genre:{id}"}},
}

func libraryPolicy() CachePolicy {
	policy := DefaultCachePolicy()
	policy.Routes = libraryRoutes
	return policy
}

func keyFor(path string) string {
	return BuildCacheKey(httptest.NewRequest(http.MethodGet, path, nil))
}

func TestRouteTemplateMatchAndExpand(t *testing.T) {
	routes, err := compileRoutes(libraryRoutes)
	if err != nil {
		t.Fatal(err)
	}
	match, params, ok := routes.template("/books/42/authors")
	if !ok || match != "/books/{id}/authors" || params["id"] != "42" {
		t.Fatalf("template(/books/42/authors) = %q %v %v", match, params, ok)
	}
	if match, params, ok := routes.template("/books"); !ok || match != "/books" || len(params) != 0 {
		t.Fatalf("template(/books) = %q %v %v", match, params, ok)
	}
	if _, _, ok := routes.template("/unknown/1"); ok {
		t.Fatal("a path without a route must not match")
	}

	route, ok := routes.byMatch("/books/{id}/authors")
	if !ok {
		t.Fatal("byMatch did not find a configured route")
	}
	if got, ok := route.expand(map[string]string{"id": "7"}); !ok || got != "/books/7/authors" {
		t.Fatalf("expand = %q %v", got, ok)
	}
	if _, ok := route.expand(map[string]string{"other": "7"}); ok {
		t.Fatal("expand must fail when a placeholder has no value")
	}
	if _, ok := routes.byMatch("/missing/{id}"); ok {
		t.Fatal("byMatch found a route that is not configured")
	}
	list, _ := routes.byMatch("/books")
	if got, ok := list.expand(nil); !ok || got != "/books" {
		t.Fatalf("expand of a route without placeholders = %q %v", got, ok)
	}
}

// learnChain plays the card -> next chain for the given books, the way a
// client sends it (Referer on the card).
func learnChain(t *testing.T, h http.Handler, next string, books ...int) {
	t.Helper()
	for _, id := range books {
		card := fmt.Sprintf("/books/%d", id)
		get(t, h, card)
		get(t, h, fmt.Sprintf(next, id), "Referer", "http://proxy"+card)
	}
}

func TestTemplatePrefetchWarmsUnseenResource(t *testing.T) {
	var calls atomic.Int32
	upstream := ordersUpstream(t, &calls)
	h, pf, _ := prefetchSetup(t, upstream.URL, libraryPolicy())

	learnChain(t, h, "/books/%d/authors", 1, 2, 3)
	// Books 2 and 3 already get their authors prefetched from the template.
	waitFor(t, func() bool {
		s := pf.Snapshot()
		return s.Enqueued == s.Executed+s.Failed+s.Dropped+s.Canceled
	})
	reg := h.MetricsRegistry()
	prefetchBefore := labeledCounter(t, reg, "proxy_upstream_requests_total", "kind", "prefetch")
	executedBefore := pf.Snapshot().Executed

	get(t, h, "/books/99")
	waitFor(t, func() bool { return pf.Snapshot().Executed == executedBefore+1 })
	if got := labeledCounter(t, reg, "proxy_upstream_requests_total", "kind", "prefetch") - prefetchBefore; got != 1 {
		t.Fatalf("prefetch upstream requests for book 99 = %v, want 1", got)
	}
	rec := get(t, h, "/books/99/authors", "Referer", "http://proxy/books/99")
	if cacheStatus(rec) != "PREFETCH" {
		t.Fatalf("authors of an unseen book: expected PREFETCH, got %s", cacheStatus(rec))
	}
	if !strings.Contains(rec.Body.String(), "/books/99/authors") {
		t.Fatalf("prefetched the wrong route: %s", rec.Body.String())
	}
}

func TestTemplateTransitionRequiresSameParams(t *testing.T) {
	upstream := ordersUpstream(t, nil)
	pf := &recordingPrefetcher{}
	h, err := NewHandlerWithPolicy(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), pf, upstream.URL, libraryPolicy())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		get(t, h, fmt.Sprintf("/books/%d", i))
		get(t, h, "/genres/7", "Referer", fmt.Sprintf("http://proxy/books/%d", i))
	}
	pf.keys = nil
	get(t, h, "/books/99")
	if len(pf.keys) != 0 {
		t.Fatalf("/books/{id} -> /genres/7 must not be generalized, prefetched %v", pf.keys)
	}
	if got := h.routePredictor.Predict("/books/{id}"); len(got) != 0 {
		t.Fatalf("template predictor learned %v", got)
	}

	// A route without placeholders needs no values and is generalized.
	get(t, h, "/books", "Referer", "http://proxy/books/1")
	if got := h.routePredictor.Predict("/books/{id}"); len(got) != 1 || got[0].Key != "/books" {
		t.Fatalf("transition to /books not generalized: %v", got)
	}
}

func TestConcretePredictionWinsOverTemplate(t *testing.T) {
	upstream := ordersUpstream(t, nil)
	pf := &recordingPrefetcher{}
	h, err := NewHandlerWithPolicy(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), pf, upstream.URL, libraryPolicy())
	if err != nil {
		t.Fatal(err)
	}
	learnChain(t, h, "/books/%d/authors", 1, 2, 3)
	// Book 5 is always followed by a genre page.
	for i := 0; i < 3; i++ {
		get(t, h, "/books/5")
		get(t, h, "/genres/7", "Referer", "http://proxy/books/5")
	}
	h.cache.Delete(context.Background(), keyFor("/genres/7"))
	pf.keys = nil
	h.maybePrefetchForRequest(context.Background(), httptest.NewRequest(http.MethodGet, "/books/5", nil), "/books/5")
	if len(pf.keys) != 1 || pf.keys[0] != keyFor("/genres/7") {
		t.Fatalf("expected the concrete prediction /genres/7, got %v", pf.keys)
	}

	// Without a confident concrete prediction the template one is used.
	pf.keys = nil
	h.maybePrefetchForRequest(context.Background(), httptest.NewRequest(http.MethodGet, "/books/8", nil), "/books/8")
	if len(pf.keys) != 1 || pf.keys[0] != keyFor("/books/8/authors") {
		t.Fatalf("expected the template prediction /books/8/authors, got %v", pf.keys)
	}
}

func TestNoTemplatePrefetchWithoutRoutes(t *testing.T) {
	upstream := ordersUpstream(t, nil)
	pf := &recordingPrefetcher{}
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), pf, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	if h.routePredictor != nil {
		t.Fatal("template predictor created without routes")
	}
	learnChain(t, h, "/books/%d/authors", 1, 2, 3)
	pf.keys = nil
	get(t, h, "/books/99")
	if len(pf.keys) != 0 {
		t.Fatalf("prefetched %v for an unseen book without routes", pf.keys)
	}
}
