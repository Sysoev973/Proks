package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/events"
	"proks/internal/predictor"
	"proks/internal/prefetcher"

	"github.com/prometheus/client_golang/prometheus"
)

// fanoutStream delivers every event to all subscribers, like RabbitMQ does
// with one queue per replica.
type fanoutStream struct {
	mu       sync.Mutex
	handlers []events.Handler
}

func (s *fanoutStream) Subscribe(_ context.Context, h events.Handler) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, h)
	return func() {}, nil
}

func (s *fanoutStream) Publish(ctx context.Context, evt events.Event) error {
	s.mu.Lock()
	handlers := append([]events.Handler(nil), s.handlers...)
	s.mu.Unlock()
	for _, h := range handlers {
		if err := h(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}

func ordersUpstream(t *testing.T, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			calls.Add(1)
		}
		w.Header().Set("Cache-Control", "max-age=60")
		_, _ = io.WriteString(w, "order "+r.URL.Path)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, h http.Handler, path string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func cacheStatus(rec *httptest.ResponseRecorder) string {
	return rec.Header().Get("X-Cache-Status")
}

// A made-up API that the proxy knows nothing about: /orders/{id} tagged
// order:{id} by the route config. A purge of order:42 reaches every replica.
func TestPurgeByTagOnEveryReplicaForAnyAPI(t *testing.T) {
	upstream := ordersUpstream(t, nil)
	policy := DefaultCachePolicy()
	policy.Routes = []Route{{Match: "/orders/{id}", Tags: []string{"order:{id}"}}}
	stream := &fanoutStream{}
	var replicas []*Handler
	for i := 0; i < 2; i++ {
		h, err := NewHandlerWithPolicy(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL, policy)
		if err != nil {
			t.Fatal(err)
		}
		h.SetEventStream(stream)
		if err := h.StartEventConsumer(context.Background()); err != nil {
			t.Fatal(err)
		}
		replicas = append(replicas, h)
	}
	for _, h := range replicas {
		get(t, h, "/orders/42")
		get(t, h, "/orders/7")
		if got := cacheStatus(get(t, h, "/orders/42")); got != "HIT" {
			t.Fatalf("expected HIT before purge, got %s", got)
		}
	}

	if err := stream.Publish(context.Background(), events.Event{Type: events.TypeInvalidate, Tags: []string{"order:42"}}); err != nil {
		t.Fatal(err)
	}
	for i, h := range replicas {
		if got := cacheStatus(get(t, h, "/orders/42")); got != "MISS" {
			t.Fatalf("replica %d: expected MISS after purge, got %s", i, got)
		}
		if got := cacheStatus(get(t, h, "/orders/7")); got != "HIT" {
			t.Fatalf("replica %d: order:7 must stay cached, got %s", i, got)
		}
	}
}

func TestResponseTagsAcceptAnyPrefix(t *testing.T) {
	tags := map[string]string{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/surrogate":
			w.Header().Set("Surrogate-Key", "order:1  customer:9")
		case "/cache-tag":
			w.Header().Set("Cache-Tag", "invoice:5, tenant:acme")
		case "/legacy":
			w.Header().Set("X-Cache-Depends-On", "warehouse:3")
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	h.SetEventStream(events.NewInMemoryBroker())
	if err := h.StartEventConsumer(context.Background()); err != nil {
		t.Fatal(err)
	}
	tags["/surrogate"] = "customer:9"
	tags["/cache-tag"] = "tenant:acme"
	tags["/legacy"] = "warehouse:3"
	for path, tag := range tags {
		get(t, h, path)
		if err := h.HandleEvent(context.Background(), events.Event{Type: events.TypeInvalidate, Tags: []string{tag}}); err != nil {
			t.Fatal(err)
		}
		if got := cacheStatus(get(t, h, path)); got != "MISS" {
			t.Fatalf("%s: expected tag %s to invalidate the response, got %s", path, tag, got)
		}
	}
}

func TestResponseTagsAreLimited(t *testing.T) {
	headers := http.Header{}
	headers.Set("Surrogate-Key", "ok:1 has\x01ctl "+strings.Repeat("x", events.MaxTagLength+1)+" ok:1")
	headers.Set("Cache-Tag", "a b, ok:2")
	got := routeTable(nil).tagsForResponse(httptest.NewRequest(http.MethodGet, "/", nil), headers)
	if strings.Join(got, ",") != "ok:1,ok:2" {
		t.Fatalf("expected only valid, unique tags, got %q", got)
	}

	var many []string
	for i := 0; i < maxResponseTags+10; i++ {
		many = append(many, fmt.Sprintf("t:%d", i))
	}
	headers = http.Header{}
	headers.Set("Surrogate-Key", strings.Join(many, " "))
	if got := routeTable(nil).tagsForResponse(httptest.NewRequest(http.MethodGet, "/", nil), headers); len(got) != maxResponseTags {
		t.Fatalf("expected at most %d tags, got %d", maxResponseTags, len(got))
	}
	if routeTable(nil).tagsForResponse(nil, headers) != nil {
		t.Fatal("expected no tags without a request")
	}
}

func TestRouteTemplates(t *testing.T) {
	table, err := compileRoutes([]Route{
		{Match: "/orders/{id}/items/{item}", Tags: []string{"order:{id}", "item:{id}-{item}"}},
		{Match: "/orders", Tags: []string{"list:orders", "list:orders:{query.status}"}},
		{Match: "/", Tags: []string{"root:home"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"/orders/42/items/7":      "order:42,item:42-7",
		"/orders?status=open":     "list:orders,list:orders:open",
		"/orders":                 "list:orders",
		"/orders/42/items/a%20b":  "order:42",
		"/":                       "root:home",
		"/customers/1":            "",
		"/orders/42/items/7/more": "",
	}
	for target, want := range cases {
		got := table.tagsFor(httptest.NewRequest(http.MethodGet, target, nil))
		if strings.Join(got, ",") != want {
			t.Errorf("%s: got %q, want %q", target, got, want)
		}
	}

	for _, bad := range [][]Route{
		{{Match: "orders/{id}"}},
		{{Match: "/orders/{}"}},
		{{Match: "/orders/{id}/{id}"}},
		{{Match: "/orders/{id}", Tags: []string{"order:{missing}"}}},
		{{Match: "/orders/{id}", Tags: []string{"order {id}"}}},
		{{Match: "/orders/{id}", Tags: []string{"order:{id"}}},
	} {
		if _, err := compileRoutes(bad); err == nil {
			t.Errorf("expected %+v to be rejected", bad)
		}
	}
	if _, err := NewHandlerWithPolicy(cache.NewInMemoryCache(1, nil), predictor.NewMarkov(), noopPrefetcher{}, "http://x", CachePolicy{Routes: []Route{{Match: "x"}}}); err == nil {
		t.Fatal("expected the handler to reject invalid routes")
	}
}

func TestLoadRoutes(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "routes.yaml")
	if err := os.WriteFile(good, []byte("routes:\n  - match: /orders/{id}\n    tags: [\"order:{id}\"]\n  - match: /orders\n    tags: [list:orders]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	routes, err := LoadRoutes(good)
	if err != nil || len(routes) != 2 || routes[1].Tags[0] != "list:orders" {
		t.Fatalf("unexpected routes %+v (%v)", routes, err)
	}
	for name, content := range map[string]string{
		"syntax.yaml":  "routes: [",
		"invalid.yaml": "routes:\n  - match: orders\n",
	} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRoutes(path); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := LoadRoutes(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("expected a missing file to be reported")
	}
	if _, err := LoadRoutes("../../routes.example.yaml"); err != nil {
		t.Fatalf("the example config must be valid: %v", err)
	}
}

func TestPurgeByKey(t *testing.T) {
	upstream := ordersUpstream(t, nil)
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	get(t, h, "/orders/1")
	get(t, h, "/orders/2")
	key := BuildCacheKey(httptest.NewRequest(http.MethodGet, "/orders/1", nil))
	if err := h.HandleEvent(context.Background(), events.Event{Type: events.TypeInvalidate, Keys: []string{key}}); err != nil {
		t.Fatal(err)
	}
	if got := cacheStatus(get(t, h, "/orders/1")); got != "MISS" {
		t.Fatalf("expected the purged key to miss, got %s", got)
	}
	if got := cacheStatus(get(t, h, "/orders/2")); got != "HIT" {
		t.Fatalf("expected other keys to stay, got %s", got)
	}
	if h.HandleEvent(context.Background(), events.Event{Type: "bogus"}) == nil {
		t.Fatal("expected an invalid event to be rejected")
	}
	if got := counterValue(t, h.MetricsRegistry(), "proxy_invalidations_total"); got != 1 {
		t.Fatalf("expected 1 invalidation target recorded, got %v", got)
	}
}

func TestPurgeByKeyDuringFetchIsNotLost(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := gatedUpstream(t, "v1", &calls, release)
	c := cache.NewInMemoryCache(32, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- get(t, h, "/orders/1") }()
	waitFor(t, func() bool { return calls.Load() == 1 })
	key := BuildCacheKey(httptest.NewRequest(http.MethodGet, "/orders/1", nil))
	if err := h.HandleEvent(context.Background(), events.Event{Type: events.TypeInvalidate, Keys: []string{key}}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if rec := <-done; rec.Header().Get("X-Cache-Reason") != "INVALIDATED" || c.Size() != 0 {
		t.Fatalf("expected the in-flight response not to be stored, reason=%s size=%d", rec.Header().Get("X-Cache-Reason"), c.Size())
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

// prefetchSetup wires a handler to a real AsyncPrefetcher whose loader is
// Handler.Prefetch, the way main does with PREFETCH_ENABLED=true.
func prefetchSetup(t *testing.T, upstreamURL string, policy CachePolicy) (*Handler, *prefetcher.AsyncPrefetcher, cache.Cache) {
	t.Helper()
	c := cache.NewInMemoryCache(32, nil)
	var h *Handler
	pf := prefetcher.NewAsyncPrefetcher(1, 8, time.Second, func(ctx context.Context, key string) error {
		return h.Prefetch(ctx, key)
	}, nil)
	t.Cleanup(pf.Stop)
	h, err := NewHandlerWithPolicy(c, predictor.NewMarkov(), pf, upstreamURL, policy)
	if err != nil {
		t.Fatal(err)
	}
	pf.SetOutcomeHook(func(key string, success bool) {
		if success {
			h.MarkPrefetched(key)
		}
	})
	return h, pf, c
}

func TestPrefetchServesPredictedRoute(t *testing.T) {
	var calls atomic.Int32
	upstream := ordersUpstream(t, &calls)
	h, pf, c := prefetchSetup(t, upstream.URL, DefaultCachePolicy())

	for i := 0; i < 3; i++ {
		get(t, h, "/a", "X-Tenant", "acme")
		get(t, h, "/b", "X-Tenant", "acme", "Referer", "http://proxy/a")
	}
	keyB := BuildCacheKey(func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/b", nil)
		r.Header.Set("X-Tenant", "acme")
		return r
	}())
	c.Delete(context.Background(), keyB) // as if it had expired
	before := calls.Load()

	get(t, h, "/a", "X-Tenant", "acme")
	waitFor(t, func() bool { return pf.Snapshot().Executed == 1 })
	if calls.Load() != before+1 {
		t.Fatalf("expected exactly one upstream fetch for the prefetch, got %d", calls.Load()-before)
	}
	rec := get(t, h, "/b", "X-Tenant", "acme", "Referer", "http://proxy/a")
	if cacheStatus(rec) != "PREFETCH" || rec.Header().Get("X-Cache-Reason") != "MARKOV" {
		t.Fatalf("expected PREFETCH/MARKOV, got %s/%s", cacheStatus(rec), rec.Header().Get("X-Cache-Reason"))
	}
	if !strings.Contains(rec.Body.String(), "/b") {
		t.Fatalf("prefetched the wrong route: %s", rec.Body.String())
	}
	if got := cacheStatus(get(t, h, "/b", "X-Tenant", "acme")); got != "HIT" {
		t.Fatalf("the PREFETCH label must be used once, got %s", got)
	}
}

func TestPrefetchSkipsPersonalRequests(t *testing.T) {
	upstream := ordersUpstream(t, nil)
	policy := DefaultCachePolicy()
	policy.AllowCookie = true
	pf := &recordingPrefetcher{}
	h, err := NewHandlerWithPolicy(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), pf, upstream.URL, policy)
	if err != nil {
		t.Fatal(err)
	}
	h.trend = nil
	h.predictor.Update("/a", "/b")
	for _, headers := range [][]string{
		{"Cookie", "session=1"},
		{"Authorization", "Bearer x"},
	} {
		h.maybePrefetchForRequest(context.Background(), func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/a", nil)
			r.Header.Set(headers[0], headers[1])
			return r
		}(), "/a")
	}
	h.maybePrefetchForRequest(context.Background(), httptest.NewRequest(http.MethodPost, "/a", nil), "/a")
	if len(pf.keys) != 0 {
		t.Fatalf("personal or non-GET requests must not be prefetched, got %v", pf.keys)
	}
	h.maybePrefetchForRequest(context.Background(), httptest.NewRequest(http.MethodGet, "/a", nil), "/a")
	if len(pf.keys) != 1 {
		t.Fatalf("expected an anonymous GET to be prefetched, got %v", pf.keys)
	}
}

func TestPrefetchOnlySucceedsWhenStored(t *testing.T) {
	status := atomic.Int32{}
	status.Store(http.StatusOK)
	noStore := atomic.Bool{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if noStore.Load() {
			w.Header().Set("Cache-Control", "no-store")
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer upstream.Close()
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	queue := func(path string) string {
		req, key, ok := h.prefetchTemplate(httptest.NewRequest(http.MethodGet, "/a", nil), path)
		if !ok {
			t.Fatal("expected a template")
		}
		h.rememberPrefetchRequest(key, req)
		return key
	}

	if err := h.Prefetch(context.Background(), "unknown"); !errors.Is(err, errPrefetchUnknown) {
		t.Fatalf("expected errPrefetchUnknown, got %v", err)
	}
	noStore.Store(true)
	if err := h.Prefetch(context.Background(), queue("/x")); !errors.Is(err, errPrefetchNotStored) {
		t.Fatalf("expected errPrefetchNotStored, got %v", err)
	}
	noStore.Store(false)
	key := queue("/y")
	if err := h.Prefetch(context.Background(), key); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	h.rememberPrefetchRequest(key, httptest.NewRequest(http.MethodGet, "/y", nil))
	if err := h.Prefetch(context.Background(), key); !errors.Is(err, errPrefetchSkipped) {
		t.Fatalf("expected an already cached key to be skipped, got %v", err)
	}

	status.Store(http.StatusBadGateway)
	if err := h.Prefetch(context.Background(), queue("/z")); err == nil {
		t.Fatal("expected a 5xx to fail the prefetch")
	}
	if err := h.Prefetch(context.Background(), queue("/w")); !errors.Is(err, errPrefetchThrottled) {
		t.Fatalf("expected prefetch to pause after an upstream error, got %v", err)
	}
}

func TestPrefetchBudget(t *testing.T) {
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, "http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	h.SetPrefetchBudget(1)
	if !h.prefetchAllowed() || h.prefetchAllowed() {
		t.Fatal("expected a budget of one request per second")
	}
	h.SetPrefetchBudget(0)
	for i := 0; i < 100; i++ {
		if !h.prefetchAllowed() {
			t.Fatal("expected no limit with a zero budget")
		}
	}
	// An unreachable upstream pauses prefetch.
	get(t, h, "/a")
	if h.prefetchAllowed() {
		t.Fatal("expected prefetch to pause after an upstream error")
	}
}

func TestPrefetchRequestsAreBounded(t *testing.T) {
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, "http://x")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxPrefetchRequests+10; i++ {
		h.rememberPrefetchRequest(fmt.Sprint(i), httptest.NewRequest(http.MethodGet, "/", nil))
	}
	if len(h.prefetchRequests) != maxPrefetchRequests {
		t.Fatalf("expected %d templates, got %d", maxPrefetchRequests, len(h.prefetchRequests))
	}
	h.prefetchRequests["0"] = prefetchRequest{req: httptest.NewRequest(http.MethodGet, "/", nil), added: time.Now().Add(-2 * prefetchRequestTTL)}
	if _, ok := h.takePrefetchRequest("0"); ok {
		t.Fatal("expected an old template to be ignored")
	}
	h.rememberPrefetchRequest("new", httptest.NewRequest(http.MethodGet, "/", nil))
	if _, ok := h.takePrefetchRequest("new"); !ok {
		t.Fatal("expected room for a new template")
	}
}

func TestCollapsedAndTooLargeAreCounted(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := gatedUpstream(t, "book", &calls, release)
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			get(t, h, "/orders/1")
		}()
	}
	waitFor(t, func() bool { return calls.Load() == 1 })
	waitForFlightJoined(t)
	close(release)
	wg.Wait()
	if got := counterValue(t, h.MetricsRegistry(), "proxy_upstream_collapsed_total"); got != 2 {
		t.Fatalf("expected 2 collapsed requests, got %v", got)
	}

	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer big.Close()
	h, err = NewHandler(cache.New(cache.Config{Capacity: 32, MaxBytes: 2048, Shards: 1}), predictor.NewMarkov(), noopPrefetcher{}, big.URL)
	if err != nil {
		t.Fatal(err)
	}
	get(t, h, "/big")
	if got := counterValue(t, h.MetricsRegistry(), "proxy_cache_too_large_total"); got != 1 {
		t.Fatalf("expected one TOO_LARGE, got %v", got)
	}
}

func TestPrefetchDoesNotForwardClientAddress(t *testing.T) {
	forwarded := make(chan []string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded <- r.Header.Values("X-Forwarded-For")
		w.Header().Set("Cache-Control", "max-age=60")
	}))
	defer upstream.Close()
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := httptest.NewRequest(http.MethodGet, "/a", nil)
	client.RemoteAddr = "203.0.113.7:4321"
	client.Header.Set("X-Forwarded-For", "198.51.100.1")
	req, key, ok := h.prefetchTemplate(client, "/b")
	if !ok {
		t.Fatal("expected a template")
	}
	h.rememberPrefetchRequest(key, req)
	if err := h.Prefetch(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if got := <-forwarded; len(got) != 0 {
		t.Fatalf("a prefetch must not carry the client's address, got X-Forwarded-For %v", got)
	}
}
