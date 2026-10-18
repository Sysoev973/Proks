package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proks/internal/events"
)

func envMap(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestBookUpdatedRequiresAdminToken(t *testing.T) {
	broker := events.NewInMemoryBroker()
	h := requireAdminToken("secret", newInvalidationHandler(broker, events.TypeBookUpdated))
	body := `{"type":"book.updated","book_id":"1","version":2}`

	cases := []struct {
		name   string
		auth   string
		status int
	}{
		{"missing", "", http.StatusUnauthorized},
		{"wrong", "Bearer nope", http.StatusUnauthorized},
		{"no scheme", "secret", http.StatusUnauthorized},
		{"valid", "Bearer secret", http.StatusAccepted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, bookUpdatedPath, strings.NewReader(body))
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tc.status {
				t.Fatalf("expected %d, got %d", tc.status, rr.Code)
			}
		})
	}
}

func TestInvalidationHandlerValidatesRequests(t *testing.T) {
	cases := []struct {
		name      string
		eventType string
		method    string
		body      string
		status    int
	}{
		{"book missing id", events.TypeBookUpdated, http.MethodPost, `{"type":"book.updated"}`, http.StatusBadRequest},
		{"book wrong type", events.TypeBookUpdated, http.MethodPost, `{"type":"invalidate","tags":["a:1"]}`, http.StatusBadRequest},
		{"get", events.TypeInvalidate, http.MethodGet, ``, http.StatusMethodNotAllowed},
		{"not json", events.TypeInvalidate, http.MethodPost, `{`, http.StatusBadRequest},
		{"empty purge", events.TypeInvalidate, http.MethodPost, `{}`, http.StatusBadRequest},
		{"bad tag", events.TypeInvalidate, http.MethodPost, `{"tags":["a b"]}`, http.StatusBadRequest},
		{"purge tags", events.TypeInvalidate, http.MethodPost, `{"tags":["order:42"],"version":3}`, http.StatusAccepted},
		{"purge keys", events.TypeInvalidate, http.MethodPost, `{"keys":["|||/orders/42|"]}`, http.StatusAccepted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newInvalidationHandler(events.NewInMemoryBroker(), tc.eventType)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(tc.method, purgePath, strings.NewReader(tc.body)))
			if rr.Code != tc.status {
				t.Fatalf("expected %d, got %d: %s", tc.status, rr.Code, rr.Body.String())
			}
		})
	}
}

type failingStream struct{}

func (failingStream) Subscribe(context.Context, events.Handler) (func(), error) {
	return func() {}, nil
}
func (failingStream) Publish(context.Context, events.Event) error {
	return io.ErrClosedPipe
}

func TestInvalidationHandlerReportsPublishFailure(t *testing.T) {
	rr := httptest.NewRecorder()
	newInvalidationHandler(failingStream{}, events.TypeInvalidate).ServeHTTP(rr,
		httptest.NewRequest(http.MethodPost, purgePath, strings.NewReader(`{"tags":["a:1"]}`)))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(envMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UpstreamURL != "http://localhost:8081" || cfg.ListenAddr != ":8080" {
		t.Fatalf("unexpected addresses: %+v", cfg)
	}
	if cfg.CacheCapacity != 10_000 || cfg.CacheMaxBytes != 256<<20 {
		t.Fatalf("unexpected cache size: %+v", cfg)
	}
	if cfg.StaleWhileRevalidate != 10*time.Second || cfg.StaleIfError != 5*time.Minute {
		t.Fatalf("unexpected stale windows: %+v", cfg)
	}
	if cfg.CacheTTL != 5*time.Minute || cfg.CacheMaxTTL != 0 {
		t.Fatalf("unexpected TTL defaults: %+v", cfg)
	}
	if cfg.CacheRefreshAhead != 0 || cfg.CacheRefreshMinHits != 3 || cfg.CacheRefreshRPS != 10 {
		t.Fatalf("refresh-ahead must be off by default: %+v", cfg)
	}
	if cfg.PrefetchEnabled || cfg.PrefetchRPS != 20 || cfg.PrefetchTimeout != time.Second {
		t.Fatalf("prefetch must be off by default with a budget: %+v", cfg)
	}
	if cfg.LogLevel != slog.LevelInfo || cfg.RabbitConnectTimeout != time.Minute || cfg.RabbitRequired {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigParsesValues(t *testing.T) {
	routes := filepath.Join(t.TempDir(), "routes.yaml")
	if err := os.WriteFile(routes, []byte("routes:\n  - match: /orders/{id}\n    tags: [\"order:{id}\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(envMap(map[string]string{
		"UPSTREAM_URL":           "http://up:9000",
		"CACHE_CAPACITY":         "42",
		"CACHE_TTL":              "1m",
		"CACHE_MAX_TTL":          "20s",
		"CACHE_REFRESH_AHEAD":    "0.1",
		"CACHE_REFRESH_MIN_HITS": "5",
		"CACHE_REFRESH_RPS":      "50",
		"EWMA_ALPHA":             "0.5",
		"PREFETCH_ENABLED":       "true",
		"PREFETCH_TIMEOUT":       "250ms",
		"LOG_LEVEL":              "debug",
		"LOG_HIT_SAMPLE":         "10",
		"ROUTES_CONFIG":          routes,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UpstreamURL != "http://up:9000" || cfg.CacheCapacity != 42 || cfg.EWMAAlpha != 0.5 ||
		!cfg.PrefetchEnabled || cfg.PrefetchTimeout != 250*time.Millisecond ||
		cfg.LogLevel != slog.LevelDebug || cfg.LogHitSample != 10 ||
		cfg.CacheTTL != time.Minute || cfg.CacheMaxTTL != 20*time.Second ||
		cfg.CacheRefreshAhead != 0.1 || cfg.CacheRefreshMinHits != 5 || cfg.CacheRefreshRPS != 50 {
		t.Fatalf("values not parsed: %+v", cfg)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].Match != "/orders/{id}" {
		t.Fatalf("routes not loaded: %+v", cfg.Routes)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	for key, value := range map[string]string{
		"CACHE_CAPACITY":               "-1",
		"CACHE_MAX_BYTES":              "lots",
		"EWMA_ALPHA":                   "high",
		"CACHE_STALE_WHILE_REVALIDATE": "-5s",
		"CACHE_TTL":                    "0s",
		"CACHE_MAX_TTL":                "soon",
		"CACHE_REFRESH_AHEAD":          "0.6",
		"CACHE_REFRESH_MIN_HITS":       "-1",
		"RABBITMQ_REQUIRED":            "maybe",
		"LOG_LEVEL":                    "loud",
		"ROUTES_CONFIG":                filepath.Join(t.TempDir(), "missing.yaml"),
	} {
		t.Run(key, func(t *testing.T) {
			_, err := loadConfig(envMap(map[string]string{key: value}))
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("expected an error naming %s, got %v", key, err)
			}
		})
	}
	if _, err := loadConfig(envMap(map[string]string{"CACHE_CAPACITY": "0"})); err == nil {
		t.Fatal("expected zero capacity to be rejected")
	}
}

func TestEventStreamFallsBackToInMemoryWithoutRabbitURL(t *testing.T) {
	stream, closeFn, err := newEventStream(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	if _, ok := stream.(*events.InMemoryBroker); !ok {
		t.Fatalf("expected in-memory broker, got %T", stream)
	}
}

func TestEventStreamRequiredRabbitFails(t *testing.T) {
	t.Setenv("PROXY_REPLICA_ID", "test")
	t.Setenv("EVENT_LOG_PATH", t.TempDir()+"/events.log")
	t.Setenv("EVENT_VERSION_STATE_PATH", t.TempDir()+"/state.json")
	cfg := Config{RabbitURL: "amqp://guest:guest@127.0.0.1:1/", RabbitRequired: true}
	if _, _, err := newEventStream(cfg); err == nil {
		t.Fatal("expected error when rabbitmq is required but unreachable")
	}

	cfg.RabbitRequired = false
	stream, closeFn, err := newEventStream(cfg)
	if err != nil {
		t.Fatalf("expected fallback, got %v", err)
	}
	defer closeFn()
	if _, ok := stream.(*events.InMemoryBroker); !ok {
		t.Fatalf("expected in-memory fallback, got %T", stream)
	}
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestStructuredLoggingWritesOneLinePerRequest(t *testing.T) {
	logs := captureLogs(t)
	h := withStructuredLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Request-ID") != "" && r.Context().Value("request_id") != "req-1" {
			t.Errorf("request ID not propagated: %v", r.Context().Value("request_id"))
		}
		w.Header().Set("X-Cache-Status", "MISS")
		w.Header().Set("X-Cache-Reason", "LRU")
		w.WriteHeader(http.StatusTeapot)
	}), 1)
	req := httptest.NewRequest(http.MethodGet, "/books/1", nil)
	req.Header.Set("X-Request-ID", "req-1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Header().Get("X-Request-ID") != "req-1" {
		t.Fatalf("expected X-Request-ID to be echoed, got %q", rr.Header().Get("X-Request-ID"))
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one log line, got %d: %s", len(lines), logs)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["msg"] != "request_finished" || entry["status"] != float64(http.StatusTeapot) ||
		entry["cache_status"] != "MISS" || entry["reason"] != "LRU" || entry["duration"] == nil {
		t.Fatalf("unexpected log entry: %v", entry)
	}

	generated := httptest.NewRecorder()
	h.ServeHTTP(generated, httptest.NewRequest(http.MethodGet, "/", nil))
	if generated.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected a generated X-Request-ID")
	}
}

func TestStructuredLoggingSamplesHits(t *testing.T) {
	logs := captureLogs(t)
	h := withStructuredLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Cache-Status", "HIT")
		_, _ = w.Write([]byte("ok"))
	}), 5)
	for i := 0; i < 10; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}
	if n := strings.Count(logs.String(), "request_finished"); n != 2 {
		t.Fatalf("expected 2 of 10 HITs to be logged, got %d", n)
	}
}

func TestProxyReadinessFollowsUpstream(t *testing.T) {
	var ready atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/ready" || !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	h := newProxyReadinessHandler(upstream.URL)

	check := func(want int) {
		t.Helper()
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
		if rr.Code != want {
			t.Fatalf("expected %d, got %d", want, rr.Code)
		}
	}
	check(http.StatusServiceUnavailable)
	ready.Store(true)
	check(http.StatusOK)
	upstream.Close()
	check(http.StatusServiceUnavailable)

	rr := httptest.NewRecorder()
	newProxyReadinessHandler("http://bad host").ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for an invalid upstream URL, got %d", rr.Code)
	}
}

func startRun(t *testing.T, cfg Config) (public, admin string, stop func() error) {
	t.Helper()
	captureLogs(t)
	type addrs struct{ public, admin net.Addr }
	bound := make(chan addrs, 1)
	cfg.onListen = func(p, a net.Addr) { bound <- addrs{p, a} }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	select {
	case a := <-bound:
		public = "http://" + a.public.String()
		if a.admin != nil {
			admin = "http://" + a.admin.String()
		}
	case err := <-done:
		cancel()
		t.Fatalf("run returned early: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("run did not start listening")
	}
	return public, admin, func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("run did not stop after cancel")
			return nil
		}
	}
}

func testConfig(t *testing.T, upstream string) Config {
	t.Helper()
	cfg, err := loadConfig(envMap(map[string]string{
		"UPSTREAM_URL":     upstream,
		"LISTEN_ADDR":      "127.0.0.1:0",
		"CACHE_CAPACITY":   "64",
		"PREFETCH_ENABLED": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestRunServesAndShutsDownOnCancel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Surrogate-Key", "order:42")
		_, _ = io.WriteString(w, "order")
	}))
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL)
	cfg.AdminToken = "secret"
	public, _, stop := startRun(t, cfg)

	get := func(path string) *http.Response {
		t.Helper()
		resp, err := http.Get(public + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp
	}
	if resp := get("/orders/42"); resp.Header.Get("X-Cache-Status") != "MISS" {
		t.Fatalf("expected MISS, got %s", resp.Header.Get("X-Cache-Status"))
	}
	if resp := get("/orders/42"); resp.Header.Get("X-Cache-Status") != "HIT" {
		t.Fatalf("expected HIT, got %s", resp.Header.Get("X-Cache-Status"))
	}

	req, _ := http.NewRequest(http.MethodPost, public+purgePath, strings.NewReader(`{"tags":["order:42"]}`))
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected purge to be accepted, got %d", resp.StatusCode)
	}
	if resp := get("/orders/42"); resp.Header.Get("X-Cache-Status") != "MISS" {
		t.Fatalf("expected MISS after purge, got %s", resp.Header.Get("X-Cache-Status"))
	}
	if resp := get("/health/live"); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected live, got %d", resp.StatusCode)
	}
	if resp := get("/metrics"); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected metrics, got %d", resp.StatusCode)
	}

	if err := stop(); err != nil {
		t.Fatalf("expected a clean shutdown, got %v", err)
	}
	if _, err := http.Get(public + "/health/live"); err == nil {
		t.Fatal("expected the server to be closed")
	}
}

func TestRunServesAdminOnSeparatePort(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL)
	cfg.AdminAddr = "127.0.0.1:0"
	public, admin, stop := startRun(t, cfg)
	defer func() {
		if err := stop(); err != nil {
			t.Fatal(err)
		}
	}()

	post := func(base string) int {
		resp, err := http.Post(base+purgePath, "application/json", strings.NewReader(`{"tags":["a:1"]}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if got := post(public); got != http.StatusNotFound {
		t.Fatalf("public port must not serve admin paths, got %d", got)
	}
	if got := post(admin); got != http.StatusAccepted {
		t.Fatalf("admin port must serve purge, got %d", got)
	}
}

func TestRunDisablesAdminWithoutTokenOrPort(t *testing.T) {
	cfg := testConfig(t, "http://127.0.0.1:1")
	public, _, stop := startRun(t, cfg)
	defer func() { _ = stop() }()
	resp, err := http.Post(public+bookUpdatedPath, "application/json", strings.NewReader(`{"type":"book.updated","book_id":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestRunFailsOnBadAddress(t *testing.T) {
	captureLogs(t)
	cfg := testConfig(t, "http://127.0.0.1:1")
	cfg.ListenAddr = "256.0.0.1:bad"
	if err := run(context.Background(), cfg); err == nil {
		t.Fatal("expected a listen error")
	}
	cfg = testConfig(t, "http://127.0.0.1:1")
	cfg.AdminAddr = "256.0.0.1:bad"
	if err := run(context.Background(), cfg); err == nil {
		t.Fatal("expected an admin listen error")
	}
	cfg = testConfig(t, "://bad")
	if err := run(context.Background(), cfg); err == nil {
		t.Fatal("expected an invalid upstream URL to be rejected")
	}
}

// BenchmarkAccessLog measures what the request_finished line adds to a
// request, with JSON output to io.Discard.
func BenchmarkAccessLog(b *testing.B) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Cache-Status", "HIT")
		_, _ = w.Write([]byte("ok"))
	})
	for _, tc := range []struct {
		name    string
		handler http.Handler
	}{
		{"none", next},
		{"every-request", withStructuredLogging(next, 1)},
		{"hit-sample-100", withStructuredLogging(next, 100)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, nil)))
			defer slog.SetDefault(prev)
			req := httptest.NewRequest(http.MethodGet, "/books/1", nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tc.handler.ServeHTTP(httptest.NewRecorder(), req)
			}
		})
	}
}
