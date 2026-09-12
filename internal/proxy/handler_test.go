package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/events"
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
	h.prefetchThreshold = 0.6
	h.maybePrefetch(context.Background(), "book_view:1")
	if len(pf.keys) != 1 || pf.keys[0] != "book_view:2" {
		t.Fatalf("expected prefetch for top candidate book_view:2, got %+v", pf.keys)
	}
}

func TestMaybePrefetchSkipsPollutedCache(t *testing.T) {
	filter := cache.NewTinyLFU()
	for i := 0; i < 12; i++ {
		filter.Increment("book_view:3")
	}
	window := cache.NewLRUWindow(4)
	c := cache.NewInMemoryCache(2, cache.NewTinyLFUAdmission(filter, window, 2, 0.5))
	_ = c.Set(context.Background(), cache.Item{Key: "book_view:9", Value: []byte("a")}, cache.Metrics{})
	_ = c.Set(context.Background(), cache.Item{Key: "book_view:10", Value: []byte("b")}, cache.Metrics{})

	pf := &recordingPrefetcher{}
	pred := predictor.NewMarkov()
	pred.Update("book_view:1", "book_view:2")
	pred.Update("book_view:1", "book_view:2")
	pred.Update("book_view:1", "book_view:2")

	h, err := NewHandler(c, pred, pf, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}
	h.trend = nil
	h.prefetchThreshold = 0.5
	if _, ok := c.Admission().(*cache.TinyLFUAdmission); !ok {
		t.Fatal("expected TinyLFU admission to be active")
	}
	h.maybePrefetch(context.Background(), "book_view:1")
	if len(pf.keys) != 0 {
		t.Fatalf("expected polluted cache to suppress prefetch, got %+v", pf.keys)
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
func TestBookUpdatedEndpointInvalidatesByVersion(t *testing.T) {
	c := cache.NewInMemoryCache(32, nil)
	key := "u|t|ru|book_view:42|"
	_ = c.Set(context.Background(), cache.Item{Key: key, Value: []byte("old"), StatusCode: http.StatusOK, ExpiresAt: time.Now().Add(time.Minute), Version: 12}, cache.Metrics{})

	pf := prefetcher.NewAsyncPrefetcher(1, 8, 20*time.Millisecond, func(ctx context.Context, key string) error { return nil }, nil)
	defer pf.Stop()

	h, err := NewHandler(c, predictor.NewMarkov(), pf, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}
	broker := events.NewInMemoryBroker()
	h.SetEventStream(broker)
	if err := h.StartEventConsumer(context.Background()); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/internal/events/book.updated", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var evt events.BookUpdateEvent
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		if err := broker.Publish(r.Context(), evt); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	mux.Handle("/", h)

	req := httptest.NewRequest(http.MethodPost, "/internal/events/book.updated", strings.NewReader(`{"book_id":"42","version":13,"type":"book.updated"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rr.Code, rr.Body.String())
	}

	time.Sleep(25 * time.Millisecond)
	if _, err := c.Get(context.Background(), key); err == nil {
		t.Fatal("expected stale book cache entry to be invalidated after versioned event")
	}
}

// Тест перехода между главами
func TestE2EBook(t *testing.T) {
	// 1. Мок upstream-сервера
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"123","title":"Go Internals"}`))
	}))
	defer upstream.Close()

	// Запуск кэша, прокси и шины событий
	c := cache.NewInMemoryCache(32, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	broker := events.NewInMemoryBroker()
	h.SetEventStream(broker)
	if err := h.StartEventConsumer(context.Background()); err != nil {
		t.Fatalf("failed to start event consumer: %v", err)
	}

	// Настройка роутера
	mux := http.NewServeMux()
	mux.Handle("/internal/events/book.updated", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var evt events.BookUpdateEvent
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		if err := broker.Publish(r.Context(), evt); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	mux.Handle("/", h)

	// Заполнение кэша и получение статуса HIT
	req1 := httptest.NewRequest(http.MethodGet, "/books/123", nil)
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1) // Чтение 1: MISS (наполнение кэша)

	req2 := httptest.NewRequest(http.MethodGet, "/books/123", nil)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2) // Чтение 2: Должен быть HIT

	if status := rec2.Header().Get("X-Cache-Status"); status != "HIT" {
		t.Fatalf("Шаг А: ожидали X-Cache-Status: HIT, получили: %s", status)
	}

	// Отправка события обновления книги через HTTP POST
	body := strings.NewReader(`{"book_id": "123", "version": 1}`)
	eventReq := httptest.NewRequest(http.MethodPost, "/internal/events/book.updated", body)
	eventReq.Header.Set("Content-Type", "application/json")
	eventRec := httptest.NewRecorder()
	mux.ServeHTTP(eventRec, eventReq)

	if eventRec.Code != http.StatusAccepted {
		t.Fatalf("Шаг Б: ожидали статус 202 Accepted, получили: %d", eventRec.Code)
	}

	// Пауза для обработки события асинхронным воркером
	time.Sleep(30 * time.Millisecond)

	// Проверка инвалидации кэша (MISS)
	req3 := httptest.NewRequest(http.MethodGet, "/books/123", nil)
	rec3 := httptest.NewRecorder()
	mux.ServeHTTP(rec3, req3) // MISS

	if status := rec3.Header().Get("X-Cache-Status"); status == "HIT" {
		t.Fatalf("Шаг В: ожидали инвалидацию кэша (MISS), но получили HIT")
	}
}
func TestServeHTTPMetrics(t *testing.T) {
	c := cache.NewInMemoryCache(10, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "cache_size") {
		t.Fatalf("expected metrics output, got: %s", rr.Body.String())
	}
}
