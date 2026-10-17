package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/predictor"
)

type flushRecorder struct {
	header http.Header
	ch     chan string
}

func (r *flushRecorder) Header() http.Header { return r.header }
func (r *flushRecorder) WriteHeader(int)     {}
func (r *flushRecorder) Write(p []byte) (int, error) {
	r.ch <- string(p)
	return len(p), nil
}
func (r *flushRecorder) Flush() {}

func TestCachePolicySeparatesMethodsAndBypassesPOST(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, r.Method)
	}))
	defer upstream.Close()

	c := cache.NewInMemoryCache(10, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/resource", nil))
	post := httptest.NewRecorder()
	h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/resource", strings.NewReader("body")))
	getAgain := httptest.NewRecorder()
	h.ServeHTTP(getAgain, httptest.NewRequest(http.MethodGet, "/resource", nil))

	if get.Code != http.StatusOK || getAgain.Code != http.StatusOK || post.Code != http.StatusOK {
		t.Fatalf("unexpected statuses: GET=%d POST=%d GET-again=%d", get.Code, post.Code, getAgain.Code)
	}
	if post.Header().Get("X-Cache-Status") != "BYPASS" {
		t.Fatalf("expected POST bypass, got %s", post.Header().Get("X-Cache-Status"))
	}
	if getAgain.Header().Get("X-Cache-Status") != "HIT" {
		t.Fatalf("expected GET after POST to hit cached GET, got %s", getAgain.Header().Get("X-Cache-Status"))
	}
	if calls.Load() != 2 {
		t.Fatalf("expected one GET and one POST upstream call, got %d", calls.Load())
	}
}

func TestCachePolicyDoesNotStoreErrors(t *testing.T) {
	for _, errorStatus := range []int{http.StatusNotFound, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(errorStatus), func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(errorStatus)
					_, _ = io.WriteString(w, "error")
					return
				}

				_, _ = io.WriteString(w, "ok")
			}))
			defer upstream.Close()

			h, err := NewHandler(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			first := httptest.NewRecorder()
			h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/error", nil))
			second := httptest.NewRecorder()
			h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/error", nil))
			if first.Code != errorStatus || second.Code != http.StatusOK {
				t.Fatalf("expected %d then 200, got %d then %d", errorStatus, first.Code, second.Code)
			}
			if calls.Load() != 2 {
				t.Fatalf("expected error response not to be cached, got %d calls", calls.Load())
			}
		})
	}
}

func TestCachePolicyRejectsUnsafeResponseHeaders(t *testing.T) {
	for _, header := range []struct {
		name  string
		value string
	}{
		{"Cache-Control", "no-store"},
		{"Cache-Control", "private"},
		{"Cache-Control", "no-cache"},
		{"Set-Cookie", "session=secret"},
		{"Vary", "*"},
	} {
		t.Run(header.name+"_"+header.value, func(t *testing.T) {
			headers := make(http.Header)
			headers.Set(header.name, header.value)
			decision, _ := DefaultCachePolicy().responseDecision(nil, http.StatusOK, headers, 0)
			if decision.Allowed {
				t.Fatalf("expected %s=%s to bypass cache", header.name, header.value)
			}
		})
	}
}

func TestCachePolicyRejectsExpiredExpires(t *testing.T) {
	headers := make(http.Header)
	headers.Set("Expires", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat))
	decision, _ := DefaultCachePolicy().responseDecision(nil, http.StatusOK, headers, 0)
	if decision.Allowed || decision.Reason != "EXPIRED" {
		t.Fatalf("expected expired response to bypass with EXPIRED, got %+v", decision)
	}
}

func TestUnknownMethodAlwaysBypasses(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	h, err := NewHandler(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("PATCH", "/unknown", nil))
	if rec.Header().Get("X-Cache-Status") != "BYPASS" {
		t.Fatalf("expected unknown method to bypass, got %s", rec.Header().Get("X-Cache-Status"))
	}
}

func TestStreamingResponseIsRejectedByDefault(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if f, ok := w.(http.Flusher); ok {
			_, _ = io.WriteString(w, "data: one\n\n")
			f.Flush()
		}
	}))
	defer upstream.Close()
	h, err := NewHandler(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil))
	if rec.Code != http.StatusBadGateway || rec.Header().Get("X-Cache-Reason") != "STREAMING" {
		t.Fatalf("expected streaming response to be rejected with 502/STREAMING, got %d/%s", rec.Code, rec.Header().Get("X-Cache-Reason"))
	}

}

func TestStreamingResponsePassesWhenEnabled(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
	}))
	defer upstream.Close()
	policy := DefaultCachePolicy()
	policy.AllowStreaming = true
	h, err := NewHandlerWithPolicy(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL, policy)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stream", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "data: one\n\n" {
		t.Fatalf("expected streaming proxy pass, got %d/%q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Cache-Status") != "BYPASS" {
		t.Fatalf("expected streaming response to bypass cache, got %s", rec.Header().Get("X-Cache-Status"))
	}
}

func TestPrepareUpstreamHeadersAndResponseFiltering(t *testing.T) {
	seen := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.Header().Set("X-Upstream-Book-Version", "42")
		w.Header().Set("Connection", "X-Remove-Me")
		w.Header().Set("X-Remove-Me", "secret")
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	h, err := NewHandler(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/headers", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	req.Header.Set("Connection", "X-Remove-Me")
	req.Header.Set("X-Remove-Me", "client")
	req.Header.Set("X-Book-Version", "999")
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	headers := <-seen
	if headers.Get("X-Remove-Me") != "" || headers.Get("X-Book-Version") != "" ||
		headers.Get("X-Forwarded-For") != "192.0.2.10" {
		t.Fatalf("unsafe forwarding headers reached upstream: %+v", headers)
	}
	if headers.Get("Via") == "" || headers.Get("X-Forwarded-Proto") != "http" {
		t.Fatalf("missing forwarding headers: %+v", headers)
	}
	if rec.Header().Get("X-Upstream-Book-Version") != "" || rec.Body.String() != "ok" {
		t.Fatalf("internal response header leaked or body changed: headers=%+v body=%q", rec.Header(), rec.Body.String())
	}
}

func TestStreamResponseFlushesEachChunk(t *testing.T) {
	reader, writer := io.Pipe()
	recorder := &flushRecorder{header: make(http.Header), ch: make(chan string, 2)}
	done := make(chan error, 1)
	go func() { done <- streamResponse(context.Background(), recorder, reader) }()
	_, _ = writer.Write([]byte("first"))
	select {
	case chunk := <-recorder.ch:
		if chunk != "first" {
			t.Fatalf("expected first chunk, got %q", chunk)
		}
	case <-time.After(time.Second):
		t.Fatal("first chunk was not delivered before stream completion")
	}
	_ = writer.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCachePolicySeparatesCachedPOSTFromGET(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, r.Method)
	}))
	defer upstream.Close()

	policy := DefaultCachePolicy()
	policy.CacheMethods[http.MethodPost] = true
	h, err := NewHandlerWithPolicy(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL, policy)
	if err != nil {
		t.Fatal(err)
	}
	post := httptest.NewRecorder()
	h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/same", strings.NewReader("body")))
	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/same", nil))
	postAgain := httptest.NewRecorder()
	h.ServeHTTP(postAgain, httptest.NewRequest(http.MethodPost, "/same", strings.NewReader("body")))

	if post.Header().Get("X-Cache-Status") != "MISS" || get.Header().Get("X-Cache-Status") != "MISS" || postAgain.Header().Get("X-Cache-Status") != "HIT" {
		t.Fatalf("expected separate POST/GET entries, got POST=%s GET=%s POST-again=%s",
			post.Header().Get("X-Cache-Status"), get.Header().Get("X-Cache-Status"), postAgain.Header().Get("X-Cache-Status"))
	}
	if calls.Load() != 2 {
		t.Fatalf("expected separate cached method variants, got %d upstream calls", calls.Load())
	}
}

func TestCachePolicyUsesResponseTTL(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=2")
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	c := cache.NewInMemoryCache(10, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/ttl", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	item, err := c.Get(context.Background(), BuildCacheKey(req))
	if err != nil {
		t.Fatal(err)
	}
	remaining := time.Until(item.ExpiresAt)
	if remaining < time.Second || remaining > 3*time.Second {
		t.Fatalf("expected approximately 2 second TTL, got %s", remaining)
	}
}

func TestCachePolicyUsesDifferentFallbackTTLs(t *testing.T) {
	policy := DefaultCachePolicy()
	policy.CacheErrors = true
	policy.AllowedStatus = []StatusRange{{Min: 200, Max: 299}, {Min: 500, Max: 599}}
	policy.SuccessTTL = 5 * time.Minute
	policy.ErrorTTL = 3 * time.Second

	successDecision, successTTL := policy.responseDecision(nil, http.StatusOK, http.Header{}, 0)
	errorDecision, errorTTL := policy.responseDecision(nil, http.StatusInternalServerError, http.Header{}, 0)
	if !successDecision.Allowed || !errorDecision.Allowed {
		t.Fatal("expected both explicitly allowed response classes to be cacheable")
	}
	if successTTL != policy.SuccessTTL || errorTTL != policy.ErrorTTL {
		t.Fatalf("expected separate TTLs, got success=%s error=%s", successTTL, errorTTL)
	}
}

func TestServeHTTPRejectsBodiesOverLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/response" {
			_, _ = io.WriteString(w, strings.Repeat("a", maxRequestBodySize+1))
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	h, err := NewHandler(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	exact := httptest.NewRecorder()
	h.ServeHTTP(exact, httptest.NewRequest(http.MethodPost, "/body", strings.NewReader(strings.Repeat("a", maxRequestBodySize))))
	if exact.Code != http.StatusOK {
		t.Fatalf("expected exact-limit request to pass, got %d", exact.Code)
	}

	tooLarge := httptest.NewRecorder()
	h.ServeHTTP(tooLarge, httptest.NewRequest(http.MethodPost, "/body", strings.NewReader(strings.Repeat("a", maxRequestBodySize+1))))
	if tooLarge.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized request, got %d", tooLarge.Code)
	}

	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/response", nil))
	if response.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 for oversized upstream response, got %d", response.Code)
	}
	if c := h.cache.Size(); c != 0 {
		t.Fatalf("expected oversized upstream response not to be cached, got size %d", c)
	}
}

func TestCachePolicyNeverStoresPartialContent(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", "bytes 0-3/10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, "0123")
			return
		}
		_, _ = io.WriteString(w, "0123456789")
	}))
	defer upstream.Close()

	h, err := NewHandler(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	ranged := httptest.NewRequest(http.MethodGet, "/file", nil)
	ranged.Header.Set("Range", "bytes=0-3")
	partial := httptest.NewRecorder()
	h.ServeHTTP(partial, ranged)
	if partial.Code != http.StatusPartialContent || partial.Header().Get("X-Cache-Reason") != "RANGE" {
		t.Fatalf("expected ranged request to bypass, got %d %s", partial.Code, partial.Header().Get("X-Cache-Reason"))
	}

	full := httptest.NewRecorder()
	h.ServeHTTP(full, httptest.NewRequest(http.MethodGet, "/file", nil))
	if full.Body.String() != "0123456789" {
		t.Fatalf("full request got truncated body %q", full.Body.String())
	}

	decision, _ := DefaultCachePolicy().responseDecision(httptest.NewRequest(http.MethodGet, "/file", nil), http.StatusPartialContent, http.Header{}, 4)
	if decision.Allowed || decision.Reason != "PARTIAL" {
		t.Fatalf("206 must not be cacheable, got %+v", decision)
	}
}

func TestChunkedResponseIsBufferedAndCached(t *testing.T) {
	body := strings.Repeat("x", 5*1024)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body[:1024])
		w.(http.Flusher).Flush() // forces chunked transfer encoding
		_, _ = io.WriteString(w, body[1024:])
	}))
	defer upstream.Close()

	h, err := NewHandler(cache.NewInMemoryCache(10, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/books", nil))
	if first.Code != http.StatusOK || first.Body.String() != body {
		t.Fatalf("chunked response failed: %d len=%d", first.Code, first.Body.Len())
	}
	if first.Header().Get("Content-Length") != "5120" {
		t.Fatalf("expected buffered Content-Length, got %q", first.Header().Get("Content-Length"))
	}
	second := httptest.NewRecorder()
	h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/books", nil))
	if second.Header().Get("X-Cache-Status") != "HIT" || second.Body.String() != body {
		t.Fatalf("expected cached chunked body, got %s len=%d", second.Header().Get("X-Cache-Status"), second.Body.Len())
	}
}

func TestMaxTTLCapsResponseFreshness(t *testing.T) {
	policy := DefaultCachePolicy()
	policy.MaxTTL = 20 * time.Second
	long := http.Header{"Cache-Control": {"max-age=3600"}}
	if decision, ttl := policy.responseDecision(nil, http.StatusOK, long, 0); !decision.Allowed || ttl != 20*time.Second {
		t.Fatalf("max-age=3600 with MaxTTL=20s gave %+v ttl=%s, want 20s", decision, ttl)
	}
	short := http.Header{"Cache-Control": {"max-age=5"}}
	if _, ttl := policy.responseDecision(nil, http.StatusOK, short, 0); ttl != 5*time.Second {
		t.Fatalf("a shorter upstream TTL must be kept, got %s", ttl)
	}
	if _, ttl := policy.responseDecision(nil, http.StatusOK, http.Header{}, 0); ttl != 20*time.Second {
		t.Fatalf("SuccessTTL above MaxTTL must be capped, got %s", ttl)
	}
}
