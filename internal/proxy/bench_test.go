package proxy

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"proks/internal/cache"
	"proks/internal/predictor"
)

func withLogLevel(b *testing.B, level slog.Level) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: level})))
	b.Cleanup(func() { slog.SetDefault(prev) })
}

var logLevels = []struct {
	name  string
	level slog.Level
}{
	{"info", slog.LevelInfo},
	{"debug", slog.LevelDebug},
}

func BenchmarkServeHTTPHit(b *testing.B) {
	for _, lv := range logLevels {
		b.Run("log="+lv.name, func(b *testing.B) {
			withLogLevel(b, lv.level)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, `{"id":1,"title":"Go"}`)
			}))
			defer upstream.Close()
			h, err := NewHandler(cache.NewTinyLFURuntimeCache(10_000, 0), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
			if err != nil {
				b.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, "/books/1", nil)
			h.ServeHTTP(httptest.NewRecorder(), req)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Header().Get("X-Cache-Status") != "HIT" {
					b.Fatalf("expected HIT, got %s", rec.Header().Get("X-Cache-Status"))
				}
			}
		})
	}
}

func BenchmarkServeHTTPMiss(b *testing.B) {
	for _, lv := range logLevels {
		b.Run("log="+lv.name, func(b *testing.B) {
			withLogLevel(b, lv.level)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, `{"id":1,"title":"Go"}`)
			}))
			defer upstream.Close()
			h, err := NewHandler(cache.NewTinyLFURuntimeCache(1<<20, 0), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/"+strconv.Itoa(i), nil))
				if rec.Header().Get("X-Cache-Status") != "MISS" {
					b.Fatalf("expected MISS, got %s", rec.Header().Get("X-Cache-Status"))
				}
			}
		})
	}
}
