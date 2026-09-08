package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"proks/internal/observability"
	"strings"
	"syscall"
	"time"

	"proks/internal/cache"
	"proks/internal/events"
	"proks/internal/predictor"
	"proks/internal/prefetcher"
	"proks/internal/proxy"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	upstream := getenv("UPSTREAM_URL", "http://localhost:8081")
	addr := getenv("LISTEN_ADDR", ":8080")

	c := cache.NewTinyLFURuntimeCache(10_000)
	pred := predictor.NewMarkov()
	tracker := predictor.NewTrendTracker(0.35, 3, 0.6)
	rabbitmqURL := getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	broker, err := events.NewRabbitBroker(rabbitmqURL)
	if err != nil {
		log.Fatalf("failed to connect rabbitmq: %v", err)
	}
	defer broker.Close()

	pf := prefetcher.NewAsyncPrefetcher(4, 1024, 100*time.Millisecond, func(ctx context.Context, key string) error {
		return nil
	}, log.Default())
	pf.SetOutcomeHook(func(key string, success bool) {
		tracker.ObservePrefetch(key, success)
	})
	defer pf.Stop()

	h, err := proxy.NewHandler(c, pred, pf, upstream)
	if err != nil {
		log.Fatalf("build handler: %v", err)
	}
	h.SetTrend(tracker)
	h.SetEventStream(broker)
	if err := h.StartEventConsumer(context.Background()); err != nil {
		log.Fatalf("subscribe book updates: %v", err)
	}

	go runPrefetchMetricsLoop(context.Background(), pf, h.Metrics(), 2*time.Second)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(h.MetricsRegistry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/internal/events/book.updated", func(w http.ResponseWriter, r *http.Request) {
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
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	})
	mux.Handle("/", h)

	srv := &http.Server{
		Addr:              addr,
		Handler:           withStructuredLogging(mux),
		ReadHeaderTimeout: 2 * time.Second,
	}

	go func() {
		log.Printf("proxy listening on %s, upstream %s", addr, upstream)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func withStructuredLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = fmt.Sprintf("%d-%s", time.Now().UnixNano(), strings.TrimSpace(r.RemoteAddr))
		}
		ctx := context.WithValue(r.Context(), "request_id", requestID)
		logger := slog.With("component", "http", "request_id", requestID, "method", r.Method, "path", r.URL.Path)
		w.Header().Set("X-Request-ID", requestID)
		logger.Info("request_started")
		next.ServeHTTP(w, r.WithContext(ctx))
		logger.Info("request_finished")
	})
}
func runPrefetchMetricsLoop(ctx context.Context, pf *prefetcher.AsyncPrefetcher, m *observability.Metrics, interval time.Duration) {
	var last prefetcher.Stats
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := pf.Snapshot()
			m.RecordQueueDepth(pf.QueueDepth())
			m.RecordPrefetchDelta(
				snap.Executed-last.Executed,
				snap.Dropped-last.Dropped,
				snap.Canceled-last.Canceled,
				snap.Failed-last.Failed,
			)
			last = snap
		}
	}
}
