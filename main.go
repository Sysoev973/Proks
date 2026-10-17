package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"proks/internal/cache"
	"proks/internal/events"
	"proks/internal/observability"
	"proks/internal/predictor"
	"proks/internal/prefetcher"
	"proks/internal/proxy"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	upstream := getenv("UPSTREAM_URL", "http://localhost:8081")
	addr := getenv("LISTEN_ADDR", ":8080")

	c := cache.NewTinyLFURuntimeCache(
		int(getenvUint64("CACHE_CAPACITY", 10_000)),
		int64(getenvUint64("CACHE_MAX_BYTES", 256<<20)),
	)
	pred := predictor.NewMarkov()

	alpha := getenvFloat("EWMA_ALPHA", 0.35)
	minObs := getenvUint64("EWMA_MIN_OBSERVATIONS", 3)
	cutoff := getenvFloat("EWMA_CUTOFF", 0.6)
	tracker := predictor.NewTrendTracker(alpha, minObs, cutoff)

	broker, closeBroker, err := newEventStream()
	if err != nil {
		log.Fatalf("event stream: %v", err)
	}
	defer closeBroker()

	var prefetchLoader prefetcher.FetchFunc
	pf := prefetcher.NewAsyncPrefetcher(4, 1024, 100*time.Millisecond, prefetchLoader, log.Default())
	defer pf.Stop()

	policy := proxy.DefaultCachePolicy()
	policy.StaleWhileRevalidate = getenvDuration("CACHE_STALE_WHILE_REVALIDATE", 10*time.Second)
	policy.StaleIfError = getenvDuration("CACHE_STALE_IF_ERROR", 5*time.Minute)
	if limit := c.MaxItemBytes(); limit > 0 && limit < policy.MaxBodySize {
		log.Printf("WARNING: CACHE_MAX_BYTES gives each cache shard %d bytes, so responses larger than that are served but never cached (max body is %d); raise CACHE_MAX_BYTES or lower CACHE_CAPACITY to use fewer shards", limit, policy.MaxBodySize)
	}
	h, err := proxy.NewHandlerWithPolicy(c, pred, pf, upstream, policy)
	if err != nil {
		log.Fatalf("build handler: %v", err)
	}
	pf.SetOutcomeHook(func(key string, success bool) {
		if success {
			h.MarkPrefetched(key)
		}
		tracker.ObservePrefetch(key, success)
	})
	h.SetTrend(tracker)
	h.SetEventStream(broker)
	if err := h.StartEventConsumer(context.Background()); err != nil {
		log.Fatalf("subscribe book updates: %v", err)
	}

	go runPrefetchMetricsLoop(context.Background(), pf, h.Metrics(), 2*time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/health/ready", newProxyReadinessHandler(upstream))
	mux.Handle("/metrics", promhttp.HandlerFor(h.MetricsRegistry(), promhttp.HandlerOpts{}))

	adminToken := os.Getenv("ADMIN_TOKEN")
	adminAddr := os.Getenv("ADMIN_LISTEN_ADDR")
	bookUpdated := requireAdminToken(adminToken, newBookUpdatedHandler(broker))
	var adminSrv *http.Server
	// Unless served on the public port explicitly, the path must answer 404
	// rather than fall through to the upstream proxy.
	var publicBookUpdated http.Handler = http.NotFoundHandler()
	switch {
	case adminAddr != "":
		adminMux := http.NewServeMux()
		adminMux.Handle(bookUpdatedPath, bookUpdated)
		adminSrv = &http.Server{
			Addr:              adminAddr,
			Handler:           withStructuredLogging(adminMux),
			ReadHeaderTimeout: 2 * time.Second,
		}
	case adminToken != "":
		publicBookUpdated = bookUpdated
	default:
		log.Printf("%s is disabled: set ADMIN_TOKEN and/or ADMIN_LISTEN_ADDR to enable it", bookUpdatedPath)
	}
	mux.Handle(bookUpdatedPath, publicBookUpdated)
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
	if adminSrv != nil {
		go func() {
			log.Printf("admin listening on %s", adminAddr)
			if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("admin listen: %v", err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if adminSrv != nil {
		_ = adminSrv.Shutdown(ctx)
	}
}

const bookUpdatedPath = "/internal/events/book.updated"

func newBookUpdatedHandler(stream events.Stream) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var evt events.BookUpdateEvent
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		if err := events.ValidateBookUpdateEvent(evt); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := stream.Publish(r.Context(), evt); err != nil {
			slog.Error("publish book.updated failed", "error", err)
			http.Error(w, "event not published", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}
}

// requireAdminToken rejects requests without "Authorization: Bearer <token>".
func requireAdminToken(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="admin"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// newEventStream connects to RabbitMQ when RABBITMQ_URL is set and falls back
func newEventStream() (events.Stream, func(), error) {
	noop := func() {}
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		log.Printf("RABBITMQ_URL is not set; using in-memory event broker (invalidation is local to this replica)")
		return events.NewInMemoryBroker(), noop, nil
	}
	timeout := 60 * time.Second
	if v, err := time.ParseDuration(os.Getenv("RABBITMQ_CONNECT_TIMEOUT")); err == nil && v >= 0 {
		timeout = v
	}
	broker, err := connectRabbitBroker(rabbitmqURL, timeout)
	if err == nil {
		return broker, broker.Close, nil
	}
	if required, _ := strconv.ParseBool(os.Getenv("RABBITMQ_REQUIRED")); required {
		return nil, noop, fmt.Errorf("connect rabbitmq: %w", err)
	}
	log.Printf("WARNING: rabbitmq unavailable (%v); using in-memory event broker (invalidation is local to this replica)", err)
	return events.NewInMemoryBroker(), noop, nil
}

func newProxyReadinessHandler(upstream string) http.HandlerFunc {
	client := &http.Client{Timeout: 2 * time.Second}
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream+"/health/ready", nil)
		if err != nil {
			http.Error(w, "upstream is not ready", http.StatusServiceUnavailable)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "upstream is not ready", http.StatusServiceUnavailable)
			return
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				slog.Debug("failed to close readiness response body", "error", err)
			}
		}()
		if resp.StatusCode != http.StatusOK {
			http.Error(w, "upstream is not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func connectRabbitBroker(amqpURL string, timeout time.Duration) (*events.RabbitBroker, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		broker, err := events.NewRabbitBroker(amqpURL)
		if err == nil {
			return broker, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out after %s: %w", timeout, lastErr)
		}
		time.Sleep(time.Second)
	}
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

func getenvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil && d >= 0 {
		return d
	}
	return fallback
}

func getenvUint64(key string, fallback uint64) uint64 {
	v := os.Getenv(key)
	if u, err := strconv.ParseUint(v, 10, 64); err == nil {
		return u
	}
	return fallback
}
