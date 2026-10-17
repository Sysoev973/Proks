package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
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
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	setupLogging(cfg)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		slog.Error("proxy stopped", "error", err)
		os.Exit(1)
	}
}

type Config struct {
	UpstreamURL string
	ListenAddr  string
	// AdminAddr, when set, serves the admin endpoints on a separate listener
	// only; the public port then answers them with 404.
	AdminAddr  string
	AdminToken string

	CacheCapacity        uint64
	CacheMaxBytes        uint64
	CacheTTL             time.Duration
	CacheMaxTTL          time.Duration
	StaleWhileRevalidate time.Duration
	StaleIfError         time.Duration
	Routes               []proxy.Route

	EWMAAlpha           float64
	EWMAMinObservations uint64
	EWMACutoff          float64

	PrefetchEnabled bool
	PrefetchWorkers uint64
	PrefetchQueue   uint64
	PrefetchTimeout time.Duration
	PrefetchRPS     float64

	RabbitURL            string
	RabbitConnectTimeout time.Duration
	RabbitRequired       bool

	LogLevel slog.Level
	// LogHitSample logs one of every LogHitSample HIT requests; 0 or 1 logs all.
	LogHitSample uint64

	// onListen is called with the bound addresses once run is serving; tests
	// use it to find ports chosen by ":0".
	onListen func(public, admin net.Addr)
}

// envReader parses environment variables, collecting every invalid value.
type envReader struct {
	getenv func(string) string
	errs   []error
}

func (e *envReader) string(key, fallback string) string {
	if v := strings.TrimSpace(e.getenv(key)); v != "" {
		return v
	}
	return fallback
}

func (e *envReader) uint(key string, fallback uint64) uint64 {
	v := strings.TrimSpace(e.getenv(key))
	if v == "" {
		return fallback
	}
	u, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a non-negative integer", key, v))
		return fallback
	}
	return u
}

func (e *envReader) float(key string, fallback float64) float64 {
	v := strings.TrimSpace(e.getenv(key))
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a number", key, v))
		return fallback
	}
	return f
}

func (e *envReader) duration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(e.getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a non-negative duration", key, v))
		return fallback
	}
	return d
}

func (e *envReader) bool(key string, fallback bool) bool {
	v := strings.TrimSpace(e.getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a boolean", key, v))
		return fallback
	}
	return b
}

func (e *envReader) level(key string, fallback slog.Level) slog.Level {
	v := strings.TrimSpace(e.getenv(key))
	if v == "" {
		return fallback
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(v)); err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %q is not a log level (debug, info, warn, error)", key, v))
		return fallback
	}
	return level
}

// loadConfig reads the configuration from getenv (os.Getenv in production).
// Unset variables take their defaults; set but invalid ones are errors.
func loadConfig(getenv func(string) string) (Config, error) {
	e := &envReader{getenv: getenv}
	cfg := Config{
		UpstreamURL:          e.string("UPSTREAM_URL", "http://localhost:8081"),
		ListenAddr:           e.string("LISTEN_ADDR", ":8080"),
		AdminAddr:            e.string("ADMIN_LISTEN_ADDR", ""),
		AdminToken:           e.string("ADMIN_TOKEN", ""),
		CacheCapacity:        e.uint("CACHE_CAPACITY", 10_000),
		CacheMaxBytes:        e.uint("CACHE_MAX_BYTES", 256<<20),
		CacheTTL:             e.duration("CACHE_TTL", 5*time.Minute),
		CacheMaxTTL:          e.duration("CACHE_MAX_TTL", 0),
		StaleWhileRevalidate: e.duration("CACHE_STALE_WHILE_REVALIDATE", 10*time.Second),
		StaleIfError:         e.duration("CACHE_STALE_IF_ERROR", 5*time.Minute),
		EWMAAlpha:            e.float("EWMA_ALPHA", 0.35),
		EWMAMinObservations:  e.uint("EWMA_MIN_OBSERVATIONS", 3),
		EWMACutoff:           e.float("EWMA_CUTOFF", 0.6),
		PrefetchEnabled:      e.bool("PREFETCH_ENABLED", false),
		PrefetchWorkers:      e.uint("PREFETCH_WORKERS", 4),
		PrefetchQueue:        e.uint("PREFETCH_QUEUE", 1024),
		PrefetchTimeout:      e.duration("PREFETCH_TIMEOUT", time.Second),
		PrefetchRPS:          e.float("PREFETCH_RPS", 20),
		RabbitURL:            e.string("RABBITMQ_URL", ""),
		RabbitConnectTimeout: e.duration("RABBITMQ_CONNECT_TIMEOUT", 60*time.Second),
		RabbitRequired:       e.bool("RABBITMQ_REQUIRED", false),
		LogLevel:             e.level("LOG_LEVEL", slog.LevelInfo),
		LogHitSample:         e.uint("LOG_HIT_SAMPLE", 1),
	}
	if routesFile := e.string("ROUTES_CONFIG", ""); routesFile != "" {
		routes, err := proxy.LoadRoutes(routesFile)
		if err != nil {
			e.errs = append(e.errs, fmt.Errorf("ROUTES_CONFIG: %w", err))
		}
		cfg.Routes = routes
	}
	if cfg.CacheCapacity == 0 {
		e.errs = append(e.errs, errors.New("CACHE_CAPACITY must be positive"))
	}
	if cfg.CacheTTL == 0 {
		e.errs = append(e.errs, errors.New("CACHE_TTL must be positive"))
	}
	return cfg, errors.Join(e.errs...)
}

func setupLogging(cfg Config) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))
}

// run serves the proxy until ctx is canceled, then shuts both servers down.
func run(ctx context.Context, cfg Config) error {
	c := cache.NewTinyLFURuntimeCache(int(cfg.CacheCapacity), int64(cfg.CacheMaxBytes))
	pred := predictor.NewMarkov()
	tracker := predictor.NewTrendTracker(cfg.EWMAAlpha, cfg.EWMAMinObservations, cfg.EWMACutoff)

	broker, closeBroker, err := newEventStream(cfg)
	if err != nil {
		return fmt.Errorf("event stream: %w", err)
	}
	defer closeBroker()

	// The loader needs the handler and the handler needs the prefetcher; h is
	// assigned before the first request can enqueue anything.
	var h *proxy.Handler
	var prefetchLoader prefetcher.FetchFunc
	if cfg.PrefetchEnabled {
		prefetchLoader = func(ctx context.Context, key string) error { return h.Prefetch(ctx, key) }
	}
	pf := prefetcher.NewAsyncPrefetcher(int(cfg.PrefetchWorkers), int(cfg.PrefetchQueue), cfg.PrefetchTimeout, prefetchLoader, log.Default())
	defer pf.Stop()

	policy := proxy.DefaultCachePolicy()
	policy.SuccessTTL = cfg.CacheTTL
	policy.MaxTTL = cfg.CacheMaxTTL
	policy.StaleWhileRevalidate = cfg.StaleWhileRevalidate
	policy.StaleIfError = cfg.StaleIfError
	policy.Routes = cfg.Routes
	if limit := c.MaxItemBytes(); limit > 0 && limit < policy.MaxBodySize {
		slog.Warn("responses larger than one cache shard are served but never cached; raise CACHE_MAX_BYTES or lower CACHE_CAPACITY to use fewer shards",
			"shard_bytes", limit, "max_body_bytes", policy.MaxBodySize)
	}
	h, err = proxy.NewHandlerWithPolicy(c, pred, pf, cfg.UpstreamURL, policy)
	if err != nil {
		return fmt.Errorf("build handler: %w", err)
	}
	h.SetPrefetchBudget(cfg.PrefetchRPS)
	pf.SetOutcomeHook(func(key string, success bool) {
		if success {
			h.MarkPrefetched(key)
		}
		tracker.ObservePrefetch(key, success)
	})
	h.SetTrend(tracker)
	h.SetEventStream(broker)
	if err := h.StartEventConsumer(ctx); err != nil {
		return fmt.Errorf("subscribe to invalidations: %w", err)
	}

	go runPrefetchMetricsLoop(ctx, pf, h.Metrics(), 2*time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/health/ready", newProxyReadinessHandler(cfg.UpstreamURL))
	mux.Handle("/metrics", promhttp.HandlerFor(h.MetricsRegistry(), promhttp.HandlerOpts{}))

	admin := http.NewServeMux()
	admin.Handle(purgePath, newInvalidationHandler(broker, events.TypeInvalidate))
	admin.Handle(bookUpdatedPath, newInvalidationHandler(broker, events.TypeBookUpdated))
	adminHandler := requireAdminToken(cfg.AdminToken, admin)
	var adminSrv *http.Server
	// Unless served on the public port explicitly, the admin paths must answer
	// 404 rather than fall through to the upstream proxy.
	var publicAdmin http.Handler = http.NotFoundHandler()
	switch {
	case cfg.AdminAddr != "":
		adminSrv = &http.Server{
			Handler:           withStructuredLogging(adminHandler, 1),
			ReadHeaderTimeout: 2 * time.Second,
		}
	case cfg.AdminToken != "":
		publicAdmin = adminHandler
	default:
		slog.Warn("admin endpoints are disabled: set ADMIN_TOKEN and/or ADMIN_LISTEN_ADDR to enable them", "paths", []string{purgePath, bookUpdatedPath})
	}
	mux.Handle(purgePath, publicAdmin)
	mux.Handle(bookUpdatedPath, publicAdmin)
	mux.Handle("/", h)

	srv := &http.Server{
		Handler:           withStructuredLogging(mux, cfg.LogHitSample),
		ReadHeaderTimeout: 2 * time.Second,
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	var adminLn net.Listener
	if adminSrv != nil {
		if adminLn, err = net.Listen("tcp", cfg.AdminAddr); err != nil {
			_ = ln.Close()
			return fmt.Errorf("admin listen: %w", err)
		}
	}

	errc := make(chan error, 2)
	slog.Info("proxy listening", "addr", ln.Addr().String(), "upstream", cfg.UpstreamURL, "prefetch", cfg.PrefetchEnabled)
	go func() { errc <- srv.Serve(ln) }()
	var adminAddr net.Addr
	if adminSrv != nil {
		adminAddr = adminLn.Addr()
		slog.Info("admin listening", "addr", adminAddr.String())
		go func() { errc <- adminSrv.Serve(adminLn) }()
	}
	if cfg.onListen != nil {
		cfg.onListen(ln.Addr(), adminAddr)
	}

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errc:
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if adminSrv != nil {
		shutdownErr = errors.Join(shutdownErr, adminSrv.Shutdown(shutdownCtx))
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.Join(serveErr, shutdownErr)
	}
	return shutdownErr
}

const (
	purgePath       = "/admin/purge"
	bookUpdatedPath = "/internal/events/book.updated"
	maxAdminBody    = 1 << 20
)

// newInvalidationHandler publishes an invalidation to every replica.
// For eventType "invalidate" (POST /admin/purge) the body is
// {"tags": [...], "keys": [...], "version": n}; for the legacy
// "book.updated" endpoint it is {"type": "book.updated", "book_id": "1"}.
func newInvalidationHandler(stream events.Stream, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var evt events.Event
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAdminBody)).Decode(&evt); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		if eventType == events.TypeInvalidate {
			evt.Type = events.TypeInvalidate
		} else if evt.Type != eventType {
			http.Error(w, fmt.Sprintf("type must be %q", eventType), http.StatusBadRequest)
			return
		}
		if _, err := events.Normalize(evt); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := stream.Publish(r.Context(), evt); err != nil {
			slog.Error("publish invalidation failed", "error", err)
			http.Error(w, "event not published", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
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
// to an in-memory broker unless RABBITMQ_REQUIRED is set.
func newEventStream(cfg Config) (events.Stream, func(), error) {
	noop := func() {}
	if cfg.RabbitURL == "" {
		slog.Warn("RABBITMQ_URL is not set; using in-memory event broker (invalidation is local to this replica)")
		return events.NewInMemoryBroker(), noop, nil
	}
	broker, err := connectRabbitBroker(cfg.RabbitURL, cfg.RabbitConnectTimeout)
	if err == nil {
		return broker, broker.Close, nil
	}
	if cfg.RabbitRequired {
		return nil, noop, fmt.Errorf("connect rabbitmq: %w", err)
	}
	slog.Warn("rabbitmq unavailable; using in-memory event broker (invalidation is local to this replica)", "error", err)
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

// statusRecorder remembers the status code written by the wrapped handler.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// withStructuredLogging sets X-Request-ID and writes one request_finished
// line per request with the HTTP status, the cache status and reason, and the
// duration. With hitSample > 1 only every hitSample-th HIT is logged.
func withStructuredLogging(next http.Handler, hitSample uint64) http.Handler {
	var hits atomic.Uint64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = fmt.Sprintf("%d-%s", start.UnixNano(), strings.TrimSpace(r.RemoteAddr))
		}
		ctx := context.WithValue(r.Context(), "request_id", requestID)
		w.Header().Set("X-Request-ID", requestID)
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r.WithContext(ctx))

		cacheStatus := rec.Header().Get("X-Cache-Status")
		if cacheStatus == string(cache.StatusHit) && hitSample > 1 && hits.Add(1)%hitSample != 0 {
			return
		}
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		slog.LogAttrs(ctx, slog.LevelInfo, "request_finished",
			slog.String("request_id", requestID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", status),
			slog.String("cache_status", cacheStatus),
			slog.String("reason", rec.Header().Get("X-Cache-Reason")),
			slog.Duration("duration", time.Since(start)),
		)
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
