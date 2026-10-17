package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"proks/internal/cache"
	"proks/internal/events"
	"proks/internal/observability"
	"proks/internal/predictor"
	"proks/internal/prefetcher"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

const maxRequestBodySize = 10 * 1024 * 1024

var (
	errRequestBodyTooLarge  = errors.New("proxy: request body exceeds configured maximum size")
	errResponseBodyTooLarge = errors.New("proxy: upstream response body exceeds configured maximum size")
	errStreamingUnsupported = errors.New("proxy: streaming responses are not supported by this policy")
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func setBufferedContentLength(headers http.Header, size int) {
	headers.Set("Content-Length", strconv.Itoa(size))
}

func isStreamingResponse(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

type Handler struct {
	cache             cache.Cache
	predictor         predictor.Predictor
	prefetcher        prefetcher.Prefetcher
	trend             *predictor.TrendTracker
	upstream          *url.URL
	httpClient        *http.Client
	prefetchThreshold float64
	prefetchedMu      sync.Mutex
	prefetched        map[string]time.Time
	eventStream       events.Stream
	metrics           *observability.Metrics
	policy            CachePolicy
	inflight          singleflight.Group
	invalidations     *invalidationTracker
	routes            routeTable

	prefetchMu          sync.Mutex
	prefetchRequests    map[string]prefetchRequest
	prefetchLimiter     *rateLimiter
	prefetchPausedUntil atomic.Int64
}

type upstreamResponse struct {
	body    []byte
	status  int
	headers http.Header
	stream  io.ReadCloser
}

func NewHandler(cache cache.Cache, pred predictor.Predictor, pf prefetcher.Prefetcher, upstream string) (*Handler, error) {
	return NewHandlerWithPolicy(cache, pred, pf, upstream, DefaultCachePolicy())
}

func NewHandlerWithPolicy(cache cache.Cache, pred predictor.Predictor, pf prefetcher.Prefetcher, upstream string, policy CachePolicy) (*Handler, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, err
	}
	routes, err := compileRoutes(policy.Routes)
	if err != nil {
		return nil, err
	}
	h := &Handler{
		cache:             cache,
		predictor:         pred,
		prefetcher:        pf,
		trend:             predictor.NewTrendTracker(0.35, 3, 0.6),
		upstream:          u,
		prefetchThreshold: 0.7,
		prefetched:        make(map[string]time.Time),
		metrics:           observability.NewMetrics(),
		policy:            policy.normalized(),
		invalidations:     newInvalidationTracker(),
		routes:            routes,
		prefetchLimiter:   newRateLimiter(defaultPrefetchRPS),
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.URL.Path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		_, _ = fmt.Fprintf(w, "# HELP cache_size Current cache entries\n")
		_, _ = fmt.Fprintf(w, "cache_size %d\n", h.cache.Size())
		_, _ = fmt.Fprintf(w, "# HELP cache_capacity Max cache capacity\n")
		_, _ = fmt.Fprintf(w, "cache_capacity %d\n", h.cache.Capacity())
		_, _ = fmt.Fprintf(w, "# HELP cache_bytes Approximate bytes held by cache entries\n")
		_, _ = fmt.Fprintf(w, "cache_bytes %d\n", h.cache.Bytes())
		return
	}
	ctx := r.Context()
	decision := h.policy.requestDecision(r)
	key := decision.Key
	if key == "" {
		key = BuildCacheKey(r)
	}
	status := string(cache.StatusMiss)
	reason := string(cache.ReasonLRU)

	defer func() {
		if h.metrics != nil {
			h.metrics.RecordRequest(status, reason, time.Since(start))
		}
		if slog.Default().Enabled(ctx, slog.LevelDebug) {
			slog.Debug("cache_decision",
				"request_id", requestIDFromContext(ctx),
				"key", key,
				"status", status,
				"reason", reason,
				"duration", time.Since(start),
			)
		}
	}()

	var staleItem *cache.Item
	if decision.Allowed {
		if item, fresh, err := h.cache.Lookup(ctx, key); err == nil {
			h.recordAccess(key)
			current := routeKey(r.URL.Path)
			h.recordTransition(r, current)
			if fresh {
				if h.consumePrefetchSignal(key) {
					h.recordTrend(key, true)
					status = string(cache.StatusPrefetch)
					reason = string(cache.ReasonMarkov)
				} else {
					status = string(cache.StatusHit)
					reason = string(cache.ReasonLRU)
				}
				writeCachedItem(w, item, status, reason)
				h.maybePrefetchForRequest(ctx, r, current)
				return
			}
			swr, sie := h.policy.staleWindows(item.Headers)
			now := time.Now()
			if now.Before(item.ExpiresAt.Add(swr)) {
				h.revalidate(r, key)
				status = string(cache.StatusStale)
				reason = "REVALIDATING"
				writeCachedItem(w, item, status, reason)
				return
			}
			if now.Before(item.ExpiresAt.Add(sie)) {
				staleItem = &item
			}
		}
	}

	upstream, follower, err := h.fetch(ctx, r, key, decision.Allowed)
	if follower && h.metrics != nil {
		h.metrics.RecordCollapsed()
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		h.pausePrefetch()
		if h.metrics != nil {
			kind := "other"
			if errors.Is(err, context.DeadlineExceeded) {
				kind = "timeout"
			}
			h.metrics.RecordUpstreamError(kind)
		}
	} else if err == nil && upstream.status >= http.StatusInternalServerError {
		h.pausePrefetch()
	}
	if staleItem != nil && (err != nil || upstream.status >= http.StatusInternalServerError) {
		status = string(cache.StatusStale)
		reason = "STALE_IF_ERROR"
		writeCachedItem(w, *staleItem, status, reason)
		return
	}
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			status = string(cache.StatusBypass)
			reason = "BODY_SIZE"
			w.Header().Set("X-Cache-Status", status)
			w.Header().Set("X-Cache-Reason", reason)
			return
		}
		if errors.Is(err, errResponseBodyTooLarge) {
			http.Error(w, "upstream response body too large", http.StatusBadGateway)
			status = string(cache.StatusBypass)
			reason = "BODY_SIZE"
			w.Header().Set("X-Cache-Status", status)
			w.Header().Set("X-Cache-Reason", reason)
			return
		}
		if errors.Is(err, errStreamingUnsupported) {
			http.Error(w, "streaming response is not supported", http.StatusBadGateway)
			status = string(cache.StatusBypass)
			reason = "STREAMING"
			w.Header().Set("X-Cache-Status", status)
			w.Header().Set("X-Cache-Reason", reason)
			return
		}
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}

	if upstream.stream != nil {
		copyHeaders(w.Header(), upstream.headers)
		w.Header().Del("Content-Length")
		w.Header().Set("X-Cache-Status", string(cache.StatusBypass))
		w.Header().Set("X-Cache-Reason", "STREAMING")
		w.WriteHeader(upstream.status)
		if err := streamResponse(r.Context(), w, upstream.stream); err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("streaming response failed", "error", err)
		}
		_ = upstream.stream.Close()
		status = string(cache.StatusBypass)
		reason = "STREAMING"
		return
	}
	respBytes, code, hdr := upstream.body, upstream.status, upstream.headers
	if h.metrics != nil {
		h.metrics.RecordPollution(upstream.pollution)
	}

	copyHeaders(w.Header(), hdr)
	setBufferedContentLength(w.Header(), len(respBytes))
	if upstream.stored {
		status = string(cache.StatusMiss)
		reason = string(cache.ReasonLRU)
		if follower {
			reason = "COLLAPSED"
		}
	} else {
		status = string(cache.StatusBypass)
		if !decision.Allowed {
			reason = decision.Reason
		} else {
			reason = upstream.decision.Reason
		}
	}
	w.Header().Set("X-Cache-Status", status)
	w.Header().Set("X-Cache-Reason", reason)

	w.WriteHeader(code)
	_, _ = w.Write(respBytes)

	current := routeKey(r.URL.Path)
	h.recordTransition(r, current)
	if decision.Allowed {
		h.maybePrefetchForRequest(ctx, r, current)
	}
}

func writeCachedItem(w http.ResponseWriter, item cache.Item, status, reason string) {
	if item.Headers != nil {
		copyHeaders(w.Header(), item.Headers)
	}
	setBufferedContentLength(w.Header(), len(item.Value))
	w.Header().Set("X-Cache-Status", status)
	w.Header().Set("X-Cache-Reason", reason)
	statusCode := item.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	w.WriteHeader(statusCode)
	_, _ = w.Write(item.Value)
}

// the cache. Collapsed requests share one fetchResult, so it is read-only.
type fetchResult struct {
	upstreamResponse
	decision  cacheDecision
	stored    bool
	pollution float64
}

// client that disconnects does not fail the others.
func (h *Handler) fetch(ctx context.Context, r *http.Request, key string, cacheable bool) (res fetchResult, follower bool, err error) {
	if !cacheable || !h.collapsible(r) {
		res, err = h.fetchAndStore(ctx, r, key, cacheable)
		return res, false, err
	}
	ran := false
	ch := h.inflight.DoChan(key, func() (interface{}, error) {
		ran = true
		return h.fetchAndStore(context.WithoutCancel(ctx), r, key, true)
	})
	select {
	case <-ctx.Done():
		return fetchResult{}, false, ctx.Err()
	case out := <-ch:
		res, _ = out.Val.(fetchResult)
		if ran || out.Err != nil || res.stored {
			return res, !ran, out.Err
		}
		res, err = h.fetchAndStore(ctx, r, key, false)
		return res, false, err
	}
}

func (h *Handler) revalidate(r *http.Request, key string) {
	req := r.Clone(withUpstreamKind(context.WithoutCancel(r.Context()), upstreamKindRevalidate))
	h.inflight.DoChan(key, func() (interface{}, error) {
		return h.fetchAndStore(req.Context(), req, key, true)
	})
}

// makes its response differ from the cached one.
func (h *Handler) collapsible(r *http.Request) bool {
	if h.policy.AllowStreaming {
		return false
	}
	if r.Body != nil && r.Body != http.NoBody {
		return false
	}
	for _, name := range []string{"If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since", "If-Range"} {
		if r.Header.Get(name) != "" {
			return false
		}
	}
	return true
}

const (
	upstreamKindClient     = "client"
	upstreamKindPrefetch   = "prefetch"
	upstreamKindRevalidate = "revalidate"
)

type upstreamKindKey struct{}

// withUpstreamKind labels the upstream requests made under ctx for
// proxy_upstream_requests_total. A collapsed request is counted once, under
// the kind of the request that led it.
func withUpstreamKind(ctx context.Context, kind string) context.Context {
	return context.WithValue(ctx, upstreamKindKey{}, kind)
}

func upstreamKindFrom(ctx context.Context) string {
	if kind, ok := ctx.Value(upstreamKindKey{}).(string); ok {
		return kind
	}
	return upstreamKindClient
}

func (h *Handler) fetchAndStore(ctx context.Context, r *http.Request, key string, cacheable bool) (fetchResult, error) {
	token := h.invalidations.begin()
	if h.metrics != nil {
		h.metrics.RecordUpstreamRequest(upstreamKindFrom(ctx))
	}
	upstream, err := h.fetchUpstream(ctx, r)
	res := fetchResult{upstreamResponse: upstream}
	if err != nil || upstream.stream != nil {
		return res, err
	}
	var ttl time.Duration
	res.decision, ttl = h.policy.responseDecision(r, upstream.status, upstream.headers, len(upstream.body))
	if !cacheable || !res.decision.Allowed {
		return res, nil
	}
	expiresAt := time.Now().Add(ttl)
	swr, sie := h.policy.staleWindows(upstream.headers)
	item := cache.Item{
		Key:        key,
		Value:      upstream.body,
		Headers:    upstream.headers,
		StatusCode: upstream.status,
		ExpiresAt:  expiresAt,
		StaleUntil: expiresAt.Add(max(swr, sie)),
		Version:    versionFromResponse(upstream.headers),
		Tags:       h.routes.tagsForResponse(r, upstream.headers),
	}
	if limited, ok := h.cache.(interface{ MaxItemBytes() int64 }); ok {
		if limit := limited.MaxItemBytes(); limit > 0 && cache.ItemSize(item) > limit {
			res.decision = cacheDecision{Reason: "TOO_LARGE"}
			if h.metrics != nil {
				h.metrics.RecordTooLarge()
			}
			return res, nil
		}
	}
	h.recordFrequency(key)
	metrics := h.computeMetrics(key)
	// The window is touched after the store: a key enters it on its first
	// miss and is admitted over an established key only on the next one.
	defer h.touchWindow(key)
	var invalidated bool
	res.stored, invalidated = h.invalidations.storeIfCurrent(token, append(slices.Clip(item.Tags), keyTag(key)), item.Version, func() bool {
		return h.cache.Set(ctx, item, metrics)
	})
	if invalidated {
		res.decision = cacheDecision{Reason: "INVALIDATED"}
	}
	res.pollution = metrics.Pollution
	return res, nil
}

func (h *Handler) invalidateResource(ctx context.Context, resourceID string, version int64) {
	// Recorded before the purge so that fetches already in flight do not
	// store what they read before it.
	h.invalidations.invalidate(resourceID, version)
	for _, key := range h.cache.KeysForTag(ctx, resourceID) {
		item, _, err := h.cache.Lookup(ctx, key)
		if err != nil || version == 0 || item.Version == 0 || item.Version <= version {
			h.cache.Delete(ctx, key)
		}
	}
}

func keyTag(key string) string {
	return "\x00key:" + key
}

func (h *Handler) invalidateKey(ctx context.Context, key string) {
	h.invalidations.invalidate(keyTag(key), 0)
	h.cache.Delete(ctx, key)
}

func requestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return "unknown"
	}
	if v, ok := ctx.Value("request_id").(string); ok && v != "" {
		return v
	}
	return "unknown"
}

func (h *Handler) MetricsRegistry() *prometheus.Registry {
	if h == nil || h.metrics == nil {
		return prometheus.NewRegistry()
	}
	return h.metrics.Registry()
}

func versionFromResponse(headers http.Header) int64 {
	value := headers.Get("X-Upstream-Book-Version")
	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil || version < 0 {
		return 0
	}
	return version
}

func (h *Handler) SetEventStream(stream events.Stream) {
	h.eventStream = stream
}

func (h *Handler) StartEventConsumer(ctx context.Context) error {
	if h.eventStream == nil {
		return nil
	}
	_, err := h.eventStream.Subscribe(ctx, h.HandleEvent)
	return err
}

// HandleEvent applies an invalidation: every cached response carrying one of
// the event's tags (unless it is newer than the event's version) and every
func (h *Handler) HandleEvent(ctx context.Context, event events.Event) error {
	event, err := events.Normalize(event)
	if err != nil {
		return err
	}
	if h.cache == nil {
		return nil
	}
	for _, tag := range event.Tags {
		h.invalidateResource(ctx, tag, event.Version)
	}
	for _, key := range event.Keys {
		h.invalidateKey(ctx, key)
	}
	if h.metrics != nil {
		h.metrics.RecordInvalidation(len(event.Tags), len(event.Keys))
	}
	return nil
}

func (h *Handler) recordAccess(key string) {
	h.recordFrequency(key)
	h.touchWindow(key)
}

func (h *Handler) tinyLFU() *cache.TinyLFUAdmission {
	if c, ok := h.cache.(*cache.InMemoryCache); ok {
		if adm, ok := c.Admission().(*cache.TinyLFUAdmission); ok {
			return adm
		}
	}
	return nil
}

func (h *Handler) recordFrequency(key string) {
	if adm := h.tinyLFU(); adm != nil && adm.Filter != nil {
		adm.Filter.Increment(key)
	}
}

func (h *Handler) touchWindow(key string) {
	if adm := h.tinyLFU(); adm != nil && adm.Window != nil {
		adm.Window.Touch(key)
	}
}

func (h *Handler) SetTrend(tracker *predictor.TrendTracker) {
	h.trend = tracker
}

func (h *Handler) recordTrend(key string, success bool) {
	if h.trend == nil || key == "" {
		return
	}
	h.trend.ObservePrefetch(key, success)
}

// MarkPrefetched records that key was loaded into the cache by a successful
// prefetch, so the next HIT for it is reported as PREFETCH. Call it only after
func (h *Handler) MarkPrefetched(key string) {
	if key == "" {
		return
	}
	h.prefetchedMu.Lock()
	defer h.prefetchedMu.Unlock()
	if h.prefetched == nil {
		h.prefetched = make(map[string]time.Time)
	}
	// Prefetched keys that are never hit would otherwise stay forever.
	if len(h.prefetched) >= maxPrefetchSignals {
		for k, seen := range h.prefetched {
			if time.Since(seen) > prefetchSignalTTL {
				delete(h.prefetched, k)
			}
		}
	}
	h.prefetched[key] = time.Now()
}

const (
	prefetchSignalTTL  = 5 * time.Minute
	maxPrefetchSignals = 1024
)

func (h *Handler) consumePrefetchSignal(key string) bool {
	if key == "" {
		return false
	}
	h.prefetchedMu.Lock()
	defer h.prefetchedMu.Unlock()
	if h.prefetched == nil {
		return false
	}
	seen, ok := h.prefetched[key]
	if !ok {
		return false
	}
	delete(h.prefetched, key)
	return time.Since(seen) <= prefetchSignalTTL
}

func (h *Handler) computeMetrics(key string) cache.Metrics {
	metrics := cache.Metrics{Frequency: 1, Pollution: 0, CandidateFrequency: 1, CurrentSize: 0}
	if c, ok := h.cache.(*cache.InMemoryCache); ok {
		metrics.CurrentSize = c.Size()
		if metrics.CurrentSize < c.Capacity()/2 {
			return metrics
		}
		if adm, ok := c.Admission().(*cache.TinyLFUAdmission); ok {
			if adm.Filter != nil {
				freq := adm.Filter.Estimate(key)
				if freq == 0 {
					freq = 1
				}
				metrics.Frequency = float64(freq)
				metrics.CandidateFrequency = freq
			}
			if victimKey, ok := c.VictimFor(key); ok {
				metrics.VictimKey = victimKey
				metrics.VictimFrequency = adm.Frequency(victimKey)
				metrics.Pollution = adm.PollutantScore(key, victimKey)
			}
		}
	}
	return metrics
}

func (h *Handler) recordTransition(r *http.Request, current string) {
	if r == nil || current == "" {
		return
	}
	prev := resolvePreviousRoute(r)
	if prev == "" || prev == current {
		return
	}
	h.predictor.Update(prev, current)
	if h.trend != nil {
		h.trend.ObserveTransition(prev, current, true)
	}
}

func resolvePreviousRoute(r *http.Request) string {
	if r == nil {
		return ""
	}
	for _, key := range []string{"X-Prev-Route-Key", "X-Previous-Route-Key", "X-Previous-Route", "X-Last-Route", "X-Route-Previous"} {
		if v := r.Header.Get(key); v != "" {
			return normalizeRouteKeyFromString(v)
		}
	}
	if ref := r.Referer(); ref != "" {
		if u, err := url.Parse(ref); err == nil && u.Path != "" {
			return normalizeRouteKeyFromString(u.Path)
		}
	}
	if q := r.URL.Query(); q.Get("prev") != "" {
		return normalizeRouteKeyFromString(q.Get("prev"))
	}
	if v := r.Context().Value("prev_route"); v != nil {
		if s, ok := v.(string); ok && s != "" {
			return normalizeRouteKeyFromString(s)
		}
	}
	return ""
}

func normalizeRouteKeyFromString(s string) string {
	if s == "" {
		return ""
	}
	if u, err := url.Parse(s); err == nil {
		s = u.Path
	}
	return routeKey(s)
}

// maybePrefetchForRequest queues the most likely next route after r for
// prefetching when the predictor is confident enough. The queued key is the
// cache key a client would hit for that route; the request template is kept
// for Prefetch.
func (h *Handler) maybePrefetchForRequest(ctx context.Context, r *http.Request, current string) {
	if current == "" || r == nil {
		return
	}
	cacheCtx := context.Background()
	candidates := h.predictor.Predict(current)
	if len(candidates) == 0 {
		return
	}
	top := candidates[0]
	adjustedProbability := top.Probability
	if h.trend != nil {
		adjustedProbability = h.trend.AdjustProbability(top.Key, top.Probability)
	}
	if adjustedProbability < h.prefetchThreshold {
		return
	}
	if pf, ok := h.prefetcher.(interface{ Enabled() bool }); ok && !pf.Enabled() {
		return
	}
	prefetchReq, prefetchKey, ok := h.prefetchTemplate(r, top.Key)
	if !ok {
		return
	}
	if _, err := h.cache.Get(cacheCtx, prefetchKey); err == nil {
		return
	}
	if h.cacheIsPolluted(prefetchKey) {
		return
	}
	h.rememberPrefetchRequest(prefetchKey, prefetchReq)
	prefetchCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// The key is marked as prefetched only by MarkPrefetched, once the load has
	// actually stored the item; marking at enqueue time would label ordinary
	// HITs as PREFETCH.
	h.prefetcher.Enqueue(prefetchCtx, prefetchKey, prefetcher.ReasonMarkov)
	if h.metrics != nil {
		h.metrics.RecordPrefetchEnqueued()
	}
}

func (h *Handler) cacheIsPolluted(candidateKey string) bool {
	c, ok := h.cache.(*cache.InMemoryCache)
	if !ok || c == nil {
		return false
	}
	if c.Size() < c.Capacity()/2 {
		return false
	}
	admission, ok := c.Admission().(*cache.TinyLFUAdmission)
	if !ok || admission == nil || admission.Filter == nil {
		return false
	}
	if victimKey, ok := c.VictimFor(candidateKey); ok {
		if victimKey == candidateKey {
			return true
		}
		candidateFreq := admission.Filter.Estimate(candidateKey)
		victimFreq := admission.Filter.Estimate(victimKey)
		if candidateFreq == 0 && victimFreq == 0 {
			return true
		}
		if victimFreq > 0 && candidateFreq <= victimFreq && candidateFreq < admission.MinFrequency {
			return true
		}
		pollution := admission.PollutantScore(candidateKey, victimKey)
		if victimFreq > candidateFreq && pollution > admission.MaxPollutionRatio {
			return true
		}
	}
	return false
}

func BuildCacheKey(r *http.Request) string {
	if r == nil {
		return ""
	}
	return buildPolicyCacheKey(r, DefaultCachePolicy())
}

func normalizeQuery(v url.Values) string {
	if len(v) == 0 {
		return ""
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		vals := append([]string(nil), v[k]...)
		sort.Strings(vals)
		parts = append(parts, k+"="+strings.Join(vals, ","))
	}
	return strings.Join(parts, "&")
}

func (h *Handler) fetchUpstream(ctx context.Context, in *http.Request) (upstreamResponse, error) {
	var empty upstreamResponse
	policy := h.policy.normalized()
	if in.Body != nil && in.Body != http.NoBody {
		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer bufferPool.Put(buf)

		maxBodySize := policy.MaxBodySize
		if in.ContentLength > maxBodySize {
			return empty, errRequestBodyTooLarge
		}
		limitedReader := io.LimitReader(in.Body, maxBodySize+1)
		if _, err := buf.ReadFrom(limitedReader); err != nil {
			return empty, err
		}
		if int64(buf.Len()) > maxBodySize {
			return empty, errRequestBodyTooLarge
		}
		_ = in.Body.Close()

		bodyBytes := buf.Bytes()
		bodyCopy := make([]byte, len(bodyBytes))
		copy(bodyCopy, bodyBytes)

		in.Body = io.NopCloser(bytes.NewReader(bodyCopy))
	}

	upstreamURL := h.upstream.ResolveReference(&url.URL{Path: in.URL.Path, RawQuery: in.URL.RawQuery}).String()
	outReq, err := http.NewRequestWithContext(ctx, in.Method, upstreamURL, in.Body)
	if err != nil {
		return empty, err
	}
	outReq.Header = prepareUpstreamHeaders(in, policy.TrustForwardedHeaders)

	client := h.httpClient
	if policy.AllowStreaming {
		streamingClient := *h.httpClient
		streamingClient.Timeout = 0
		client = &streamingClient
	}
	resp, err := client.Do(outReq)
	if err != nil {
		return empty, err
	}
	if !policy.AllowStreaming && isStreamingResponse(resp) {
		_ = resp.Body.Close()
		return empty, errStreamingUnsupported
	}
	if policy.AllowStreaming && isStreamingResponse(resp) {
		return upstreamResponse{status: resp.StatusCode, headers: resp.Header.Clone(), stream: resp.Body}, nil
	}
	defer func() { _ = resp.Body.Close() }()

	bufResp := bufferPool.Get().(*bytes.Buffer)
	bufResp.Reset()
	defer bufferPool.Put(bufResp)

	maxBodySize := policy.MaxBodySize
	if resp.ContentLength > maxBodySize {
		return empty, errResponseBodyTooLarge
	}
	limitedRespReader := io.LimitReader(resp.Body, maxBodySize+1)
	if _, err := bufResp.ReadFrom(limitedRespReader); err != nil {
		return empty, err
	}
	if int64(bufResp.Len()) > maxBodySize {
		return empty, errResponseBodyTooLarge
	}

	respBodyCopy := make([]byte, bufResp.Len())
	copy(respBodyCopy, bufResp.Bytes())

	return upstreamResponse{body: respBodyCopy, status: resp.StatusCode, headers: resp.Header.Clone()}, nil
}

func copyHeaders(dst, src http.Header) {
	connectionHeaders := make(map[string]bool)
	for _, name := range hopByHopHeaders(src) {
		connectionHeaders[http.CanonicalHeaderKey(name)] = true
	}
	for k, vals := range src {
		if isHopByHopHeader(k) || connectionHeaders[http.CanonicalHeaderKey(k)] ||
			strings.EqualFold(k, "X-Upstream-Book-Version") {
			continue
		}
		dst.Del(k)
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}

func isHopByHopHeader(name string) bool {
	switch strings.ToLower(name) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func hopByHopHeaders(headers http.Header) []string {
	var names []string
	for _, value := range headers.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			if strings.TrimSpace(name) != "" {
				names = append(names, strings.TrimSpace(name))
			}
		}
	}
	return names
}

func prepareUpstreamHeaders(request *http.Request, trustForwarded bool) http.Header {
	out := request.Header.Clone()
	connectionHeaders := make(map[string]bool)
	for _, name := range hopByHopHeaders(out) {
		connectionHeaders[http.CanonicalHeaderKey(name)] = true
	}
	for name := range out {
		if isHopByHopHeader(name) || connectionHeaders[http.CanonicalHeaderKey(name)] ||
			strings.EqualFold(name, "X-Upstream-Book-Version") {
			out.Del(name)
		}
	}
	out.Del("X-Book-Version")
	clientIP := request.RemoteAddr
	if host, _, err := net.SplitHostPort(clientIP); err == nil {
		clientIP = host
	}
	if !trustForwarded {
		out.Del("X-Forwarded-For")
	}
	appendForwarded(out, "X-Forwarded-For", clientIP)
	proto := "http"
	if request.TLS != nil {
		proto = "https"
	}
	out.Set("X-Forwarded-Proto", proto)
	out.Set("X-Forwarded-Host", request.Host)
	out.Add("Via", "1.1 proks")
	return out
}

func streamResponse(ctx context.Context, w http.ResponseWriter, src io.Reader) error {
	flusher, canFlush := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := src.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func appendForwarded(headers http.Header, name, value string) {
	if value == "" {
		return
	}
	if current := headers.Get(name); current != "" {
		headers.Set(name, current+", "+value)
	} else {
		headers.Set(name, value)
	}
}

func (h *Handler) Metrics() *observability.Metrics {
	return h.metrics
}
