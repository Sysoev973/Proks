package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"proks/internal/cache"
	"proks/internal/events"
	"proks/internal/observability"
	"proks/internal/predictor"
	"proks/internal/prefetcher"
)

type Handler struct {
	cache             cache.Cache
	predictor         predictor.Predictor
	prefetcher        prefetcher.Prefetcher
	trend             *predictor.TrendTracker
	upstream          *url.URL
	httpClient        *http.Client
	prefetchThreshold float64
	defaultTTL        time.Duration
	prefetchedMu      sync.Mutex
	prefetched        map[string]time.Time
	eventStream       events.Stream
	metrics           *observability.Metrics
}

func NewHandler(cache cache.Cache, pred predictor.Predictor, pf prefetcher.Prefetcher, upstream string) (*Handler, error) {
	u, err := url.Parse(upstream)
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
		defaultTTL:        5 * time.Minute,
		prefetched:        make(map[string]time.Time),
		metrics:           observability.NewMetrics(),
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
	ctx := r.Context()
	key := BuildCacheKey(r)
	status := string(cache.StatusMiss)
	reason := string(cache.ReasonLRU)

	defer func() {
		if h.metrics != nil {
			h.metrics.RecordRequest(status, reason, time.Since(start))
		}
	}()

	if item, err := h.cache.Get(ctx, key); err == nil {
		h.recordAccess(key)
		current := normalizeRouteKey(r)
		h.recordTransition(r, current)
		if h.consumePrefetchSignal(key) {
			h.recordTrend(key, true)
		}
		status = string(cache.StatusHit)
		reason = string(cache.ReasonLRU)
		w.Header().Set("X-Cache-Status", status)
		w.Header().Set("X-Cache-Reason", reason)
		statusCode := item.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		w.WriteHeader(statusCode)
		_, _ = w.Write(item.Value)
		h.maybePrefetchForRequest(ctx, r, current)
		return
	}

	respBytes, code, hdr, err := h.fetchUpstream(ctx, r)
	if err != nil {
		if h.metrics != nil {
			h.metrics.RecordUpstreamError()
		}
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}

	metrics := h.computeMetrics(key)
	h.recordAccess(key)
	stored := h.cache.Set(ctx, cache.Item{
		Key:        key,
		Value:      respBytes,
		StatusCode: code,
		ExpiresAt:  time.Now().Add(h.defaultTTL),
		Version:    h.versionFromRequest(r),
	}, metrics)
	if h.metrics != nil {
		h.metrics.RecordPollution(metrics.Pollution)
	}

	copyHeaders(w.Header(), hdr)
	if stored {
		status = string(cache.StatusMiss)
		reason = string(cache.ReasonLRU)
		w.Header().Set("X-Cache-Status", status)
		w.Header().Set("X-Cache-Reason", reason)
	} else {
		status = string(cache.StatusBypass)
		reason = string(cache.ReasonStale)
		w.Header().Set("X-Cache-Status", status)
		w.Header().Set("X-Cache-Reason", reason)
	}

	w.WriteHeader(code)
	_, _ = w.Write(respBytes)

	current := normalizeRouteKey(r)
	h.recordTransition(r, current)
	h.maybePrefetchForRequest(ctx, r, current)
}

func (h *Handler) InvalidateBook(ctx context.Context, key string) {
	if key == "" || h.cache == nil {
		return
	}
	for _, candidate := range h.cache.Snapshot(ctx).Keys {
		if candidate == key || strings.Contains(candidate, "book_view:"+key) || strings.Contains(candidate, "book:"+key) || strings.Contains(candidate, "book_id="+key) || strings.Contains(candidate, key+"|") {
			h.cache.Delete(ctx, candidate)
		}
	}
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

func (h *Handler) versionFromRequest(r *http.Request) int64 {
	if r == nil {
		return 0
	}
	if v := r.Header.Get("X-Book-Version"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		}
	}
	if q := r.URL.Query(); q.Get("version") != "" {
		v, err := strconv.ParseInt(q.Get("version"), 10, 64)
		if err == nil {
			return v
		}
	}
	return 0
}

func (h *Handler) SetEventStream(stream events.Stream) {
	h.eventStream = stream
}

func (h *Handler) StartEventConsumer(ctx context.Context) error {
	if h.eventStream == nil {
		return nil
	}
	_, err := h.eventStream.Subscribe(ctx, h.HandleBookUpdated)
	return err
}

func (h *Handler) HandleBookUpdated(ctx context.Context, event events.BookUpdateEvent) error {
	if event.BookID == "" || h.cache == nil {
		return nil
	}
	for _, key := range h.cache.Snapshot(ctx).Keys {
		if !strings.Contains(key, "book_view:"+event.BookID) && !strings.Contains(key, "book:"+event.BookID) && !strings.Contains(key, "book_id="+event.BookID) && !strings.Contains(key, "book/"+event.BookID) {
			continue
		}
		if item, err := h.cache.Get(ctx, key); err == nil {
			if item.Version == 0 || item.Version <= event.Version {
				h.cache.Delete(ctx, key)
			}
			continue
		}
		h.cache.Delete(ctx, key)
	}
	if h.metrics != nil {
		h.metrics.RecordRequest("MISS", "INVALIDATED", 0)
	}
	return nil
}

func (h *Handler) recordAccess(key string) {
	if c, ok := h.cache.(*cache.InMemoryCache); ok {
		if adm, ok := c.Admission().(*cache.TinyLFUAdmission); ok {
			if adm.Filter != nil {
				adm.Filter.Increment(key)
			}
			if adm.Window != nil {
				adm.Window.Touch(key)
			}
		}
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

func (h *Handler) markPrefetch(key string) {
	if key == "" {
		return
	}
	h.prefetchedMu.Lock()
	defer h.prefetchedMu.Unlock()
	if h.prefetched == nil {
		h.prefetched = make(map[string]time.Time)
	}
	h.prefetched[key] = time.Now()
}

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
	return time.Since(seen) <= 5*time.Minute
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
			if metrics.CurrentSize > 0 {
				if snap := c.Snapshot(context.Background()); len(snap.Keys) > 0 {
					victimKey := snap.Keys[len(snap.Keys)-1]
					victimFreq := adm.Frequency(victimKey)
					metrics.VictimKey = victimKey
					metrics.VictimFrequency = victimFreq
					metrics.Pollution = adm.PollutantScore(key, victimKey)
				}
			}
		}
	}
	return metrics
}

func (h *Handler) maybePrefetch(_ context.Context, current string) {
	h.maybePrefetchForRequest(nil, nil, current)
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
	if strings.HasPrefix(s, "book_view:") || strings.HasPrefix(s, "category_view:") || strings.HasPrefix(s, "root") {
		return s
	}
	if strings.Contains(s, "http") {
		if u, err := url.Parse(s); err == nil {
			s = u.Path
		}
	}
	trimmed := strings.Trim(s, "/")
	if trimmed == "" {
		return "root"
	}
	return normalizeRouteKey(&http.Request{URL: &url.URL{Path: "/" + trimmed}})
}

func (h *Handler) maybePrefetchForRequest(ctx context.Context, r *http.Request, current string) {
	if current == "" {
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
	prefetchKey := top.Key
	if r != nil {
		prefetchKey = buildScopedCacheKey(r, top.Key)
	}
	if _, err := h.cache.Get(cacheCtx, prefetchKey); err == nil {
		return
	}
	if h.cacheIsPolluted(prefetchKey) {
		return
	}
	prefetchCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	h.prefetcher.Enqueue(prefetchCtx, prefetchKey, prefetcher.ReasonMarkov)
	if h.metrics != nil {
		h.metrics.RecordPrefetch(true, false, false)
	}
	h.markPrefetch(prefetchKey)
	if ctx != nil && ctx.Err() != nil {
		return
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
	if snap := c.Snapshot(context.Background()); len(snap.Keys) > 0 {
		victimKey := snap.Keys[len(snap.Keys)-1]
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
	return buildScopedCacheKey(r, normalizeRouteKey(r))
}

func buildScopedCacheKey(r *http.Request, route string) string {
	if r == nil {
		return route
	}
	userID := r.Header.Get("X-User-ID")
	tenant := r.Header.Get("X-Tenant")
	locale := r.Header.Get("X-Locale")
	query := normalizeQuery(r.URL.Query())
	return strings.Join([]string{userID, tenant, locale, route, query}, "|")
}

func normalizeRouteKey(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) >= 2 && parts[0] == "books" {
		return "book_view:" + parts[1]
	}
	if len(parts) >= 2 && parts[0] == "categories" {
		return "category_view:" + parts[1]
	}
	if path == "" {
		return "root"
	}
	return strings.ReplaceAll(path, "/", ":")
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

func (h *Handler) fetchUpstream(ctx context.Context, in *http.Request) ([]byte, int, http.Header, error) {
	bodyCopy, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, 0, nil, err
	}
	in.Body = io.NopCloser(bytes.NewReader(bodyCopy))

	upstreamURL := h.upstream.ResolveReference(&url.URL{Path: in.URL.Path, RawQuery: in.URL.RawQuery}).String()
	outReq, err := http.NewRequestWithContext(ctx, in.Method, upstreamURL, bytes.NewReader(bodyCopy))
	if err != nil {
		return nil, 0, nil, err
	}
	outReq.Header = in.Header.Clone()

	resp, err := h.httpClient.Do(outReq)
	if err != nil {
		return nil, 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, nil, err
	}
	return body, resp.StatusCode, resp.Header.Clone(), nil
}

func copyHeaders(dst, src http.Header) {
	for k, vals := range src {
		dst.Del(k)
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}
