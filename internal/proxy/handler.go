package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"proks/internal/cache"
	"proks/internal/predictor"
	"proks/internal/prefetcher"
)

type Handler struct {
	cache             cache.Cache
	predictor         predictor.Predictor
	prefetcher        prefetcher.Prefetcher
	upstream          *url.URL
	httpClient        *http.Client
	prefetchThreshold float64
	defaultTTL        time.Duration
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
		upstream:          u,
		prefetchThreshold: 0.7,
		defaultTTL:        5 * time.Minute,
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
	ctx := r.Context()
	key := BuildCacheKey(r)

	if item, err := h.cache.Get(ctx, key); err == nil {
		h.recordAccess(key)
		w.Header().Set("X-Cache-Status", string(cache.StatusHit))
		w.Header().Set("X-Cache-Reason", string(cache.ReasonLRU))
		status := item.StatusCode
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write(item.Value)
		h.maybePrefetch(ctx, key)
		return
	}

	respBytes, code, hdr, err := h.fetchUpstream(ctx, r)
	if err != nil {
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
	}, metrics)

	copyHeaders(w.Header(), hdr)
	if stored {
		w.Header().Set("X-Cache-Status", string(cache.StatusMiss))
		w.Header().Set("X-Cache-Reason", string(cache.ReasonLRU))
	} else {
		w.Header().Set("X-Cache-Status", string(cache.StatusBypass))
		w.Header().Set("X-Cache-Reason", string(cache.ReasonStale))
	}

	w.WriteHeader(code)
	_, _ = w.Write(respBytes)

	prev := r.Header.Get("X-Prev-Route-Key")
	if prev != "" {
		h.predictor.Update(prev, normalizeRouteKey(r))
	}
	h.maybePrefetch(ctx, key)
}

func (h *Handler) InvalidateBook(ctx context.Context, key string) {
	h.cache.Delete(ctx, key)
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
	cacheCtx := context.Background()
	candidates := h.predictor.Predict(current)
	if len(candidates) == 0 {
		return
	}
	top := candidates[0]
	if top.Probability < h.prefetchThreshold {
		return
	}
	if _, err := h.cache.Get(cacheCtx, top.Key); err == nil {
		return
	}
	prefetchCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	h.prefetcher.Enqueue(prefetchCtx, top.Key, prefetcher.ReasonMarkov)
}

func BuildCacheKey(r *http.Request) string {
	userID := r.Header.Get("X-User-ID")
	tenant := r.Header.Get("X-Tenant")
	locale := r.Header.Get("X-Locale")
	route := normalizeRouteKey(r)
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
