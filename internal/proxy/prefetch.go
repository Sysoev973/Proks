package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

var (
	errPrefetchUnknown   = errors.New("proxy: no request template for prefetch key")
	errPrefetchThrottled = errors.New("proxy: prefetch budget exhausted or upstream unhealthy")
	errPrefetchSkipped   = errors.New("proxy: prefetch skipped, key already cached or cache polluted")
	errPrefetchNotStored = errors.New("proxy: prefetched response was not stored")
)

const (
	maxPrefetchRequests  = 1024
	prefetchRequestTTL   = time.Minute
	prefetchPauseOnError = 5 * time.Second
	defaultPrefetchRPS   = 20
)

// prefetchRequest is what a client following the current request to the
// predicted route would send: GET, the route's path, and the headers that
// are part of the cache key. Nothing else from the original request is kept.
type prefetchRequest struct {
	req   *http.Request
	added time.Time
}

// prefetchTemplate builds the request for route on behalf of r and returns it
// with its cache key. Requests carrying credentials are never prefetched,
// even when the policy allows caching them.
func (h *Handler) prefetchTemplate(r *http.Request, route string) (*http.Request, string, bool) {
	if r == nil || r.Method != http.MethodGet || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
		return nil, "", false
	}
	req, err := http.NewRequest(http.MethodGet, route, nil)
	if err != nil {
		return nil, "", false
	}
	// RemoteAddr stays empty: the client did not send this request, so the
	// upstream must not see its IP in X-Forwarded-For.
	req.Host = r.Host
	for _, name := range h.policy.keyHeaders() {
		if values := r.Header.Values(name); len(values) > 0 {
			req.Header[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
		}
	}
	decision := h.policy.requestDecision(req)
	if !decision.Allowed {
		return nil, "", false
	}
	return req, decision.Key, true
}

func (h *Handler) rememberPrefetchRequest(key string, req *http.Request) {
	h.prefetchMu.Lock()
	defer h.prefetchMu.Unlock()
	if h.prefetchRequests == nil {
		h.prefetchRequests = make(map[string]prefetchRequest)
	}
	if len(h.prefetchRequests) >= maxPrefetchRequests {
		for k, pr := range h.prefetchRequests {
			if time.Since(pr.added) > prefetchRequestTTL {
				delete(h.prefetchRequests, k)
			}
		}
		if len(h.prefetchRequests) >= maxPrefetchRequests {
			return
		}
	}
	h.prefetchRequests[key] = prefetchRequest{req: req, added: time.Now()}
}

func (h *Handler) takePrefetchRequest(key string) (*http.Request, bool) {
	h.prefetchMu.Lock()
	defer h.prefetchMu.Unlock()
	pr, ok := h.prefetchRequests[key]
	delete(h.prefetchRequests, key)
	if !ok || time.Since(pr.added) > prefetchRequestTTL {
		return nil, false
	}
	return pr.req, true
}

// SetPrefetchBudget limits prefetch requests to upstream to rps per second;
// rps <= 0 means no limit.
func (h *Handler) SetPrefetchBudget(rps float64) {
	h.prefetchMu.Lock()
	defer h.prefetchMu.Unlock()
	if rps <= 0 {
		h.prefetchLimiter = nil
		return
	}
	h.prefetchLimiter = newRateLimiter(rps)
}

// pausePrefetch stops prefetching for a while after an upstream error, so
// prefetch does not add load to an upstream that is already failing.
func (h *Handler) pausePrefetch() {
	h.prefetchPausedUntil.Store(time.Now().Add(prefetchPauseOnError).UnixNano())
}

func (h *Handler) prefetchAllowed() bool {
	if time.Now().UnixNano() < h.prefetchPausedUntil.Load() {
		return false
	}
	h.prefetchMu.Lock()
	limiter := h.prefetchLimiter
	h.prefetchMu.Unlock()
	return limiter == nil || limiter.allow(time.Now())
}

// Prefetch loads the response for a key queued by maybePrefetchForRequest.
// It goes through the same path as a cache miss (single flight, cache
// policy, invalidation tracking) and returns nil only when the response was
// stored, so callers can trust a nil error as "the cache is warm".
func (h *Handler) Prefetch(ctx context.Context, key string) error {
	req, ok := h.takePrefetchRequest(key)
	if !ok {
		return errPrefetchUnknown
	}
	if !h.prefetchAllowed() {
		return errPrefetchThrottled
	}
	if _, err := h.cache.Get(ctx, key); err == nil || h.cacheIsPolluted(key) {
		return errPrefetchSkipped
	}
	res, _, err := h.fetch(ctx, req.WithContext(ctx), key, true)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			h.pausePrefetch()
		}
		return err
	}
	if res.status >= http.StatusInternalServerError {
		h.pausePrefetch()
		return fmt.Errorf("proxy: prefetch upstream status %d", res.status)
	}
	if !res.stored {
		return fmt.Errorf("%w: %s", errPrefetchNotStored, res.decision.Reason)
	}
	return nil
}

type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	tokens float64
	last   time.Time
}

func newRateLimiter(rps float64) *rateLimiter {
	return &rateLimiter{rate: rps, tokens: rps, last: time.Now()}
}

func (l *rateLimiter) allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tokens = min(max(1, l.rate), l.tokens+now.Sub(l.last).Seconds()*l.rate)
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
