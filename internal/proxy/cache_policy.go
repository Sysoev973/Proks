package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CachePolicy controls which requests and responses may be stored.
type CachePolicy struct {
	CacheMethods          map[string]bool
	AllowedStatus         []StatusRange
	SuccessTTL            time.Duration
	ErrorTTL              time.Duration
	RespectResponseTTL    bool
	StaleWhileRevalidate  time.Duration
	StaleIfError          time.Duration
	CacheErrors           bool
	VaryHeaders           []string
	Dimensions            []string
	AllowAuthorization    bool
	AllowCookie           bool
	AllowSetCookie        bool
	AllowStreaming        bool
	TrustForwardedHeaders bool
	MaxBodySize           int64
	// Routes give responses invalidation tags by path template; see Route.
	Routes []Route
}

type StatusRange struct {
	Min int
	Max int
}

type cacheDecision struct {
	Allowed bool
	Key     string
	Reason  string
}

func DefaultCachePolicy() CachePolicy {
	return CachePolicy{
		CacheMethods:       map[string]bool{http.MethodGet: true},
		AllowedStatus:      []StatusRange{{Min: http.StatusOK, Max: 299}},
		SuccessTTL:         5 * time.Minute,
		ErrorTTL:           time.Minute,
		RespectResponseTTL: true,
		VaryHeaders:        []string{"Accept-Encoding"},
		AllowStreaming:     false,
		MaxBodySize:        maxRequestBodySize,
	}
}

func (p CachePolicy) normalized() CachePolicy {
	if len(p.CacheMethods) == 0 {
		p.CacheMethods = map[string]bool{http.MethodGet: true}
	} else {
		methods := make(map[string]bool, len(p.CacheMethods))
		for method, enabled := range p.CacheMethods {
			methods[strings.ToUpper(method)] = enabled
		}
		p.CacheMethods = methods
	}
	if len(p.AllowedStatus) == 0 {
		p.AllowedStatus = []StatusRange{{Min: http.StatusOK, Max: 299}}
	}
	if p.SuccessTTL <= 0 {
		p.SuccessTTL = 5 * time.Minute
	}
	if p.ErrorTTL <= 0 {
		p.ErrorTTL = time.Minute
	}
	if p.MaxBodySize <= 0 {
		p.MaxBodySize = maxRequestBodySize
	}
	if p.StaleWhileRevalidate < 0 {
		p.StaleWhileRevalidate = 0
	}
	if p.StaleIfError < 0 {
		p.StaleIfError = 0
	}
	return p
}

func (p CachePolicy) requestDecision(r *http.Request) cacheDecision {
	p = p.normalized()
	if r == nil || !p.CacheMethods[strings.ToUpper(r.Method)] {
		return cacheDecision{Reason: "METHOD"}
	}
	if hasNoStoreOrPrivate(r.Header.Get("Cache-Control")) {
		return cacheDecision{Reason: "REQUEST_CACHE_CONTROL"}
	}
	if r.Header.Get("Range") != "" {
		return cacheDecision{Reason: "RANGE"}
	}
	if r.Header.Get("Authorization") != "" && !p.AllowAuthorization {
		return cacheDecision{Reason: "AUTHORIZATION"}
	}
	if r.Header.Get("Cookie") != "" && !p.AllowCookie {
		return cacheDecision{Reason: "COOKIE"}
	}
	return cacheDecision{Allowed: true, Key: buildPolicyCacheKey(r, p)}
}

func (p CachePolicy) responseDecision(r *http.Request, status int, headers http.Header, bodySize int) (cacheDecision, time.Duration) {
	p = p.normalized()
	if bodySize > int(p.MaxBodySize) {
		return cacheDecision{Reason: "BODY_SIZE"}, 0
	}
	// A partial body must never be stored under the key of the full resource.
	if status == http.StatusPartialContent || headers.Get("Content-Range") != "" {
		return cacheDecision{Reason: "PARTIAL"}, 0
	}
	if !statusAllowed(status, p.AllowedStatus) {
		return cacheDecision{Reason: "STATUS"}, 0
	}
	if status >= 400 && !p.CacheErrors {
		return cacheDecision{Reason: "ERROR"}, 0
	}
	if hasNoStoreOrPrivate(headers.Get("Cache-Control")) {
		return cacheDecision{Reason: "RESPONSE_CACHE_CONTROL"}, 0
	}
	if headers.Get("Set-Cookie") != "" && !p.AllowSetCookie {
		return cacheDecision{Reason: "SET_COOKIE"}, 0
	}
	vary := parseVary(headers.Values("Vary"))
	if vary["*"] {
		return cacheDecision{Reason: "VARY_STAR"}, 0
	}
	allowedVary := make(map[string]bool, len(p.VaryHeaders))
	for _, name := range p.VaryHeaders {
		allowedVary[http.CanonicalHeaderKey(name)] = true
	}
	for name := range vary {
		if !allowedVary[http.CanonicalHeaderKey(name)] {
			return cacheDecision{Reason: "VARY_HEADER"}, 0
		}
	}
	ttl := p.SuccessTTL
	if status >= 400 {
		ttl = p.ErrorTTL
	}
	if p.RespectResponseTTL {
		if responseTTL, ok, expired := ttlFromHeaders(headers); expired {
			return cacheDecision{Reason: "EXPIRED"}, 0
		} else if ok {
			ttl = responseTTL
		}
	}
	return cacheDecision{Allowed: true}, ttl
}

func (p CachePolicy) staleWindows(headers http.Header) (swr, sie time.Duration) {
	swr, sie = p.StaleWhileRevalidate, p.StaleIfError
	for _, directive := range strings.Split(strings.ToLower(headers.Get("Cache-Control")), ",") {
		parts := strings.SplitN(strings.TrimSpace(directive), "=", 2)
		name := strings.TrimSpace(parts[0])
		if name == "must-revalidate" || name == "proxy-revalidate" {
			return 0, 0
		}
		if len(parts) != 2 {
			continue
		}
		seconds, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || seconds < 0 {
			continue
		}
		switch name {
		case "stale-while-revalidate":
			swr = time.Duration(seconds) * time.Second
		case "stale-if-error":
			sie = time.Duration(seconds) * time.Second
		}
	}
	return swr, sie
}

func statusAllowed(status int, ranges []StatusRange) bool {
	for _, r := range ranges {
		if status >= r.Min && status <= r.Max {
			return true
		}
	}
	return false
}

func hasNoStoreOrPrivate(value string) bool {
	for _, directive := range strings.Split(strings.ToLower(value), ",") {
		switch strings.TrimSpace(strings.SplitN(directive, "=", 2)[0]) {
		case "private", "no-store", "no-cache":
			return true
		}
	}
	return false
}

func ttlFromHeaders(headers http.Header) (time.Duration, bool, bool) {
	for _, directive := range strings.Split(strings.ToLower(headers.Get("Cache-Control")), ",") {
		parts := strings.SplitN(strings.TrimSpace(directive), "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) != "max-age" {
			continue
		}
		seconds, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err == nil && seconds >= 0 {
			if seconds == 0 {
				return 0, false, true
			}
			return time.Duration(seconds) * time.Second, true, false
		}
	}
	if expires := headers.Get("Expires"); expires != "" {
		if at, err := http.ParseTime(expires); err == nil {
			ttl := time.Until(at)
			if ttl <= 0 {
				return 0, false, true
			}
			return ttl, true, false
		}
	}
	return 0, false, false
}

func parseVary(values []string) map[string]bool {
	result := make(map[string]bool)
	for _, value := range values {
		for _, name := range strings.Split(value, ",") {
			name = strings.TrimSpace(name)
			if name != "" {
				result[name] = true
			}
		}
	}
	return result
}

func fingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// requestKeyHeaders are the request headers that are always part of the
// cache key, in key order.
var requestKeyHeaders = []string{"X-User-ID", "X-Tenant", "X-Locale"}

// keyHeaders lists every request header that can change the cache key.
func (p CachePolicy) keyHeaders() []string {
	names := append([]string(nil), requestKeyHeaders...)
	names = append(names, p.VaryHeaders...)
	return append(names, p.Dimensions...)
}

func buildPolicyCacheKey(r *http.Request, p CachePolicy) string {
	parts := make([]string, 0, len(requestKeyHeaders)+2+len(p.VaryHeaders)+len(p.Dimensions))
	for _, name := range requestKeyHeaders {
		parts = append(parts, r.Header.Get(name))
	}
	parts = append(parts, routeKey(r.URL.Path), normalizeQuery(r.URL.Query()))
	if r.Method != http.MethodGet {
		parts = append([]string{strings.ToUpper(r.Method)}, parts...)
	}
	if p.AllowAuthorization && r.Header.Get("Authorization") != "" {
		parts = append(parts, "authorization="+fingerprint(r.Header.Get("Authorization")))
	}
	if p.AllowCookie && r.Header.Get("Cookie") != "" {
		parts = append(parts, "cookie="+fingerprint(r.Header.Get("Cookie")))
	}
	for _, name := range p.VaryHeaders {
		name = http.CanonicalHeaderKey(name)
		parts = append(parts, "vary:"+name+"="+fingerprint(r.Header.Get(name)))
	}
	for _, name := range p.Dimensions {
		parts = append(parts, "dimension:"+name+"="+fingerprint(r.Header.Get(name)))
	}
	return strings.Join(parts, "|")
}
