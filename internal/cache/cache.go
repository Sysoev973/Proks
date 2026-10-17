package cache

import (
	"container/list"
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var ErrNotFound = errors.New("cache: key not found")

type CacheStatus string

const (
	StatusHit      CacheStatus = "HIT"
	StatusMiss     CacheStatus = "MISS"
	StatusBypass   CacheStatus = "BYPASS"
	StatusPrefetch CacheStatus = "PREFETCH"
	StatusStale    CacheStatus = "STALE"
)

type CacheReason string

const (
	ReasonLRU    CacheReason = "LRU"
	ReasonMarkov CacheReason = "MARKOV"
	ReasonStale  CacheReason = "STALE"
)

type Item struct {
	Key        string
	Value      []byte
	Headers    http.Header
	StatusCode int
	ExpiresAt  time.Time
	StaleUntil time.Time
	Version    int64
	Tags       []string
}

// fresh reports whether the item may be served without revalidation.
func (it Item) fresh(now time.Time) bool {
	return it.ExpiresAt.IsZero() || now.Before(it.ExpiresAt)
}

// when the item stops being servable at all; zero means never.
func (it Item) removeAt() time.Time {
	if it.ExpiresAt.IsZero() {
		return time.Time{}
	}
	if it.StaleUntil.After(it.ExpiresAt) {
		return it.StaleUntil
	}
	return it.ExpiresAt
}

type Snapshot struct {
	Size int
	Keys []string
}

type Metrics struct {
	Frequency          float64
	Pollution          float64
	CandidateFrequency uint32
	VictimFrequency    uint32
	CurrentSize        int
	VictimKey          string
	// NoEviction is set by Set when the item fits without evicting anything;
	// admission then has nobody to protect.
	NoEviction bool
}

// Cache is the storage the proxy handler works against. InMemoryCache is the
// only implementation today; a shared L2 (Redis) or a peer cache (groupcache
// style) can be added by implementing this interface, without changes to the
// handler. The handler only uses InMemoryCache-specific features (TinyLFU
// admission counters, VictimFor) through optional type assertions and skips
// them for other stores. KeysForTag must cover every key stored with that
// tag, since invalidation deletes exactly those keys.
type Cache interface {
	Get(ctx context.Context, key string) (Item, error)
	Lookup(ctx context.Context, key string) (item Item, fresh bool, err error)
	Set(ctx context.Context, item Item, metrics Metrics) bool
	Delete(ctx context.Context, key string)
	KeysForTag(ctx context.Context, tag string) []string
	Snapshot(ctx context.Context) Snapshot
	Size() int
	Capacity() int
	Bytes() int64
}

// Store is the name the architecture docs use for Cache.
type Store = Cache

var _ Store = (*InMemoryCache)(nil)

type AdmissionPolicy interface {
	ShouldStore(item Item, metrics Metrics) bool
}

type frequencyAwareAdmission interface {
	Frequency(key string) uint32
}

type alwaysAdmission struct{}

func (alwaysAdmission) ShouldStore(_ Item, _ Metrics) bool { return true }

const entryOverhead = 128

type entry struct {
	item  Item
	size  int64
	ele   *list.Element
	timer *time.Timer
}

type shard struct {
	mu       sync.Mutex
	items    map[string]*entry
	lru      *list.List
	tags     map[string]map[string]struct{}
	capacity int
	maxBytes int64
	bytes    int64
}

type Config struct {
	// Capacity is the maximum number of entries; 0 means 1024.
	Capacity int
	// MaxBytes bounds the approximate memory held by entries; 0 means no limit.
	MaxBytes int64
	// Shards is the number of independently locked LRU segments; 0 picks one
	// shard per 256 entries, at most 16.
	Shards    int
	Admission AdmissionPolicy
}

type InMemoryCache struct {
	shards    []*shard
	capacity  int
	maxBytes  int64
	admission AdmissionPolicy
	onEvict   atomic.Pointer[func(string)]
}

func (c *InMemoryCache) SetEvictionCallback(callback func(string)) {
	if callback == nil {
		c.onEvict.Store(nil)
		return
	}
	c.onEvict.Store(&callback)
}

func New(cfg Config) *InMemoryCache {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1024
	}
	if cfg.MaxBytes < 0 {
		cfg.MaxBytes = 0
	}
	if cfg.Admission == nil {
		cfg.Admission = alwaysAdmission{}
	}
	n := cfg.Shards
	if n <= 0 {
		n = cfg.Capacity / 256
		if n > 16 {
			n = 16
		}
	}
	if n > cfg.Capacity {
		n = cfg.Capacity
	}
	if n < 1 {
		n = 1
	}
	c := &InMemoryCache{
		shards:    make([]*shard, n),
		capacity:  cfg.Capacity,
		maxBytes:  cfg.MaxBytes,
		admission: cfg.Admission,
	}
	perShard := (cfg.Capacity + n - 1) / n
	for i := range c.shards {
		c.shards[i] = &shard{
			items:    make(map[string]*entry, perShard),
			lru:      list.New(),
			tags:     make(map[string]map[string]struct{}),
			capacity: perShard,
			maxBytes: cfg.MaxBytes / int64(n),
		}
	}
	return c
}

func NewInMemoryCache(capacity int, admission AdmissionPolicy) *InMemoryCache {
	return New(Config{Capacity: capacity, Admission: admission})
}

func NewTinyLFURuntimeCache(capacity int, maxBytes int64) *InMemoryCache {
	if capacity <= 0 {
		capacity = 1024
	}
	width := defaultCountMinWidth
	for width < capacity {
		width *= 2
	}
	filter := NewTinyLFUSized(width, uint64(10*capacity))
	window := NewLRUWindow(32)
	admission := NewTinyLFUAdmission(filter, window, 2, 0.7)
	return New(Config{Capacity: capacity, MaxBytes: maxBytes, Admission: admission})
}

func (c *InMemoryCache) shardFor(key string) *shard {
	if len(c.shards) == 1 {
		return c.shards[0]
	}
	// FNV-1a, inlined to avoid allocating a hasher per call.
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return c.shards[h%uint64(len(c.shards))]
}

func (c *InMemoryCache) Get(ctx context.Context, key string) (Item, error) {
	item, fresh, err := c.Lookup(ctx, key)
	if err != nil {
		return Item{}, err
	}
	if !fresh {
		return Item{}, ErrNotFound
	}
	return item, nil
}

func (c *InMemoryCache) Lookup(_ context.Context, key string) (Item, bool, error) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return Item{}, false, ErrNotFound
	}
	now := time.Now()
	if at := e.item.removeAt(); !at.IsZero() && !now.Before(at) {
		c.removeLocked(s, key)
		return Item{}, false, ErrNotFound
	}
	s.lru.MoveToFront(e.ele)
	return copyItem(e.item), e.item.fresh(now), nil
}

func (c *InMemoryCache) Set(_ context.Context, item Item, metrics Metrics) bool {
	if item.Key == "" {
		return false
	}
	size := itemSize(item)
	s := c.shardFor(item.Key)
	if s.maxBytes > 0 && size > s.maxBytes {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.items[item.Key]
	metrics.NoEviction = !s.wouldOverflowLocked(size, existing)
	if !metrics.NoEviction {
		if back := s.lru.Back(); back != nil {
			if victimKey, ok := back.Value.(string); ok && victimKey != item.Key {
				if fa, ok := c.admission.(frequencyAwareAdmission); ok {
					metrics.VictimKey = victimKey
					metrics.VictimFrequency = fa.Frequency(victimKey)
				}
			}
		}
	}
	if !c.admission.ShouldStore(item, metrics) {
		return false
	}

	stored := copyItem(item)
	if existing != nil {
		if existing.timer != nil {
			existing.timer.Stop()
		}
		s.untagLocked(item.Key, existing.item.Tags)
		s.bytes -= existing.size
		existing.item = stored
		existing.size = size
		existing.timer = c.expirationTimer(s, item.Key, stored.removeAt())
		s.lru.MoveToFront(existing.ele)
	} else {
		ele := s.lru.PushFront(item.Key)
		s.items[item.Key] = &entry{item: stored, size: size, ele: ele, timer: c.expirationTimer(s, item.Key, stored.removeAt())}
	}
	s.bytes += size
	s.tagLocked(item.Key, stored.Tags)

	for s.overLimitLocked() {
		back := s.lru.Back()
		if back == nil {
			break
		}
		k, _ := back.Value.(string)
		if k == item.Key {
			break
		}
		c.removeLocked(s, k)
	}
	return true
}

func (c *InMemoryCache) Delete(_ context.Context, key string) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	c.removeLocked(s, key)
}

func (c *InMemoryCache) KeysForTag(_ context.Context, tag string) []string {
	var keys []string
	for _, s := range c.shards {
		s.mu.Lock()
		for key := range s.tags[tag] {
			keys = append(keys, key)
		}
		s.mu.Unlock()
	}
	return keys
}

func (c *InMemoryCache) Snapshot(_ context.Context) Snapshot {
	var keys []string
	for _, s := range c.shards {
		s.mu.Lock()
		for e := s.lru.Front(); e != nil; e = e.Next() {
			if k, ok := e.Value.(string); ok {
				keys = append(keys, k)
			}
		}
		s.mu.Unlock()
	}
	return Snapshot{Size: len(keys), Keys: keys}
}

// VictimFor returns the key that would be evicted next from the shard that
// key belongs to (its LRU tail) in O(1).
func (c *InMemoryCache) VictimFor(key string) (string, bool) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	back := s.lru.Back()
	if back == nil {
		return "", false
	}
	victim, ok := back.Value.(string)
	return victim, ok
}

func (c *InMemoryCache) Admission() AdmissionPolicy {
	return c.admission
}

func (c *InMemoryCache) Size() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += len(s.items)
		s.mu.Unlock()
	}
	return n
}

func (c *InMemoryCache) Capacity() int {
	return c.capacity
}

// Bytes returns the approximate memory held by stored entries.
func (c *InMemoryCache) Bytes() int64 {
	var n int64
	for _, s := range c.shards {
		s.mu.Lock()
		n += s.bytes
		s.mu.Unlock()
	}
	return n
}

// MaxItemBytes is the largest item Set accepts: the byte budget of one shard,
// since an item lives in a single shard. 0 means unlimited.
func (c *InMemoryCache) MaxItemBytes() int64 {
	return c.shards[0].maxBytes
}

// ItemSize is the size Set charges against the byte limit for item.
func ItemSize(item Item) int64 {
	return itemSize(item)
}

// MaxBytes returns the configured byte limit; 0 means unlimited.
func (c *InMemoryCache) MaxBytes() int64 {
	return c.maxBytes
}

func (s *shard) wouldOverflowLocked(size int64, existing *entry) bool {
	count := len(s.items)
	bytes := s.bytes + size
	if existing != nil {
		bytes -= existing.size
	} else {
		count++
	}
	return count > s.capacity || (s.maxBytes > 0 && bytes > s.maxBytes)
}

func (s *shard) overLimitLocked() bool {
	return len(s.items) > s.capacity || (s.maxBytes > 0 && s.bytes > s.maxBytes)
}

func (s *shard) tagLocked(key string, tags []string) {
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		keys := s.tags[tag]
		if keys == nil {
			keys = make(map[string]struct{})
			s.tags[tag] = keys
		}
		keys[key] = struct{}{}
	}
}

func (s *shard) untagLocked(key string, tags []string) {
	for _, tag := range tags {
		keys := s.tags[tag]
		delete(keys, key)
		if len(keys) == 0 {
			delete(s.tags, tag)
		}
	}
}

func (c *InMemoryCache) removeLocked(s *shard, key string) {
	e, ok := s.items[key]
	if !ok {
		return
	}
	delete(s.items, key)
	s.lru.Remove(e.ele)
	s.bytes -= e.size
	s.untagLocked(key, e.item.Tags)
	if e.timer != nil {
		e.timer.Stop()
	}
	if cb := c.onEvict.Load(); cb != nil {
		(*cb)(key)
	}
}

func (c *InMemoryCache) expirationTimer(s *shard, key string, at time.Time) *time.Timer {
	if at.IsZero() {
		return nil
	}
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	return time.AfterFunc(delay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if e, ok := s.items[key]; ok {
			if removeAt := e.item.removeAt(); !removeAt.IsZero() && !time.Now().Before(removeAt) {
				c.removeLocked(s, key)
			}
		}
	})
}

func itemSize(item Item) int64 {
	size := int64(entryOverhead + len(item.Key) + len(item.Value))
	for name, values := range item.Headers {
		size += int64(len(name))
		for _, v := range values {
			size += int64(len(v))
		}
	}
	for _, tag := range item.Tags {
		size += int64(len(tag))
	}
	return size
}

func copyItem(item Item) Item {
	item.Value = cloneBytes(item.Value)
	item.Headers = item.Headers.Clone()
	if item.Tags != nil {
		item.Tags = append([]string(nil), item.Tags...)
	}
	return item
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
