package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("cache: key not found")

type CacheStatus string

const (
	StatusHit      CacheStatus = "HIT"
	StatusMiss     CacheStatus = "MISS"
	StatusBypass   CacheStatus = "BYPASS"
	StatusPrefetch CacheStatus = "PREFETCH"
)

type CacheReason string

const (
	ReasonLRU         CacheReason = "LRU"
	ReasonMarkov      CacheReason = "MARKOV"
	ReasonStale       CacheReason = "STALE"
	ReasonInvalidated CacheReason = "INVALIDATED"
)

type Item struct {
	Key        string
	Value      []byte
	StatusCode int
	ExpiresAt  time.Time
	Version    int64
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
}

type Cache interface {
	Get(ctx context.Context, key string) (Item, error)
	Set(ctx context.Context, item Item, metrics Metrics) bool
	Delete(ctx context.Context, key string)
	Snapshot(ctx context.Context) Snapshot
}

type AdmissionPolicy interface {
	ShouldStore(item Item, metrics Metrics) bool
}

type frequencyAwareAdmission interface {
	Frequency(key string) uint32
}

type alwaysAdmission struct{}

func (alwaysAdmission) ShouldStore(_ Item, _ Metrics) bool { return true }

type entry struct {
	item Item
	ele  *list.Element
}

type InMemoryCache struct {
	mu        sync.RWMutex
	items     map[string]*entry
	lru       *list.List
	capacity  int
	admission AdmissionPolicy
}

func NewInMemoryCache(capacity int, admission AdmissionPolicy) *InMemoryCache {
	if capacity <= 0 {
		capacity = 1024
	}
	if admission == nil {
		admission = alwaysAdmission{}
	}
	return &InMemoryCache{
		items:     make(map[string]*entry, capacity),
		lru:       list.New(),
		capacity:  capacity,
		admission: admission,
	}
}

func NewTinyLFURuntimeCache(capacity int) *InMemoryCache {
	if capacity <= 0 {
		capacity = 1024
	}
	filter := NewTinyLFU()
	window := NewLRUWindow(32)
	admission := NewTinyLFUAdmission(filter, window, 2, 0.7)
	return NewInMemoryCache(capacity, admission)
}

func (c *InMemoryCache) Get(_ context.Context, key string) (Item, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok {
		return Item{}, ErrNotFound
	}
	if !e.item.ExpiresAt.IsZero() && time.Now().After(e.item.ExpiresAt) {
		c.removeLocked(key)
		return Item{}, ErrNotFound
	}
	c.lru.MoveToFront(e.ele)
	cloned := e.item
	cloned.Value = cloneBytes(e.item.Value)
	return cloned, nil
}

func (c *InMemoryCache) Set(_ context.Context, item Item, metrics Metrics) bool {
	if item.Key == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.items) >= c.capacity {
		if back := c.lru.Back(); back != nil {
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

	if e, ok := c.items[item.Key]; ok {
		e.item = copyItem(item)
		c.lru.MoveToFront(e.ele)
		return true
	}
	ele := c.lru.PushFront(item.Key)
	c.items[item.Key] = &entry{item: copyItem(item), ele: ele}
	if len(c.items) > c.capacity {
		back := c.lru.Back()
		if back != nil {
			k, _ := back.Value.(string)
			c.removeLocked(k)
		}
	}
	return true
}

func (c *InMemoryCache) Delete(_ context.Context, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(key)
}

func (c *InMemoryCache) Snapshot(_ context.Context) Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.items))
	for e := c.lru.Front(); e != nil; e = e.Next() {
		if k, ok := e.Value.(string); ok {
			keys = append(keys, k)
		}
	}
	return Snapshot{Size: len(c.items), Keys: keys}
}

func (c *InMemoryCache) Admission() AdmissionPolicy {
	return c.admission
}

func (c *InMemoryCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

func (c *InMemoryCache) Capacity() int {
	return c.capacity
}

func (c *InMemoryCache) removeLocked(key string) {
	e, ok := c.items[key]
	if !ok {
		return
	}
	delete(c.items, key)
	c.lru.Remove(e.ele)
}

func copyItem(item Item) Item {
	item.Value = cloneBytes(item.Value)
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
