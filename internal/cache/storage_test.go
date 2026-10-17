package cache

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestByteLimitEvictsLRUTail(t *testing.T) {
	ctx := context.Background()
	c := New(Config{Capacity: 100, MaxBytes: 3 * (entryOverhead + 1 + 1000), Shards: 1})
	for _, k := range []string{"a", "b", "c"} {
		if !c.Set(ctx, Item{Key: k, Value: make([]byte, 1000)}, Metrics{}) {
			t.Fatalf("expected %s to be stored", k)
		}
	}
	_, _ = c.Get(ctx, "a")
	if !c.Set(ctx, Item{Key: "d", Value: make([]byte, 1000)}, Metrics{}) {
		t.Fatal("expected d to be stored")
	}
	if _, err := c.Get(ctx, "b"); err == nil {
		t.Fatal("expected LRU tail b to be evicted by the byte limit")
	}
	if c.Size() != 3 || c.Bytes() > c.MaxBytes() {
		t.Fatalf("size=%d bytes=%d max=%d", c.Size(), c.Bytes(), c.MaxBytes())
	}
}

func TestItemLargerThanShardBudgetIsRejected(t *testing.T) {
	c := New(Config{Capacity: 10, MaxBytes: 1024, Shards: 1})
	if c.Set(context.Background(), Item{Key: "big", Value: make([]byte, 2048)}, Metrics{}) {
		t.Fatal("expected oversized item to be rejected")
	}
	if c.Bytes() != 0 {
		t.Fatalf("expected no bytes accounted, got %d", c.Bytes())
	}
}

func TestReplacingItemUpdatesBytesAndTags(t *testing.T) {
	ctx := context.Background()
	c := New(Config{Capacity: 10})
	c.Set(ctx, Item{Key: "k", Value: make([]byte, 100), Tags: []string{"book:1"}}, Metrics{})
	c.Set(ctx, Item{Key: "k", Value: make([]byte, 10), Tags: []string{"book:2"}}, Metrics{})
	if got := c.Bytes(); got != entryOverhead+1+10+6 {
		t.Fatalf("unexpected bytes after replace: %d", got)
	}
	if keys := c.KeysForTag(ctx, "book:1"); len(keys) != 0 {
		t.Fatalf("old tag must be dropped, got %v", keys)
	}
	if keys := c.KeysForTag(ctx, "book:2"); len(keys) != 1 {
		t.Fatalf("new tag must be indexed, got %v", keys)
	}
}

func TestLookupServesStaleWithinWindow(t *testing.T) {
	ctx := context.Background()
	c := New(Config{Capacity: 10})
	now := time.Now()
	c.Set(ctx, Item{Key: "k", Value: []byte("v"), ExpiresAt: now.Add(-time.Millisecond), StaleUntil: now.Add(time.Minute)}, Metrics{})
	if _, err := c.Get(ctx, "k"); err == nil {
		t.Fatal("Get must not return stale items")
	}
	item, fresh, err := c.Lookup(ctx, "k")
	if err != nil || fresh || string(item.Value) != "v" {
		t.Fatalf("expected stale item, got fresh=%v err=%v", fresh, err)
	}
	c.Set(ctx, Item{Key: "gone", ExpiresAt: now.Add(-time.Minute), StaleUntil: now.Add(-time.Second)}, Metrics{})
	if _, _, err := c.Lookup(ctx, "gone"); err == nil {
		t.Fatal("expected item past StaleUntil to be removed")
	}
}

func TestDefaultShardCount(t *testing.T) {
	if n := len(NewInMemoryCache(4, nil).shards); n != 1 {
		t.Fatalf("small caches keep exact LRU in one shard, got %d", n)
	}
	if n := len(NewInMemoryCache(10_000, nil).shards); n != 16 {
		t.Fatalf("expected 16 shards, got %d", n)
	}
}

// TestTagIndexDoesNotLeakUnderChurn races stores, deletes, expiry and capacity
// eviction; once everything is gone, no tag may still point at a key.
func TestTagIndexDoesNotLeakUnderChurn(t *testing.T) {
	ctx := context.Background()
	c := New(Config{Capacity: 64, MaxBytes: 64 * 400, Shards: 4})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("k%d", rng.Intn(200))
				switch rng.Intn(4) {
				case 0:
					c.Delete(ctx, key)
				case 1:
					_, _, _ = c.Lookup(ctx, key)
				default:
					c.Set(ctx, Item{
						Key:       key,
						Value:     []byte(strings.Repeat("x", rng.Intn(200))),
						ExpiresAt: time.Now().Add(time.Duration(rng.Intn(5)) * time.Millisecond),
						Tags:      []string{fmt.Sprintf("book:%d", rng.Intn(10)), "list:books"},
					}, Metrics{})
				}
			}
		}(int64(w))
	}
	wg.Wait()

	for _, key := range c.Snapshot(ctx).Keys {
		c.Delete(ctx, key)
	}
	time.Sleep(20 * time.Millisecond)
	for _, s := range c.shards {
		s.mu.Lock()
		tags, bytes := len(s.tags), s.bytes
		s.mu.Unlock()
		if tags != 0 || bytes != 0 {
			t.Fatalf("shard leaked: tags=%d bytes=%d", tags, bytes)
		}
	}
}
