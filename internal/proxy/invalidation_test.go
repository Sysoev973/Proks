package proxy

import (
	"context"
	"net/http"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/events"
	"proks/internal/predictor"
	"proks/internal/prefetcher"
)

func TestHandlerInvalidateBookRemovesMatchingKeys(t *testing.T) {
	c := cache.NewInMemoryCache(32, nil)
	itemKey := "u|t|ru|book_view:42|"
	_ = c.Set(context.Background(), cache.Item{Key: itemKey, Value: []byte("old"), StatusCode: http.StatusOK, ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})
	_ = c.Set(context.Background(), cache.Item{Key: "u|t|ru|category_view:fiction|", Value: []byte("category"), StatusCode: http.StatusOK, ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})

	pf := prefetcher.NewAsyncPrefetcher(1, 8, 20*time.Millisecond, func(ctx context.Context, key string) error { return nil }, nil)
	defer pf.Stop()

	h, err := NewHandler(c, predictor.NewMarkov(), pf, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}

	if err := h.HandleBookUpdated(context.Background(), events.BookUpdateEvent{BookID: "42", Version: 7, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Get(context.Background(), itemKey); err == nil {
		t.Fatal("expected book-specific cache key to be invalidated")
	}
	if _, err := c.Get(context.Background(), "u|t|ru|category_view:fiction|"); err != nil {
		t.Fatal("expected unrelated category key to remain")
	}
}

func TestHandlerInvalidateBookUsesStructuredIDs(t *testing.T) {
	c := cache.NewInMemoryCache(32, nil)
	book1 := "u|t|ru|book_view:1|"
	book10 := "u|t|ru|book_view:10|"
	_ = c.Set(context.Background(), cache.Item{Key: book1, Value: []byte("one"), ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})
	_ = c.Set(context.Background(), cache.Item{Key: book10, Value: []byte("ten"), ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}

	if err := h.HandleBookUpdated(context.Background(), events.BookUpdateEvent{BookID: "1", Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), book1); err == nil {
		t.Fatal("expected book:1 to be invalidated")
	}
	if _, err := c.Get(context.Background(), book10); err != nil {
		t.Fatal("book:10 must not be invalidated by book:1")
	}
}

func TestHandlerBookInvalidationKeepsUnrelatedDerivedViews(t *testing.T) {
	c := cache.NewInMemoryCache(32, nil)
	detail := "u|t|ru|book_view:42|"
	category := "u|t|ru|category_view:fiction|"
	_ = c.Set(context.Background(), cache.Item{Key: detail, Value: []byte("detail"), ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})
	_ = c.Set(context.Background(), cache.Item{Key: category, Value: []byte("category"), ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}
	h.registerResources([]string{"book:42"}, detail)
	h.registerResources([]string{"category:fiction"}, category)

	if err := h.HandleBookUpdated(context.Background(), events.BookUpdateEvent{BookID: "42", Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), detail); err == nil {
		t.Fatal("expected book detail to be invalidated")
	}
	if _, err := c.Get(context.Background(), category); err != nil {
		t.Fatal("unrelated category must remain cached")
	}
}

func TestHandlerDependencyIndexPrunesRemovedKeys(t *testing.T) {
	c := cache.NewInMemoryCache(32, nil)
	key := "u|t|ru|books|"
	_ = c.Set(context.Background(), cache.Item{Key: key, Value: []byte("list"), ExpiresAt: time.Now().Add(time.Minute)}, cache.Metrics{})
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}

	h.registerResources([]string{"book:42"}, key)
	c.Delete(context.Background(), key)
	h.registerResources([]string{"list:books"}, "new-key")

	h.indexMu.RLock()
	defer h.indexMu.RUnlock()
	if _, ok := h.resourceKeys["book:42"]; ok {
		t.Fatal("expected stale resource index entry to be pruned")
	}

}

func TestHandlerDependencyIndexPrunesExpiredKeysAutomatically(t *testing.T) {
	c := cache.NewInMemoryCache(32, nil)
	key := "u|t|ru|books|"
	_ = c.Set(context.Background(), cache.Item{Key: key, Value: []byte("list"), ExpiresAt: time.Now().Add(20 * time.Millisecond)}, cache.Metrics{})
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, "http://example.com")
	if err != nil {
		t.Fatal(err)
	}
	h.registerResources([]string{"book:42"}, key)
	time.Sleep(50 * time.Millisecond)

	h.indexMu.RLock()
	defer h.indexMu.RUnlock()
	if _, ok := h.resourceKeys["book:42"]; ok {
		t.Fatal("expected expired cache entry to be removed from dependency index")
	}
}
