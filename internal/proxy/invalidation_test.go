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
