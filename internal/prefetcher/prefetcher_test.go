package prefetcher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncPrefetcherDeduplicates(t *testing.T) {
	var count int64
	p := NewAsyncPrefetcher(1, 8, 50*time.Millisecond, func(ctx context.Context, key string) error {
		atomic.AddInt64(&count, 1)
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	defer p.Stop()

	p.Enqueue(context.Background(), "book_view:10", ReasonMarkov)
	p.Enqueue(context.Background(), "book_view:10", ReasonMarkov)
	p.Enqueue(context.Background(), "book_view:10", ReasonMarkov)

	time.Sleep(80 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != 1 {
		t.Fatalf("expected exactly 1 deduplicated fetch, got %d", got)
	}
}

func TestAsyncPrefetcherQueueOverflow(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	p := NewAsyncPrefetcher(1, 1, time.Second, func(ctx context.Context, key string) error {
		startedOnce.Do(func() { close(started) })
		<-release
		return nil
	}, nil)
	defer close(release)
	defer p.Stop()

	p.Enqueue(context.Background(), "book_view:20", ReasonMarkov)
	<-started
	p.Enqueue(context.Background(), "book_view:21", ReasonMarkov)
	p.Enqueue(context.Background(), "book_view:22", ReasonMarkov)

	if got := p.Snapshot().Dropped; got == 0 {
		t.Fatalf("expected queue overflow to be counted as dropped")
	}
}

func TestAsyncPrefetcherTimeoutIsTrackedAsCanceled(t *testing.T) {
	p := NewAsyncPrefetcher(1, 8, 20*time.Millisecond, func(ctx context.Context, key string) error {
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	defer p.Stop()

	p.Enqueue(context.Background(), "book_view:30", ReasonMarkov)
	time.Sleep(100 * time.Millisecond)
	if got := p.Snapshot().Canceled; got == 0 {
		t.Fatalf("expected timeout/cancel to be tracked, got canceled=%d", got)
	}
}
