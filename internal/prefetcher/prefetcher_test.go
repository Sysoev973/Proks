package prefetcher

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
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

func TestAsyncPrefetcherWithoutLoaderRecordsNothing(t *testing.T) {
	p := NewAsyncPrefetcher(1, 8, 20*time.Millisecond, nil, nil)
	defer p.Stop()
	var outcomes int64
	p.SetOutcomeHook(func(string, bool) { atomic.AddInt64(&outcomes, 1) })

	if p.Enabled() {
		t.Fatal("prefetcher without loader must report disabled")
	}
	p.Enqueue(context.Background(), "book_view:40", ReasonMarkov)
	time.Sleep(30 * time.Millisecond)

	if got := atomic.LoadInt64(&outcomes); got != 0 {
		t.Fatalf("expected no outcomes, got %d", got)
	}
	if snap := p.Snapshot(); snap != (Stats{}) {
		t.Fatalf("expected zero stats, got %+v", snap)
	}
}

type outcome struct {
	key     string
	success bool
}

func newObserved(t *testing.T, fetch FetchFunc, logs *bytes.Buffer) (*AsyncPrefetcher, chan outcome) {
	t.Helper()
	var logger *log.Logger
	if logs != nil {
		logger = log.New(logs, "", 0)
	}
	p := NewAsyncPrefetcher(1, 4, time.Second, fetch, logger)
	t.Cleanup(p.Stop)
	outcomes := make(chan outcome, 4)
	p.SetOutcomeHook(func(key string, success bool) { outcomes <- outcome{key, success} })
	return p, outcomes
}

func waitOutcome(t *testing.T, outcomes chan outcome) outcome {
	t.Helper()
	select {
	case o := <-outcomes:
		return o
	case <-time.After(5 * time.Second):
		t.Fatal("no prefetch outcome")
		return outcome{}
	}
}

func TestAsyncPrefetcherReportsOutcomes(t *testing.T) {
	var logs bytes.Buffer
	results := map[string]error{
		"ok":       nil,
		"failed":   errors.New("upstream down"),
		"canceled": context.DeadlineExceeded,
	}
	p, outcomes := newObserved(t, func(_ context.Context, key string) error { return results[key] }, &logs)

	for _, key := range []string{"ok", "failed", "canceled"} {
		p.Enqueue(context.Background(), key, ReasonMarkov)
		got := waitOutcome(t, outcomes)
		if got.key != key || got.success != (results[key] == nil) {
			t.Fatalf("%s: unexpected outcome %+v", key, got)
		}
	}
	waitFor := func(cond func(Stats) bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond(p.Snapshot()) {
			if time.Now().After(deadline) {
				t.Fatalf("unexpected stats %+v", p.Snapshot())
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitFor(func(s Stats) bool { return s.Enqueued == 3 && s.Executed == 1 && s.Failed == 1 && s.Canceled == 1 })
	p.Stop() // waits for the worker, which logs after reporting the outcome
	if !strings.Contains(logs.String(), "prefetch failed key=failed") || !strings.Contains(logs.String(), "prefetch canceled key=canceled") {
		t.Fatalf("expected failures to be logged, got %q", logs.String())
	}
}

func TestAsyncPrefetcherDropsWhenClosedOrCanceled(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	p, _ := newObserved(t, func(context.Context, string) error {
		started <- struct{}{}
		<-release
		return nil
	}, nil)
	defer close(release)

	p.Enqueue(context.Background(), "", ReasonMarkov)
	p.Enqueue(context.Background(), "running", ReasonMarkov)
	<-started
	for i := 0; i < 4; i++ {
		p.Enqueue(context.Background(), "queued-"+strconv.Itoa(i), ReasonMarkov)
	}
	if p.QueueDepth() != 4 {
		t.Fatalf("expected a full queue of 4, got %d", p.QueueDepth())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.Enqueue(ctx, "late", ReasonMarkov)
	if got := p.Snapshot(); got.Enqueued != 5 || got.Dropped != 1 {
		t.Fatalf("expected the canceled enqueue to be dropped, got %+v", got)
	}

	closed := NewAsyncPrefetcher(0, 0, 0, func(context.Context, string) error { return nil }, nil)
	closed.Stop()
	closed.Stop()
	closed.Enqueue(context.Background(), "after-stop", ReasonMarkov)
	if got := closed.Snapshot().Dropped; got != 1 {
		t.Fatalf("expected an enqueue after Stop to be dropped, got %d", got)
	}
}
