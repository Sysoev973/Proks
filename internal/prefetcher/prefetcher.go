package prefetcher

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type PrefetchReason string

const (
	ReasonMarkov PrefetchReason = "MARKOV"
)

type Prefetcher interface {
	Enqueue(ctx context.Context, key string, reason PrefetchReason)
}

type FetchFunc func(ctx context.Context, key string) error

type Stats struct {
	Enqueued uint64
	Dropped  uint64
	Executed uint64
	Failed   uint64
	Canceled uint64
}

type AsyncPrefetcher struct {
	sf      callGroup
	fetch   FetchFunc
	jobs    chan job
	wg      sync.WaitGroup
	timeout time.Duration
	mu      sync.RWMutex
	closed  bool
	logger  *log.Logger
	stats   prefetchStats
}

type prefetchStats struct {
	enqueued uint64
	dropped  uint64
	executed uint64
	failed   uint64
	canceled uint64
}

type callGroup interface {
	Do(key string, fn func() (interface{}, error)) (interface{}, error, bool)
}

type localSingleflight struct {
	mu sync.Mutex
	m  map[string]*inflightCall
}

type inflightCall struct {
	wg  sync.WaitGroup
	val interface{}
	err error
	dup int
}

func newLocalSingleflight() *localSingleflight {
	return &localSingleflight{m: make(map[string]*inflightCall)}
}

func (g *localSingleflight) Do(key string, fn func() (interface{}, error)) (interface{}, error, bool) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		c.dup++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &inflightCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	shared := c.dup > 0
	g.mu.Unlock()
	return c.val, c.err, shared
}

type job struct {
	key    string
	reason PrefetchReason
}

func NewAsyncPrefetcher(workers, queueSize int, timeout time.Duration, fetch FetchFunc, logger *log.Logger) *AsyncPrefetcher {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = 128
	}
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	p := &AsyncPrefetcher{
		sf:      newLocalSingleflight(),
		fetch:   fetch,
		jobs:    make(chan job, queueSize),
		timeout: timeout,
		logger:  logger,
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *AsyncPrefetcher) Enqueue(ctx context.Context, key string, reason PrefetchReason) {
	if key == "" {
		return
	}
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()
	if closed {
		atomic.AddUint64(&p.stats.dropped, 1)
		p.logf("prefetch dropped: service closed key=%s reason=%s", key, reason)
		return
	}
	select {
	case p.jobs <- job{key: key, reason: reason}:
		atomic.AddUint64(&p.stats.enqueued, 1)
	case <-ctx.Done():
		atomic.AddUint64(&p.stats.dropped, 1)
		p.logf("prefetch dropped: context done key=%s reason=%s err=%v", key, reason, ctx.Err())
	default:
		atomic.AddUint64(&p.stats.dropped, 1)
		p.logf("prefetch dropped: queue full key=%s reason=%s", key, reason)
	}
}

func (p *AsyncPrefetcher) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.jobs)
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *AsyncPrefetcher) Snapshot() Stats {
	return Stats{
		Enqueued: atomic.LoadUint64(&p.stats.enqueued),
		Dropped:  atomic.LoadUint64(&p.stats.dropped),
		Executed: atomic.LoadUint64(&p.stats.executed),
		Failed:   atomic.LoadUint64(&p.stats.failed),
		Canceled: atomic.LoadUint64(&p.stats.canceled),
	}
}

func (p *AsyncPrefetcher) worker() {
	defer p.wg.Done()
	for j := range p.jobs {
		if p.fetch == nil {
			continue
		}
		_, err, _ := p.sf.Do(j.key, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
			defer cancel()
			return nil, p.fetch(ctx, j.key)
		})
		if err == nil {
			atomic.AddUint64(&p.stats.executed, 1)
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			atomic.AddUint64(&p.stats.canceled, 1)
			p.logf("prefetch canceled key=%s reason=%s err=%v", j.key, j.reason, err)
			continue
		}
		atomic.AddUint64(&p.stats.failed, 1)
		p.logf("prefetch failed key=%s reason=%s err=%v", j.key, j.reason, err)
	}
}

func (p *AsyncPrefetcher) logf(format string, args ...interface{}) {
	if p.logger == nil {
		return
	}
	p.logger.Printf(format, args...)
}
