package proxy

import (
	"sync"
	"time"
)

type invalidationTracker struct {
	mu   sync.RWMutex
	seq  uint64
	tags map[string]tagInvalidation
}

type tagInvalidation struct {
	seq              uint64
	version          int64
	unconditionalSeq uint64
	at               time.Time
}

const (
	invalidationRetention = time.Minute
	invalidationPruneSize = 4096
)

func newInvalidationTracker() *invalidationTracker {
	return &invalidationTracker{tags: make(map[string]tagInvalidation)}
}

// fetchToken is taken before going upstream.
type fetchToken struct {
	seq     uint64
	started time.Time
}

func (t *invalidationTracker) begin() fetchToken {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return fetchToken{seq: t.seq, started: time.Now()}
}

// invalidate must be called before the tagged keys are deleted.
func (t *invalidationTracker) invalidate(tag string, version int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	now := time.Now()
	entry := t.tags[tag]
	entry.seq, entry.version, entry.at = t.seq, version, now
	if version == 0 {
		entry.unconditionalSeq = t.seq
	}
	t.tags[tag] = entry
	if len(t.tags) > invalidationPruneSize {
		for name, e := range t.tags {
			if now.Sub(e.at) > invalidationRetention {
				delete(t.tags, name)
			}
		}
	}
}

func (t *invalidationTracker) storeIfCurrent(token fetchToken, tags []string, responseVersion int64, store func() bool) (stored, invalidated bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if time.Since(token.started) > invalidationRetention {
		return false, true
	}
	if t.seq != token.seq {
		for _, tag := range tags {
			e, ok := t.tags[tag]
			if !ok || e.seq <= token.seq {
				continue
			}
			if e.unconditionalSeq > token.seq || responseVersion == 0 || responseVersion <= e.version {
				return false, true
			}
		}
	}
	return store(), false
}
