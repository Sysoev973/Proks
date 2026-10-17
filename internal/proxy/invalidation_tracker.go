package proxy

import (
	"sync"
	"time"
)

// invalidationTracker closes the race between an upstream fetch and a purge:
// a response read before an invalidation of one of its tags must not be stored
// after the purge already ran.
//
// Each invalidation takes the next value of a global sequence and records it
// on the tag. A fetch remembers the sequence before it goes upstream and, once
// it knows the response's tags (some only arrive in X-Cache-Depends-On),
// refuses to store if any of them was invalidated since. The check and the
// Set run under the read lock and the invalidation bumps under the write lock,
// so a purge either happens before the check or finds the stored item.
type invalidationTracker struct {
	mu   sync.RWMutex
	seq  uint64
	tags map[string]tagInvalidation
}

type tagInvalidation struct {
	// seq is the latest invalidation of the tag, version its event version.
	seq     uint64
	version int64
	// unconditionalSeq is the latest invalidation without a version, which
	// purges regardless of the response version.
	unconditionalSeq uint64
	at               time.Time
}

const (
	// invalidationRetention bounds how long a tag's invalidation is remembered.
	// A fetch that started earlier than this is never stored, so forgetting an
	// older entry cannot let stale data in.
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

// storeIfCurrent runs store while holding off invalidations, unless one of
// tags was invalidated after token was taken. A response carrying a version
// newer than a versioned invalidation is still stored.
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
