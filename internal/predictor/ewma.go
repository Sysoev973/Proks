package predictor

import "sync"

type EWMA struct {
	mu          sync.RWMutex
	alpha       float64
	current     float64
	initialized bool
}

func NewEWMA(alpha float64) *EWMA {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}
	return &EWMA{alpha: alpha}
}

func (e *EWMA) Add(value float64) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.initialized {
		e.current = value
		e.initialized = true
		return e.current
	}
	e.current = e.alpha*value + (1-e.alpha)*e.current
	return e.current
}

func (e *EWMA) Value() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current
}

// TrendTracker maintains smoothed confidence per route key.
// It is only used to gate prefetch confidence, not to decide unconditional caching.
type TrendTracker struct {
	mu              sync.RWMutex
	alpha           float64
	minObservations uint64
	cutoff          float64
	observations    map[string]uint64
	scores          map[string]*EWMA
}

func NewTrendTracker(alpha float64, minObservations uint64, cutoff float64) *TrendTracker {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}
	if cutoff <= 0 || cutoff > 1 {
		cutoff = 0.6
	}
	return &TrendTracker{
		alpha:           alpha,
		minObservations: minObservations,
		cutoff:          cutoff,
		observations:    make(map[string]uint64),
		scores:          make(map[string]*EWMA),
	}
}

func (t *TrendTracker) Observe(key string, value float64) float64 {
	if key == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.scores[key]; !ok {
		t.scores[key] = NewEWMA(t.alpha)
	}
	t.observations[key]++
	return t.scores[key].Add(value)
}

func (t *TrendTracker) Confidence(key string) float64 {
	if key == "" {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	obs, ok := t.observations[key]
	if !ok || obs < t.minObservations {
		return 0
	}
	e, ok := t.scores[key]
	if !ok || e == nil {
		return 0
	}
	v := e.Value()
	if v < t.cutoff {
		return 0
	}
	return v
}

func (t *TrendTracker) ObserveOutcome(key string, success bool) float64 {
	if key == "" {
		return 0
	}
	if success {
		return t.Observe(key, 1.0)
	}
	return t.Observe(key, 0.1)
}

func (t *TrendTracker) ObserveTransition(prev, next string, success bool) float64 {
	if prev == "" || next == "" {
		return 0
	}
	return t.ObserveOutcome(prev+"->"+next, success)
}

func (t *TrendTracker) ObservePrefetch(key string, success bool) float64 {
	return t.ObserveOutcome(key, success)
}

func (t *TrendTracker) AdjustProbability(key string, probability float64) float64 {
	if probability <= 0 {
		return 0
	}
	t.mu.RLock()
	_, hasObservations := t.observations[key]
	t.mu.RUnlock()
	if !hasObservations {
		return probability
	}
	conf := t.Confidence(key)
	if conf <= 0 {
		return 0
	}
	return probability * conf
}

func (t *TrendTracker) Reset(key string) {
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.observations, key)
	delete(t.scores, key)
}
