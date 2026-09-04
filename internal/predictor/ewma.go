package predictor

import "sync"

type EWMA struct {
	mu      sync.RWMutex
	alpha   float64
	current float64
	init    bool
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
	if !e.init {
		e.current = value
		e.init = true
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
