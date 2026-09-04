package cache

import "sync"

type TinyLFU struct {
	mu       sync.RWMutex
	counters map[string]uint32
}

func NewTinyLFU() *TinyLFU {
	return &TinyLFU{counters: make(map[string]uint32)}
}

func (t *TinyLFU) Increment(key string) {
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counters[key]++
}

func (t *TinyLFU) Estimate(key string) uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.counters[key]
}

func (t *TinyLFU) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counters = make(map[string]uint32)
}

type TinyLFUAdmission struct {
	Filter            *TinyLFU
	MinFrequency      uint32
	MaxPollutionRatio float64
}

func (a TinyLFUAdmission) ShouldStore(item Item, metrics Metrics) bool {
	if item.Key == "" || a.Filter == nil {
		return false
	}
	if metrics.Pollution > a.MaxPollutionRatio {
		return false
	}
	return a.Filter.Estimate(item.Key) >= a.MinFrequency
}
