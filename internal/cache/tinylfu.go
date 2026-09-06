package cache

import (
	"sync"
)

const (
	defaultCountMinWidth = 512
	defaultCountMinDepth = 2
	defaultTinyLFUAging  = 256
)

// CountMinSketch is a compact frequency estimator used by TinyLFU.
// It stores a small matrix of counters and returns the minimum count
// across hash rows for a given key.
type CountMinSketch struct {
	mu         sync.RWMutex
	width      int
	depth      int
	counters   [][]uint32
	agingStep  uint64
	ageCounter uint64
}

func NewCountMinSketch(width, depth int) *CountMinSketch {
	if width <= 0 {
		width = defaultCountMinWidth
	}
	if depth <= 0 {
		depth = defaultCountMinDepth
	}
	c := &CountMinSketch{
		width:     width,
		depth:     depth,
		counters:  make([][]uint32, depth),
		agingStep: defaultTinyLFUAging,
	}
	for i := range c.counters {
		c.counters[i] = make([]uint32, width)
	}
	return c
}

func (c *CountMinSketch) Increment(key string) {
	if key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.depth; i++ {
		idx := c.index(key, i)
		if c.counters[i][idx] < ^uint32(0) {
			c.counters[i][idx]++
		}
	}
	c.ageCounter++
	if c.ageCounter%c.agingStep == 0 {
		c.age()
	}
}

func (c *CountMinSketch) Estimate(key string) uint32 {
	if key == "" {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	minimum := ^uint32(0)
	for i := 0; i < c.depth; i++ {
		idx := c.index(key, i)
		v := c.counters[i][idx]
		if v < minimum {
			minimum = v
		}
	}
	if minimum == ^uint32(0) {
		return 0
	}
	return minimum
}

func (c *CountMinSketch) age() {
	for i := range c.counters {
		for j := range c.counters[i] {
			if c.counters[i][j] > 1 {
				c.counters[i][j] = c.counters[i][j] >> 1
			}
		}
	}
}

func (c *CountMinSketch) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.counters {
		for j := range c.counters[i] {
			c.counters[i][j] = 0
		}
	}
	c.ageCounter = 0
}

func (c *CountMinSketch) index(key string, salt int) int {
	return int(hashString32(key, salt) % uint32(c.width))
}

func hashString32(key string, salt int) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	h ^= uint32(salt)
	h *= 16777619
	return h
}

// TinyLFU is a small admission filter that estimates recency/frequency
// using a Count-Min Sketch. It is intentionally lightweight and does not
// replace storage; it only decides whether an item should pass admission.
type TinyLFU struct {
	mu     sync.RWMutex
	sketch *CountMinSketch
}

func NewTinyLFU() *TinyLFU {
	return &TinyLFU{sketch: NewCountMinSketch(defaultCountMinWidth, defaultCountMinDepth)}
}

func (t *TinyLFU) Increment(key string) {
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sketch.Increment(key)
}

func (t *TinyLFU) Estimate(key string) uint32 {
	if key == "" {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sketch.Estimate(key)
}

func (t *TinyLFU) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sketch.Reset()
}

type TinyLFUAdmission struct {
	Filter            *TinyLFU
	Window            *LRUWindow
	MinFrequency      uint32
	MaxPollutionRatio float64
}

func NewTinyLFUAdmission(filter *TinyLFU, window *LRUWindow, minFrequency uint32, maxPollution float64) *TinyLFUAdmission {
	return &TinyLFUAdmission{
		Filter:            filter,
		Window:            window,
		MinFrequency:      minFrequency,
		MaxPollutionRatio: maxPollution,
	}
}

func (a *TinyLFUAdmission) Frequency(key string) uint32 {
	if a == nil || a.Filter == nil {
		return 0
	}
	return a.Filter.Estimate(key)
}

func (a *TinyLFUAdmission) PollutantScore(candidateKey, victimKey string) float64 {
	if a == nil || a.Filter == nil {
		return 0
	}
	candidateFreq := a.Filter.Estimate(candidateKey)
	victimFreq := uint32(0)
	if victimKey != "" {
		victimFreq = a.Filter.Estimate(victimKey)
	}
	if candidateFreq == 0 || victimFreq == 0 {
		return 0
	}
	return 1.0 - (float64(candidateFreq) / (float64(victimFreq) + float64(candidateFreq)))
}

func (a *TinyLFUAdmission) ShouldStore(item Item, metrics Metrics) bool {
	if item.Key == "" {
		return false
	}
	if a == nil {
		return true
	}
	if a.Window != nil && a.Window.Contains(item.Key) {
		return true
	}
	if a.Filter == nil {
		return true
	}

	candidateFreq := metrics.CandidateFrequency
	if candidateFreq == 0 {
		candidateFreq = a.Filter.Estimate(item.Key)
	}
	if candidateFreq == 0 {
		return true
	}

	if metrics.VictimFrequency > 0 && candidateFreq < metrics.VictimFrequency {
		return false
	}
	if metrics.Pollution > 0 && a.MaxPollutionRatio > 0 && metrics.Pollution > a.MaxPollutionRatio {
		return false
	}
	if metrics.VictimKey != "" && a.MaxPollutionRatio > 0 && candidateFreq >= a.MinFrequency {
		pollution := a.PollutantScore(item.Key, metrics.VictimKey)
		if pollution > a.MaxPollutionRatio {
			return false
		}
	}
	if a.MinFrequency > 0 && candidateFreq < a.MinFrequency {
		return false
	}
	return true
}
