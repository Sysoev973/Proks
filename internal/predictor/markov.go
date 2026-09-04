package predictor

import (
	"sort"
	"sync"
)

type Candidate struct {
	Key         string
	Probability float64
}

type Predictor interface {
	Update(prev, next string)
	Predict(current string) []Candidate
}

type Transition struct {
	Next   string
	Weight uint64
}

type Markov struct {
	mu    sync.RWMutex
	graph map[string]map[string]uint64
}

func NewMarkov() *Markov {
	return &Markov{graph: make(map[string]map[string]uint64)}
}

func (m *Markov) Update(prev, next string) {
	if prev == "" || next == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.graph[prev]; !ok {
		m.graph[prev] = make(map[string]uint64)
	}
	m.graph[prev][next]++
}

func (m *Markov) Predict(current string) []Candidate {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nexts, ok := m.graph[current]
	if !ok || len(nexts) == 0 {
		return nil
	}
	var total uint64
	for _, w := range nexts {
		total += w
	}
	if total == 0 {
		return nil
	}

	out := make([]Candidate, 0, len(nexts))
	for k, w := range nexts {
		out = append(out, Candidate{Key: k, Probability: float64(w) / float64(total)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Probability > out[j].Probability })
	return out
}
