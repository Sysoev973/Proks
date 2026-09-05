package cache

import (
	"container/list"
	"sync"
)

// LRUWindow is a compact hot buffer for newly admitted or frequently touched keys.
// It keeps only a small set of recent keys and works as a pre-admission filter
// before full TinyLFU admission.
type LRUWindow struct {
	mu         sync.Mutex
	windowSize int
	lru        *list.List
	entries    map[string]*list.Element
}

func NewLRUWindow(size int) *LRUWindow {
	if size <= 0 {
		size = 64
	}
	return &LRUWindow{
		windowSize: size,
		lru:        list.New(),
		entries:    make(map[string]*list.Element, size),
	}
}

func (w *LRUWindow) Touch(key string) bool {
	if key == "" {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if ele, ok := w.entries[key]; ok {
		w.lru.MoveToFront(ele)
		return true
	}
	w.entries[key] = w.lru.PushFront(key)
	if w.lru.Len() > w.windowSize {
		back := w.lru.Back()
		if back != nil {
			k, _ := back.Value.(string)
			w.lru.Remove(back)
			delete(w.entries, k)
		}
	}
	return true
}

func (w *LRUWindow) Contains(key string) bool {
	if key == "" {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.entries[key]
	return ok
}

func (w *LRUWindow) Reset(key string) {
	if key == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if ele, ok := w.entries[key]; ok {
		w.lru.Remove(ele)
		delete(w.entries, key)
	}
}

func (w *LRUWindow) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}
