package cache

import (
	"container/list"
	"sync"
)

type LRUWindow struct {
	mu         sync.Mutex
	windowSize int
	lru        *list.List
	entries    map[string]*list.Element
	touches    map[string]int
}

func NewLRUWindow(size int) *LRUWindow {
	if size <= 0 {
		size = 64
	}
	return &LRUWindow{
		windowSize: size,
		lru:        list.New(),
		entries:    make(map[string]*list.Element, size),
		touches:    make(map[string]int, size),
	}
}

func (w *LRUWindow) Touch(key string) bool {
	if key == "" {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	w.touches[key]++
	if ele, ok := w.entries[key]; ok {
		w.lru.MoveToFront(ele)
	} else {
		ele = w.lru.PushFront(key)
		w.entries[key] = ele
	}

	if len(w.entries) > w.windowSize {
		back := w.lru.Back()
		if back != nil {
			evictKey, _ := back.Value.(string)
			w.lru.Remove(back)
			delete(w.entries, evictKey)
			delete(w.touches, evictKey)
		}
	}
	return w.touches[key] >= 2
}

func (w *LRUWindow) Reset(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if ele, ok := w.entries[key]; ok {
		w.lru.Remove(ele)
		delete(w.entries, key)
	}
	delete(w.touches, key)
}
