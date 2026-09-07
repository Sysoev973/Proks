package events

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrEmptyBookID = errors.New("events: empty book id")

type BookUpdateEvent struct {
	BookID   string    `json:"book_id"`
	Version  int64     `json:"version"`
	Tenant   string    `json:"tenant,omitempty"`
	Occurred time.Time `json:"occurred_at"`
	Type     string    `json:"type"`
}

type EventHandler func(context.Context, BookUpdateEvent) error

type Stream interface {
	Publish(ctx context.Context, event BookUpdateEvent) error
	Subscribe(ctx context.Context, handler EventHandler) (func(), error)
}

type InMemoryBroker struct {
	mu         sync.RWMutex
	listeners  []EventHandler
	maxVersion map[string]int64
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{listeners: make([]EventHandler, 0, 4), maxVersion: make(map[string]int64)}
}

func (b *InMemoryBroker) Publish(ctx context.Context, event BookUpdateEvent) error {
	if event.BookID == "" {
		return ErrEmptyBookID
	}
	if event.Type == "" {
		event.Type = "book.updated"
	}
	if event.Occurred.IsZero() {
		event.Occurred = time.Now().UTC()
	}

	b.mu.Lock()
	last := b.maxVersion[event.BookID]
	if event.Version > 0 && event.Version <= last {
		b.mu.Unlock()
		return nil
	}
	if event.Version > 0 {
		b.maxVersion[event.BookID] = event.Version
	}
	listeners := append([]EventHandler(nil), b.listeners...)
	b.mu.Unlock()

	for _, handler := range listeners {
		go func(h EventHandler) {
			if err := h(ctx, event); err != nil {
				_ = err
			}
		}(handler)
	}
	return nil
}

func (b *InMemoryBroker) Subscribe(_ context.Context, handler EventHandler) (func(), error) {
	if handler == nil {
		return nil, errors.New("events: nil handler")
	}
	b.mu.Lock()
	b.listeners = append(b.listeners, handler)
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		for i, h := range b.listeners {
			if fmt.Sprintf("%p", h) == fmt.Sprintf("%p", handler) {
				b.listeners = append(b.listeners[:i], b.listeners[i+1:]...)
				return
			}
		}
	}, nil
}

func (b *InMemoryBroker) LastVersion(bookID string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maxVersion[bookID]
}
