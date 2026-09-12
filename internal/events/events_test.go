package events

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestInMemoryBrokerDeduplicatesDuplicateVersion(t *testing.T) {
	broker := NewInMemoryBroker()
	var count int64
	unsub, err := broker.Subscribe(context.Background(), func(_ context.Context, event BookUpdateEvent) error {
		atomic.AddInt64(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()

	if err := broker.Publish(context.Background(), BookUpdateEvent{BookID: "42", Version: 10, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), BookUpdateEvent{BookID: "42", Version: 10, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), BookUpdateEvent{BookID: "42", Version: 11, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(25 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != 2 {
		t.Fatalf("expected 2 delivered events (v10,v11), got %d", got)
	}
}

func TestInMemoryBrokerRejectsOutOfOrderVersion(t *testing.T) {
	broker := NewInMemoryBroker()
	if err := broker.Publish(context.Background(), BookUpdateEvent{BookID: "book-5", Version: 20, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), BookUpdateEvent{BookID: "book-5", Version: 19, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if got := broker.LastVersion("book-5"); got != 20 {
		t.Fatalf("expected last version 20, got %d", got)
	}
}
