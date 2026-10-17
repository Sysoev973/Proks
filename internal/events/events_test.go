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
	unsub, err := broker.Subscribe(context.Background(), func(_ context.Context, event Event) error {
		atomic.AddInt64(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()

	if err := broker.Publish(context.Background(), Event{BookID: "42", Version: 10, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), Event{BookID: "42", Version: 10, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), Event{BookID: "42", Version: 11, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(25 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != 2 {
		t.Fatalf("expected 2 delivered events (v10,v11), got %d", got)
	}
}

func TestInMemoryBrokerRejectsOutOfOrderVersion(t *testing.T) {
	broker := NewInMemoryBroker()
	if err := broker.Publish(context.Background(), Event{BookID: "book-5", Version: 20, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), Event{BookID: "book-5", Version: 19, Type: "book.updated"}); err != nil {
		t.Fatal(err)
	}
	if got := broker.LastVersion("book:book-5"); got != 20 {
		t.Fatalf("expected last version 20, got %d", got)
	}
}

func TestInMemoryBrokerDeliversUnversionedEvents(t *testing.T) {
	broker := NewInMemoryBroker()
	var count int64
	unsub, err := broker.Subscribe(context.Background(), func(_ context.Context, event Event) error {
		atomic.AddInt64(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	defer unsub()

	_ = broker.Publish(context.Background(), Event{BookID: "42", Version: 0, Type: "book.updated"})
	_ = broker.Publish(context.Background(), Event{BookID: "42", Version: 0, Type: "book.updated"})
	time.Sleep(25 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != 2 {
		t.Fatalf("expected both unversioned events to be delivered, got %d", got)
	}

}

func TestInMemoryBrokerRejectsEventWithoutBookID(t *testing.T) {
	broker := NewInMemoryBroker()
	if err := broker.Publish(context.Background(), Event{Type: "book.updated", Version: 1}); err == nil {
		t.Fatal("expected invalid event to be rejected")
	}
}

func TestInMemoryBrokerRejectsInvalidTypeAndVersion(t *testing.T) {
	broker := NewInMemoryBroker()
	for _, event := range []Event{
		{Type: "book.created", BookID: "42", Version: 1},
		{Type: "book.updated", BookID: "42", Version: -1},
	} {
		if err := broker.Publish(context.Background(), event); err == nil {
			t.Fatalf("expected invalid event to be rejected: %+v", event)
		}
	}
}

func TestInMemoryBrokerRetriesAfterHandlerError(t *testing.T) {
	broker := NewInMemoryBroker()
	var attempts int64
	_, err := broker.Subscribe(context.Background(), func(_ context.Context, _ Event) error {
		if atomic.AddInt64(&attempts, 1) == 1 {
			return context.Canceled
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	event := Event{Type: "book.updated", BookID: "42", Version: 10}
	if err := broker.Publish(context.Background(), event); err == nil {
		t.Fatal("expected handler error to be returned")
	}
	if err := broker.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got := broker.LastVersion("book:42"); got != 10 {
		t.Fatalf("expected successful retry to commit version 10, got %d", got)
	}
}

func TestInMemoryBrokerZeroVersionAfterVersionedEvent(t *testing.T) {
	broker := NewInMemoryBroker()
	var count int64
	_, err := broker.Subscribe(context.Background(), func(_ context.Context, _ Event) error {
		atomic.AddInt64(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	event := func(version int64) Event {
		return Event{Type: "book.updated", BookID: "42", Version: version}
	}
	_ = broker.Publish(context.Background(), event(10))
	_ = broker.Publish(context.Background(), event(0))
	_ = broker.Publish(context.Background(), event(9))
	if count != 2 {
		t.Fatalf("expected v10 and v0 delivery only, got %d", count)
	}
}

func TestInMemoryBrokerGeneratesAndDeduplicatesEventID(t *testing.T) {
	broker := NewInMemoryBroker()
	var count int64
	_, err := broker.Subscribe(context.Background(), func(_ context.Context, event Event) error {
		if event.EventID == "" {
			t.Fatal("expected generated event ID")
		}
		atomic.AddInt64(&count, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	event := Event{Type: "book.updated", BookID: "42", Version: 10, EventID: "evt-1"}
	if err := broker.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt64(&count); got != 1 {
		t.Fatalf("expected one delivery for duplicate event ID, got %d", got)
	}
}

// Keys are not versioned, so a key purge must be delivered even when its
// tags were already invalidated at this version.
func TestKeyPurgeIsNotDroppedAsSuperseded(t *testing.T) {
	broker := NewInMemoryBroker()
	var delivered []Event
	if _, err := broker.Subscribe(context.Background(), func(_ context.Context, evt Event) error {
		delivered = append(delivered, evt)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = broker.Publish(context.Background(), Event{Type: TypeInvalidate, Tags: []string{"order:1"}, Version: 5})
	_ = broker.Publish(context.Background(), Event{Type: TypeInvalidate, Tags: []string{"order:1"}, Keys: []string{"k"}, Version: 5})
	_ = broker.Publish(context.Background(), Event{Type: TypeInvalidate, Tags: []string{"order:1"}, Version: 4})
	if len(delivered) != 2 || len(delivered[1].Keys) != 1 {
		t.Fatalf("expected the event with keys to be delivered and the older tag-only one skipped, got %+v", delivered)
	}
}
