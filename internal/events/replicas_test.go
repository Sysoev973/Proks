package events

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProcessedEventIDsAreBounded(t *testing.T) {
	b := &RabbitBroker{processed: make(map[string]struct{}), maxVersion: make(map[string]int64)}
	for i := 0; i < maxProcessedEvents+10; i++ {
		b.rememberLocked(fmt.Sprintf("e%d", i))
	}
	if len(b.processed) != maxProcessedEvents || len(b.processedOrder) != maxProcessedEvents {
		t.Fatalf("expected %d remembered IDs, got map=%d order=%d", maxProcessedEvents, len(b.processed), len(b.processedOrder))
	}
	if _, ok := b.processed["e0"]; ok {
		t.Fatal("expected the oldest ID to be forgotten")
	}
	if _, ok := b.processed[fmt.Sprintf("e%d", maxProcessedEvents+9)]; !ok {
		t.Fatal("expected the newest ID to be remembered")
	}
}

func TestEventLogCompactionKeepsOnlyPendingEvents(t *testing.T) {
	b := &RabbitBroker{
		eventLogPath: filepath.Join(t.TempDir(), "events.log"),
		processed:    make(map[string]struct{}),
		maxVersion:   make(map[string]int64),
	}
	for i := 0; i < eventLogCompactEvery-2; i++ {
		evt := Event{EventID: fmt.Sprintf("done-%d", i), Type: "book.updated", BookID: "1"}
		if err := b.appendEvent(evt); err != nil {
			t.Fatal(err)
		}
		b.rememberLocked(evt.EventID)
	}
	b.maxVersion["book:2"] = 5
	if err := b.appendEvent(Event{EventID: "old-version", Type: "book.updated", BookID: "2", Version: 4}); err != nil {
		t.Fatal(err)
	}
	// The 1000th append compacts; this event is still pending, so it stays.
	if err := b.appendEvent(Event{EventID: "pending", Type: "book.updated", BookID: "3", Version: 1}); err != nil {
		t.Fatal(err)
	}

	logged, err := b.loadEventLog()
	if err != nil {
		t.Fatal(err)
	}
	if len(logged) != 1 || logged[0].EventID != "pending" {
		t.Fatalf("expected only the pending event to survive compaction, got %d events: %+v", len(logged), logged[:min(len(logged), 3)])
	}
	if b.appendedSinceCompact != 0 {
		t.Fatalf("expected compaction to reset the counter, got %d", b.appendedSinceCompact)
	}
}

func TestReplicasUseDistinctQueues(t *testing.T) {
	t.Setenv("PROXY_REPLICA_ID", "proxy-1")
	first := consumerQueueName("book.updated")
	t.Setenv("PROXY_REPLICA_ID", "proxy-2")
	second := consumerQueueName("book.updated")
	if first == second {
		t.Fatalf("replicas must not share a queue, both got %q", first)
	}
}

// TestEveryReplicaReceivesEachEvent needs a running RabbitMQ, for example
// RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ go test ./internal/events/
func TestEveryReplicaReceivesEachEvent(t *testing.T) {
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL is not set")
	}
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	newReplica := func(name string) (*RabbitBroker, chan Event) {
		dir := t.TempDir()
		t.Setenv("PROXY_REPLICA_ID", name+"-"+run)
		t.Setenv("EVENT_LOG_PATH", filepath.Join(dir, "events.log"))
		t.Setenv("EVENT_VERSION_STATE_PATH", filepath.Join(dir, "state.json"))
		t.Setenv("EVENT_QUEUE_TTL", "1m")
		b, err := NewRabbitBroker(url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		got := make(chan Event, 1)
		stop, err := b.Subscribe(context.Background(), func(_ context.Context, evt Event) error {
			got <- evt
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(stop)
		return b, got
	}
	first, gotFirst := newReplica("a")
	_, gotSecond := newReplica("b")
	time.Sleep(time.Second) // consumers declare and bind their queues asynchronously

	if err := first.Publish(context.Background(), Event{Type: TypeInvalidate, Tags: []string{"order:42"}, Version: 1}); err != nil {
		t.Fatal(err)
	}
	for name, ch := range map[string]chan Event{"a": gotFirst, "b": gotSecond} {
		select {
		case evt := <-ch:
			if len(evt.Tags) != 1 || evt.Tags[0] != "order:42" {
				t.Fatalf("replica %s got wrong event %+v", name, evt)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("replica %s did not receive the invalidation", name)
		}
	}
}
