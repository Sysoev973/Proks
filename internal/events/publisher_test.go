package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newFakePublisher(t *testing.T, d *fakeDialer) *RabbitPublisher {
	t.Helper()
	p, err := newRabbitPublisher("amqp://fake", d.dial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestRabbitPublisherNeedsNoReplicaConfiguration(t *testing.T) {
	t.Setenv("PROXY_REPLICA_ID", "")
	t.Setenv("EVENT_LOG_PATH", "")
	t.Setenv("EVENT_VERSION_STATE_PATH", "")
	d := &fakeDialer{}
	p := newFakePublisher(t, d)

	evt := Event{EventID: "outbox-1", Type: TypeInvalidate, Tags: []string{"book:7"}, Version: 3}
	if err := p.Publish(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	ch := d.channel(0)
	if ch.keys[0] != routingKey || ch.published[0].DeliveryMode != amqp.Persistent {
		t.Fatalf("unexpected publish: key=%q mode=%d", ch.keys[0], ch.published[0].DeliveryMode)
	}
	var sent Event
	if err := json.Unmarshal(ch.published[0].Body, &sent); err != nil {
		t.Fatal(err)
	}
	if sent.EventID != "outbox-1" || sent.Version != 3 || len(sent.Tags) != 1 || sent.Tags[0] != "book:7" {
		t.Fatalf("event changed on the way: %+v", sent)
	}
}

func TestRabbitPublisherRejectsInvalidEvents(t *testing.T) {
	d := &fakeDialer{}
	p := newFakePublisher(t, d)
	if err := p.Publish(context.Background(), Event{Type: TypeInvalidate}); err == nil {
		t.Fatal("expected an event without tags or keys to be rejected")
	}
	if d.channel(0).publishCount() != 0 {
		t.Fatal("an invalid event must not reach the broker")
	}
}

func TestRabbitPublisherReopensChannelAfterFailure(t *testing.T) {
	d := &fakeDialer{prepared: []*fakeChannel{newFakeChannel(confirmNack)}}
	p := newFakePublisher(t, d)

	if err := p.Publish(context.Background(), bookEvent); err == nil {
		t.Fatal("expected a nack to fail the publish")
	}
	if !d.channel(0).isClosed() {
		t.Fatal("expected the failed channel to be closed")
	}
	// No retry inside Publish: the outbox keeps the row and calls again.
	if d.count() != 1 {
		t.Fatalf("Publish must not retry by itself, dialed %d times", d.count())
	}
	if err := p.Publish(context.Background(), bookEvent); err != nil {
		t.Fatalf("expected the next publish to succeed on a new channel, got %v", err)
	}
	if d.channel(1).publishCount() != 1 {
		t.Fatal("expected the event on the second channel")
	}
}

func TestRabbitPublisherDoesNotCountLateConfirm(t *testing.T) {
	late := newFakeChannel(confirmNever)
	d := &fakeDialer{prepared: []*fakeChannel{late, newFakeChannel(confirmNack)}}
	p := newFakePublisher(t, d)
	p.confirmTimeout = 20 * time.Millisecond

	if err := p.Publish(context.Background(), bookEvent); !errors.Is(err, errConfirmTimeout) {
		t.Fatalf("expected a confirm timeout, got %v", err)
	}
	late.confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
	if err := p.Publish(context.Background(), bookEvent); err == nil {
		t.Fatal("the late ACK of the first message must not confirm the second one")
	}
}

func TestRabbitPublisherReportsDialFailure(t *testing.T) {
	d := &fakeDialer{}
	p := newFakePublisher(t, d)
	d.channel(0).drop()

	d.mu.Lock()
	d.err = errors.New("broker down")
	d.mu.Unlock()
	if err := p.Publish(context.Background(), bookEvent); err == nil {
		t.Fatal("expected the publish on a dropped channel to fail")
	}
	if err := p.Publish(context.Background(), bookEvent); err == nil {
		t.Fatal("expected the reconnect to fail while the broker is down")
	}

	d.mu.Lock()
	d.err = nil
	d.mu.Unlock()
	if err := p.Publish(context.Background(), bookEvent); err != nil {
		t.Fatalf("expected the publish to recover with the broker, got %v", err)
	}
}

func TestNewRabbitPublisherFailsWithoutBroker(t *testing.T) {
	d := &fakeDialer{err: errors.New("connection refused")}
	if _, err := newRabbitPublisher("amqp://fake", d.dial); err == nil {
		t.Fatal("expected an error when the broker is unreachable")
	}
}
