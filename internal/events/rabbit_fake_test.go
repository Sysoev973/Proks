package events

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// confirmMode says how a fake channel answers a publish.
type confirmMode int

const (
	confirmAck confirmMode = iota
	confirmNack
	confirmNever
)

type fakeChannel struct {
	mu         sync.Mutex
	confirm    confirmMode
	closeChans []chan *amqp.Error
	confirms   chan amqp.Confirmation
	cancels    chan string
	deliveries chan amqp.Delivery
	published  []amqp.Publishing
	keys       []string
	queues     map[string]amqp.Table
	bindings   []string
	closed     bool
}

func newFakeChannel(mode confirmMode) *fakeChannel {
	return &fakeChannel{
		confirm:    mode,
		deliveries: make(chan amqp.Delivery, 16),
		queues:     make(map[string]amqp.Table),
	}
}

func (c *fakeChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error {
	return nil
}

func (c *fakeChannel) QueueDeclare(name string, _, _, _, _ bool, args amqp.Table) (amqp.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues[name] = args
	return amqp.Queue{Name: name}, nil
}

func (c *fakeChannel) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindings = append(c.bindings, exchange+"/"+key+"->"+name)
	return nil
}

func (c *fakeChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return c.deliveries, nil
}

func (c *fakeChannel) Confirm(bool) error { return nil }

func (c *fakeChannel) PublishWithContext(_ context.Context, _, key string, _, _ bool, msg amqp.Publishing) error {
	c.mu.Lock()
	c.published = append(c.published, msg)
	c.keys = append(c.keys, key)
	confirms, mode := c.confirms, c.confirm
	tag := uint64(len(c.published))
	c.mu.Unlock()
	switch mode {
	case confirmAck:
		confirms <- amqp.Confirmation{DeliveryTag: tag, Ack: true}
	case confirmNack:
		confirms <- amqp.Confirmation{DeliveryTag: tag, Ack: false}
	}
	return nil
}

func (c *fakeChannel) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeChans = append(c.closeChans, ch)
	return ch
}

func (c *fakeChannel) NotifyCancel(ch chan string) chan string {
	c.cancels = ch
	return ch
}

func (c *fakeChannel) NotifyPublish(ch chan amqp.Confirmation) chan amqp.Confirmation {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirms = ch
	return ch
}

// drop simulates the server closing the channel.
func (c *fakeChannel) drop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.closeChans {
		ch <- &amqp.Error{Code: amqp.ChannelError, Reason: "test: channel dropped"}
	}
}

func (c *fakeChannel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *fakeChannel) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeChannel) publishCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.published)
}

type fakeConn struct{ ch *fakeChannel }

func (c *fakeConn) Channel() (amqpChannel, error) { return c.ch, nil }
func (c *fakeConn) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	return ch
}
func (c *fakeConn) Close() error { return nil }

// fakeDialer hands out the prepared channels in order, then healthy ones.
type fakeDialer struct {
	mu       sync.Mutex
	prepared []*fakeChannel
	dialed   []*fakeChannel
	err      error
}

func (d *fakeDialer) dial(string) (amqpConnection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	ch := newFakeChannel(confirmAck)
	if len(d.prepared) > 0 {
		ch, d.prepared = d.prepared[0], d.prepared[1:]
	}
	d.dialed = append(d.dialed, ch)
	return &fakeConn{ch: ch}, nil
}

func (d *fakeDialer) channel(i int) *fakeChannel {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i >= len(d.dialed) {
		return nil
	}
	return d.dialed[i]
}

func (d *fakeDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.dialed)
}

func setBrokerEnv(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("PROXY_REPLICA_ID", "test")
	t.Setenv("EVENT_LOG_PATH", filepath.Join(dir, "events.log"))
	t.Setenv("EVENT_VERSION_STATE_PATH", filepath.Join(dir, "state.json"))
	t.Setenv("EVENT_QUEUE_TTL", "")
	return dir
}

func newFakeBroker(t *testing.T, d *fakeDialer) *RabbitBroker {
	t.Helper()
	b, err := newRabbitBroker("amqp://fake", d.dial)
	if err != nil {
		t.Fatal(err)
	}
	b.retryBackoff = time.Millisecond
	t.Cleanup(b.Close)
	return b
}

var bookEvent = Event{Type: TypeInvalidate, Tags: []string{"book:1"}, Version: 1}

func TestPublishUsesInvalidateRoutingKey(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	b := newFakeBroker(t, d)
	if err := b.Publish(context.Background(), bookEvent); err != nil {
		t.Fatal(err)
	}
	ch := d.channel(0)
	if ch.keys[0] != routingKey || ch.published[0].DeliveryMode != amqp.Persistent {
		t.Fatalf("unexpected publish: key=%q mode=%d", ch.keys[0], ch.published[0].DeliveryMode)
	}
	var sent Event
	if err := json.Unmarshal(ch.published[0].Body, &sent); err != nil || sent.EventID == "" {
		t.Fatalf("expected an event with a generated ID, got %+v (%v)", sent, err)
	}
	logged, err := b.loadEventLog()
	if err != nil || len(logged) != 1 {
		t.Fatalf("expected the event in the durable log, got %v (%v)", logged, err)
	}
}

func TestPublishReconnectsAfterChannelClose(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	b := newFakeBroker(t, d)
	first := d.channel(0)
	first.drop()

	if err := b.Publish(context.Background(), bookEvent); err != nil {
		t.Fatal(err)
	}
	if first.publishCount() != 0 || !first.isClosed() {
		t.Fatal("expected the dropped channel to be closed and not used")
	}
	if second := d.channel(1); second == nil || second.publishCount() != 1 {
		t.Fatal("expected the event to be published on a new channel")
	}
}

func TestPublishFailsWhenReconnectFails(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	b := newFakeBroker(t, d)
	d.channel(0).drop()
	d.mu.Lock()
	d.err = errors.New("broker down")
	d.mu.Unlock()
	if err := b.Publish(context.Background(), bookEvent); err == nil {
		t.Fatal("expected publish to fail while the broker is unreachable")
	}
}

func TestConfirmTimeoutDiscardsPublisher(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{prepared: []*fakeChannel{newFakeChannel(confirmNever)}}
	b := newFakeBroker(t, d)
	b.confirmTimeout = 20 * time.Millisecond

	if err := b.Publish(context.Background(), bookEvent); err != nil {
		t.Fatalf("expected the retry on a new channel to succeed, got %v", err)
	}
	if !d.channel(0).isClosed() {
		t.Fatal("expected the channel that lost the confirm to be closed")
	}
	if d.channel(1).publishCount() != 1 {
		t.Fatal("expected the event to be republished on a new channel")
	}

	// A caller deadline drops the publisher without retrying.
	d.mu.Lock()
	d.prepared = []*fakeChannel{newFakeChannel(confirmNever)}
	d.mu.Unlock()
	if err := b.reconnectPublisher(); err != nil {
		t.Fatal(err)
	}
	b.confirmTimeout = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Publish(ctx, bookEvent); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	b.mu.RLock()
	discarded := b.publisherCh == nil
	b.mu.RUnlock()
	if !discarded || !d.channel(2).isClosed() {
		t.Fatal("expected the publisher to be discarded after the deadline")
	}
}

func TestLateConfirmIsNotCounted(t *testing.T) {
	setBrokerEnv(t)
	late := newFakeChannel(confirmNever)
	d := &fakeDialer{prepared: []*fakeChannel{late, newFakeChannel(confirmNack), newFakeChannel(confirmNack)}}
	b := newFakeBroker(t, d)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Publish(ctx, bookEvent); err == nil {
		t.Fatal("expected the first publish to time out")
	}
	// The broker acknowledges the first message only now.
	late.confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}

	if err := b.Publish(context.Background(), bookEvent); err == nil {
		t.Fatal("the late ACK of the first message must not confirm the second one")
	}
}

// deliveryRecorder records how each delivery was settled.
type deliveryRecorder struct {
	mu      sync.Mutex
	acks    int
	nacks   int
	requeue bool
	settled chan struct{}
}

func newDeliveryRecorder() *deliveryRecorder {
	return &deliveryRecorder{settled: make(chan struct{}, 16)}
}

func (r *deliveryRecorder) Ack(uint64, bool) error {
	r.mu.Lock()
	r.acks++
	r.mu.Unlock()
	r.settled <- struct{}{}
	return nil
}

func (r *deliveryRecorder) Nack(_ uint64, _ bool, requeue bool) error {
	r.mu.Lock()
	r.nacks++
	r.requeue = r.requeue || requeue
	r.mu.Unlock()
	r.settled <- struct{}{}
	return nil
}

func (r *deliveryRecorder) Reject(tag uint64, requeue bool) error { return r.Nack(tag, false, requeue) }

func (r *deliveryRecorder) wait(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-r.settled:
		case <-time.After(5 * time.Second):
			t.Fatalf("delivery %d was not settled", i+1)
		}
	}
}

func deliver(ch *fakeChannel, rec *deliveryRecorder, body []byte) {
	ch.deliveries <- amqp.Delivery{Acknowledger: rec, DeliveryTag: 1, Body: body}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func waitForDial(t *testing.T, d *fakeDialer, n int) *fakeChannel {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for d.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d dials, got %d", n, d.count())
		}
		time.Sleep(time.Millisecond)
	}
	return d.channel(n - 1)
}

func TestConsumeSendsToDLQAfterRetries(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	b := newFakeBroker(t, d)
	var attempts atomic.Int32
	stop, err := b.Subscribe(context.Background(), func(context.Context, Event) error {
		attempts.Add(1)
		return errors.New("handler failed")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	consumer := waitForDial(t, d, 2)

	rec := newDeliveryRecorder()
	deliver(consumer, rec, mustJSON(t, Event{EventID: "e1", Type: TypeInvalidate, Tags: []string{"book:1"}}))
	rec.wait(t, 1)
	if rec.nacks != 1 || rec.requeue {
		t.Fatalf("expected one Nack without requeue (dead-lettered), got acks=%d nacks=%d requeue=%v", rec.acks, rec.nacks, rec.requeue)
	}
	if got := attempts.Load(); got != handlerAttempts {
		t.Fatalf("expected %d handler attempts, got %d", handlerAttempts, got)
	}
	consumer.mu.Lock()
	args := consumer.queues[b.queueName]
	_, dlq := consumer.queues[b.queueName+"_dlq"]
	bindings := append([]string(nil), consumer.bindings...)
	consumer.mu.Unlock()
	if !dlq || args["x-dead-letter-routing-key"] != b.queueName+"_dlq" {
		t.Fatalf("expected the queue to dead-letter into %s_dlq, args=%v", b.queueName, args)
	}
	if len(bindings) != 2 {
		t.Fatalf("expected bindings for invalidate and book.updated, got %v", bindings)
	}
}

func TestConsumeAppliesDeduplicatesAndRejects(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	b := newFakeBroker(t, d)
	var applied atomic.Int32
	stop, err := b.Subscribe(context.Background(), func(_ context.Context, evt Event) error {
		if evt.Tags[0] != "book:7" {
			t.Errorf("legacy event was not normalized: %+v", evt)
		}
		applied.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	consumer := waitForDial(t, d, 2)

	rec := newDeliveryRecorder()
	legacy := mustJSON(t, Event{EventID: "e1", Type: TypeBookUpdated, BookID: "7", Version: 3})
	deliver(consumer, rec, legacy)
	deliver(consumer, rec, legacy)                                                                            // same event ID
	deliver(consumer, rec, mustJSON(t, Event{EventID: "e2", Type: TypeBookUpdated, BookID: "7", Version: 2})) // older version
	deliver(consumer, rec, []byte("{"))
	deliver(consumer, rec, mustJSON(t, Event{EventID: "e3", Type: "book.created", BookID: "7"}))
	rec.wait(t, 5)

	if applied.Load() != 1 {
		t.Fatalf("expected the event to be applied once, got %d", applied.Load())
	}
	if rec.acks != 3 || rec.nacks != 2 {
		t.Fatalf("expected 3 acks and 2 nacks, got %d/%d", rec.acks, rec.nacks)
	}
	if b.LastVersion("book:7") != 3 {
		t.Fatalf("expected version 3 for book:7, got %d", b.LastVersion("book:7"))
	}
}

func TestConsumerReconnectsAfterChannelClose(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	b := newFakeBroker(t, d)
	got := make(chan Event, 1)
	stop, err := b.Subscribe(context.Background(), func(_ context.Context, evt Event) error {
		got <- evt
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	first := waitForDial(t, d, 2)
	first.drop()
	second := waitForDial(t, d, 3)
	if !first.isClosed() {
		t.Fatal("expected the dropped consumer channel to be closed")
	}
	deliver(second, newDeliveryRecorder(), mustJSON(t, bookEvent))
	select {
	case evt := <-got:
		if evt.Tags[0] != "book:1" {
			t.Fatalf("unexpected event %+v", evt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expected the reconnected consumer to receive the event")
	}
}

func TestSubscribeReplaysPendingEventLog(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	b := newFakeBroker(t, d)
	pending := Event{EventID: "pending", Type: TypeInvalidate, Tags: []string{"order:42"}, Version: 2}
	applied := Event{EventID: "applied", Type: TypeInvalidate, Tags: []string{"order:1"}}
	for _, evt := range []Event{pending, applied} {
		if err := b.appendEvent(evt); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.markProcessed(applied); err != nil {
		t.Fatal(err)
	}

	var replayed []string
	stop, err := b.Subscribe(context.Background(), func(_ context.Context, evt Event) error {
		replayed = append(replayed, evt.EventID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stop()
	if len(replayed) != 1 || replayed[0] != "pending" {
		t.Fatalf("expected only the pending event to be replayed, got %v", replayed)
	}
	if logged, _ := b.loadEventLog(); len(logged) != 0 {
		t.Fatalf("expected the log to be compacted after replay, got %v", logged)
	}
}

func TestSubscribeFailsWhenReplayFails(t *testing.T) {
	setBrokerEnv(t)
	b := newFakeBroker(t, &fakeDialer{})
	if err := b.appendEvent(bookEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe(context.Background(), func(context.Context, Event) error {
		return errors.New("handler failed")
	}); err == nil {
		t.Fatal("expected Subscribe to fail when a logged event cannot be applied")
	}
}

func TestVersionsSurviveRestart(t *testing.T) {
	setBrokerEnv(t)
	d := &fakeDialer{}
	first := newFakeBroker(t, d)
	evt := Event{EventID: "e1", Type: TypeInvalidate, Tags: []string{"book:1", "list:books"}, Version: 5}
	if err := first.markProcessed(evt); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second := newFakeBroker(t, d)
	if second.LastVersion("book:1") != 5 || second.LastVersion("list:books") != 5 {
		t.Fatalf("expected versions to survive a restart, got %v", second.maxVersion)
	}
	if !second.alreadyApplied(Event{EventID: "e1", Type: TypeInvalidate, Tags: []string{"other:1"}}) {
		t.Fatal("expected processed event IDs to survive a restart")
	}
	if !second.alreadyApplied(Event{EventID: "e2", Type: TypeInvalidate, Tags: []string{"book:1"}, Version: 4}) {
		t.Fatal("expected an older version to be recognized as applied")
	}
}

func TestLegacyVersionStateIsMigrated(t *testing.T) {
	dir := setBrokerEnv(t)
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"1":3,"book:2":4}`), 0o600); err != nil {
		t.Fatal(err)
	}
	b := newFakeBroker(t, &fakeDialer{})
	if b.LastVersion("book:1") != 3 || b.LastVersion("book:2") != 4 {
		t.Fatalf("expected legacy book IDs to become tags, got %v", b.maxVersion)
	}

	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newRabbitBroker("amqp://fake", (&fakeDialer{}).dial); err == nil {
		t.Fatal("expected a corrupt state file to be reported")
	}
}

func TestNewRabbitBrokerRequiresConfiguration(t *testing.T) {
	d := &fakeDialer{}
	cases := map[string]func(t *testing.T){
		"replica":   func(t *testing.T) { t.Setenv("PROXY_REPLICA_ID", "") },
		"log":       func(t *testing.T) { t.Setenv("EVENT_LOG_PATH", "") },
		"state":     func(t *testing.T) { t.Setenv("EVENT_VERSION_STATE_PATH", "") },
		"queue ttl": func(t *testing.T) { t.Setenv("EVENT_QUEUE_TTL", "soon") },
		"dial":      func(*testing.T) { d.err = errors.New("refused") },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			setBrokerEnv(t)
			d.err = nil
			breakIt(t)
			if _, err := newRabbitBroker("amqp://fake", d.dial); err == nil {
				t.Fatal("expected an error")
			}
		})
	}

	setBrokerEnv(t)
	t.Setenv("EVENT_QUEUE_TTL", "1m")
	d.err = nil
	b := newFakeBroker(t, d)
	if b.queueTTL != time.Minute {
		t.Fatalf("expected queue TTL 1m, got %s", b.queueTTL)
	}
	stop, err := b.Subscribe(context.Background(), func(context.Context, Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	consumer := waitForDial(t, d, 2)
	deadline := time.Now().Add(5 * time.Second)
	for {
		consumer.mu.Lock()
		args := consumer.queues[b.queueName]
		consumer.mu.Unlock()
		if args != nil {
			if args["x-expires"] != int64(60000) {
				t.Fatalf("expected x-expires=60000, got %v", args["x-expires"])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queue was not declared")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNormalize(t *testing.T) {
	valid := []Event{
		{Type: TypeInvalidate, Tags: []string{"order:42", "list:orders", "search:война"}},
		{Type: TypeInvalidate, Keys: []string{"|||/orders/42|"}},
		{Type: TypeBookUpdated, BookID: "1", Version: 2},
	}
	for _, evt := range valid {
		if _, err := Normalize(evt); err != nil {
			t.Errorf("expected %+v to be valid: %v", evt, err)
		}
	}
	tooMany := make([]string, MaxEventTargets+1)
	for i := range tooMany {
		tooMany[i] = "t:1"
	}
	invalid := []Event{
		{Type: "", Tags: []string{"a:1"}},
		{Type: TypeInvalidate},
		{Type: TypeInvalidate, Tags: []string{"has space"}},
		{Type: TypeInvalidate, Tags: []string{"a,b"}},
		{Type: TypeInvalidate, Tags: []string{string(make([]byte, MaxTagLength+1))}},
		{Type: TypeInvalidate, Keys: []string{""}},
		{Type: TypeInvalidate, Tags: tooMany},
		{Type: TypeInvalidate, Tags: []string{"a:1"}, Version: -1},
		{Type: TypeInvalidate, Tags: []string{"a:1"}, BookID: "1"},
		{Type: TypeBookUpdated},
	}
	for _, evt := range invalid {
		if _, err := Normalize(evt); err == nil {
			t.Errorf("expected %+v to be rejected", evt)
		}
	}
}
