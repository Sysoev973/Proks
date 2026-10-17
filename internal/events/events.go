package events

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BookUpdateEvent struct {
	EventID string `json:"event_id"`
	Type    string `json:"type,omitempty"`
	BookID  string `json:"book_id"`
	Version int64  `json:"version"`
}

type Event = BookUpdateEvent

type Handler func(ctx context.Context, event BookUpdateEvent) error

type Stream interface {
	Subscribe(ctx context.Context, h Handler) (func(), error)
	Publish(ctx context.Context, evt BookUpdateEvent) error
}

func ValidateBookUpdateEvent(evt BookUpdateEvent) error {
	if evt.Type != "book.updated" {
		return fmt.Errorf("book.updated: invalid type %q", evt.Type)
	}
	if evt.BookID == "" {
		return fmt.Errorf("book.updated: book_id is required")
	}
	if evt.Version < 0 {
		return fmt.Errorf("book.updated: version must be non-negative")
	}
	return nil
}

func ensureEventID(evt *BookUpdateEvent) {
	if strings.TrimSpace(evt.EventID) != "" {
		return
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		evt.EventID = fmt.Sprintf("event-%d", time.Now().UnixNano())
		return
	}
	evt.EventID = fmt.Sprintf("%x", buf)
}

// === InMemoryBroker ===

type InMemoryBroker struct {
	handler    Handler
	mu         sync.RWMutex
	maxVersion map[string]int64
	processed  map[string]struct{}
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		maxVersion: make(map[string]int64),
		processed:  make(map[string]struct{}),
	}
}

func (b *InMemoryBroker) Subscribe(_ context.Context, h Handler) (func(), error) {
	b.mu.Lock()
	b.handler = h
	b.mu.Unlock()

	stopFn := func() {
		b.mu.Lock()
		b.handler = nil
		b.mu.Unlock()
	}

	return stopFn, nil
}

func (b *InMemoryBroker) Publish(ctx context.Context, evt BookUpdateEvent) error {
	ensureEventID(&evt)
	if err := ValidateBookUpdateEvent(evt); err != nil {
		return err
	}
	b.mu.Lock()
	if _, ok := b.processed[evt.EventID]; ok {
		b.mu.Unlock()
		return nil
	}
	lastVer := b.maxVersion[evt.BookID]
	if evt.Version > 0 && evt.Version <= lastVer {
		b.mu.Unlock()
		return nil
	}
	h := b.handler
	b.mu.Unlock()

	if h != nil {
		if err := h(ctx, evt); err != nil {
			return err
		}
		b.mu.Lock()
		b.processed[evt.EventID] = struct{}{}
		if evt.Version > 0 && evt.Version > b.maxVersion[evt.BookID] {
			b.maxVersion[evt.BookID] = evt.Version
		}
		b.mu.Unlock()
	} else {
		b.mu.Lock()
		b.processed[evt.EventID] = struct{}{}
		if evt.Version > 0 {
			b.maxVersion[evt.BookID] = evt.Version
		}
		b.mu.Unlock()
	}
	return nil
}

func (b *InMemoryBroker) LastVersion(bookID string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maxVersion[bookID]
}

//RabbitBroker

type RabbitBroker struct {
	conn                   *amqp.Connection
	ch                     *amqp.Channel
	publisherConn          *amqp.Connection
	publisherCh            *amqp.Channel
	publisherClosed        <-chan *amqp.Error
	publisherChannelClosed <-chan *amqp.Error
	publisherConfirms      <-chan amqp.Confirmation
	publishMu              sync.Mutex
	amqpURL                string
	queueName              string
	statePath              string
	eventLogPath           string
	queueTTL               time.Duration
	handler                Handler
	mu                     sync.RWMutex
	maxVersion             map[string]int64
	processed              map[string]struct{}
	processedOrder         []string
	logMu                  sync.Mutex
	appendedSinceCompact   int
}

// maxProcessedEvents bounds the remembered event IDs. Forgetting an old ID is
// safe: versioned events are still ordered by maxVersion, and re-applying an
// unversioned invalidation only drops cache entries again.
const maxProcessedEvents = 10_000

// rememberLocked records eventID as processed, forgetting the oldest IDs
// beyond maxProcessedEvents.
func (b *RabbitBroker) rememberLocked(eventID string) {
	if _, ok := b.processed[eventID]; ok {
		return
	}
	b.processed[eventID] = struct{}{}
	b.processedOrder = append(b.processedOrder, eventID)
	if over := len(b.processedOrder) - maxProcessedEvents; over > 0 {
		for _, old := range b.processedOrder[:over] {
			delete(b.processed, old)
		}
		b.processedOrder = append([]string(nil), b.processedOrder[over:]...)
	}
}

type persistedState struct {
	Versions  map[string]int64 `json:"versions"`
	Processed []string         `json:"processed"`
}

func (b *RabbitBroker) loadVersions() error {
	if b.statePath == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Clean(b.statePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read version state: %w", err)
	}
	var state persistedState
	if json.Unmarshal(data, &state) == nil && state.Versions != nil {
		b.maxVersion = state.Versions
		for _, eventID := range state.Processed {
			b.rememberLocked(eventID)
		}
		return nil
	}
	if err := json.Unmarshal(data, &b.maxVersion); err != nil {
		return fmt.Errorf("decode version state: %w", err)
	}
	return nil
}

func (b *RabbitBroker) persistVersionsLocked() error {
	if b.statePath == "" {
		return nil
	}
	if dir := filepath.Dir(b.statePath); dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	data, err := json.Marshal(persistedState{Versions: b.maxVersion, Processed: b.processedOrder})
	if err != nil {
		return err
	}
	tmp := b.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, b.statePath)
}

func (b *RabbitBroker) loadEventLog() ([]BookUpdateEvent, error) {
	if b.eventLogPath == "" {
		return nil, errors.New("EVENT_LOG_PATH is required")
	}
	data, err := os.ReadFile(filepath.Clean(b.eventLogPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read event log: %w", err)
	}
	var result []BookUpdateEvent
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var evt BookUpdateEvent
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			return nil, fmt.Errorf("decode event log: %w", err)
		}
		if err := ValidateBookUpdateEvent(evt); err != nil {
			return nil, fmt.Errorf("invalid event log record: %w", err)
		}
		result = append(result, evt)
	}
	return result, nil
}

// eventLogCompactEvery is how many appended events trigger a compaction.
const eventLogCompactEvery = 1000

// alreadyApplied reports whether replaying evt would be a no-op.
func (b *RabbitBroker) alreadyApplied(evt BookUpdateEvent) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.processed[evt.EventID]; ok {
		return true
	}
	return evt.Version > 0 && evt.Version <= b.maxVersion[evt.BookID]
}

// compactEventLogLocked rewrites the event log keeping only events that have
// not been applied yet, so the log stays small and startup replays only what
// is still pending. b.logMu must be held.
func (b *RabbitBroker) compactEventLogLocked() error {
	if b.eventLogPath == "" {
		return nil
	}
	logged, err := b.loadEventLog()
	if err != nil {
		return err
	}
	var buf []byte
	for _, evt := range logged {
		if b.alreadyApplied(evt) {
			continue
		}
		data, err := json.Marshal(evt)
		if err != nil {
			return err
		}
		buf = append(append(buf, data...), '\n')
	}
	tmp := b.eventLogPath + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, b.eventLogPath); err != nil {
		return err
	}
	b.appendedSinceCompact = 0
	return nil
}

func (b *RabbitBroker) appendEvent(evt BookUpdateEvent) error {
	if b.eventLogPath == "" {
		return nil
	}
	b.logMu.Lock()
	defer b.logMu.Unlock()
	if dir := filepath.Dir(b.eventLogPath); dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Clean(b.eventLogPath), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	if err == nil {
		err = file.Sync()
	}
	// Closed before compaction: the log cannot be replaced while open on Windows.
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	b.appendedSinceCompact++
	if b.appendedSinceCompact >= eventLogCompactEvery {
		if err := b.compactEventLogLocked(); err != nil {
			log.Printf("[events] event log compaction failed: %v", err)
		}
	}
	return nil
}

func (b *RabbitBroker) markProcessed(evt BookUpdateEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rememberLocked(evt.EventID)
	if evt.Version > 0 && evt.Version > b.maxVersion[evt.BookID] {
		b.maxVersion[evt.BookID] = evt.Version
	}
	return b.persistVersionsLocked()
}

func (b *RabbitBroker) isProcessed(eventID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.processed[eventID]
	return ok
}

func openPublisher(amqpURL string) (*amqp.Connection, *amqp.Channel, <-chan *amqp.Error, <-chan *amqp.Error, <-chan amqp.Confirmation, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("publisher amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("publisher amqp channel: %w", err)
	}
	if err := ch.ExchangeDeclare("events", "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return conn, ch, conn.NotifyClose(make(chan *amqp.Error, 1)), ch.NotifyClose(make(chan *amqp.Error, 1)), ch.NotifyPublish(make(chan amqp.Confirmation, 1)), nil
}

func NewRabbitBroker(amqpURL string) (*RabbitBroker, error) {
	queueName := consumerQueueName("book.updated")
	if queueName == "" {
		return nil, errors.New("PROXY_REPLICA_ID is required")
	}
	eventLogPath := strings.TrimSpace(os.Getenv("EVENT_LOG_PATH"))
	if eventLogPath == "" {
		return nil, errors.New("EVENT_LOG_PATH is required")
	}
	statePath := strings.TrimSpace(os.Getenv("EVENT_VERSION_STATE_PATH"))
	if statePath == "" {
		return nil, errors.New("EVENT_VERSION_STATE_PATH is required")
	}
	var queueTTL time.Duration
	if v := strings.TrimSpace(os.Getenv("EVENT_QUEUE_TTL")); v != "" {
		ttl, err := time.ParseDuration(v)
		if err != nil || ttl <= 0 {
			return nil, fmt.Errorf("EVENT_QUEUE_TTL must be a positive duration, got %q", v)
		}
		queueTTL = ttl
	}
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("amqp channel: %w", err)
	}

	err = ch.ExchangeDeclare("events", "topic", true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}
	publisherConn, publisherCh, publisherClosed, publisherChannelClosed, publisherConfirms, err := openPublisher(amqpURL)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	broker := &RabbitBroker{
		conn:                   conn,
		ch:                     ch,
		publisherConn:          publisherConn,
		publisherCh:            publisherCh,
		publisherClosed:        publisherClosed,
		publisherChannelClosed: publisherChannelClosed,
		publisherConfirms:      publisherConfirms,
		amqpURL:                amqpURL,
		queueName:              queueName,
		queueTTL:               queueTTL,
		statePath:              statePath,
		eventLogPath:           eventLogPath,
		maxVersion:             make(map[string]int64),
		processed:              make(map[string]struct{}),
	}
	if err := broker.loadVersions(); err != nil {
		broker.Close()
		return nil, err
	}
	return broker, nil
}

func (b *RabbitBroker) Subscribe(ctx context.Context, h Handler) (func(), error) {
	b.mu.Lock()
	b.handler = h
	b.mu.Unlock()

	cancelCtx, cancel := context.WithCancel(ctx)
	events, err := b.loadEventLog()
	if err != nil {
		cancel()
		return nil, err
	}
	for _, evt := range events {
		if b.isProcessed(evt.EventID) {
			continue
		}
		b.mu.RLock()
		lastVersion := b.maxVersion[evt.BookID]
		b.mu.RUnlock()
		if evt.Version > 0 && evt.Version <= lastVersion {
			continue
		}
		if err := b.processWithRetry(cancelCtx, h, evt, 3); err != nil {
			cancel()
			return nil, fmt.Errorf("replay event %s: %w", evt.EventID, err)
		}
		if err := b.markProcessed(evt); err != nil {
			cancel()
			return nil, fmt.Errorf("persist replay state: %w", err)
		}
	}
	b.logMu.Lock()
	err = b.compactEventLogLocked()
	b.logMu.Unlock()
	if err != nil {
		log.Printf("[events] event log compaction failed: %v", err)
	}
	go b.consumeWithReconnect(cancelCtx, h)

	return cancel, nil
}

func (b *RabbitBroker) consumeWithReconnect(ctx context.Context, h Handler) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		conn, ch, msgs, closed, canceled, err := b.openConsumer()
		if err != nil {
			log.Printf("[events] consumer reconnect failed: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		b.mu.Lock()
		b.conn, b.ch = conn, ch
		b.mu.Unlock()
		b.consumeUntilClosed(ctx, msgs, closed, canceled, h)
		_ = ch.Close()
		_ = conn.Close()
	}
}

func (b *RabbitBroker) openConsumer() (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, <-chan string, error) {
	conn, err := amqp.Dial(b.amqpURL)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	if err := ch.ExchangeDeclare("events", "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": b.queueName + "_dlq",
	}
	if b.queueTTL > 0 {
		// Lets RabbitMQ delete the queue of a replica that is gone for good.
		// Changing it on an existing queue fails with PRECONDITION_FAILED, so the
		// old queue must be deleted first.
		args["x-expires"] = b.queueTTL.Milliseconds()
	}
	if _, err := ch.QueueDeclare(b.queueName+"_dlq", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	q, err := ch.QueueDeclare(b.queueName, true, false, false, false, args)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	if err := ch.QueueBind(q.Name, "book.updated", "events", false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	return conn, ch, msgs, ch.NotifyClose(make(chan *amqp.Error, 1)), ch.NotifyCancel(make(chan string, 1)), nil
}

func (b *RabbitBroker) consumeUntilClosed(ctx context.Context, msgs <-chan amqp.Delivery, closed <-chan *amqp.Error, canceled <-chan string, h Handler) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-closed:
			return
		case <-canceled:
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			var ev BookUpdateEvent
			if err := json.Unmarshal(d.Body, &ev); err != nil || ValidateBookUpdateEvent(ev) != nil {
				_ = d.Nack(false, false)
				continue
			}
			if b.isProcessed(ev.EventID) {
				_ = d.Ack(false)
				continue
			}
			b.mu.RLock()
			lastVersion := b.maxVersion[ev.BookID]
			b.mu.RUnlock()
			if ev.Version > 0 && ev.Version <= lastVersion {
				_ = d.Ack(false)
				continue
			}
			if err := b.processWithRetry(ctx, h, ev, 3); err != nil {
				log.Printf("[events] failed to process event: %v", err)
				_ = d.Nack(false, false)
				continue
			}
			if err := b.markProcessed(ev); err != nil {
				log.Printf("[events] failed to persist event state: %v", err)
				_ = d.Nack(false, false)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

func (b *RabbitBroker) consume(ctx context.Context, msgs <-chan amqp.Delivery, h Handler) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			var ev BookUpdateEvent
			if err := json.Unmarshal(d.Body, &ev); err != nil {
				d.Nack(false, false)
				continue
			}
			if err := ValidateBookUpdateEvent(ev); err != nil {
				d.Nack(false, false)
				continue
			}
			if b.isProcessed(ev.EventID) {
				d.Ack(false)
				continue
			}

			b.mu.Lock()
			lastVer := b.maxVersion[ev.BookID]
			if ev.Version > 0 && ev.Version <= lastVer {
				b.mu.Unlock()
				d.Ack(false)
				continue
			}
			b.mu.Unlock()

			if err := b.processWithRetry(ctx, h, ev, 3); err != nil {
				log.Printf("[events] Failed to process event after retries, sending to DLQ: %v", err)
				d.Nack(false, false)
			} else {
				if err := b.markProcessed(ev); err != nil {
					log.Printf("[events] failed to persist event state: %v", err)
					d.Nack(false, false)
					continue
				}
				d.Ack(false)
			}
		}
	}
}

func (b *RabbitBroker) processWithRetry(ctx context.Context, h Handler, ev BookUpdateEvent, maxAttempts int) error {
	var err error
	backoff := 100 * time.Millisecond

	for i := 0; i < maxAttempts; i++ {
		if err = h(ctx, ev); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

func consumerQueueName(topic string) string {
	replicaID := os.Getenv("PROXY_REPLICA_ID")
	if strings.TrimSpace(replicaID) == "" {
		return ""
	}
	return "proxy_" + topic + "_" + replicaID
}

func (b *RabbitBroker) Publish(ctx context.Context, evt BookUpdateEvent) error {
	ensureEventID(&evt)
	if err := ValidateBookUpdateEvent(evt); err != nil {
		return err
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if b.queueName == "" {
		return errors.New("PROXY_REPLICA_ID is required")
	}
	if err := b.appendEvent(evt); err != nil {
		return fmt.Errorf("append event log: %w", err)
	}
	b.publishMu.Lock()
	defer b.publishMu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		err := b.publishOnce(ctx, body)
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			b.discardPublisher()
			return ctx.Err()
		}
		if err := b.reconnectPublisher(); err != nil {
			lastErr = err
			break
		}
	}
	return lastErr
}

func (b *RabbitBroker) publishOnce(ctx context.Context, body []byte) error {
	b.mu.RLock()
	ch := b.publisherCh
	closed := b.publisherClosed
	channelClosed := b.publisherChannelClosed
	confirms := b.publisherConfirms
	b.mu.RUnlock()
	if ch == nil || closed == nil || channelClosed == nil || confirms == nil {
		return errors.New("rabbitmq publisher channel unavailable")
	}
	select {
	case <-closed:
		return errors.New("rabbitmq publisher connection is closed")
	case <-channelClosed:
		return errors.New("rabbitmq publisher channel is closed")
	default:
	}
	if err := ch.PublishWithContext(ctx, "events", "book.updated", false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return err
	}
	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return errors.New("rabbitmq publisher confirm rejected")
		}
		return nil
	case <-closed:
		return errors.New("rabbitmq publisher connection closed before confirm")
	case <-channelClosed:
		return errors.New("rabbitmq publisher channel closed before confirm")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *RabbitBroker) reconnectPublisher() error {
	b.mu.RLock()
	amqpURL := b.amqpURL
	oldConn := b.publisherConn
	oldCh := b.publisherCh
	b.mu.RUnlock()
	conn, ch, closed, channelClosed, confirms, err := openPublisher(amqpURL)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.publisherConn = conn
	b.publisherCh = ch
	b.publisherClosed = closed
	b.publisherChannelClosed = channelClosed
	b.publisherConfirms = confirms
	b.mu.Unlock()
	if oldCh != nil {
		_ = oldCh.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}
	return nil
}

func (b *RabbitBroker) discardPublisher() {
	b.mu.Lock()
	oldConn := b.publisherConn
	oldCh := b.publisherCh
	b.publisherConn = nil
	b.publisherCh = nil
	b.publisherClosed = nil
	b.publisherChannelClosed = nil
	b.publisherConfirms = nil
	b.mu.Unlock()
	if oldCh != nil {
		_ = oldCh.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}
}

func (b *RabbitBroker) Close() {
	b.publishMu.Lock()
	defer b.publishMu.Unlock()
	if b.publisherCh != nil {
		_ = b.publisherCh.Close()
	}
	if b.publisherConn != nil {
		_ = b.publisherConn.Close()
	}
	if b.ch != nil {
		_ = b.ch.Close()
	}
	if b.conn != nil {
		_ = b.conn.Close()
	}
}
