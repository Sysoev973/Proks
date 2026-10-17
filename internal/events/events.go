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
	"unicode"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// TypeInvalidate drops every cached response carrying one of Tags and the
	// responses stored under Keys.
	TypeInvalidate = "invalidate"
	// TypeBookUpdated is the legacy form {"type":"book.updated","book_id":"1"}.
	// It is accepted everywhere and turned into an invalidate event with the
	// tag "book:<book_id>".
	TypeBookUpdated = "book.updated"
)

const (
	MaxTagLength    = 256
	MaxKeyLength    = 4096
	MaxEventTargets = 1000
)

// Event is an invalidation published to every proxy replica.
type Event struct {
	EventID string   `json:"event_id"`
	Type    string   `json:"type"`
	Tags    []string `json:"tags,omitempty"`
	Keys    []string `json:"keys,omitempty"`
	// BookID is only read from legacy book.updated events.
	BookID  string `json:"book_id,omitempty"`
	Version int64  `json:"version"`
}

type Handler func(ctx context.Context, event Event) error

type Stream interface {
	Subscribe(ctx context.Context, h Handler) (func(), error)
	Publish(ctx context.Context, evt Event) error
}

// ValidTag reports whether tag can be used for invalidation: non-empty, at
// most MaxTagLength bytes, letters, digits and ":_-./@+=" only. Any prefix
// is accepted.
func ValidTag(tag string) bool {
	if tag == "" || len(tag) > MaxTagLength {
		return false
	}
	for _, r := range tag {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune(":_-./@+=", r) {
			continue
		}
		return false
	}
	return true
}

// Normalize validates evt and rewrites a legacy book.updated event into an
// invalidate event.
func Normalize(evt Event) (Event, error) {
	switch evt.Type {
	case TypeBookUpdated:
		if evt.BookID == "" {
			return evt, fmt.Errorf("book.updated: book_id is required")
		}
		evt.Type = TypeInvalidate
		evt.Tags = append(evt.Tags, "book:"+evt.BookID)
		evt.BookID = ""
	case TypeInvalidate:
		if evt.BookID != "" {
			return evt, fmt.Errorf("invalidate: book_id is only allowed with type %q", TypeBookUpdated)
		}
	default:
		return evt, fmt.Errorf("invalid event type %q", evt.Type)
	}
	if evt.Version < 0 {
		return evt, fmt.Errorf("%s: version must be non-negative", evt.Type)
	}
	if len(evt.Tags)+len(evt.Keys) == 0 {
		return evt, fmt.Errorf("%s: tags or keys are required", evt.Type)
	}
	if len(evt.Tags)+len(evt.Keys) > MaxEventTargets {
		return evt, fmt.Errorf("%s: at most %d tags and keys per event", evt.Type, MaxEventTargets)
	}
	for _, tag := range evt.Tags {
		if !ValidTag(tag) {
			return evt, fmt.Errorf("%s: invalid tag %q", evt.Type, tag)
		}
	}
	for _, key := range evt.Keys {
		if key == "" || len(key) > MaxKeyLength {
			return evt, fmt.Errorf("%s: invalid key", evt.Type)
		}
	}
	return evt, nil
}

func ensureEventID(evt *Event) {
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

// supersededLocked reports whether every tag of a versioned event has
// already been invalidated at this or a newer version. Unversioned events and
// events with keys are never superseded: keys carry no version, so dropping
// the event would lose the key purge.
func supersededLocked(maxVersion map[string]int64, evt Event) bool {
	if evt.Version <= 0 || len(evt.Tags) == 0 || len(evt.Keys) > 0 {
		return false
	}
	for _, tag := range evt.Tags {
		if evt.Version > maxVersion[tag] {
			return false
		}
	}
	return true
}

func commitVersionLocked(maxVersion map[string]int64, evt Event) {
	if evt.Version <= 0 {
		return
	}
	for _, tag := range evt.Tags {
		if evt.Version > maxVersion[tag] {
			maxVersion[tag] = evt.Version
		}
	}
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

func (b *InMemoryBroker) Publish(ctx context.Context, evt Event) error {
	ensureEventID(&evt)
	evt, err := Normalize(evt)
	if err != nil {
		return err
	}
	b.mu.Lock()
	if _, ok := b.processed[evt.EventID]; ok {
		b.mu.Unlock()
		return nil
	}
	if supersededLocked(b.maxVersion, evt) {
		b.mu.Unlock()
		return nil
	}
	h := b.handler
	b.mu.Unlock()

	if h != nil {
		if err := h(ctx, evt); err != nil {
			return err
		}
	}
	b.mu.Lock()
	b.processed[evt.EventID] = struct{}{}
	commitVersionLocked(b.maxVersion, evt)
	b.mu.Unlock()
	return nil
}

// LastVersion returns the newest version invalidated for tag.
func (b *InMemoryBroker) LastVersion(tag string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maxVersion[tag]
}

// === AMQP seams ===
//
// RabbitBroker talks to RabbitMQ only through these interfaces, so tests can
// drop connections, delay or lose publisher confirms and fail handlers
// without a broker.

type amqpChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Confirm(noWait bool) error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyCancel(c chan string) chan string
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Close() error
}

type amqpConnection interface {
	Channel() (amqpChannel, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	Close() error
}

type dialFunc func(url string) (amqpConnection, error)

type amqpConn struct{ *amqp.Connection }

func (c amqpConn) Channel() (amqpChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func dialAMQP(url string) (amqpConnection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return amqpConn{conn}, nil
}

// === RabbitBroker ===

const (
	exchangeName = "events"
	// routingKey is what Publish uses; consumers also bind the legacy key so
	// events from older publishers keep arriving during a rolling upgrade.
	routingKey       = TypeInvalidate
	legacyRoutingKey = TypeBookUpdated

	defaultConfirmTimeout = 5 * time.Second
	defaultRetryBackoff   = 100 * time.Millisecond
	handlerAttempts       = 3
)

var errConfirmTimeout = errors.New("rabbitmq publisher confirm timed out")

type RabbitBroker struct {
	dial                   dialFunc
	conn                   amqpConnection
	ch                     amqpChannel
	publisherConn          amqpConnection
	publisherCh            amqpChannel
	publisherClosed        <-chan *amqp.Error
	publisherChannelClosed <-chan *amqp.Error
	publisherConfirms      <-chan amqp.Confirmation
	publishMu              sync.Mutex
	amqpURL                string
	queueName              string
	statePath              string
	eventLogPath           string
	queueTTL               time.Duration
	confirmTimeout         time.Duration
	retryBackoff           time.Duration
	handler                Handler
	mu                     sync.RWMutex
	maxVersion             map[string]int64
	processed              map[string]struct{}
	processedOrder         []string
	logMu                  sync.Mutex
	appendedSinceCompact   int
}

const maxProcessedEvents = 10_000

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
		b.maxVersion = migrateVersionKeys(state.Versions)
		for _, eventID := range state.Processed {
			b.rememberLocked(eventID)
		}
		return nil
	}
	var versions map[string]int64
	if err := json.Unmarshal(data, &versions); err != nil {
		return fmt.Errorf("decode version state: %w", err)
	}
	b.maxVersion = migrateVersionKeys(versions)
	return nil
}

func migrateVersionKeys(versions map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(versions))
	for key, version := range versions {
		if !strings.Contains(key, ":") {
			key = "book:" + key
		}
		if version > out[key] {
			out[key] = version
		}
	}
	return out
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

func (b *RabbitBroker) loadEventLog() ([]Event, error) {
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
	var result []Event
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var evt Event
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			return nil, fmt.Errorf("decode event log: %w", err)
		}
		evt, err := Normalize(evt)
		if err != nil {
			return nil, fmt.Errorf("invalid event log record: %w", err)
		}
		result = append(result, evt)
	}
	return result, nil
}

// eventLogCompactEvery is how many appended events trigger a compaction.
const eventLogCompactEvery = 1000

// alreadyApplied reports whether replaying evt would be a no-op.
func (b *RabbitBroker) alreadyApplied(evt Event) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.processed[evt.EventID]; ok {
		return true
	}
	return supersededLocked(b.maxVersion, evt)
}

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

func (b *RabbitBroker) appendEvent(evt Event) error {
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

func (b *RabbitBroker) markProcessed(evt Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rememberLocked(evt.EventID)
	commitVersionLocked(b.maxVersion, evt)
	return b.persistVersionsLocked()
}

// LastVersion returns the newest version invalidated for tag.
func (b *RabbitBroker) LastVersion(tag string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maxVersion[tag]
}

type publisher struct {
	conn          amqpConnection
	ch            amqpChannel
	closed        <-chan *amqp.Error
	channelClosed <-chan *amqp.Error
	confirms      <-chan amqp.Confirmation
}

func openPublisher(dial dialFunc, amqpURL string) (publisher, error) {
	conn, err := dial(amqpURL)
	if err != nil {
		return publisher{}, fmt.Errorf("publisher amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return publisher{}, fmt.Errorf("publisher amqp channel: %w", err)
	}
	if err := ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return publisher{}, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return publisher{}, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return publisher{
		conn:          conn,
		ch:            ch,
		closed:        conn.NotifyClose(make(chan *amqp.Error, 1)),
		channelClosed: ch.NotifyClose(make(chan *amqp.Error, 1)),
		confirms:      ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}, nil
}

func NewRabbitBroker(amqpURL string) (*RabbitBroker, error) {
	return newRabbitBroker(amqpURL, dialAMQP)
}

func newRabbitBroker(amqpURL string, dial dialFunc) (*RabbitBroker, error) {
	// The queue keeps its historical name so a restarted replica picks up
	// the durable queue (and any messages) it had before invalidate events.
	queueName := consumerQueueName(legacyRoutingKey)
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
	pub, err := openPublisher(dial, amqpURL)
	if err != nil {
		return nil, err
	}

	broker := &RabbitBroker{
		dial:                   dial,
		publisherConn:          pub.conn,
		publisherCh:            pub.ch,
		publisherClosed:        pub.closed,
		publisherChannelClosed: pub.channelClosed,
		publisherConfirms:      pub.confirms,
		amqpURL:                amqpURL,
		queueName:              queueName,
		queueTTL:               queueTTL,
		statePath:              statePath,
		eventLogPath:           eventLogPath,
		confirmTimeout:         defaultConfirmTimeout,
		retryBackoff:           defaultRetryBackoff,
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
		if b.alreadyApplied(evt) {
			continue
		}
		if err := b.processWithRetry(cancelCtx, h, evt, handlerAttempts); err != nil {
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
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.consumeWithReconnect(cancelCtx, h)
	}()

	return func() {
		cancel()
		<-done
	}, nil
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
		b.mu.Lock()
		b.conn, b.ch = nil, nil
		b.mu.Unlock()
		_ = ch.Close()
		_ = conn.Close()
	}
}

func (b *RabbitBroker) openConsumer() (amqpConnection, amqpChannel, <-chan amqp.Delivery, <-chan *amqp.Error, <-chan string, error) {
	conn, err := b.dial(b.amqpURL)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	fail := func(err error) (amqpConnection, amqpChannel, <-chan amqp.Delivery, <-chan *amqp.Error, <-chan string, error) {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, nil, nil, err
	}
	if err := ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil); err != nil {
		return fail(err)
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
		return fail(err)
	}
	q, err := ch.QueueDeclare(b.queueName, true, false, false, false, args)
	if err != nil {
		return fail(err)
	}
	for _, key := range []string{routingKey, legacyRoutingKey} {
		if err := ch.QueueBind(q.Name, key, exchangeName, false, nil); err != nil {
			return fail(err)
		}
	}
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fail(err)
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
			var ev Event
			if err := json.Unmarshal(d.Body, &ev); err != nil {
				_ = d.Nack(false, false)
				continue
			}
			ev, err := Normalize(ev)
			if err != nil {
				_ = d.Nack(false, false)
				continue
			}
			if b.alreadyApplied(ev) {
				_ = d.Ack(false)
				continue
			}
			if err := b.processWithRetry(ctx, h, ev, handlerAttempts); err != nil {
				log.Printf("[events] failed to process event %s after %d attempts, sending to DLQ: %v", ev.EventID, handlerAttempts, err)
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

func (b *RabbitBroker) processWithRetry(ctx context.Context, h Handler, ev Event, maxAttempts int) error {
	var err error
	backoff := b.retryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}

	for i := 0; i < maxAttempts; i++ {
		if err = h(ctx, ev); err == nil {
			return nil
		}
		if i == maxAttempts-1 {
			break
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

func (b *RabbitBroker) Publish(ctx context.Context, evt Event) error {
	ensureEventID(&evt)
	evt, err := Normalize(evt)
	if err != nil {
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
		// Also after a confirm timeout: the old channel is closed, so a late
		// confirm for this message can never be taken for the retry's.
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
	timeout := b.confirmTimeout
	b.mu.RUnlock()
	if ch == nil || closed == nil || channelClosed == nil || confirms == nil {
		return errors.New("rabbitmq publisher channel unavailable")
	}
	if timeout <= 0 {
		timeout = defaultConfirmTimeout
	}
	select {
	case <-closed:
		return errors.New("rabbitmq publisher connection is closed")
	case <-channelClosed:
		return errors.New("rabbitmq publisher channel is closed")
	default:
	}
	if err := ch.PublishWithContext(ctx, exchangeName, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
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
	case <-timer.C:
		return errConfirmTimeout
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
	pub, err := openPublisher(b.dial, amqpURL)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.publisherConn = pub.conn
	b.publisherCh = pub.ch
	b.publisherClosed = pub.closed
	b.publisherChannelClosed = pub.channelClosed
	b.publisherConfirms = pub.confirms
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
	b.discardPublisher()
	b.mu.Lock()
	ch, conn := b.ch, b.conn
	b.mu.Unlock()
	if ch != nil {
		_ = ch.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
}
