package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BookUpdateEvent struct {
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

// === InMemoryBroker ===

type InMemoryBroker struct {
	handler    Handler
	mu         sync.RWMutex
	maxVersion map[string]int64
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		maxVersion: make(map[string]int64),
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
	b.mu.Lock()
	lastVer := b.maxVersion[evt.BookID]
	if evt.Version <= lastVer {
		b.mu.Unlock()
		return nil
	}
	b.maxVersion[evt.BookID] = evt.Version
	h := b.handler
	b.mu.Unlock()

	if h != nil {
		_ = h(ctx, evt)
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
	conn       *amqp.Connection
	ch         *amqp.Channel
	handler    Handler
	mu         sync.RWMutex
	maxVersion map[string]int64
}

func NewRabbitBroker(amqpURL string) (*RabbitBroker, error) {
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

	return &RabbitBroker{
		conn:       conn,
		ch:         ch,
		maxVersion: make(map[string]int64),
	}, nil
}

func (b *RabbitBroker) Subscribe(ctx context.Context, h Handler) (func(), error) {
	b.mu.Lock()
	b.handler = h
	b.mu.Unlock()

	topic := "book.updated"
	qName := "proxy_" + topic
	dlqName := qName + "_dlq"

	_, err := b.ch.QueueDeclare(dlqName, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": dlqName,
	}

	q, err := b.ch.QueueDeclare(qName, true, false, false, false, args)
	if err != nil {
		return nil, err
	}

	err = b.ch.QueueBind(q.Name, topic, "events", false, nil)
	if err != nil {
		return nil, err
	}

	msgs, err := b.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	go b.consume(cancelCtx, msgs, h)

	return cancel, nil
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

			b.mu.Lock()
			lastVer := b.maxVersion[ev.BookID]
			if ev.Version <= lastVer {
				b.mu.Unlock()
				d.Ack(false)
				continue
			}
			b.mu.Unlock()

			if err := b.processWithRetry(ctx, h, ev, 3); err != nil {
				log.Printf("[events] Failed to process event after retries, sending to DLQ: %v", err)
				d.Nack(false, false)
			} else {
				b.mu.Lock()
				b.maxVersion[ev.BookID] = ev.Version
				b.mu.Unlock()
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

func (b *RabbitBroker) Publish(ctx context.Context, evt BookUpdateEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	return b.ch.PublishWithContext(
		ctx,
		"events",
		"book.updated",
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

func (b *RabbitBroker) Close() {
	if b.ch != nil {
		b.ch.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}
