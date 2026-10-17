package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"proks/internal/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	outboxBatchSize      = 100
	outboxPublishTimeout = 5 * time.Second
	outboxMaxBackoff     = 30 * time.Second
)

type eventPublisher interface {
	Publish(ctx context.Context, evt events.Event) error
}

// outboxRelay moves rows from invalidation_outbox to RabbitMQ as invalidate
// events. A row is deleted
// only after its publish is confirmed, so a crash between commit and publish
// delays the invalidation instead of losing it. Delivery is at-least-once:
// a crash after the confirm but before the delete republishes the same
// event_id, which consumers drop as a duplicate.
type outboxRelay struct {
	db       *pgxpool.Pool
	connect  func() (eventPublisher, error)
	pub      eventPublisher
	interval time.Duration
	wake     chan struct{}
	logger   *log.Logger
}

func newOutboxRelay(db *pgxpool.Pool, connect func() (eventPublisher, error), interval time.Duration, logger *log.Logger) *outboxRelay {
	return &outboxRelay{
		db:       db,
		connect:  connect,
		interval: interval,
		wake:     make(chan struct{}, 1),
		logger:   logger,
	}
}

// Notify asks the relay to drain the outbox now instead of at the next poll.
func (r *outboxRelay) Notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *outboxRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	backoff := r.interval
	for {
		if err := r.drain(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Printf("[outbox] relay failed, retrying in %s: %v", backoff, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, outboxMaxBackoff)
			continue
		}
		backoff = r.interval
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-ticker.C:
		}
	}
}

func (r *outboxRelay) drain(ctx context.Context) error {
	for {
		n, err := r.relayBatch(ctx)
		if err != nil {
			return err
		}
		if n < outboxBatchSize {
			return nil
		}
	}
}

type outboxRow struct {
	id      int64
	eventID string
	tags    []string
	version int64
}

// relayBatch publishes up to outboxBatchSize rows in id order and returns how
// many were published. It stops at the first failure so later updates of the
// same tags are not sent ahead of earlier ones. FOR UPDATE SKIP LOCKED lets
// several upstream replicas run relays without publishing the same row twice
// concurrently.
func (r *outboxRelay) relayBatch(ctx context.Context) (int, error) {
	if r.pub == nil {
		pub, err := r.connect()
		if err != nil {
			return 0, fmt.Errorf("connect publisher: %w", err)
		}
		r.pub = pub
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// noinspection SqlResolve
	rows, err := tx.Query(
		ctx,
		// language=PostgreSQL
		`SELECT id, event_id, tags, version
	 FROM invalidation_outbox
	 ORDER BY id
	 LIMIT $1
	 FOR UPDATE SKIP LOCKED`,
		outboxBatchSize,
	)
	if err != nil {
		return 0, err
	}
	batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outboxRow, error) {
		var o outboxRow
		err := row.Scan(&o.id, &o.eventID, &o.tags, &o.version)
		return o, err
	})
	if err != nil {
		return 0, err
	}

	published := make([]int64, 0, len(batch))
	var publishErr error
	for _, row := range batch {
		pubCtx, cancel := context.WithTimeout(ctx, outboxPublishTimeout)
		publishErr = r.pub.Publish(pubCtx, events.Event{
			EventID: row.eventID,
			Type:    events.TypeInvalidate,
			Tags:    row.tags,
			Version: row.version,
		})
		cancel()
		if publishErr != nil {
			// noinspection SqlResolve
			if _, err := tx.Exec(
				ctx,
				// language=PostgreSQL
				`UPDATE invalidation_outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
				row.id, publishErr.Error(),
			); err != nil {
				return 0, err
			}
			break
		}
		published = append(published, row.id)
	}
	if len(published) > 0 {
		// noinspection SqlResolve
		if _, err := tx.Exec(
			ctx,
			// language=PostgreSQL
			`DELETE FROM invalidation_outbox WHERE id = ANY($1)`,
			published,
		); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if publishErr != nil {
		return len(published), fmt.Errorf("publish outbox event: %w", publishErr)
	}
	return len(published), nil
}
