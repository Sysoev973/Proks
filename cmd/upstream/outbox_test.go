package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"proks/internal/events"
	"proks/migrations"

	"github.com/jackc/pgx/v5/pgxpool"
)

type fakePublisher struct {
	mu        sync.Mutex
	failNext  int
	published []events.Event
}

func (f *fakePublisher) Publish(_ context.Context, evt events.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return errors.New("broker unavailable")
	}
	f.published = append(f.published, evt)
	return nil
}

func (f *fakePublisher) eventsWithTag(tag string) []events.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []events.Event
	for _, evt := range f.published {
		for _, t := range evt.Tags {
			if t == tag {
				out = append(out, evt)
				break
			}
		}
	}
	return out
}

func TestPutRejectsInvalidInputWithoutTouchingDatabase(t *testing.T) {
	notified := 0
	handler := newUpstreamHandler(nil, func() { notified++ })
	cases := []struct {
		name string
		path string
		body string
	}{
		{name: "non-numeric id", path: "/books/abc", body: `{"title":"x"}`},
		{name: "malformed json", path: "/books/1", body: `{"title":`},
		{name: "unknown field", path: "/books/1", body: `{"title":"x","version":99}`},
		{name: "empty title", path: "/books/1", body: `{"title":"   "}`},
		{name: "nothing to update", path: "/books/1", body: `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, tc.path, strings.NewReader(tc.body)))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
	if notified != 0 {
		t.Fatalf("a rejected PUT must not wake the relay, notified %d times", notified)
	}
}

func openTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DB_DSN")
	}
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL or DB_DSN to run the PostgreSQL integration test")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, migrations.BookVersionsOutbox); err != nil {
		t.Fatalf("apply outbox migration: %v", err)
	}
	return db
}

func TestRelayRetriesUntilConfirmedAndKeepsOrder(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()

	// The relay drains the whole outbox, so the test starts from an empty one
	// and only works with rows it wrote itself.
	if _, err := db.Exec(ctx, "DELETE FROM invalidation_outbox"); err != nil {
		t.Fatal(err)
	}
	for v := int64(1); v <= 3; v++ {
		if _, err := db.Exec(ctx, "INSERT INTO invalidation_outbox (tags, version) VALUES ($1, $2)", []string{"book:424242"}, v); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(ctx, "INSERT INTO invalidation_outbox (tags, version) VALUES ($1, 0)", []string{"list:books"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(ctx, "DELETE FROM invalidation_outbox") })

	pub := &fakePublisher{failNext: 1}
	relay := newOutboxRelay(db, func() (eventPublisher, error) { return pub, nil }, time.Hour, nil)

	if _, err := relay.relayBatch(ctx); err == nil {
		t.Fatal("expected the first relay attempt to report the publish failure")
	}
	var attempts, left int
	if err := db.QueryRow(ctx, "SELECT attempts FROM invalidation_outbox ORDER BY id LIMIT 1").Scan(&attempts); err != nil {
		t.Fatalf("the row must survive a failed publish: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if err := db.QueryRow(ctx, "SELECT count(*) FROM invalidation_outbox").Scan(&left); err != nil || left != 4 {
		t.Fatalf("a failed first row must stop the batch, %d rows left (%v)", left, err)
	}

	if err := relay.drain(ctx); err != nil {
		t.Fatalf("drain after recovery: %v", err)
	}
	published := pub.eventsWithTag("book:424242")
	if len(published) != 3 {
		t.Fatalf("published %d book events, want 3", len(published))
	}
	for i, evt := range published {
		if evt.Type != events.TypeInvalidate || evt.Version != int64(i+1) || evt.EventID == "" {
			t.Fatalf("event %d = %+v, want invalidate v%d with an event_id", i, evt, i+1)
		}
	}
	if lists := pub.eventsWithTag("list:books"); len(lists) != 1 || lists[0].Version != 0 {
		t.Fatalf("list purge = %+v, want one unversioned event", lists)
	}
	if err := db.QueryRow(ctx, "SELECT count(*) FROM invalidation_outbox").Scan(&left); err != nil || left != 0 {
		t.Fatalf("%d outbox rows left after confirmed publish, want 0 (%v)", left, err)
	}
}

func TestRelayReportsConnectFailure(t *testing.T) {
	db := openTestDatabase(t)
	relay := newOutboxRelay(db, func() (eventPublisher, error) { return nil, errors.New("broker down") }, time.Hour, nil)
	if _, err := relay.relayBatch(context.Background()); err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("expected the connect error, got %v", err)
	}
}

func TestGenreChangeQueuesUnversionedListPurge(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()

	var bookID, genreID, otherGenre int64
	if err := db.QueryRow(ctx, "SELECT book_id, genre_id FROM books WHERE genre_id IS NOT NULL ORDER BY book_id LIMIT 1").Scan(&bookID, &genreID); err != nil {
		t.Fatalf("read a book with a genre: %v", err)
	}
	if err := db.QueryRow(ctx, "SELECT genre_id FROM genres WHERE genre_id <> $1 ORDER BY genre_id LIMIT 1", genreID).Scan(&otherGenre); err != nil {
		t.Fatalf("read another genre: %v", err)
	}
	var lastOutbox int64
	if err := db.QueryRow(ctx, "SELECT COALESCE(MAX(id), 0) FROM invalidation_outbox").Scan(&lastOutbox); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(ctx, "UPDATE books SET genre_id = $2 WHERE book_id = $1", bookID, genreID)
		_, _ = db.Exec(ctx, "DELETE FROM invalidation_outbox WHERE id > $1", lastOutbox)
	})

	rec := httptest.NewRecorder()
	path := "/books/" + strconv.FormatInt(bookID, 10)
	body := `{"genre_id":` + strconv.FormatInt(otherGenre, 10) + `}`
	newUpstreamHandler(db, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodPut, path, strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d; body=%s", rec.Code, rec.Body.String())
	}

	rows, err := db.Query(ctx, "SELECT tags, version FROM invalidation_outbox WHERE id > $1 ORDER BY id", lastOutbox)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var tags []string
		var version int64
		if err := rows.Scan(&tags, &version); err != nil {
			t.Fatal(err)
		}
		got = append(got, strings.Join(tags, ",")+"@"+strconv.FormatBool(version > 0))
	}
	want := "book:" + strconv.FormatInt(bookID, 10) + "@true list:books@false"
	if strings.Join(got, " ") != want {
		t.Fatalf("outbox rows = %v, want %s", got, want)
	}
}
