package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUpstreamAgainstPostgres(t *testing.T) {
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
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	var bookID int64
	if err := db.QueryRow(ctx, "SELECT book_id FROM books ORDER BY book_id LIMIT 1").Scan(&bookID); err != nil {
		t.Fatalf("read a seeded book: %v", err)
	}

	handler := newUpstreamHandler(db)

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("readiness status = %d, want %d", ready.Code, http.StatusOK)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/"+strconv.FormatInt(bookID, 10), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("book status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") == "" {
		t.Fatal("book response did not include Content-Type")
	}
}
