package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Book struct {
	ID              int64   `json:"id"`
	Title           string  `json:"title"`
	ISBN            *string `json:"isbn,omitempty"`
	PublicationYear *int32  `json:"publication_year,omitempty"`
	IsRare          bool    `json:"is_rare"`
	HasEbook        bool    `json:"has_ebook"`
}

func main() {
	databaseURL := os.Getenv("DB_DSN")
	if databaseURL == "" {
		log.Fatal("DB_DSN is required")
	}

	ctx := context.Background()
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("failed to create database pool: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	mux := newUpstreamHandler(db)

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	log.Println("Upstream listening on", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func newUpstreamHandler(db *pgxpool.Pool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, "database not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/books/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/books/")
		if id == "" {
			http.Error(w, "book id is required", http.StatusBadRequest)
			return
		}
		if _, err := strconv.ParseInt(id, 10, 64); err != nil {
			http.Error(w, "invalid book id", http.StatusBadRequest)
			return
		}

		var book Book
		// noinspection SqlResolve
		err := db.QueryRow(
			r.Context(),
			// language=PostgreSQL
			`SELECT book_id, title, isbn, publication_year, is_rare, has_ebook
	 FROM books
	 WHERE book_id = $1`,
			id,
		).Scan(
			&book.ID,
			&book.Title,
			&book.ISBN,
			&book.PublicationYear,
			&book.IsRare,
			&book.HasEbook,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "book not found", http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("failed to query book %q: %v", id, err)
			http.Error(w, "failed to query book", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(book); err != nil {
			log.Printf("failed to encode book response: %v", err)
		}
	})
	return mux
}
