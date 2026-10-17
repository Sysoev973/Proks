// Command upstream is the demo library API behind the proxy. Each endpoint
// exercises a different caching case: entities with long and short TTLs,
// lists and searches depending on many entities, a personal response that
// must not be cached, and a write that invalidates what it changed.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Book struct {
	ID              int64   `json:"id"`
	GenreID         *int64  `json:"genre_id,omitempty"`
	Title           string  `json:"title"`
	ISBN            *string `json:"isbn,omitempty"`
	PublicationYear *int32  `json:"publication_year,omitempty"`
	IsRare          bool    `json:"is_rare"`
	HasEbook        bool    `json:"has_ebook"`
}

type Author struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
}

type Genre struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Loan struct {
	ID        int64      `json:"id"`
	BookID    int64      `json:"book_id"`
	Title     string     `json:"title"`
	IssueDate time.Time  `json:"issue_date"`
	DueDate   time.Time  `json:"due_date"`
	Returned  *time.Time `json:"returned_at,omitempty"`
	Status    string     `json:"status"`
}

const pageSize = 20

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

	inv := newInvalidator(os.Getenv("INVALIDATION_URL"), os.Getenv("INVALIDATION_TOKEN"))
	if inv == nil {
		log.Println("INVALIDATION_URL is not set; PUT /books/{id} will not invalidate the proxy cache")
	}
	mux := newUpstreamHandler(db, inv)

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	log.Println("Upstream listening on", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func newUpstreamHandler(db *pgxpool.Pool, inv *invalidator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, "database not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /books/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		book, err := queryBook(r.Context(), db, id)
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "book not found", http.StatusNotFound)
			return
		}
		if err != nil {
			serverError(w, "query book", err)
			return
		}
		tags := []string{bookTag(book.ID)}
		if book.GenreID != nil {
			tags = append(tags, genreTag(*book.GenreID))
		}
		writeJSON(w, r, cachePolicy{maxAge: 5 * time.Minute, surrogateKeys: tags}, book)
	})

	mux.HandleFunc("GET /books/{id}/authors", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		rows, err := db.Query(r.Context(),
			// language=PostgreSQL
			`SELECT a.author_id, a.full_name
			   FROM book_authors ba JOIN authors a ON a.author_id = ba.author_id
			  WHERE ba.book_id = $1
			  ORDER BY a.author_id`, id)
		if err != nil {
			serverError(w, "query authors", err)
			return
		}
		authors, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Author, error) {
			var a Author
			err := row.Scan(&a.ID, &a.FullName)
			return a, err
		})
		if err != nil {
			serverError(w, "scan authors", err)
			return
		}
		deps := []string{bookTag(id)}
		for _, a := range authors {
			deps = append(deps, "author:"+strconv.FormatInt(a.ID, 10))
		}
		writeJSON(w, r, cachePolicy{maxAge: 5 * time.Minute, dependsOn: deps}, authors)
	})

	mux.HandleFunc("GET /books", func(w http.ResponseWriter, r *http.Request) {
		page, ok := queryInt(w, r, "page", 1)
		if !ok {
			return
		}
		genre, ok := queryInt(w, r, "genre", 0)
		if !ok {
			return
		}
		rows, err := db.Query(r.Context(),
			`SELECT book_id, genre_id, title, isbn, publication_year, is_rare, has_ebook
			   FROM books
			  WHERE $1 = 0 OR genre_id = $1
			  ORDER BY book_id
			  LIMIT $2 OFFSET $3`, genre, pageSize, (page-1)*pageSize)
		if err != nil {
			serverError(w, "query books", err)
			return
		}
		books, err := pgx.CollectRows(rows, scanBook)
		if err != nil {
			serverError(w, "scan books", err)
			return
		}
		tags := []string{"list:books"}
		if genre != 0 {
			tags = append(tags, genreTag(genre))
		}
		writeJSON(w, r, cachePolicy{maxAge: time.Minute, surrogateKeys: append(tags, bookTags(books)...)}, books)
	})

	mux.HandleFunc("GET /genres", func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query(r.Context(),
			`SELECT genre_id, name FROM genres ORDER BY genre_id`)
		if err != nil {
			serverError(w, "query genres", err)
			return
		}
		genres, err := pgx.CollectRows(rows, scanGenre)
		if err != nil {
			serverError(w, "scan genres", err)
			return
		}
		tags := []string{"list:genres"}
		for _, g := range genres {
			tags = append(tags, genreTag(g.ID))
		}
		writeJSON(w, r, cachePolicy{maxAge: time.Hour, surrogateKeys: tags}, genres)
	})

	mux.HandleFunc("GET /genres/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		rows, err := db.Query(r.Context(),
			// language=PostgreSQL
			`SELECT genre_id, name FROM genres WHERE genre_id = $1`, id)
		if err != nil {
			serverError(w, "query genre", err)
			return
		}
		genre, err := pgx.CollectExactlyOneRow(rows, scanGenre)
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "genre not found", http.StatusNotFound)
			return
		}
		if err != nil {
			serverError(w, "scan genre", err)
			return
		}
		writeJSON(w, r, cachePolicy{maxAge: time.Hour, surrogateKeys: []string{genreTag(id)}}, genre)
	})

	mux.HandleFunc("GET /search", func(w http.ResponseWriter, r *http.Request) {
		q := strings.TrimSpace(r.URL.Query().Get("q"))
		if q == "" {
			http.Error(w, "q is required", http.StatusBadRequest)
			return
		}
		rows, err := db.Query(r.Context(),
			// language=PostgreSQL
			`SELECT book_id, genre_id, title, isbn, publication_year, is_rare, has_ebook
			   FROM books
			  WHERE title ILIKE '%' || $1 || '%'
			  ORDER BY book_id
			  LIMIT $2`, q, pageSize)
		if err != nil {
			serverError(w, "search books", err)
			return
		}
		books, err := pgx.CollectRows(rows, scanBook)
		if err != nil {
			serverError(w, "scan search", err)
			return
		}
		tags := append([]string{"list:books"}, bookTags(books)...)
		writeJSON(w, r, cachePolicy{maxAge: 30 * time.Second, surrogateKeys: tags}, books)
	})

	mux.HandleFunc("GET /readers/{id}/loans", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		rows, err := db.Query(r.Context(),
			// language=PostgreSQL
			`SELECT l.loan_id, b.book_id, b.title, l.issue_date, l.due_date, l.actual_return_date, l.status
			   FROM loans l
			   JOIN book_items i ON i.item_id = l.item_id
			   JOIN books b ON b.book_id = i.book_id
			  WHERE l.reader_id = $1
			  ORDER BY l.issue_date DESC`, id)
		if err != nil {
			serverError(w, "query loans", err)
			return
		}
		loans, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Loan, error) {
			var l Loan
			err := row.Scan(&l.ID, &l.BookID, &l.Title, &l.IssueDate, &l.DueDate, &l.Returned, &l.Status)
			return l, err
		})
		if err != nil {
			serverError(w, "scan loans", err)
			return
		}
		// Personal data: the proxy must pass it through (BYPASS).
		writeJSON(w, r, cachePolicy{private: true}, loans)
	})

	mux.HandleFunc("PUT /books/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		var patch bookPatch
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&patch); err != nil {
			http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := patch.validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var oldGenre *int64
		var book Book
		err := pgx.BeginFunc(r.Context(), db, func(tx pgx.Tx) error {
			if err := tx.QueryRow(r.Context(), `SELECT genre_id FROM books WHERE book_id = $1 FOR UPDATE`, id).Scan(&oldGenre); err != nil {
				return err
			}
			rows, err := tx.Query(r.Context(),
				// language=PostgreSQL
				`UPDATE books SET
				        title            = COALESCE($2, title),
				        isbn             = COALESCE($3, isbn),
				        publication_year = COALESCE($4, publication_year),
				        is_rare          = COALESCE($5, is_rare),
				        has_ebook        = COALESCE($6, has_ebook),
				        genre_id         = COALESCE($7, genre_id)
				  WHERE book_id = $1
				RETURNING book_id, genre_id, title, isbn, publication_year, is_rare, has_ebook`,
				id, patch.Title, patch.ISBN, patch.PublicationYear, patch.IsRare, patch.HasEbook, patch.GenreID)
			if err != nil {
				return err
			}
			book, err = pgx.CollectExactlyOneRow(rows, scanBook)
			return err
		})
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "book not found", http.StatusNotFound)
			return
		}
		if err != nil {
			serverError(w, "update book", err)
			return
		}

		tags := []string{bookTag(id)}
		if !sameGenre(oldGenre, book.GenreID) {
			tags = append(tags, "list:books")
		}
		invalidated := inv.purge(r.Context(), tags)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Book        Book     `json:"book"`
			Invalidated []string `json:"invalidated"`
		}{book, invalidated})
	})
	return mux
}

type bookPatch struct {
	Title           *string `json:"title"`
	ISBN            *string `json:"isbn"`
	PublicationYear *int32  `json:"publication_year"`
	IsRare          *bool   `json:"is_rare"`
	HasEbook        *bool   `json:"has_ebook"`
	GenreID         *int64  `json:"genre_id"`
}

func (p bookPatch) validate() error {
	if p.Title != nil && (strings.TrimSpace(*p.Title) == "" || len(*p.Title) > 255) {
		return errors.New("title must be 1..255 characters")
	}
	if p.ISBN != nil && len(*p.ISBN) > 20 {
		return errors.New("isbn must be at most 20 characters")
	}
	if p.Title == nil && p.ISBN == nil && p.PublicationYear == nil && p.IsRare == nil && p.HasEbook == nil && p.GenreID == nil {
		return errors.New("nothing to update")
	}
	return nil
}

func sameGenre(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func scanBook(row pgx.CollectableRow) (Book, error) {
	var b Book
	err := row.Scan(&b.ID, &b.GenreID, &b.Title, &b.ISBN, &b.PublicationYear, &b.IsRare, &b.HasEbook)
	return b, err
}

func scanGenre(row pgx.CollectableRow) (Genre, error) {
	var g Genre
	err := row.Scan(&g.ID, &g.Name)
	return g, err
}

func queryBook(ctx context.Context, db *pgxpool.Pool, id int64) (Book, error) {
	rows, err := db.Query(ctx,
		// language=PostgreSQL
		`SELECT book_id, genre_id, title, isbn, publication_year, is_rare, has_ebook
		   FROM books
		  WHERE book_id = $1`, id)
	if err != nil {
		return Book{}, err
	}
	return pgx.CollectExactlyOneRow(rows, scanBook)
}

func bookTag(id int64) string  { return "book:" + strconv.FormatInt(id, 10) }
func genreTag(id int64) string { return "genre:" + strconv.FormatInt(id, 10) }

func bookTags(books []Book) []string {
	tags := make([]string, len(books))
	for i, b := range books {
		tags[i] = bookTag(b.ID)
	}
	return tags
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

func queryInt(w http.ResponseWriter, r *http.Request, name string, fallback int64) (int64, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return fallback, true
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 || (name == "page" && n == 0) || n > 1_000_000 {
		http.Error(w, "invalid "+name, http.StatusBadRequest)
		return 0, false
	}
	return n, true
}

func serverError(w http.ResponseWriter, what string, err error) {
	log.Printf("failed to %s: %v", what, err)
	http.Error(w, "failed to "+what, http.StatusInternalServerError)
}

// cachePolicy is what a response tells the proxy about caching it.
type cachePolicy struct {
	maxAge        time.Duration
	private       bool
	surrogateKeys []string
	dependsOn     []string
}

// writeJSON writes v with Cache-Control, an ETag and the invalidation tags,
// answering 304 when the client already has this version.
func writeJSON(w http.ResponseWriter, r *http.Request, p cachePolicy, v any) {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(v); err != nil {
		serverError(w, "encode response", err)
		return
	}
	sum := sha256.Sum256(body.Bytes())
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`

	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("ETag", etag)
	if p.private {
		h.Set("Cache-Control", "private, no-store")
	} else {
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(p.maxAge.Seconds())))
	}
	if len(p.surrogateKeys) > 0 {
		h.Set("Surrogate-Key", strings.Join(p.surrogateKeys, " "))
	}
	if len(p.dependsOn) > 0 {
		h.Set("X-Cache-Depends-On", strings.Join(p.dependsOn, ", "))
	}
	if match := r.Header.Get("If-None-Match"); match != "" && match == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	_, _ = w.Write(body.Bytes())
}

// invalidator asks the proxy to purge tags after a write
// (POST /admin/purge); the proxy fans the purge out to every replica.
type invalidator struct {
	url    string
	token  string
	client *http.Client
}

func newInvalidator(url, token string) *invalidator {
	if url == "" {
		return nil
	}
	return &invalidator{url: url, token: token, client: &http.Client{Timeout: 2 * time.Second}}
}

func (inv *invalidator) purge(ctx context.Context, tags []string) []string {
	if inv == nil {
		return nil
	}
	body, _ := json.Marshal(map[string][]string{"tags": tags})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, inv.url, bytes.NewReader(body))
	if err != nil {
		log.Printf("invalidation request: %v", err)
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	if inv.token != "" {
		req.Header.Set("Authorization", "Bearer "+inv.token)
	}
	resp, err := inv.client.Do(req)
	if err != nil {
		log.Printf("invalidation failed for %v: %v", tags, err)
		return nil
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		log.Printf("invalidation of %v refused: %s", tags, resp.Status)
		return nil
	}
	return tags
}
