package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWriteJSONSetsCacheHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil),
		cachePolicy{maxAge: 300e9, surrogateKeys: []string{"book:1", "genre:2"}, dependsOn: []string{"author:3"}},
		map[string]int{"id": 1})
	h := rec.Header()
	if h.Get("Cache-Control") != "public, max-age=300" || h.Get("Surrogate-Key") != "book:1 genre:2" ||
		h.Get("X-Cache-Depends-On") != "author:3" || h.Get("ETag") == "" {
		t.Fatalf("unexpected headers: %v", h)
	}

	req := httptest.NewRequest(http.MethodGet, "/books/1", nil)
	req.Header.Set("If-None-Match", h.Get("ETag"))
	rec = httptest.NewRecorder()
	writeJSON(rec, req, cachePolicy{maxAge: 300e9}, map[string]int{"id": 1})
	if rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
		t.Fatalf("expected 304 for a matching ETag, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	writeJSON(rec, httptest.NewRequest(http.MethodGet, "/readers/1/loans", nil), cachePolicy{private: true}, []int{})
	if rec.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("personal responses must be private, got %q", rec.Header().Get("Cache-Control"))
	}
}

func TestInvalidatorPostsTagsToProxy(t *testing.T) {
	var mu sync.Mutex
	var got map[string][]string
	var auth string
	status := http.StatusAccepted
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		auth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(status)
	}))
	defer proxy.Close()

	inv := newInvalidator(proxy.URL+"/admin/purge", "secret")
	if tags := inv.purge(context.Background(), []string{"book:1", "list:books"}); len(tags) != 2 {
		t.Fatalf("expected both tags to be accepted, got %v", tags)
	}
	if auth != "Bearer secret" || strings.Join(got["tags"], ",") != "book:1,list:books" {
		t.Fatalf("unexpected purge request: auth=%q body=%v", auth, got)
	}

	status = http.StatusUnauthorized
	if tags := inv.purge(context.Background(), []string{"book:1"}); tags != nil {
		t.Fatalf("a refused purge must be reported, got %v", tags)
	}
	proxy.Close()
	if tags := inv.purge(context.Background(), []string{"book:1"}); tags != nil {
		t.Fatalf("an unreachable proxy must be reported, got %v", tags)
	}
	if newInvalidator("", "") != nil || (*invalidator)(nil).purge(context.Background(), []string{"a:1"}) != nil {
		t.Fatal("expected no invalidator without a URL")
	}
}

func TestRequestValidation(t *testing.T) {
	title, empty, long := "Новое", " ", strings.Repeat("x", 256)
	for _, tc := range []struct {
		patch bookPatch
		ok    bool
	}{
		{bookPatch{Title: &title}, true},
		{bookPatch{}, false},
		{bookPatch{Title: &empty}, false},
		{bookPatch{Title: &long}, false},
		{bookPatch{ISBN: &long}, false},
	} {
		if err := tc.patch.validate(); (err == nil) != tc.ok {
			t.Errorf("%+v: got %v", tc.patch, err)
		}
	}

	for target, want := range map[string]int{
		"/?page=2":  http.StatusOK,
		"/":         http.StatusOK,
		"/?page=0":  http.StatusBadRequest,
		"/?page=-1": http.StatusBadRequest,
		"/?page=x":  http.StatusBadRequest,
	} {
		rec := httptest.NewRecorder()
		if _, ok := queryInt(rec, httptest.NewRequest(http.MethodGet, target, nil), "page", 1); ok {
			rec.WriteHeader(http.StatusOK)
		}
		if rec.Code != want {
			t.Errorf("%s: got %d, want %d", target, rec.Code, want)
		}
	}

	one, two := int64(1), int64(2)
	if !sameGenre(nil, nil) || sameGenre(&one, nil) || sameGenre(&one, &two) || !sameGenre(&one, &one) {
		t.Fatal("sameGenre is wrong")
	}
}

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
	id := strconv.FormatInt(bookID, 10)

	var purged []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Tags []string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		purged = body.Tags
		w.WriteHeader(http.StatusAccepted)
	}))
	defer proxy.Close()
	handler := newUpstreamHandler(db, newInvalidator(proxy.URL, ""))

	do := func(method, target, body string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(body)))
		return rec
	}

	if rec := do(http.MethodGet, "/health/ready", ""); rec.Code != http.StatusOK {
		t.Fatalf("readiness status = %d", rec.Code)
	}
	cases := []struct {
		target       string
		status       int
		cacheControl string
		tag          string
	}{
		{"/books/" + id, http.StatusOK, "public, max-age=300", "book:" + id},
		{"/books/" + id + "/authors", http.StatusOK, "public, max-age=300", ""},
		{"/books?page=1", http.StatusOK, "public, max-age=60", "list:books"},
		{"/books?genre=1", http.StatusOK, "public, max-age=60", "genre:1"},
		{"/genres", http.StatusOK, "public, max-age=3600", "list:genres"},
		{"/genres/1", http.StatusOK, "public, max-age=3600", "genre:1"},
		{"/search?q=" + "%D0%B0", http.StatusOK, "public, max-age=30", "list:books"},
		{"/readers/1/loans", http.StatusOK, "private, no-store", ""},
		{"/books/999999999", http.StatusNotFound, "", ""},
		{"/genres/999999999", http.StatusNotFound, "", ""},
		{"/books/abc", http.StatusBadRequest, "", ""},
		{"/search", http.StatusBadRequest, "", ""},
	}
	for _, tc := range cases {
		rec := do(http.MethodGet, tc.target, "")
		if rec.Code != tc.status {
			t.Errorf("%s: status %d, want %d; body=%s", tc.target, rec.Code, tc.status, rec.Body.String())
			continue
		}
		if tc.cacheControl != "" && rec.Header().Get("Cache-Control") != tc.cacheControl {
			t.Errorf("%s: Cache-Control %q, want %q", tc.target, rec.Header().Get("Cache-Control"), tc.cacheControl)
		}
		if tc.tag != "" && !strings.Contains(" "+rec.Header().Get("Surrogate-Key")+" ", " "+tc.tag+" ") {
			t.Errorf("%s: Surrogate-Key %q lacks %s", tc.target, rec.Header().Get("Surrogate-Key"), tc.tag)
		}
	}
	if deps := do(http.MethodGet, "/books/"+id+"/authors", "").Header().Get("X-Cache-Depends-On"); !strings.HasPrefix(deps, "book:"+id) {
		t.Errorf("authors must depend on the book, got %q", deps)
	}

	var title string
	if err := db.QueryRow(ctx, "SELECT title FROM books WHERE book_id = $1", bookID).Scan(&title); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(ctx, "UPDATE books SET title = $2 WHERE book_id = $1", bookID, title) })
	rec := do(http.MethodPut, "/books/"+id, `{"title":"Изменённое название"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT status %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Join(purged, ",") != "book:"+id {
		t.Fatalf("expected the write to purge book:%s, got %v", id, purged)
	}
	body, _ := io.ReadAll(do(http.MethodGet, "/books/"+id, "").Body)
	if !strings.Contains(string(body), "Изменённое название") {
		t.Fatalf("expected the new title, got %s", body)
	}
	for _, bad := range []string{`{}`, `{"unknown":1}`, `{`} {
		if rec := do(http.MethodPut, "/books/"+id, bad); rec.Code != http.StatusBadRequest {
			t.Errorf("PUT %s: status %d, want 400", bad, rec.Code)
		}
	}
	if rec := do(http.MethodPut, "/books/999999999", `{"title":"x"}`); rec.Code != http.StatusNotFound {
		t.Errorf("PUT of a missing book: status %d, want 404", rec.Code)
	}
}
