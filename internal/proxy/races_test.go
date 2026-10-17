package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proks/internal/cache"
	"proks/internal/events"
	"proks/internal/predictor"
)

// Followers of a flight whose response was not cached must not receive the
// leader's personal response (Set-Cookie here); they go upstream themselves.
func TestFollowersDoNotShareUncacheableResponse(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			<-release
		}
		w.Header().Set("Set-Cookie", fmt.Sprintf("session=user-%d", n))
		_, _ = io.WriteString(w, "book")
	}))
	defer upstream.Close()
	h, err := NewHandler(cache.NewInMemoryCache(32, nil), predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	const n = 5
	recorders := make([]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i := range recorders {
		recorders[i] = httptest.NewRecorder()
		wg.Add(1)
		go func(rec *httptest.ResponseRecorder) {
			defer wg.Done()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
		}(recorders[i])
	}
	waitFor(t, func() bool { return calls.Load() == 1 })
	waitForFlightJoined(t)
	close(release)
	wg.Wait()

	seen := make(map[string]bool)
	for _, rec := range recorders {
		cookie := rec.Header().Get("Set-Cookie")
		if seen[cookie] {
			t.Fatalf("Set-Cookie %q was given to more than one client", cookie)
		}
		seen[cookie] = true
		if rec.Header().Get("X-Cache-Reason") != "SET_COOKIE" {
			t.Fatalf("expected SET_COOKIE bypass, got %s", rec.Header().Get("X-Cache-Reason"))
		}
	}
	if got := calls.Load(); got != n {
		t.Fatalf("expected every client to get its own upstream response, got %d calls", got)
	}
}

// waitForFlightJoined gives the other goroutines time to join the flight.
func waitForFlightJoined(t *testing.T) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
}

func TestPurgeDuringFetchIsNotLost(t *testing.T) {
	cases := []struct {
		name            string
		eventVersion    int64
		responseVersion string
		wantStored      bool
	}{
		{"unversioned purge", 0, "", false},
		{"purge newer than response", 5, "4", false},
		{"response newer than purge", 5, "6", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			release := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				<-release
				if tc.responseVersion != "" {
					w.Header().Set("X-Upstream-Book-Version", tc.responseVersion)
				}
				_, _ = io.WriteString(w, "v1")
			}))
			defer upstream.Close()
			c := cache.NewInMemoryCache(32, nil)
			h := newRoutedHandler(t, c, noopPrefetcher{}, upstream.URL)

			done := make(chan *httptest.ResponseRecorder)
			go func() {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
				done <- rec
			}()
			waitFor(t, func() bool { return calls.Load() == 1 })
			if err := h.HandleEvent(context.Background(), events.Event{Type: "book.updated", BookID: "1", Version: tc.eventVersion}); err != nil {
				t.Fatal(err)
			}
			close(release)
			rec := <-done

			if stored := c.Size() == 1; stored != tc.wantStored {
				t.Fatalf("stored=%v, want %v (reason %s)", stored, tc.wantStored, rec.Header().Get("X-Cache-Reason"))
			}
			if !tc.wantStored && rec.Header().Get("X-Cache-Reason") != "INVALIDATED" {
				t.Fatalf("expected INVALIDATED reason, got %s", rec.Header().Get("X-Cache-Reason"))
			}
		})
	}
}

func TestPurgeOfUnrelatedTagDoesNotBlockStore(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := gatedUpstream(t, "v1", &calls, release)
	c := cache.NewInMemoryCache(32, nil)
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/books/1", nil))
	}()
	waitFor(t, func() bool { return calls.Load() == 1 })
	_ = h.HandleEvent(context.Background(), events.Event{Type: "book.updated", BookID: "2"})
	close(release)
	<-done
	if c.Size() != 1 {
		t.Fatal("a purge of book:2 must not stop book:1 from being cached")
	}
}

func TestResponseLargerThanShardBudgetIsReported(t *testing.T) {
	body := strings.Repeat("x", 4096)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()
	c := cache.New(cache.Config{Capacity: 32, MaxBytes: 2048, Shards: 1})
	h, err := NewHandler(c, predictor.NewMarkov(), noopPrefetcher{}, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/books/1", nil))
	if rec.Body.String() != body || rec.Header().Get("X-Cache-Reason") != "TOO_LARGE" {
		t.Fatalf("expected TOO_LARGE bypass with full body, got %s len=%d", rec.Header().Get("X-Cache-Reason"), rec.Body.Len())
	}
}
