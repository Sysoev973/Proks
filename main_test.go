package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"proks/internal/events"
)

func TestBookUpdatedRequiresAdminToken(t *testing.T) {
	broker := events.NewInMemoryBroker()
	h := requireAdminToken("secret", newBookUpdatedHandler(broker))
	body := `{"type":"book.updated","book_id":"1","version":2}`

	cases := []struct {
		name   string
		auth   string
		status int
	}{
		{"missing", "", http.StatusUnauthorized},
		{"wrong", "Bearer nope", http.StatusUnauthorized},
		{"no scheme", "secret", http.StatusUnauthorized},
		{"valid", "Bearer secret", http.StatusAccepted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, bookUpdatedPath, strings.NewReader(body))
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tc.status {
				t.Fatalf("expected %d, got %d", tc.status, rr.Code)
			}
		})
	}
}

func TestBookUpdatedRejectsInvalidEvent(t *testing.T) {
	h := newBookUpdatedHandler(events.NewInMemoryBroker())
	req := httptest.NewRequest(http.MethodPost, bookUpdatedPath, strings.NewReader(`{"type":"book.updated"}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestEventStreamFallsBackToInMemoryWithoutRabbitURL(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "")
	stream, closeFn, err := newEventStream()
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	if _, ok := stream.(*events.InMemoryBroker); !ok {
		t.Fatalf("expected in-memory broker, got %T", stream)
	}
}

func TestEventStreamRequiredRabbitFails(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:1/")
	t.Setenv("RABBITMQ_CONNECT_TIMEOUT", "0s")
	t.Setenv("RABBITMQ_REQUIRED", "true")
	t.Setenv("PROXY_REPLICA_ID", "test")
	t.Setenv("EVENT_LOG_PATH", t.TempDir()+"/events.log")
	t.Setenv("EVENT_VERSION_STATE_PATH", t.TempDir()+"/state.json")
	if _, _, err := newEventStream(); err == nil {
		t.Fatal("expected error when rabbitmq is required but unreachable")
	}

	t.Setenv("RABBITMQ_REQUIRED", "")
	stream, closeFn, err := newEventStream()
	if err != nil {
		t.Fatalf("expected fallback, got %v", err)
	}
	defer closeFn()
	if _, ok := stream.(*events.InMemoryBroker); !ok {
		t.Fatalf("expected in-memory fallback, got %T", stream)
	}
}
