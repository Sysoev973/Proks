package cache

import (
	"context"
	"fmt"
	"testing"
)

func TestTinyLFUEstimate(t *testing.T) {
	filter := NewTinyLFU()
	filter.Increment("book:1")
	filter.Increment("book:1")
	filter.Increment("book:2")

	if got := filter.Estimate("book:1"); got != 2 {
		t.Fatalf("expected estimate 2 for book:1, got %d", got)
	}
	if got := filter.Estimate("book:2"); got != 1 {
		t.Fatalf("expected estimate 1 for book:2, got %d", got)
	}
}

func TestTinyLFUAging(t *testing.T) {
	filter := NewTinyLFU()
	filter.Increment("book:3")
	filter.Increment("book:3")
	filter.Increment("book:3")
	filter.Increment("book:3")
	filter.sketch.age()

	if got := filter.Estimate("book:3"); got != 2 {
		t.Fatalf("expected aged estimate 2, got %d", got)
	}
}

func TestLRUWindowPromotion(t *testing.T) {
	window := NewLRUWindow(2)
	if !window.Touch("b1") {
		t.Fatal("expected b1 to be touched")
	}
	if !window.Contains("b1") {
		t.Fatal("expected b1 to be hot in window")
	}
	if window.Len() != 1 {
		t.Fatalf("expected window len 1, got %d", window.Len())
	}
}

func TestTinyLFUAdmission(t *testing.T) {
	window := NewLRUWindow(4)
	window.Touch("book:hot")
	filter := NewTinyLFU()
	filter.Increment("book:hot")
	filter.Increment("book:hot")

	policy := &TinyLFUAdmission{
		Filter:            filter,
		Window:            window,
		MinFrequency:      1,
		MaxPollutionRatio: 0.5,
	}

	if !policy.ShouldStore(Item{Key: "book:hot"}, Metrics{CandidateFrequency: 2, Pollution: 0.1}) {
		t.Fatal("expected hot item to be admitted")
	}
	if policy.ShouldStore(Item{Key: "book:other"}, Metrics{CandidateFrequency: 1, Pollution: 0.9}) {
		t.Fatal("expected polluted item to be rejected")
	}
	if policy.ShouldStore(Item{Key: "book:cold"}, Metrics{CandidateFrequency: 1, Pollution: 0.1, VictimFrequency: 10}) {
		t.Fatal("expected cold low-frequency item to be rejected against victim")
	}
}

func ExampleTinyLFUAdmission_PollutantScore() {
	filter := NewTinyLFU()
	filter.Increment("book:1")
	filter.Increment("book:1")
	filter.Increment("book:1")
	filter.Increment("book:2")
	filter.Increment("book:2")
	filter.Increment("book:2")
	policy := NewTinyLFUAdmission(filter, NewLRUWindow(16), 2, 0.5)
	fmt.Printf("%.3f\n", policy.PollutantScore("book:1", "book:2"))
	// Output: 0.500
}

func TestInMemoryCacheVictimIsLRUTail(t *testing.T) {
	c := NewInMemoryCache(4, nil)
	if _, ok := c.VictimFor("a"); ok {
		t.Fatal("expected no victim in empty cache")
	}
	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(context.Background(), Item{Key: k}, Metrics{})
	}
	if got, _ := c.VictimFor("a"); got != "a" {
		t.Fatalf("expected victim a, got %q", got)
	}
	_, _ = c.Get(context.Background(), "a")
	if got, _ := c.VictimFor("a"); got != "b" {
		t.Fatalf("expected victim b after touching a, got %q", got)
	}
}
