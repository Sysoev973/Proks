package predictor

import "testing"

func TestMarkovPredictRanksByWeight(t *testing.T) {
	m := NewMarkov()
	m.Update("book_view:1", "book_view:2")
	m.Update("book_view:1", "book_view:2")
	m.Update("book_view:1", "book_view:3")

	candidates := m.Predict("book_view:1")
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}
	if candidates[0].Key != "book_view:2" {
		t.Fatalf("expected first candidate book_view:2, got %s", candidates[0].Key)
	}
	if candidates[0].Probability <= 0.5 || candidates[0].Probability > 1 {
		t.Fatalf("unexpected probability: %f", candidates[0].Probability)
	}
}

func TestTrendTrackerRequiresMinObservations(t *testing.T) {
	tracker := NewTrendTracker(0.5, 3, 0.6)
	tracker.Observe("book_view:9", 1.0)
	tracker.Observe("book_view:9", 1.0)
	if conf := tracker.Confidence("book_view:9"); conf != 0 {
		t.Fatalf("expected no confidence before min observations, got %f", conf)
	}
}

func TestTrendTrackerAdjustProbability(t *testing.T) {
	tracker := NewTrendTracker(0.5, 2, 0.6)
	tracker.Observe("book_view:2", 0.3)
	tracker.Observe("book_view:2", 0.6)
	tracker.Observe("book_view:2", 0.9)

	adjusted := tracker.AdjustProbability("book_view:2", 0.8)
	if adjusted <= 0 || adjusted < 0.48 {
		t.Fatalf("expected adjusted probability to be positive and above 0.48, got %f", adjusted)
	}
}

func TestTrendTrackerSuppressesNoise(t *testing.T) {
	tracker := NewTrendTracker(0.5, 3, 0.6)
	for i := 0; i < 5; i++ {
		tracker.Observe("book_view:noise", 0.1)
	}
	if conf := tracker.Confidence("book_view:noise"); conf != 0 {
		t.Fatalf("expected no confidence after noisy low values, got %f", conf)
	}

	tracker.Reset("book_view:noise")
	tracker.Observe("book_view:stable", 0.9)
	tracker.Observe("book_view:stable", 0.9)
	tracker.Observe("book_view:stable", 0.9)
	if conf := tracker.Confidence("book_view:stable"); conf <= 0.6 {
		t.Fatalf("expected stable trend to pass confidence cutoff, got %f", conf)
	}
}

func TestEWMAImprovesPrefetchPrecision(t *testing.T) {
	tracker := NewTrendTracker(0.5, 3, 0.6)
	for i := 0; i < 4; i++ {
		tracker.Observe("book_view:stable", 0.9)
	}
	for i := 0; i < 4; i++ {
		tracker.Observe("book_view:noise", 0.1)
	}

	stableAdjusted := tracker.AdjustProbability("book_view:stable", 0.8)
	noiseAdjusted := tracker.AdjustProbability("book_view:noise", 0.8)
	if stableAdjusted <= 0.48 {
		t.Fatalf("expected stable route to pass after EWMA, got %f", stableAdjusted)
	}
	if noiseAdjusted > 0.1 {
		t.Fatalf("expected noisy route to be suppressed by EWMA, got %f", noiseAdjusted)
	}
}

func TestPredictorEdgeCases(t *testing.T) {
	m := NewMarkov()
	m.Update("", "/b")
	m.Update("/a", "")
	if got := m.Predict("/a"); got != nil {
		t.Fatalf("empty routes must be ignored, got %v", got)
	}

	e := NewEWMA(0)
	if e.Add(1) != 1 || e.Value() != 1 {
		t.Fatal("the first sample must initialize the average")
	}
	if got := e.Add(0); got != 0.7 {
		t.Fatalf("invalid alpha must fall back to 0.3, got %v", got)
	}

	tracker := NewTrendTracker(0, 1, 0)
	if tracker.Observe("", 1) != 0 || tracker.Confidence("") != 0 || tracker.ObserveOutcome("", true) != 0 {
		t.Fatal("empty keys must be ignored")
	}
	if tracker.ObserveTransition("", "/b", true) != 0 {
		t.Fatal("transitions need both ends")
	}
	tracker.ObserveTransition("/a", "/b", true)
	if tracker.Confidence("/a->/b") != 1 {
		t.Fatalf("expected full confidence after a success, got %v", tracker.Confidence("/a->/b"))
	}
	tracker.ObservePrefetch("/c", false)
	if tracker.AdjustProbability("/c", 0.9) != 0 {
		t.Fatal("a failed prefetch below the cutoff must suppress the route")
	}
	if tracker.AdjustProbability("/c", 0) != 0 {
		t.Fatal("zero probability must stay zero")
	}
	tracker.Reset("/c")
	tracker.Reset("")
	if tracker.AdjustProbability("/c", 0.9) != 0.9 {
		t.Fatal("a reset route must use the raw probability")
	}
}
