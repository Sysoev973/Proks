package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	_ "strconv"
	"syscall"
	"time"

	"proks/internal/cache"
	"proks/internal/predictor"
	"proks/internal/prefetcher"
	"proks/internal/proxy"
)

func main() {
	upstream := getenv("UPSTREAM_URL", "http://localhost:8081")
	addr := getenv("LISTEN_ADDR", ":8080")

	c := cache.NewTinyLFURuntimeCache(10_000)
	pred := predictor.NewMarkov()

	// 1. Конфигурация EWMA из ENV с правильными типами (float64, uint64, float64)
	alpha := getenvFloat("EWMA_ALPHA", 0.35)
	minObs := getenvUint64("EWMA_MIN_OBSERVATIONS", 3)
	cutoff := getenvFloat("EWMA_CUTOFF", 0.6)

	tracker := predictor.NewTrendTracker(alpha, minObs, cutoff)

	httpClient := &http.Client{
		Timeout: 2 * time.Second,
	}

	upstreamURL := "http://localhost:8081"

	pf := prefetcher.NewAsyncPrefetcher(4, 1024, 100*time.Millisecond, func(ctx context.Context, key string) error {
		reqURL := fmt.Sprintf("%s/%s", upstreamURL, key)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("upstream returned status: %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		// Запись в кэш с точным соблюдением (ctx, cache.Item, cache.Metrics)
		c.Set(ctx, cache.Item{
			Key:        key,
			Value:      body,
			StatusCode: resp.StatusCode,
		}, cache.Metrics{})

		return nil
	}, log.Default())

	pf.SetOutcomeHook(func(key string, success bool) {
		tracker.ObservePrefetch(key, success)
	})
	defer pf.Stop()

	h, err := proxy.NewHandler(c, pred, pf, upstream)
	if err != nil {
		log.Fatalf("build handler: %v", err)
	}
	h.SetTrend(tracker)

	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 2 * time.Second,
	}

	go func() {
		log.Printf("proxy listening on %s, upstream %s", addr, upstream)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// 3. Хелперы конвертации вынесены на уровень пакета
func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func getenvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return fallback
}

func getenvUint64(key string, fallback uint64) uint64 {
	v := os.Getenv(key)
	if u, err := strconv.ParseUint(v, 10, 64); err == nil {
		return u
	}
	return fallback
}
