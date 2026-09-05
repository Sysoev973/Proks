package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
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
	pf := prefetcher.NewAsyncPrefetcher(4, 1024, 100*time.Millisecond, func(ctx context.Context, key string) error {
		return nil
	}, log.Default())
	defer pf.Stop()

	h, err := proxy.NewHandler(c, pred, pf, upstream)
	if err != nil {
		log.Fatalf("build handler: %v", err)
	}

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

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
