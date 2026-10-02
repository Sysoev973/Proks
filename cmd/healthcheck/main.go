package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: healthcheck URL")
		os.Exit(2)
	}

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(os.Args[1])
	if err != nil {
		os.Exit(1)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
