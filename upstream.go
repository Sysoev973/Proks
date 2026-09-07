package main

import (
	"log"
	"net/http"
)

func StartDemoUpstream(addr string) {
	http.HandleFunc("/books/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "123", "title": "Sample Book"}`))
	})
	log.Println("Upstream listening on", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
