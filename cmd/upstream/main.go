package main

import (
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/books/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "123", "title": "Sample Book"}`))
	})
	log.Println("Upstream listening on :8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatal(err)
	}
}
