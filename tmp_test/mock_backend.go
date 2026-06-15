package main

import (
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
)

var counter int64

func main() {
	port := "8131"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	id := port

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&counter, 1)
		w.Header().Set("X-Backend-ID", id)
		w.Header().Set("X-Request-Count", fmt.Sprintf("%d", n))
		w.WriteHeader(200)
		w.Write([]byte("backend=" + id))
	})
	http.ListenAndServe(":"+port, nil)
}
