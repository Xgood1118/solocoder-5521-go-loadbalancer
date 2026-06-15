// Mock backend server - responds with its own ID
package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	id := os.Getenv("BACKEND_ID")
	port := os.Getenv("PORT")
	failMode := os.Getenv("FAIL_MODE") // "all", "500", ""

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if failMode == "all" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if failMode == "500" && (r.URL.Path == "/bad" || r.URL.Path == "/healthz") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "backend=%s path=%s addr=%s\n", id, r.URL.Path, r.RemoteAddr)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if failMode == "500" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	fmt.Printf("mock backend %s listening on :%s fail_mode=%s\n", id, port, failMode)
	http.ListenAndServe(":"+port, mux)
}
