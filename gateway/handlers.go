package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func handleHealthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		encode(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func handleReadyz(r *readiness) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if r.ready.Load() && !r.shuttingDown.Load() {
			encode(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		encode(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
	})
}

func encode[T any](w http.ResponseWriter, status int, v T) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	return nil
}
