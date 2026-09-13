package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

type readiness struct {
	ready, shuttingDown atomic.Bool
}

func (r *readiness) serveReadyz(w http.ResponseWriter, _ *http.Request) {
	if r.ready.Load() && !r.shuttingDown.Load() {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func newMux(r *readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", r.serveReadyz)
	return mux
}

func main() {
	// ponytail: PORT is the only config; add a config struct when a second value shows up.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	r := &readiness{}
	srv := &http.Server{Addr: ":" + port, Handler: newMux(r)}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("listening on :%s", port)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	// ponytail: nothing to wait on yet; flip this after downstream deps connect.
	r.ready.Store(true)

	<-ctx.Done()
	r.shuttingDown.Store(true)
	log.Print("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
