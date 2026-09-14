package main

import (
	"log"
	"net/http"
	"sync/atomic"
)

type readiness struct {
	ready, shuttingDown atomic.Bool
}

func newServer(logger *log.Logger) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, logger)
	return mux
}

func newAdminServer(r *readiness) http.Handler {
	mux := http.NewServeMux()
	addAdminRoutes(mux, r)
	return mux
}
