package main

import (
	"log"
	"net/http"
	"sync/atomic"
)

type readiness struct {
	ready, shuttingDown atomic.Bool
}

func NewServer(logger *log.Logger) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, logger)
	return mux
}

func NewAdminServer(r *readiness) http.Handler {
	mux := http.NewServeMux()
	addAdminRoutes(mux, r)
	return mux
}
