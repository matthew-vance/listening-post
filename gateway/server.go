package main

import (
	"net/http"
	"sync/atomic"
)

type readiness struct {
	ready, shuttingDown atomic.Bool
}

func NewServer() http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux)
	return mux
}

func NewAdminServer(r *readiness) http.Handler {
	mux := http.NewServeMux()
	addAdminRoutes(mux, r)
	return mux
}
