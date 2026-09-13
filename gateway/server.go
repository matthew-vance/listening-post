package main

import (
	"net/http"
	"sync/atomic"
)

type readiness struct {
	ready, shuttingDown atomic.Bool
}

func NewServer(r *readiness) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, r)
	return mux
}
