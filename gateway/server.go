package main

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
)

type readiness struct {
	ready, shuttingDown atomic.Bool
}

func newServer(logger *slog.Logger, stations stationLookup, store heartbeatSaver, pub eventPublisher) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, logger, stations, store, pub)
	return mux
}

func newAdminServer(r *readiness, checks map[string]func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	addAdminRoutes(mux, r, checks)
	return mux
}
