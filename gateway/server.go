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

func newServer(logger *slog.Logger, reg stationRegistry, store heartbeatSaver) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, logger, reg, store)
	return mux
}

func newAdminServer(r *readiness, ping func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	addAdminRoutes(mux, r, ping)
	return mux
}
