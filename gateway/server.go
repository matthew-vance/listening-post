package main

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
)

type readiness struct {
	ready atomic.Bool
}

func newServer(logger *slog.Logger, stations stationLookup, store heartbeatSaver, pub eventPublisher) http.Handler {
	mux := http.NewServeMux()
	auth := bearerAuth(logger, stations)
	mux.Handle("POST /v1/events", auth(handleEventsPost(logger, pub)))
	mux.Handle("POST /v1/stations/heartbeat", auth(handleHeartbeatPost(logger, store)))
	return mux
}

func newAdminServer(r *readiness, checks map[string]func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r, checks))
	return mux
}
