package main

import (
	"context"
	"log/slog"
	"net/http"
)

func addRoutes(mux *http.ServeMux, logger *slog.Logger, stations stationLookup, store heartbeatSaver, pub eventPublisher) {
	auth := bearerAuth(logger, stations)
	mux.Handle("POST /v1/events", auth(handleEventsPost(logger, pub)))
	mux.Handle("POST /v1/stations/heartbeat", auth(handleHeartbeatPost(logger, store)))
}

func addAdminRoutes(mux *http.ServeMux, r *readiness, checks map[string]func(context.Context) error) {
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r, checks))
}
