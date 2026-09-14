package main

import (
	"context"
	"log/slog"
	"net/http"
)

func addRoutes(mux *http.ServeMux, logger *slog.Logger, stations stationLookup, store heartbeatSaver) {
	auth := bearerAuth(logger, stations)
	mux.Handle("POST /v1/events", auth(handleEventsPost(logger)))
	mux.Handle("POST /v1/stations/heartbeat", auth(handleHeartbeatPost(logger, store)))
}

func addAdminRoutes(mux *http.ServeMux, r *readiness, ping func(context.Context) error) {
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r, ping))
}
