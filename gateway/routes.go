package main

import (
	"context"
	"log/slog"
	"net/http"
)

func addRoutes(mux *http.ServeMux, logger *slog.Logger, reg stationRegistry, store heartbeatSaver) {
	mux.Handle("POST /v1/events", bearerAuth(reg)(handleEventsPost(logger)))
	mux.Handle("POST /v1/stations/heartbeat", bearerAuth(reg)(handleHeartbeatPost(logger, store)))
}

func addAdminRoutes(mux *http.ServeMux, r *readiness, ping func(context.Context) error) {
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r, ping))
}
