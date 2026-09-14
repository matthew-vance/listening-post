package main

import (
	"log/slog"
	"net/http"
)

func addRoutes(mux *http.ServeMux, logger *slog.Logger, reg stationRegistry) {
	mux.Handle("POST /v1/events", bearerAuth(reg)(handleEventsPost(logger)))
	mux.Handle("POST /v1/stations/heartbeat", bearerAuth(reg)(handleHeartbeatPost(logger)))
}

func addAdminRoutes(mux *http.ServeMux, r *readiness) {
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r))
}
