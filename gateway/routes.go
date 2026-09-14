package main

import (
	"log/slog"
	"net/http"
)

func addRoutes(mux *http.ServeMux, logger *slog.Logger) {
	mux.Handle("POST /v1/events", handleEventsPost(logger))
}

func addAdminRoutes(mux *http.ServeMux, r *readiness) {
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r))
}
